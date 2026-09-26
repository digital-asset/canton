// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.daml.metrics.Timed
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.backend.common.UpdatePointwiseQueries.LookupKey
import com.digitalasset.canton.platform.store.cache.InMemoryFanoutBuffer.*
import com.digitalasset.canton.platform.store.dao.events.OffsetRange
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, Mutex}
import com.google.protobuf.ByteString

import scala.collection.Searching.{Found, InsertionPoint, SearchResult}
import scala.collection.View

/** The in-memory fan-out buffer.
  *
  * This buffer stores the last ingested `maxBufferSize` accepted and rejected submission updates as
  * [[com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate]] and allows bypassing
  * IndexDB persistence fetches for recent updates for:
  *   - update streams
  *   - command completion streams
  *   - by-offset and by-update-id update lookups
  *
  * @param maxBufferSize
  *   The maximum buffer size.
  * @param metrics
  *   The Daml metrics.
  * @param maxBufferedChunkSize
  *   The maximum size of buffered chunks returned by `slice`.
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class InMemoryFanoutBuffer(
    maxBufferSize: Int,
    metrics: LedgerApiServerMetrics,
    maxBufferedChunkSize: Int,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {
  @volatile private[cache] var _bufferLog =
    Vector.empty[(Offset, TransactionLogUpdate)]
  @volatile private[cache] var _lookupMap =
    Map.empty[String, TransactionLogUpdate]
  @volatile private[cache] var _acceptedByHash =
    Map.empty[ByteString, TransactionLogUpdate.TransactionAccepted]
  @volatile private[cache] var _rejectionsByHash =
    Map.empty[ByteString, Vector[TransactionLogUpdate.TransactionRejected]]

  private val bufferMetrics = metrics.services.index.inMemoryFanoutBuffer
  private val pushTimer = bufferMetrics.push
  private val pruneTimer = bufferMetrics.prune
  private val bufferSizeHistogram = bufferMetrics.bufferSize
  private val lock = new Mutex()

  /** Appends a new event to the buffer.
    *
    * Starts evicting from the tail when `maxBufferSize` is reached.
    *
    * @param entry
    *   The buffer entry.
    */
  def push(entry: TransactionLogUpdate): Unit =
    Timed.value(
      pushTimer,
      (lock.exclusive {
        _bufferLog.lastOption.foreach {
          // Encountering a non-strictly increasing offset is an error condition.
          case (lastOffset, _) if lastOffset >= entry.offset =>
            throw UnorderedException(lastOffset, entry.offset)
          case _ =>
        }

        if (maxBufferSize <= 0) {
          // Do nothing since buffer updates are not atomic and the reads are not synchronized.
          // This ensures that reads can never see data in the buffer.
        } else {
          ensureSize(maxBufferSize - 1)(entry.traceContext)

          _bufferLog = _bufferLog :+ entry.offset -> entry
          extractEntryFromMap(entry).foreach { case (key, value) =>
            _lookupMap = _lookupMap.updated(key, value)
          }
          extractHashFromEntry(entry).foreach { case (hash, value) =>
            _acceptedByHash = _acceptedByHash.updated(hash, value)
          }
          extractRejectionHash(entry).foreach { case (hash, rejected) =>
            val existing = _rejectionsByHash.getOrElse(hash, Vector.empty)
            _rejectionsByHash = _rejectionsByHash.updated(hash, rejected +: existing)
          }
        }
      }),
    )

  /** Lookup the accepted transaction update by transaction id. */
  def lookupTransaction(
      updateId: String
  ): Option[TransactionLogUpdate.TransactionAccepted] =
    _lookupMap.get(updateId).collect { case tx: TransactionLogUpdate.TransactionAccepted => tx }

  /** Lookup the accepted transaction log update by the lookup key. */
  def lookup(
      lookupKey: LookupKey
  ): Option[TransactionLogUpdate] = lookupKey match {
    case LookupKey.ByUpdateId(updateId) => lookup(updateId.toHexString)
    case LookupKey.ByOffset(offset) => lookup(offset)
    case LookupKey.ByHash(hash) => _acceptedByHash.get(hash)
  }

  /** Lookup completions by transaction hash. Returns (accepted transaction if buffered, rejections
    * in newest-first order).
    */
  def lookupCompletionsByHash(
      hash: ByteString
  ): (
      Option[TransactionLogUpdate.TransactionAccepted],
      Vector[TransactionLogUpdate.TransactionRejected],
  ) = {
    val accepted = _acceptedByHash.get(hash)
    val rejections = _rejectionsByHash.getOrElse(hash, Vector.empty)
    (accepted, rejections)
  }

  /** Lookup the accepted transaction log update by update id. */
  private def lookup(
      updateId: String
  ): Option[TransactionLogUpdate] = _lookupMap.get(updateId)

  /** Lookup the accepted transaction log update by update offset. */
  private def lookup(
      offset: Offset
  ): Option[TransactionLogUpdate] = {
    val vectorSnapshot = _bufferLog

    val searchResult = vectorSnapshot.view.map(_._1).search(offset)

    searchResult match {
      case Found(idx) => Some(vectorSnapshot(idx)._2)
      case _ => None
    }
  }

  /** Removes entries starting from the buffer head up until `endInclusive`.
    *
    * @param endInclusive
    *   The last inclusive (highest) buffer offset to be pruned.
    */
  def prune(endInclusive: Offset): Unit =
    Timed.value(
      pruneTimer,
      (lock.exclusive {
        val dropCount = _bufferLog.view.map(_._1).search(endInclusive) match {
          case Found(foundIndex) => foundIndex + 1
          case InsertionPoint(insertionPoint) => insertionPoint
        }

        dropOldest(dropCount)
      }),
    )

  /** Remove all buffered entries */
  def flush(): Unit = (lock.exclusive {
    _bufferLog = Vector.empty
    _lookupMap = Map.empty
    _acceptedByHash = Map.empty
    _rejectionsByHash = Map.empty
  })

  private def ensureSize(targetSize: Int)(implicit traceContext: TraceContext): Unit = (
    lock.exclusive {
      val currentBufferLogSize = _bufferLog.size
      val currentLookupMapSize = _lookupMap.size

      if (currentLookupMapSize <= currentBufferLogSize) {
        bufferSizeHistogram.update(currentBufferLogSize)(MetricsContext.Empty)

        if (currentBufferLogSize > targetSize) {
          dropOldest(dropCount = currentBufferLogSize - targetSize)
        }
      } else {
        // This is an error condition. If encountered, clear the in-memory fan-out buffers.
        logger
          .error(
            s"In-memory fan-out lookup map size ($currentLookupMapSize) exceeds the buffer log size ($currentBufferLogSize). Clearing in-memory fan-out.."
          )

        flush()
      }
    }
  )

  private def dropOldest(dropCount: Int): Unit = (lock.exclusive {
    val (evicted, remainingBufferLog) = _bufferLog.splitAt(dropCount)
    val lookupKeysToEvict: View[String] =
      evicted.view.map(_._2).flatMap(extractEntryFromMap).map(_._1)
    val hashKeysToEvict: View[ByteString] =
      evicted.view.map(_._2).flatMap(extractHashFromEntry).map(_._1)

    _bufferLog = remainingBufferLog
    _lookupMap = _lookupMap -- lookupKeysToEvict
    _acceptedByHash = _acceptedByHash -- hashKeysToEvict

    val evictedRejectionHashes: View[ByteString] =
      evicted.view.map(_._2).flatMap(extractRejectionHash).map(_._1)

    _rejectionsByHash = evictedRejectionHashes.foldLeft(_rejectionsByHash) { (acc, hash) =>
      acc.get(hash) match {
        case Some(vec) if vec.nonEmpty =>
          val remaining = vec.dropRight(1)
          if (remaining.isEmpty) acc - hash
          else acc.updated(hash, remaining)
        case _ => acc // nothing buffered for this hash; leave the map unchanged
      }
    }
  })

  private def extractEntryFromMap(
      transactionLogUpdate: TransactionLogUpdate
  ): Option[(String, TransactionLogUpdate)] =
    transactionLogUpdate match {
      case txAccepted: TransactionLogUpdate.TransactionAccepted =>
        Some(txAccepted.updateId -> txAccepted)
      case reassignment: TransactionLogUpdate.ReassignmentAccepted =>
        Some(reassignment.updateId -> reassignment)
      case topologyTx: TransactionLogUpdate.TopologyTransactionEffective =>
        Some(topologyTx.updateId -> topologyTx)
      case _: TransactionLogUpdate.TransactionRejected => None
      case _: TransactionLogUpdate.ReceivedAcsCommitment => None
    }

  private def extractHashFromEntry(
      transactionLogUpdate: TransactionLogUpdate
  ): Option[(ByteString, TransactionLogUpdate.TransactionAccepted)] =
    transactionLogUpdate match {
      case txAccepted: TransactionLogUpdate.TransactionAccepted =>
        txAccepted.transactionHash.map(_.unwrap -> txAccepted)
      case _ => None

    }

  private def extractRejectionHash(
      entry: TransactionLogUpdate
  ): Option[(ByteString, TransactionLogUpdate.TransactionRejected)] =
    entry match {
      case rejected: TransactionLogUpdate.TransactionRejected =>
        rejected.completionStreamResponse.completionResponse.completion
          .flatMap(_.transactionHash)
          .map(_ -> rejected)
      case _ => None
    }

  /** Returns a slice of events from the buffer.
    *
    * @param range
    *   range of the requested events
    * @param filter
    *   A lambda function that allows pre-filtering the buffered elements before assembling slices.
    * @param limit
    *   a maximum number of elements to put in slice, use None for streaming without limit (in
    *   maxBufferedChunkSize portions)
    * @param reverseOrder
    *   If true, the events will be sliced from the most recent to the oldest
    * @return
    *   A slice of the series of events satysfying the input bounds. The slice contains information
    *   about the portion of the requested range covered by the slice that can be used to decide if
    *   subsequent requests are needed.
    */
  def slice[FilterResult](
      range: OffsetRange,
      filter: TransactionLogUpdate => Option[FilterResult],
      limit: Option[Int],
      reverseOrder: Boolean,
  ): Option[Slice[FilterResult]] = {
    val effectiveLimit = limit.getOrElse(maxBufferedChunkSize)
    assert(effectiveLimit >= 1)
    val vectorSnapshot = _bufferLog
    for {
      bufferStart <- vectorSnapshot.headOption
      bufferEnd <- vectorSnapshot.lastOption
      bufferEffectiveOffsetRange = OffsetRange(
        startInclusive = bufferStart._1,
        // based on the invariant that the range must be below the LedgerEnd and the IMFO is stretching to the LedgerEnd
        endInclusive = bufferEnd._1 max range.endInclusive,
      )
      sliceOffsetRange <- bufferEffectiveOffsetRange intersection range
    } yield {
      val bufferStartSearchResult = vectorSnapshot.view.map(_._1).search(range.startInclusive)
      val bufferEndSearchResult = vectorSnapshot.view.map(_._1).search(range.endInclusive)

      val bufferStartInclusiveIdx = indexAt(bufferStartSearchResult)
      val bufferEndExclusiveIdx = indexAfter(bufferEndSearchResult)

      val bufferSlice = vectorSnapshot
        .slice(bufferStartInclusiveIdx, bufferEndExclusiveIdx)
        .view

      val filteredLimitedSliceExtended = (if (reverseOrder) bufferSlice.reverse else bufferSlice)
        .flatMap { case (offset, tr) => filter(tr).map((offset, _)) }
        .take(effectiveLimit + 1)
        .toVector

      filteredLimitedSliceExtended.lastOption match {
        case Some((nextOffset, _)) if filteredLimitedSliceExtended.sizeIs == effectiveLimit + 1 =>
          if (reverseOrder)
            Slice(
              fromImfo = filteredLimitedSliceExtended.dropRight(1),
              offsetRange = OffsetRange(
                startInclusive = nextOffset.increment,
                endInclusive = sliceOffsetRange.endInclusive,
              ),
            )
          else
            Slice(
              fromImfo = filteredLimitedSliceExtended.dropRight(1),
              offsetRange = OffsetRange(
                startInclusive = sliceOffsetRange.startInclusive,
                // filteredLimitedSliceExtended.sizeIs == limit + 1 means that there are at least two elements, IMFO is ordered by offsets. It implies that the last element's offset must not be the the offset(1), so decrement is possible.
                endInclusive = nextOffset.decrement.getOrElse(
                  ErrorUtil.internalError(
                    new IllegalStateException(
                      "It is not possible to decrement last element's offset"
                    )
                  )(errorLoggingContext(TraceContext.empty))
                ),
              ),
            )

        case _ =>
          Slice(
            fromImfo = filteredLimitedSliceExtended,
            offsetRange = sliceOffsetRange,
          )
      }
    }
  }
}

private[platform] object InMemoryFanoutBuffer {
  final case class Slice[FilterResult](
      fromImfo: Vector[(Offset, FilterResult)],
      offsetRange: OffsetRange,
  )

  private[cache] final case class UnorderedException[O](first: O, second: O)
      extends RuntimeException(
        s"Elements appended to the buffer should have strictly increasing offsets: $first vs $second"
      )

  private[cache] def indexAt(bufferStartInclusiveSearchResult: SearchResult): Int =
    bufferStartInclusiveSearchResult match {
      case InsertionPoint(insertionPoint) => insertionPoint
      case Found(foundIndex) => foundIndex
    }

  private[cache] def indexAfter(bufferEndInclusiveSearchResult: SearchResult): Int =
    bufferEndInclusiveSearchResult match {
      case InsertionPoint(insertionPoint) => insertionPoint
      case Found(foundIndex) => foundIndex + 1
    }
}
