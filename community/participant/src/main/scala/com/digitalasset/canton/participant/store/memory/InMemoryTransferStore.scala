// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.EitherT
import cats.implicits.catsSyntaxPartialOrder
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.RequestCounter
import com.digitalasset.canton.participant.protocol.transfer.TransferData
import com.digitalasset.canton.participant.store.TransferStore
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.TransferId
import com.digitalasset.canton.protocol.messages.DeliveredTransferOutResult
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{Checked, CheckedT, ErrorUtil, MapsUtil}
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.topology.DomainId

import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import java.util.ConcurrentModificationException
import scala.concurrent.Future

class InMemoryTransferStore(
    domain: DomainId,
    override protected val loggerFactory: NamedLoggerFactory,
) extends TransferStore
    with NamedLogging {

  import TransferStore._

  private[this] val transferDataMap: TrieMap[TransferId, TransferEntry] =
    new TrieMap[TransferId, TransferEntry]

  override def addTransfer(
      transferData: TransferData
  )(implicit traceContext: TraceContext): EitherT[Future, TransferStoreError, Unit] = {
    ErrorUtil.requireArgument(
      transferData.targetDomain == domain,
      s"Domain ${domain.unwrap}: Transfer store cannot store transfer for domain ${transferData.targetDomain.unwrap}",
    )

    val transferId = transferData.transferId
    val newEntry = TransferEntry(transferData, None)
    EitherT(Future.successful {
      MapsUtil
        .updateWithConcurrentlyM_[Checked[
          TransferDataAlreadyExists,
          TransferAlreadyCompleted,
          *,
        ], TransferId, TransferEntry](
          transferDataMap,
          transferId,
          Checked.result(newEntry),
          _.mergeWith(newEntry),
        )
        .toEither
    })
  }

  override def addTransferOutResult(
      transferOutResult: DeliveredTransferOutResult
  )(implicit traceContext: TraceContext): EitherT[Future, TransferStoreError, Unit] = {
    val transferId = transferOutResult.transferId
    EitherT(Future.successful {
      MapsUtil.updateWithConcurrentlyM_[Either[TransferStoreError, *], TransferId, TransferEntry](
        transferDataMap,
        transferId,
        Left(UnknownTransferId(transferId)),
        _.addTransferOutResult(transferOutResult),
      )
    })
  }

  override def completeTransfer(transferId: TransferId, timeOfCompletion: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, Nothing, TransferStoreError, Unit] =
    CheckedT(Future.successful {
      val result = MapsUtil
        .updateWithConcurrentlyChecked_[
          TransferStoreError,
          TransferAlreadyCompleted,
          TransferId,
          TransferEntry,
        ](
          transferDataMap,
          transferId,
          Checked.abort(UnknownTransferId(transferId)),
          _.complete(timeOfCompletion),
        )
      result.toResult(())
    })

  override def deleteCompletionsSince(
      criterionInclusive: RequestCounter
  )(implicit traceContext: TraceContext): Future[Unit] = Future.successful {
    transferDataMap.foreach { case (transferId, entry) =>
      val entryTimeOfCompletion = entry.timeOfCompletion

      // Standard retry loop for clearing the completion in case the entry is updated concurrently.
      // Ensures progress as one writer succeeds in each iteration.
      @tailrec def clearCompletionCAS(): Unit = transferDataMap.get(transferId) match {
        case None =>
          () // The transfer ID has been deleted in the meantime so there's no completion to be cleared any more.
        case Some(newEntry) =>
          val newTimeOfCompletion = newEntry.timeOfCompletion
          if (newTimeOfCompletion.isDefined) {
            if (newTimeOfCompletion != entryTimeOfCompletion)
              throw new ConcurrentModificationException(
                s"Completion of transfer $transferId modified from $entryTimeOfCompletion to $newTimeOfCompletion while deleting completions."
              )
            val replaced = transferDataMap.replace(transferId, newEntry, newEntry.clearCompletion)
            if (!replaced) clearCompletionCAS()
          }
      }

      /* First use the entry from the read-only snapshot the iteration goes over
       * If the entry's completion must be cleared in the snapshot but the entry has meanwhile been modified in the table
       * (e.g., a transfer-out result has been added), then clear the table's entry.
       */
      if (entry.timeOfCompletion.exists(_.rc >= criterionInclusive)) {
        val replaced = transferDataMap.replace(transferId, entry, entry.clearCompletion)
        if (!replaced) clearCompletionCAS()
      }
    }
  }

  override def lookup(
      transferId: TransferId
  )(implicit traceContext: TraceContext): EitherT[Future, TransferLookupError, TransferData] =
    EitherT(Future.successful {
      for {
        entry <- transferDataMap.get(transferId).toRight(UnknownTransferId(transferId))
        _ <- entry.timeOfCompletion.map(TransferCompleted(transferId, _)).toLeft(())
      } yield entry.transferData
    })

  override def find(
      filterOrigin: Option[DomainId],
      filterTimestamp: Option[CantonTimestamp],
      filterSubmitter: Option[LfPartyId],
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Seq[TransferData]] =
    Future.successful {
      def filter(entry: TransferEntry): Boolean =
        entry.timeOfCompletion.isEmpty && // Always filter out completed transfers
          filterOrigin.forall(origin => entry.transferData.originDomain == origin) &&
          filterTimestamp.forall(ts => entry.transferData.transferId.requestTimestamp == ts) &&
          filterSubmitter.forall(party => entry.transferData.transferOutRequest.submitter == party)

      transferDataMap.values.to(LazyList).filter(filter).take(limit).map(_.transferData)
    }

  override def deleteTransfer(
      transferId: TransferId
  )(implicit traceContext: TraceContext): Future[Unit] =
    Future.successful {
      val _ = transferDataMap.remove(transferId)
    }

  override def findAfter(requestAfter: Option[(CantonTimestamp, DomainId)], limit: Int)(implicit
      traceContext: TraceContext
  ): Future[Seq[TransferData]] = Future.successful {
    def filter(entry: TransferEntry): Boolean =
      entry.timeOfCompletion.isEmpty && // Always filter out completed transfers
        requestAfter.forall(ts =>
          (entry.transferData.transferId.requestTimestamp, entry.transferData.originDomain) > ts
        )

    transferDataMap.values
      .to(LazyList)
      .filter(filter)
      .take(limit)
      .map(_.transferData)
      .sortBy(t => (t.transferId.requestTimestamp, t.transferId.originDomain))(
        // Explicitly use the standard ordering on two-tuples here
        // As Scala does not seem to infer the right implicits to use here
        Ordering.Tuple2(
          CantonTimestamp.orderCantonTimestamp.toOrdering,
          DomainId.orderDomainId.toOrdering,
        )
      )
  }
}
