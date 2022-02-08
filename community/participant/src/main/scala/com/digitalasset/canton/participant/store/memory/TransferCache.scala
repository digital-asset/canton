// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.EitherT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.transfer.TransferData
import com.digitalasset.canton.participant.store.TransferStore.{
  TransferAlreadyCompleted,
  TransferCompleted,
  TransferStoreError,
}
import com.digitalasset.canton.participant.store.memory.TransferCache.PendingTransferCompletion
import com.digitalasset.canton.participant.store.{TransferLookup, TransferStore}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.TransferId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{Checked, CheckedT}
import com.digitalasset.canton.{DomainId, LfPartyId}
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise}

/** Adds an in-memory cache of pending completions on top of a [[store.TransferStore]].
  * Completions appear atomic to transfer lookups that go through the cache,
  * even if they are written to the store only later.
  */
class TransferCache(transferStore: TransferStore, override val loggerFactory: NamedLoggerFactory)(
    implicit val ec: ExecutionContext
) extends TransferLookup
    with NamedLogging {

  @VisibleForTesting
  private[memory] val pendingCompletions: concurrent.Map[TransferId, PendingTransferCompletion] =
    new TrieMap[TransferId, PendingTransferCompletion]

  /** Completes the given transfer with the given `timeOfCompletion`.
    * Completion appears atomic to transfer lookups that go through the cache.
    *
    * @return The future completes when this completion or a completion of the same transfer by an earlier request
    *         has been written to the underlying [[store.TransferStore]].
    */
  def completeTransfer(transferId: TransferId, timeOfCompletion: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, Nothing, TransferStoreError, Unit] = CheckedT {
    logger.trace(
      s"Request ${timeOfCompletion.rc}: Marking transfer $transferId as completed in cache"
    )
    pendingCompletions.putIfAbsent(
      transferId,
      PendingTransferCompletion(timeOfCompletion)(),
    ) match {
      case None =>
        transferStore.completeTransfer(transferId, timeOfCompletion).value.map { result =>
          logger.trace(s"Request ${timeOfCompletion.rc}: Marked transfer $transferId as completed")
          val pendingTransferCompletion = pendingCompletions
            .remove(transferId)
            .getOrElse(
              throw new IllegalStateException(
                s"Unable to find transfer `$transferId` in pending completions"
              )
            )
          pendingTransferCompletion.completion.success(result)
          result
        }

      case Some(
            previousPendingTransferCompletion @ PendingTransferCompletion(previousTimeOfCompletion)
          ) =>
        if (previousTimeOfCompletion.rc <= timeOfCompletion.rc) {
          /* An earlier request (or the same) is already writing to the transfer store.
           * Therefore, there is no point in trying to store this later request, too.
           * It suffices to piggy-back on the earlier write and forward the result.
           */
          logger.trace(
            s"Request ${timeOfCompletion.rc}: Omitting the transfer completion write because the earlier request ${previousTimeOfCompletion.rc} is writing already."
          )
          previousPendingTransferCompletion.completion.future.map { result =>
            for {
              _ <- result
              _ <-
                if (previousTimeOfCompletion == timeOfCompletion) Checked.result(())
                else Checked.continue(TransferAlreadyCompleted(transferId, timeOfCompletion))
            } yield ()
          }
        } else {
          /* A later request is already writing to the transfer store.
           * To ensure that the earliest transfer-in request is recorded, we write the request to the store.`
           * This write happens only after the ongoing write to not disturb the error reporting for the ongoing write.
           * However, it is not necessary to add this request to the cache
           * because the cache has already marked the transfer as having been completed.
           */
          for {
            _ <- previousPendingTransferCompletion.completion.future
            _ = logger.trace(
              s"Request ${timeOfCompletion.rc}: Overwriting the transfer completion of the later request ${previousTimeOfCompletion.rc}"
            )
            result <- transferStore.completeTransfer(transferId, timeOfCompletion).value
          } yield result
        }
    }
  }

  override def lookup(transferId: TransferId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferStore.TransferLookupError, TransferData] =
    pendingCompletions.get(transferId).fold(transferStore.lookup(transferId)) {
      case PendingTransferCompletion(timeOfCompletion) =>
        EitherT.leftT(TransferCompleted(transferId, timeOfCompletion))
    }

  override def find(
      filterOrigin: Option[DomainId],
      filterRequestTimestamp: Option[CantonTimestamp],
      filterSubmitter: Option[LfPartyId],
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Seq[TransferData]] =
    transferStore
      .find(filterOrigin, filterRequestTimestamp, filterSubmitter, limit)
      .map(_.filter(transferData => !pendingCompletions.contains(transferData.transferId)))
}

object TransferCache {
  case class PendingTransferCompletion(timeOfCompletion: TimeOfChange)(
      val completion: Promise[Checked[Nothing, TransferStoreError, Unit]] =
        Promise[Checked[Nothing, TransferStoreError, Unit]]()
  )
}
