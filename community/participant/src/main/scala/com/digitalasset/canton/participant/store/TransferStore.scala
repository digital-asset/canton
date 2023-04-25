// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.LocalOffset
import com.digitalasset.canton.participant.protocol.transfer.TransferData
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.messages.DeliveredTransferOutResult
import com.digitalasset.canton.protocol.{SourceDomainId, TransferId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{Checked, CheckedT, OptionUtil}
import com.digitalasset.canton.{LfPartyId, RequestCounter}

import scala.concurrent.Future

trait TransferStore extends TransferLookup {
  import TransferStore.*

  /** Adds the transfer to the store.
    *
    * Calls to this method are idempotent, independent of the order.
    * Differences in [[protocol.transfer.TransferData!.transferOutResult]] between two calls are ignored
    * if the field is [[scala.None$]] in one of the calls. If applicable, the field content is merged.
    *
    * @throws java.lang.IllegalArgumentException if the transfer's target domain is not
    *                                            the domain this [[TransferStore]] belongs to.
    */
  def addTransfer(transferData: TransferData)(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferStoreError, Unit]

  /** Adds the given [[com.digitalasset.canton.protocol.messages.TransferOutResult]] to the transfer data in the store,
    * provided that the transfer data has previously been stored.
    *
    * The same [[com.digitalasset.canton.protocol.messages.TransferOutResult]] can be added any number of times.
    * This includes transfer-out results that are in the [[protocol.transfer.TransferData!.transferOutResult]]
    * added with [[addTransfer]].
    *
    * @param transferOutResult The transfer-out result to add
    * @return [[TransferStore$.UnknownTransferId]] if the transfer has not previously been added with [[addTransfer]].
    *         [[TransferStore$.TransferOutResultAlreadyExists]] if a different transfer-out result for the same
    *         transfer request has been added before, including as part of [[addTransfer]].
    */
  def addTransferOutResult(transferOutResult: DeliveredTransferOutResult)(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferStoreError, Unit]

  /** Marks the transfer as completed, i.e., a transfer-in request was committed.
    * If the transfer has already been completed then a [[TransferStore.TransferAlreadyCompleted]] is reported, and the
    * [[com.digitalasset.canton.participant.util.TimeOfChange]] of the completion is not changed from the old value.
    *
    * @param timeOfCompletion Provides the request counter and activeness time of the committed transfer-in request.
    */
  def completeTransfer(transferId: TransferId, timeOfCompletion: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, Nothing, TransferStoreError, Unit]

  /** Removes the transfer from the store,
    * when the transfer-out request is rejected or the transfer is pruned.
    */
  def deleteTransfer(transferId: TransferId)(implicit traceContext: TraceContext): Future[Unit]

  /** Removes all completions of transfers that have been triggered by requests with at least the given counter.
    * This method must not be called concurrently with [[completeTransfer]], but may be called concurrently with
    * [[addTransfer]] and [[addTransferOutResult]].
    *
    * Therefore, this method need not be linearizable w.r.t. [[completeTransfer]].
    * For example, if two requests `rc1` complete two transfers while [[deleteCompletionsSince]] is running for
    * some `rc <= rc1, rc2`, then there are no guarantees which of the completions of `rc1` and `rc2` remain.
    */
  def deleteCompletionsSince(criterionInclusive: RequestCounter)(implicit
      traceContext: TraceContext
  ): Future[Unit]
}

object TransferStore {
  sealed trait TransferStoreError extends Product with Serializable

  sealed trait TransferLookupError extends TransferStoreError {
    def cause: String
  }

  final case class UnknownTransferId(transferId: TransferId) extends TransferLookupError {
    override def cause: String = "unknown transfer id"
  }

  final case class TransferCompleted(transferId: TransferId, timeOfCompletion: TimeOfChange)
      extends TransferLookupError {
    override def cause: String = "transfer already completed"
  }

  final case class TransferDataAlreadyExists(old: TransferData, `new`: TransferData)
      extends TransferStoreError

  final case class TransferOutResultAlreadyExists(
      transferId: TransferId,
      old: DeliveredTransferOutResult,
      `new`: DeliveredTransferOutResult,
  ) extends TransferStoreError

  final case class TransferAlreadyCompleted(transferId: TransferId, newCompletion: TimeOfChange)
      extends TransferStoreError

  /** The data for a transfer and possible when the transfer was completed. */
  final case class TransferEntry(
      transferData: TransferData,
      timeOfCompletion: Option[TimeOfChange],
  ) {
    def isCompleted: Boolean = timeOfCompletion.nonEmpty

    def mergeWith(
        other: TransferEntry
    ): Checked[TransferDataAlreadyExists, TransferAlreadyCompleted, TransferEntry] = {
      for {
        mergedData <- Checked.fromEither(
          transferData
            .mergeWith(other.transferData)
            .toRight(TransferDataAlreadyExists(transferData, other.transferData))
        )
        mergedToc <- OptionUtil
          .mergeEqual(timeOfCompletion, other.timeOfCompletion)
          .fold[
            Checked[TransferDataAlreadyExists, TransferAlreadyCompleted, Option[TimeOfChange]]
          ] {
            val thisToC =
              timeOfCompletion.getOrElse(
                throw new IllegalStateException("Time of completion should be defined")
              )
            val otherToC =
              other.timeOfCompletion.getOrElse(
                throw new IllegalStateException("Time of completion should be defined")
              )

            Checked.continueWithResult(
              TransferAlreadyCompleted(transferData.transferId, otherToC),
              Some(thisToC),
            )
          }(Checked.result)
      } yield
        if ((mergedData eq transferData) && (mergedToc eq timeOfCompletion)) this
        else TransferEntry(mergedData, mergedToc)
    }

    def addTransferOutResult(
        transferOutResult: DeliveredTransferOutResult
    ): Either[TransferOutResultAlreadyExists, TransferEntry] =
      transferData
        .addTransferOutResult(transferOutResult)
        .toRight {
          val old = transferData.transferOutResult.getOrElse(
            throw new IllegalStateException("Transfer out result should not be empty")
          )
          TransferOutResultAlreadyExists(transferData.transferId, old, transferOutResult)
        }
        .map(TransferEntry(_, timeOfCompletion))

    def complete(
        timeOfChange: TimeOfChange
    ): Checked[TransferDataAlreadyExists, TransferAlreadyCompleted, TransferEntry] = {

      mergeWith(TransferEntry(transferData, Some(timeOfChange)))
    }

    def clearCompletion: TransferEntry = TransferEntry(transferData, None)
  }
}

trait TransferLookup {
  import TransferStore.*

  /** Looks up the given in-flight transfer and returns the data associated with the transfer.
    * @return [[scala.Left$]]([[TransferStore.UnknownTransferId]]) if the transfer is unknown;
    *         [[scala.Left$]]([[TransferStore.TransferCompleted]]) if the transfer has already been completed.
    */
  def lookup(transferId: TransferId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferLookupError, TransferData]

  /** Find utility to look for in-flight transfers. Queried by
    * [[com.digitalasset.canton.console.commands.ParticipantAdministration#transfer.search]].
    * Results need not be consistent with [[lookup]].
    */
  def find(
      filterSource: Option[SourceDomainId],
      filterRequestTimestamp: Option[CantonTimestamp],
      filterSubmitter: Option[LfPartyId],
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Seq[TransferData]]

  /** Find utility to look for in-flight transfers.
    * Transfers are ordered by the tuple (request timestamp, source domain ID), ie transfers are ordered by request timestamps
    * and ties are broken with lexicographic ordering on domain IDs.
    *
    * The ordering here has been chosen to allow a participant to fetch all the pending transfers. The ordering has to
    * be consistent accross calls and uniquely identify a pending transfer, but is otherwise arbitrary.
    *
    * @param requestAfter optionally, specify a strict lower bound for the transfers returned, according to the
    *                     (request timestamp, source domain ID) ordering
    * @param limit limit the number of results
    */
  def findAfter(requestAfter: Option[(CantonTimestamp, SourceDomainId)], limit: Int)(implicit
      traceContext: TraceContext
  ): Future[Seq[TransferData]]

  /** Find utility to look for in-flight transfers.
    * Transfers are ordered by request timestamp.
    *
    * @param sourceDomain source domain of the transfer
    * @param onlyCompletedTransferOut select only transfers that are successfully transferred-out
    * @param transferOutRequestNotAfter select only transfers whose transfer-out request counter `rc`
    *                                   satisfies `rc <= transferOutRequestNotAfter`.
    * @param stakeholders if non-empty, select only transfers of contracts whose set of stakeholders
    *                     intersects `stakeholders`.
    * @param limit limit the number of results
    */
  def findInFlight(
      sourceDomain: SourceDomainId,
      onlyCompletedTransferOut: Boolean,
      transferOutRequestNotAfter: LocalOffset,
      stakeholders: Option[NonEmpty[Set[LfPartyId]]],
      limit: Int,
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[TransferData]]
}
