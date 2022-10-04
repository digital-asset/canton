// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.{EitherT, OptionT}
import cats.syntax.option._
import cats.syntax.traverse._
import com.daml.lf.engine
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances._
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, SaltError}
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLogging, TracedLogger}
import com.digitalasset.canton.participant.protocol.ProcessingSteps.WrapsProcessorError
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.{
  MalformedPayload,
  NoMediatorError,
  ProcessorError,
}
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors
import com.digitalasset.canton.participant.protocol.submission.EncryptedViewMessageFactory.EncryptedViewMessageCreationError
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps._
import com.digitalasset.canton.participant.protocol.{ProcessingSteps, ProtocolProcessor}
import com.digitalasset.canton.participant.store.TransferStore.TransferStoreError
import com.digitalasset.canton.participant.sync.TimestampedEvent
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.protocol.messages.MediatorResponse.InvalidMediatorResponse
import com.digitalasset.canton.protocol.messages.Verdict.{
  Approve,
  MediatorReject,
  ParticipantReject,
}
import com.digitalasset.canton.protocol.messages._
import com.digitalasset.canton.sequencing.protocol.{Batch, OpenEnvelope, WithRecipients}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantAttributes
import com.digitalasset.canton.topology.{DomainId, MediatorId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{LfPartyId, RequestCounter, SequencerCounter}

import scala.collection.concurrent
import scala.concurrent.{ExecutionContext, Future, Promise}

trait TransferProcessingSteps[
    SubmissionParam,
    SubmissionResult,
    RequestViewType <: ViewType,
    Result <: SignedProtocolMessageContent,
] extends ProcessingSteps[
      SubmissionParam,
      SubmissionResult,
      RequestViewType,
      Result,
      TransferProcessorError,
    ]
    with NamedLogging {

  val participantId: ParticipantId

  protected def engine: DAMLe

  protected implicit def ec: ExecutionContext

  override type SubmissionSendError = TransferProcessorError

  override type PendingSubmissionId = RootHash

  override type PendingSubmissions = concurrent.Map[RootHash, PendingTransferSubmission]

  override type PendingSubmissionData = PendingTransferSubmission

  override type RequestError = TransferProcessorError

  override type ResultError = TransferProcessorError

  override def embedNoMediatorError(error: NoMediatorError): TransferProcessorError =
    GenericStepsError(error)

  override def removePendingSubmission(
      pendingSubmissions: concurrent.Map[RootHash, PendingTransferSubmission],
      pendingSubmissionId: RootHash,
  ): Option[PendingTransferSubmission] =
    pendingSubmissions.remove(pendingSubmissionId)

  override def postProcessSubmissionForInactiveMediator(
      declaredMediator: MediatorId,
      ts: CantonTimestamp,
      pendingSubmission: PendingTransferSubmission,
  )(implicit traceContext: TraceContext): Unit = {
    val error = SubmissionErrors.InactiveMediatorError.Error(declaredMediator, ts)
    pendingSubmission.transferCompletion.success(error.rpcStatus())
  }

  override def postProcessResult(
      verdict: Verdict,
      pendingSubmission: PendingTransferSubmission,
  )(implicit traceContext: TraceContext): Unit = {
    val status = verdict match {
      case _: Approve =>
        com.google.rpc.status.Status(com.google.rpc.Code.OK_VALUE)
      case reject: MediatorReject =>
        reject.rpcStatus()
      case reasons: ParticipantReject =>
        reasons.keyEvent.rpcStatus()
    }
    pendingSubmission.transferCompletion.success(status)
  }

  protected def performPendingSubmissionMapUpdate(
      pendingSubmissionMap: concurrent.Map[RootHash, PendingTransferSubmission],
      transferId: Option[TransferId],
      submitterLf: LfPartyId,
      rootHash: RootHash,
  ): EitherT[Future, TransferProcessorError, PendingTransferSubmission] = {
    val pendingSubmission = PendingTransferSubmission()
    val existing = pendingSubmissionMap.putIfAbsent(rootHash, pendingSubmission)
    EitherT.cond[Future](
      existing.isEmpty,
      pendingSubmission,
      DuplicateTransferTreeHash(transferId, submitterLf, rootHash): TransferProcessorError,
    )
  }

  protected def decryptTree(snapshot: DomainSnapshotSyncCryptoApi)(
      envelope: OpenEnvelope[EncryptedViewMessage[RequestViewType]]
  ): EitherT[Future, EncryptedViewMessageDecryptionError[RequestViewType], WithRecipients[
    DecryptedView
  ]]

  override def decryptViews(
      batch: NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[RequestViewType]]]],
      snapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, DecryptedViews] = {
    val result = for {
      decryptedEitherList <- batch.toNEF.traverse(decryptTree(snapshot)(_).value)
    } yield DecryptedViews(decryptedEitherList)
    EitherT.right(result)
  }

  override def pendingDataAndResponseArgsForMalformedPayloads(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      malformedPayloads: Seq[MalformedPayload],
      snapshot: DomainSnapshotSyncCryptoApi,
  ): Either[RequestError, PendingDataAndResponseArgs] =
    // TODO(M40) This will crash the SyncDomain
    Left(ReceivedNoRequests)

  protected def hostedStakeholders(
      stakeholders: List[LfPartyId],
      snapshot: TopologySnapshot,
  ): Future[List[LfPartyId]] = {
    import cats.implicits._
    stakeholders.traverseFilter { stk =>
      for {
        relationshipO <- snapshot.hostedOn(stk, participantId)
      } yield {
        relationshipO.map { _: ParticipantAttributes =>
          stk
        }
      }
    }
  }

  override def eventAndSubmissionIdForInactiveMediator(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      decryptedViews: NonEmpty[Seq[WithRecipients[DecryptedView]]],
  )(implicit traceContext: TraceContext): (Option[TimestampedEvent], Option[PendingSubmissionId]) =
    (None, decryptedViews.head1.unwrap.rootHash.some)

  override def createRejectionEvent(rejectionArgs: RejectionArgs)(implicit
      traceContext: TraceContext
  ): Either[TransferProcessorError, Option[TimestampedEvent]] =
    Right(None)

  case class TransferSubmission(
      override val batch: Batch[DefaultOpenEnvelope],
      override val pendingSubmissionId: PendingSubmissionId,
  ) extends UntrackedSubmission {

    override def maxSequencingTimeO: OptionT[Future, CantonTimestamp] = OptionT.none

    override def embedSubmissionError(
        err: ProtocolProcessor.SubmissionProcessingError
    ): TransferProcessorError =
      GenericStepsError(err)
    override def toSubmissionError(err: TransferProcessorError): TransferProcessorError = err
  }

  override def embedRequestError(
      err: ProtocolProcessor.RequestProcessingError
  ): TransferProcessorError =
    GenericStepsError(err)

  override def embedResultError(
      err: ProtocolProcessor.ResultProcessingError
  ): TransferProcessorError =
    GenericStepsError(err)

}

object TransferProcessingSteps {

  case class PendingTransferSubmission(
      transferCompletion: Promise[com.google.rpc.status.Status] =
        Promise[com.google.rpc.status.Status]()
  )

  trait PendingTransfer extends Product with Serializable {
    def requestId: RequestId

    def requestCounter: RequestCounter

    def requestSequencerCounter: SequencerCounter
  }

  trait TransferProcessorError
      extends WrapsProcessorError
      with Product
      with Serializable
      with PrettyPrinting {
    override def underlyingProcessorError(): Option[ProcessorError] = None

    override def pretty: Pretty[TransferProcessorError.this.type] = adHocPrettyInstance
  }

  case class GenericStepsError(error: ProcessorError) extends TransferProcessorError {
    override def underlyingProcessorError(): Option[ProcessorError] = Some(error)
  }

  case class SeedGeneratorError(cause: SaltError) extends TransferProcessorError {
    override def pretty: Pretty[SeedGeneratorError] = prettyOfParam(_.cause)
  }

  case class UnknownDomain(domainId: DomainId, reason: String) extends TransferProcessorError

  case class DomainNotReady(domainId: DomainId, reason: String) extends TransferProcessorError

  case class MetadataNotFound(err: engine.Error) extends TransferProcessorError

  case class CreatingTransactionIdNotFound(contractId: LfContractId) extends TransferProcessorError

  case class NoTimeProofFromDomain(domainId: DomainId) extends TransferProcessorError

  case object ReceivedNoRequests extends TransferProcessorError

  case class ReceivedMultipleRequests[T](transferIds: NonEmpty[Seq[T]])
      extends TransferProcessorError

  case class ReceivedWrongRootHash(transferOutRootHash: RootHash, rootHashMessageHash: RootHash)
      extends TransferProcessorError

  case class NoSubmissionPermission(
      transferId: Option[TransferId],
      party: LfPartyId,
      participantId: ParticipantId,
  ) extends TransferProcessorError

  case class StakeholderMismatch(
      transferId: Option[TransferId],
      declaredViewStakeholders: Set[LfPartyId],
      declaredContractStakeholders: Option[Set[LfPartyId]],
      expectedStakeholders: Either[String, Set[LfPartyId]],
  ) extends TransferProcessorError

  case class NoStakeholders private (contract: LfContractId) extends TransferProcessorError

  object NoStakeholders {
    def logAndCreate(contract: LfContractId, logger: TracedLogger)(implicit
        tc: TraceContext
    ): NoStakeholders = {
      logger.error(
        s"Attempting transfer for contract $contract without stakeholders. All contracts should have stakeholders."
      )
      NoStakeholders(contract)
    }
  }

  case class SubmittingPartyMustBeStakeholder(
      transferId: Option[TransferId],
      submittingParty: LfPartyId,
      stakeholders: Set[LfPartyId],
  ) extends TransferProcessorError

  case class TransferStoreFailed(error: TransferStoreError) extends TransferProcessorError

  case class EncryptionError(error: EncryptedViewMessageCreationError)
      extends TransferProcessorError

  case class DecryptionError[VT <: ViewType](error: EncryptedViewMessageDecryptionError[VT])
      extends TransferProcessorError

  case class DuplicateTransferTreeHash(
      transferId: Option[TransferId],
      submitterLf: LfPartyId,
      hash: RootHash,
  ) extends TransferProcessorError

  case class FailedToCreateResponse(error: InvalidMediatorResponse) extends TransferProcessorError

  case class CausalityInformationMissing(missingFor: Set[LfPartyId]) extends TransferProcessorError

  case class IncompatibleProtocolVersions(
      source: SourceProtocolVersion,
      target: TargetProtocolVersion,
  ) extends TransferProcessorError
}
