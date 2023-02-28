// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import cats.syntax.option.*
import cats.syntax.parallel.*
import com.daml.ledger.participant.state.v2.CompletionInfo
import com.daml.lf.engine
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.crypto.DomainSnapshotSyncCryptoApi
import com.digitalasset.canton.data.{CantonTimestamp, TransferSubmitterMetadata, ViewType}
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
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.*
import com.digitalasset.canton.participant.protocol.{ProcessingSteps, ProtocolProcessor}
import com.digitalasset.canton.participant.store.TransferStore.TransferStoreError
import com.digitalasset.canton.participant.sync.{LedgerSyncEvent, TimestampedEvent}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.MediatorResponse.InvalidMediatorResponse
import com.digitalasset.canton.protocol.messages.Verdict.{
  Approve,
  MediatorReject,
  ParticipantReject,
}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.{Batch, OpenEnvelope, WithRecipients}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantAttributes
import com.digitalasset.canton.topology.{DomainId, MediatorId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{LfPartyId, RequestCounter, SequencerCounter}

import scala.collection.concurrent
import scala.concurrent.{ExecutionContext, Future, Promise}

trait TransferProcessingSteps[
    SubmissionParam,
    SubmissionResult,
    RequestViewType <: ViewType,
    Result <: SignedProtocolMessageContent,
    PendingTransferType <: PendingTransfer,
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

  override type RejectionArgs = TransferProcessingSteps.RejectionArgs[PendingTransferType]

  override type RequestType <: ProcessingSteps.RequestType.Transfer
  override val requestType: RequestType

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

  override def authenticateInputContracts(
      pendingDataAndResponseArgs: PendingDataAndResponseArgs
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, Unit] = {
    // We don't authenticate input contracts on transfers
    EitherT.pure(())
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
      decryptedEitherList <- batch.toNEF.parTraverse(decryptTree(snapshot)(_).value)
    } yield DecryptedViews(
      decryptedEitherList.map(_.map(decryptedView => (decryptedView, None)))
    )
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
    import cats.implicits.*
    stakeholders.parTraverseFilter { stk =>
      for {
        relationshipO <- snapshot.hostedOn(stk, participantId)
      } yield {
        relationshipO.map { _: ParticipantAttributes =>
          stk
        }
      }
    }
  }

  // TODO(#11388): Generate rejection information (see: MultipleMediatorsBaseTest and InactiveMediatorError)
  override def eventAndSubmissionIdForInactiveMediator(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      decryptedViews: NonEmpty[Seq[WithRecipients[DecryptedView]]],
  )(implicit traceContext: TraceContext): (Option[TimestampedEvent], Option[PendingSubmissionId]) =
    (None, decryptedViews.head1.unwrap.rootHash.some)

  override def createRejectionEvent(rejectionArgs: RejectionArgs)(implicit
      traceContext: TraceContext
  ): Either[TransferProcessorError, Option[TimestampedEvent]] = {

    val RejectionArgs(pendingTransfer, rejectionReason) = rejectionArgs

    val completionInfoO = Some(
      CompletionInfo(
        actAs = List(pendingTransfer.submitterMetadata.submitter),
        applicationId = pendingTransfer.submitterMetadata.applicationId,
        commandId = pendingTransfer.submitterMetadata.commandId,
        optDeduplicationPeriod = None,
        submissionId = pendingTransfer.submitterMetadata.submissionId,
        statistics = None,
      )
    )

    rejectionReason.logWithContext(Map("requestId" -> pendingTransfer.requestId.toString))
    val rejection = LedgerSyncEvent.CommandRejected.FinalReason(rejectionReason.rpcStatus())

    val tse = completionInfoO.map(info =>
      TimestampedEvent(
        LedgerSyncEvent
          .CommandRejected(pendingTransfer.requestId.unwrap.toLf, info, rejection, requestType),
        pendingTransfer.requestCounter.asLocalOffset,
        Some(pendingTransfer.requestSequencerCounter),
      )
    )

    Right(tse)
  }

  override def decisionTimeFor(
      parameters: DynamicDomainParametersWithValidity,
      requestTs: CantonTimestamp,
  ): Either[TransferProcessorError, CantonTimestamp] =
    parameters.decisionTimeFor(requestTs).leftMap(TransferParametersError(parameters.domainId, _))

  override def participantResponseDeadlineFor(
      parameters: DynamicDomainParametersWithValidity,
      requestTs: CantonTimestamp,
  ): Either[TransferProcessorError, CantonTimestamp] =
    parameters
      .participantResponseDeadlineFor(requestTs)
      .leftMap(TransferParametersError(parameters.domainId, _))

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

    def submitterMetadata: TransferSubmitterMetadata
  }

  case class RejectionArgs[T <: PendingTransfer](pendingTransfer: T, error: LocalReject)

  trait TransferProcessorError
      extends WrapsProcessorError
      with Product
      with Serializable
      with PrettyPrinting {
    override def underlyingProcessorError(): Option[ProcessorError] = None

    override def pretty: Pretty[TransferProcessorError.this.type] = adHocPrettyInstance

    def message: String
  }

  case class GenericStepsError(error: ProcessorError) extends TransferProcessorError {
    override def underlyingProcessorError(): Option[ProcessorError] = Some(error)

    override def message: String = error.toString
  }

  case class UnknownDomain(domainId: DomainId, context: String) extends TransferProcessorError {
    override def message: String = s"Unknown domain $domainId when $context"
  }

  case object ApplicationShutdown extends TransferProcessorError {
    override def pretty: Pretty[ApplicationShutdown.type] = prettyOfObject[ApplicationShutdown.type]
    override def message: String = "Application is shutting down"
  }

  case class DomainNotReady(domainId: DomainId, context: String) extends TransferProcessorError {
    override def message: String = s"Domain $domainId is not ready when $context"
  }

  case class TransferParametersError(domainId: DomainId, context: String)
      extends TransferProcessorError {
    override def message: String = s"Unable to compute transfer parameters for $domainId: $context"
  }

  case class MetadataNotFound(err: engine.Error) extends TransferProcessorError {
    override def message: String = s"Contract metadata not found: ${err.message}"
  }

  case class CreatingTransactionIdNotFound(contractId: LfContractId)
      extends TransferProcessorError {
    override def message: String = s"Creating transaction id not found for contract `$contractId`"

  }

  case class NoTimeProofFromDomain(domainId: DomainId) extends TransferProcessorError {
    override def message: String = s"Cannot fetch time proof for domain `$domainId`"
  }

  case object ReceivedNoRequests extends TransferProcessorError {
    override def message: String = "No request found for transfer"
  }

  case class ReceivedMultipleRequests[T](transferIds: NonEmpty[Seq[T]])
      extends TransferProcessorError {
    override def message: String =
      s"Expecting a single transfer id and got several: ${transferIds.mkString(", ")}"
  }

  case class NoSubmissionPermissionIn(
      transferId: TransferId,
      party: LfPartyId,
      participantId: ParticipantId,
  ) extends TransferProcessorError {

    override def message: String =
      s"For transfer-in `$transferId`: $party does not have submission permission on $participantId"
  }

  case class NoSubmissionPermissionOut(
      contractId: LfContractId,
      party: LfPartyId,
      participantId: ParticipantId,
  ) extends TransferProcessorError {
    override def message: String =
      s"For transfer-out of `$contractId`: $party does not have submission permission on $participantId"
  }

  case class StakeholdersMismatch(
      transferId: Option[TransferId],
      declaredViewStakeholders: Set[LfPartyId],
      declaredContractStakeholders: Option[Set[LfPartyId]],
      expectedStakeholders: Either[String, Set[LfPartyId]],
  ) extends TransferProcessorError {
    override def message: String = s"For transfer `$transferId`: stakeholders mismatch"
  }

  case class NoStakeholders private (contractId: LfContractId) extends TransferProcessorError {
    override def message: String = s"Contract $contractId does not have any stakeholder"
  }

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

  case class SubmittingPartyMustBeStakeholderOut(
      contractId: LfContractId,
      submittingParty: LfPartyId,
      stakeholders: Set[LfPartyId],
  ) extends TransferProcessorError {
    override def message: String =
      s"Cannot transfer-out contract `$contractId`: submitter `$submittingParty` is not a stakeholder"
  }

  case class SubmittingPartyMustBeStakeholderIn(
      transferId: TransferId,
      submittingParty: LfPartyId,
      stakeholders: Set[LfPartyId],
  ) extends TransferProcessorError {
    override def message: String =
      s"Cannot transfer-in `$transferId`: submitter `$submittingParty` is not a stakeholder"
  }

  case class TransferStoreFailed(transferId: TransferId, error: TransferStoreError)
      extends TransferProcessorError {
    override def message: String = s"Cannot transfer `$transferId`: internal transfer store error"
  }

  case class EncryptionError(contractId: LfContractId, error: EncryptedViewMessageCreationError)
      extends TransferProcessorError {
    override def message: String = s"Cannot transfer contract `$contractId`: encryption error"
  }

  case class DecryptionError[VT <: ViewType](
      transferId: TransferId,
      error: EncryptedViewMessageDecryptionError[VT],
  ) extends TransferProcessorError {
    override def message: String = s"Cannot transfer `$transferId`: decryption error"
  }

  case class DuplicateTransferTreeHash(
      transferId: Option[TransferId],
      submitterLf: LfPartyId,
      hash: RootHash,
  ) extends TransferProcessorError {
    private def kind = transferId.map(id => s"in: `$id`").getOrElse("out")

    override def message: String = s"Cannot transfer-$kind: duplicatehash"
  }

  case class FailedToCreateResponse(transferId: TransferId, error: InvalidMediatorResponse)
      extends TransferProcessorError {
    override def message: String = s"Cannot transfer `$transferId`: failed to create response"
  }

  case class CausalityInformationMissing(transferId: TransferId, missingFor: Set[LfPartyId])
      extends TransferProcessorError {
    override def message: String = s"Cannot transfer `$transferId`: causility information missing"
  }

  case class IncompatibleProtocolVersions(
      contractId: LfContractId,
      source: SourceProtocolVersion,
      target: TargetProtocolVersion,
  ) extends TransferProcessorError {
    override def message: String =
      s"Cannot transfer contract `$contractId`: invalid transfer from domain with protocol version $source to domain with protocol version $target"
  }

  case class FieldConversionError(transferId: TransferId, field: String, error: String)
      extends TransferProcessorError {
    override def message: String = s"Cannot transfer `$transferId`: invalid conversion for `$field`"

    override def pretty: Pretty[FieldConversionError] = prettyOfClass(
      param("field", _.field.unquoted),
      param("error", _.error.unquoted),
    )
  }
}
