// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.multidomain

import cats.data.EitherT
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.data.TransferSubmitterMetadata
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.InjectionErrorGroup
import com.digitalasset.canton.error.TransactionErrorImpl
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ledger.api.multidomain.TransferSubmissionApiService.ApiTransferCommand
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.TransferProcessorError
import com.digitalasset.canton.participant.sync.SyncDomain
import com.digitalasset.canton.protocol.{LfContractId, SourceDomainId, TargetDomainId, TransferId}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{
  LedgerApplicationId,
  LedgerCommandId,
  LedgerParticipantId,
  LedgerSubmissionId,
  LfPartyId,
  LfWorkflowId,
}

import scala.concurrent.{ExecutionContext, Future}

trait TransferSubmissionApiService {
  def submit(command: ApiTransferCommand)(implicit traceContext: TraceContext): Future[Unit]
}

object TransferSubmissionApiService {
  object Error extends InjectionErrorGroup {
    @Explanation("This error results if a transfer command fails initial validation.")
    @Resolution("Check connection to the domain and the topology.")
    object TransferValidation
        extends ErrorCode(
          id = "TRANSFER_COMMAND_VALIDATION",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(
          applicationId: LedgerApplicationId,
          submissionId: Option[LedgerSubmissionId],
          error: String,
      ) extends TransactionErrorImpl(cause = s"Cannot process transfer command: $error")
    }
  }

  sealed trait ApiTransferCommand {
    def submitterMetadata(submittingParticipant: LedgerParticipantId): TransferSubmitterMetadata
  }

  object ApiTransferCommand {
    final case class In(
        submittingParty: LfPartyId,
        targetDomain: TargetDomainId,
        transferId: TransferId,
        applicationId: LedgerApplicationId,
        submissionId: Option[LedgerSubmissionId],
        commandId: LedgerCommandId,
        workflowId: Option[LfWorkflowId],
    ) extends ApiTransferCommand {
      override def submitterMetadata(
          submittingParticipant: LedgerParticipantId
      ): TransferSubmitterMetadata =
        TransferSubmitterMetadata(
          submittingParty,
          applicationId,
          submittingParticipant,
          commandId,
          submissionId,
          workflowId,
        )
    }

    final case class Out(
        submittingParty: LfPartyId,
        contractId: LfContractId,
        sourceDomain: SourceDomainId,
        targetDomain: TargetDomainId,
        applicationId: LedgerApplicationId,
        submissionId: Option[LedgerSubmissionId],
        commandId: LedgerCommandId,
        workflowId: Option[LfWorkflowId],
    ) extends ApiTransferCommand {
      override def submitterMetadata(
          submittingParticipant: LedgerParticipantId
      ): TransferSubmitterMetadata =
        TransferSubmitterMetadata(
          submittingParty,
          applicationId,
          submittingParticipant,
          commandId,
          submissionId,
          workflowId,
        )
    }
  }
}

class TransferSubmissionApiServiceImpl(
    readySyncDomainById: DomainId => Option[SyncDomain],
    protocolVersionFor: Traced[DomainId] => Future[Option[ProtocolVersion]],
    submittingParticipant: LedgerParticipantId,
    namedLoggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends TransferSubmissionApiService
    with NamedLogging {
  override def submit(
      command: ApiTransferCommand
  )(implicit traceContext: TraceContext): Future[Unit] =
    command match {
      case out: ApiTransferCommand.Out =>
        doTransfer(
          syncDomain = out.sourceDomain.unwrap,
          protocolVersionDomain = out.targetDomain.unwrap,
          applicationId = out.applicationId,
          submissionId = out.submissionId,
        )(protocolVersion =>
          _.submitTransferOut(
            submitterMetadata = command.submitterMetadata(submittingParticipant),
            contractId = out.contractId,
            targetDomain = out.targetDomain,
            targetProtocolVersion = TargetProtocolVersion(protocolVersion),
          )
        )

      case in: ApiTransferCommand.In =>
        doTransfer(
          syncDomain = in.targetDomain.unwrap,
          protocolVersionDomain = in.transferId.sourceDomain.unwrap,
          applicationId = in.applicationId,
          submissionId = in.submissionId,
        )(protocolVersion =>
          _.submitTransferIn(
            submitterMetadata = command.submitterMetadata(submittingParticipant),
            transferId = in.transferId,
            sourceProtocolVersion = SourceProtocolVersion(protocolVersion),
          )
        )
    }

  private def doTransfer[E <: TransferProcessorError, T](
      syncDomain: DomainId,
      protocolVersionDomain: DomainId,
      applicationId: LedgerApplicationId,
      submissionId: Option[LedgerSubmissionId],
  )(
      transfer: ProtocolVersion => SyncDomain => EitherT[Future, E, FutureUnlessShutdown[T]]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    for {
      syncDomain <- EitherT.fromOption[Future](
        readySyncDomainById(syncDomain),
        ifNone = LedgerApiErrors.RequestValidation.InvalidArgument
          .Reject(s"Domain ID not found: $syncDomain")
          .asGrpcError,
      )
      protocolVersion <- EitherT.fromOptionF(
        protocolVersionFor(Traced(protocolVersionDomain)),
        ifNone = LedgerApiErrors.RequestValidation.InvalidArgument
          .Reject(s"Domain ID's protocol version not found: $protocolVersionDomain")
          .asGrpcError,
      )
      _ <- transfer(protocolVersion)(syncDomain)
        .leftMap { error =>
          TransferSubmissionApiService.Error.TransferValidation
            .Error(
              applicationId = applicationId,
              submissionId = submissionId,
              error = error.message,
            )
            .asGrpcError
        }
        .mapK(FutureUnlessShutdown.outcomeK)
        .semiflatMap(Predef.identity)
        .onShutdown(
          Left(LedgerApiErrors.InternalError.Generic("Application is shutting down").asGrpcError)
        )
    } yield ()
  }.rethrowT

  override protected def loggerFactory: NamedLoggerFactory = namedLoggerFactory
}
