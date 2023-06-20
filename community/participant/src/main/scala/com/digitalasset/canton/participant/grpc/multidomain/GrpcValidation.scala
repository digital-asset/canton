// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.grpc.multidomain

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.ledger.api.{v1 as ledgerProto, v2 as mdProto}
import com.daml.lf.data.StringModule
import com.daml.nameof.NameOf.qualifiedNameOf
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.api.validation.FieldValidator
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.ledger.participant.state.v2.ReadService.ConnectedDomainRequest
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.ledger.api.multidomain.RequestInclusiveFilter
import com.digitalasset.canton.participant.ledger.api.multidomain.StateApiService.ActiveContractsRequest
import com.digitalasset.canton.participant.ledger.api.multidomain.TransferSubmissionApiService.ApiTransferCommand
import com.digitalasset.canton.participant.ledger.api.multidomain.UpdateApiService.{
  CompletionRequest,
  UpdateRequest,
}
import com.digitalasset.canton.participant.sync.UpstreamOffsetConvert
import com.digitalasset.canton.protocol.{
  LfContractId,
  LfTemplateId,
  SourceDomainId,
  TargetDomainId,
  TransferId,
}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.canton.{
  LedgerApplicationId,
  LedgerCommandId,
  LedgerSubmissionId,
  LfPartyId,
  LfWorkflowId,
}
import io.grpc.StatusRuntimeException

import java.util.UUID
import scala.util.{Failure, Success, Try}

// TODO(#11002) Consider integration with ProtoDeserializationError
private[grpc] object GrpcValidation extends HasLoggerName {
  val ParticipantEnd: ParticipantOffset =
    ParticipantOffset(
      ParticipantOffset.Value.Boundary(ParticipantOffset.ParticipantBoundary.PARTICIPANT_END)
    )

  def validateUpdatesRequest(
      request: mdProto.update_service.GetUpdatesRequest
  )(implicit loggingContext: NamedLoggingContext): Try[UpdateRequest] =
    for {
      fromExclusive <- validateMandatoryField(qualifiedNameOf(request.beginExclusive))(
        request.beginExclusive
      )
        .flatMap(validateOffsetField(qualifiedNameOf(request.beginExclusive)))
      toInclusive <- validateOffsetField(qualifiedNameOf(request.endInclusive))(
        request.endInclusive.getOrElse(ParticipantEnd)
      )

      apiTransactionFilter <- request.filter
        .toRight("empty is not allowed")
        .toFieldError(qualifiedNameOf(request.filter))
      transactionFilter <- validateTransactionFilter(apiTransactionFilter)
    } yield UpdateRequest(
      fromExclusive = fromExclusive,
      toInclusive = toInclusive,
      filters = transactionFilter,
    )

  def validateActiveContractsRequest(
      request: mdProto.state_service.GetActiveContractsRequest
  )(implicit loggingContext: NamedLoggingContext): Try[ActiveContractsRequest] =
    for {
      validAt <- validateAbsoluteOffsetField(qualifiedNameOf(request.activeAtOffset))(
        request.activeAtOffset
      )

      apiTransactionFilter <- request.filter
        .toRight("empty is not allowed")
        .toFieldError(qualifiedNameOf(request.filter))
      transactionFilter <- validateTransactionFilter(apiTransactionFilter)

    } yield ActiveContractsRequest(
      validAt = validAt,
      filters = transactionFilter,
    )

  private def validateInclusiveFilters(
      filters: ledgerProto.transaction_filter.InclusiveFilters
  )(implicit loggingContext: NamedLoggingContext): Try[Seq[LfTemplateId]] = {
    val ledgerProto.transaction_filter.InclusiveFilters(templateIds, interfaceFilters) = filters

    for {
      _ <- NonEmpty
        .from(interfaceFilters)
        .toLeft(())
        .leftMap(_ => "Filtering by interface is not supported")
        .toFieldError(qualifiedNameOf(filters.interfaceFilters))

      templateIds <- templateIds.traverse(FieldValidator.validateIdentifier).toTry
    } yield templateIds
  }

  def validateTransactionFilter(
      filter: mdProto.transaction_filter.TransactionFilter
  )(implicit
      loggingContext: NamedLoggingContext
  ): Try[RequestInclusiveFilter] = {
    filter.filtersByParty.toList
      .traverse { case (partyId, filters) =>
        for {
          lfPartyId <- validateLedgerField(LfPartyId, "party")(partyId)
          templateIds <- filters.inclusive
            .map(validateInclusiveFilters)
            .traverse(_.map(NonEmpty.from))
            .map(_.flatten)
        } yield (lfPartyId, templateIds)
      }
      .map(_.toMap)
      .map(RequestInclusiveFilter)
  }

  def validateConnectedDomainRequest(request: mdProto.state_service.GetConnectedDomainsRequest)(
      implicit loggingContext: NamedLoggingContext
  ): Try[ConnectedDomainRequest] =
    for {
      party <- validateLedgerField(LfPartyId, qualifiedNameOf(request.party))(request.party)
    } yield ConnectedDomainRequest(party)

  def validateCompletionRequest(
      request: mdProto.command_completion_service.CompletionStreamRequest
  )(implicit loggingContext: NamedLoggingContext): Try[CompletionRequest] =
    for {
      applicationId <- validateLedgerField(
        LedgerApplicationId,
        qualifiedNameOf(request.applicationId),
      )(
        request.applicationId
      )
      parties <- request.parties.traverse(
        validateLedgerField(LfPartyId, qualifiedNameOf(request.parties))
      )
      startExclusive <- request.beginExclusive.traverse(
        validateOffsetField(qualifiedNameOf(request.beginExclusive))
      )
    } yield CompletionRequest(
      applicationId = applicationId,
      parties = parties.toSet,
      startExclusive = startExclusive,
    )

  def validateTransferSubmitRequest(
      req: mdProto.command_submission_service.SubmitReassignmentRequest
  )(implicit loggingContext: NamedLoggingContext): Try[ApiTransferCommand] =
    for {
      command <- validateMandatoryField(qualifiedNameOf(req.reassignmentCommand))(
        req.reassignmentCommand
      )
      submitterParty <- validateLedgerField(LfPartyId, qualifiedNameOf(command.submitter))(
        command.submitter
      )
      applicationId <- validateLedgerField(
        LedgerApplicationId,
        qualifiedNameOf(command.applicationId),
      )(
        command.applicationId
      )
      submissionId <- validateOptionalField(qualifiedNameOf(command.submissionId))(
        from = command.submissionId,
        to = LedgerSubmissionId.fromString,
      )
      workflowId <- validateOptionalField(qualifiedNameOf(command.workflowId))(
        from = command.workflowId,
        to = LfWorkflowId.fromString,
      )

      commandIdRaw = OptionUtil
        .emptyStringAsNone(command.commandId)
        .getOrElse(UUID.randomUUID().toString)
      commandId <- validateLedgerField(LedgerCommandId, qualifiedNameOf(command.commandId))(
        commandIdRaw
      )
      apiCommand <- command.command match {
        case mdProto.reassignment_command.ReassignmentCommand.Command.Empty =>
          Failure(
            fieldError(qualifiedNameOf(command.command), "TransferCommand field cannot be empty")
          )
        case mdProto.reassignment_command.ReassignmentCommand.Command.UnassignCommand(out) =>
          validateTransferOut(
            submitterParty,
            applicationId,
            submissionId,
            commandId,
            workflowId,
            out,
          )
        case mdProto.reassignment_command.ReassignmentCommand.Command.AssignCommand(in) =>
          validateTransferIn(submitterParty, applicationId, submissionId, commandId, workflowId, in)
      }
    } yield apiCommand

  private def validateTransferIn(
      submittingParty: LfPartyId,
      applicationId: LedgerApplicationId,
      submissionId: Option[LedgerSubmissionId],
      commandId: LedgerCommandId,
      workflowId: Option[LfWorkflowId],
      in: mdProto.reassignment_command.AssignCommand,
  )(implicit
      loggingContext: NamedLoggingContext
  ): Try[ApiTransferCommand] = {
    for {
      targetDomain <- validateDomainIdField(qualifiedNameOf(in.target))(in.target)
        .map(TargetDomainId(_))
      sourceDomain <- validateDomainIdField(qualifiedNameOf(in.source))(in.source)
        .map(SourceDomainId(_))
      requestTimeStampMicros <- validateLongFromStringField(qualifiedNameOf(in.unassignId))(
        in.unassignId
      )
      transferId = TransferId(
        sourceDomain = sourceDomain,
        transferOutTimestamp = CantonTimestamp.assertFromLong(requestTimeStampMicros),
      )
    } yield ApiTransferCommand.In(
      submittingParty = submittingParty,
      targetDomain = targetDomain,
      transferId = transferId,
      applicationId = applicationId,
      submissionId = submissionId,
      commandId = commandId,
      workflowId = workflowId,
    )
  }

  private def validateTransferOut(
      submittingParty: LfPartyId,
      applicationId: LedgerApplicationId,
      submissionId: Option[LedgerSubmissionId],
      commandId: LedgerCommandId,
      workflowId: Option[LfWorkflowId],
      out: mdProto.reassignment_command.UnassignCommand,
  )(implicit
      loggingContext: NamedLoggingContext
  ): Try[ApiTransferCommand] = {
    for {
      contractId <- validateContractIdField(qualifiedNameOf(out.contractId))(out.contractId)
      sourceDomain <- validateDomainIdField(qualifiedNameOf(out.source))(out.source)
        .map(SourceDomainId(_))
      targetDomain <- validateDomainIdField(qualifiedNameOf(out.target))(out.target)
        .map(TargetDomainId(_))
    } yield ApiTransferCommand.Out(
      submittingParty = submittingParty,
      contractId = contractId,
      sourceDomain = sourceDomain,
      targetDomain = targetDomain,
      applicationId = applicationId,
      submissionId = submissionId,
      commandId = commandId,
      workflowId = workflowId,
    )
  }

  private def validateLedgerField[T](s: StringModule[T], fieldName: String)(
      value: String
  )(implicit loggingContext: NamedLoggingContext): Try[T] =
    s.fromString(value).toFieldError(fieldName)

  private def validateOptionalField[T](
      fieldName: String
  )(from: String, to: String => Either[String, T])(implicit
      loggingContext: NamedLoggingContext
  ): Try[Option[T]] = {
    Option
      .when(from.nonEmpty)(to(from).toFieldError(fieldName))
      .sequence
  }

  private def validateDomainIdField(fieldName: String)(
      stringDomainId: String
  )(implicit loggingContext: NamedLoggingContext): Try[DomainId] =
    DomainId
      .fromString(stringDomainId)
      .toFieldError(fieldName)

  private def validateAbsoluteOffsetField(
      fieldName: String
  )(offsetP: String)(implicit loggingContext: NamedLoggingContext): Try[Option[GlobalOffset]] =
    OptionUtil.emptyStringAsNone(offsetP).traverse { offset =>
      val globalOffsetE = for {
        ledgerSyncOffset <- UpstreamOffsetConvert.toLedgerSyncOffset(offset)
        globalOffset <- UpstreamOffsetConvert.toGlobalOffset(ledgerSyncOffset)
      } yield globalOffset
      globalOffsetE.toFieldError(fieldName)
    }

  private def validateOffsetField(fieldName: String)(
      offset: ParticipantOffset
  )(implicit loggingContext: NamedLoggingContext): Try[GlobalOffset] = {
    import ParticipantOffset.*

    offset.value match {
      case Value.Absolute(absoluteOffset) =>
        val globalOffsetE = for {
          ledgerSyncOffset <- UpstreamOffsetConvert.toLedgerSyncOffset(absoluteOffset)
          globalOffset <- UpstreamOffsetConvert.toGlobalOffset(ledgerSyncOffset)
        } yield globalOffset
        globalOffsetE.toFieldError(fieldName)

      case Value.Boundary(ParticipantBoundary.PARTICIPANT_BEGIN) =>
        Success(0)

      case Value.Boundary(ParticipantBoundary.PARTICIPANT_END) =>
        Success(Long.MaxValue)

      case Value.Boundary(ParticipantBoundary.Unrecognized(unrecognizedValue)) =>
        Failure(fieldError(fieldName, s"Unrecognized boundary value $unrecognizedValue"))

      case Value.Empty => Failure(fieldError(fieldName, "Offset field cannot be empty"))
    }
  }

  private def validateContractIdField(fieldName: String)(
      contractId: String
  )(implicit loggingContext: NamedLoggingContext): Try[LfContractId] =
    LfContractId
      .fromString(contractId)
      .toFieldError(fieldName)

  private def validateLongFromStringField(fieldName: String)(
      longString: String
  )(implicit loggingContext: NamedLoggingContext): Try[Long] =
    Try(longString.toLong).toEither.left
      .map(t => s"Cannot convert string field to Long: ${t.getMessage}")
      .toFieldError(fieldName)

  private def validateMandatoryField[T](fieldName: String)(
      o: Option[T]
  )(implicit loggingContext: NamedLoggingContext): Try[T] =
    o.toRight(fieldError(fieldName, "Field is required")).toTry

  private def fieldError(
      fieldName: String,
      error: String,
  )(implicit loggingContext: NamedLoggingContext): StatusRuntimeException =
    LedgerApiErrors.RequestValidation.InvalidField
      .Reject(fieldName, error)
      .asGrpcError

  implicit class EitherErrorOps[T](val e: Either[String, T]) extends AnyVal {
    def toFieldError(fieldName: String)(implicit loggingContext: NamedLoggingContext): Try[T] =
      e.leftMap(fieldError(fieldName, _)).toTry
  }
}
