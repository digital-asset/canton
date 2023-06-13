// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.grpc.multidomain

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1 as ledgerProto
import com.daml.lf.data.Ref.IdString
import com.daml.lf.data.StringModule
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.api.validation.FieldValidator
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.ledger.api.multidomain.StateApiService.{
  ActiveContractsRequest,
  ConnectedDomainRequest,
}
import com.digitalasset.canton.participant.ledger.api.multidomain.TransferSubmissionApiService.ApiTransferCommand
import com.digitalasset.canton.participant.ledger.api.multidomain.UpdateApiService.{
  CompletionRequest,
  UpdateRequest,
}
import com.digitalasset.canton.participant.protocol.v0.multidomain.TransferCommand.Command
import com.digitalasset.canton.participant.protocol.v0.multidomain as mdProto
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

  def validateUpdatesRequest(
      request: mdProto.GetUpdatesRequest
  )(implicit loggingContext: NamedLoggingContext): Try[UpdateRequest] =
    for {
      fromExclusive <- validateMandatoryField("begin")(request.begin)
        .flatMap(validateOffsetField("begin"))
      toInclusive <- validateOffsetField("end")(
        request.end.getOrElse(
          LedgerOffset.of(
            LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END)
          )
        )
      )
      party <- validateLedgerField(LfPartyId, "party")(request.party)
    } yield UpdateRequest(
      fromExclusive = fromExclusive,
      toInclusive = toInclusive,
      requestingParty = party,
    )

  def validateActiveContractsRequest(
      request: mdProto.GetActiveContractsRequest
  )(implicit loggingContext: NamedLoggingContext): Try[ActiveContractsRequest] =
    for {
      validAt <- validateAbsoluteOffsetField("valid_at_offset")(request.validAtOffset)

      apiTransactionFilter <- request.filter.toRight("empty is not allowed").toFieldError("filter")
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
        .toFieldError("interface_filters")

      templateIds <- templateIds.traverse(FieldValidator.validateIdentifier).toTry
    } yield templateIds
  }

  def validateTransactionFilter(
      filter: ledgerProto.transaction_filter.TransactionFilter
  )(implicit
      loggingContext: NamedLoggingContext
  ): Try[Map[IdString.Party, Option[NonEmpty[Seq[LfTemplateId]]]]] = {
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
  }

  def validateConnectedDomainRequest(request: mdProto.GetConnectedDomainsRequest)(implicit
      loggingContext: NamedLoggingContext
  ): Try[ConnectedDomainRequest] =
    for {
      party <- validateLedgerField(LfPartyId, "party")(request.party)
    } yield ConnectedDomainRequest(party)

  def validateCompletionRequest(
      request: mdProto.CompletionStreamRequest
  )(implicit loggingContext: NamedLoggingContext): Try[CompletionRequest] =
    for {
      applicationId <- validateLedgerField(LedgerApplicationId, "application_id")(
        request.applicationId
      )
      parties <- request.parties.traverse(validateLedgerField(LfPartyId, "parties"))
      startExclusive <- request.offset.traverse(validateOffsetField("offset"))
    } yield CompletionRequest(
      applicationId = applicationId,
      parties = parties.toSet,
      startExclusive = startExclusive,
    )

  def validateTransferSubmitRequest(
      req: mdProto.SubmitRequest
  )(implicit loggingContext: NamedLoggingContext): Try[ApiTransferCommand] =
    for {
      transferCommand <- validateMandatoryField("transfer_command")(req.transferCommand)
      submitterParty <- validateLedgerField(LfPartyId, "submitter")(transferCommand.submitter)
      applicationId <- validateLedgerField(LedgerApplicationId, "application_id")(
        transferCommand.applicationId
      )
      submissionId <- validateOptionalField("submission_id")(
        from = transferCommand.submissionId,
        to = LedgerSubmissionId.fromString,
      )
      workflowId <- validateOptionalField("workflow_id")(
        from = transferCommand.workflowId,
        to = LfWorkflowId.fromString,
      )

      commandIdRaw = OptionUtil
        .emptyStringAsNone(transferCommand.commandId)
        .getOrElse(UUID.randomUUID().toString)
      commandId <- validateLedgerField(LedgerCommandId, "command_id")(commandIdRaw)

      apiCommand <- transferCommand.command match {
        case Command.Empty =>
          Failure(fieldError("transfer_command", "TransferCommand field cannot be empty"))
        case Command.TransferOutCommand(out) =>
          validateTransferOut(
            submitterParty,
            applicationId,
            submissionId,
            commandId,
            workflowId,
            out,
          )
        case Command.TransferInCommand(in) =>
          validateTransferIn(submitterParty, applicationId, submissionId, commandId, workflowId, in)
      }
    } yield apiCommand

  private def validateTransferIn(
      submittingParty: LfPartyId,
      applicationId: LedgerApplicationId,
      submissionId: Option[LedgerSubmissionId],
      commandId: LedgerCommandId,
      workflowId: Option[LfWorkflowId],
      in: mdProto.TransferInCommand,
  )(implicit
      loggingContext: NamedLoggingContext
  ): Try[ApiTransferCommand] = {
    for {
      targetDomain <- validateDomainIdField("target")(in.target).map(TargetDomainId(_))
      sourceDomain <- validateDomainIdField("source")(in.source).map(SourceDomainId(_))
      requestTimeStampMicros <- validateLongFromStringField("transfer_out_id")(in.transferOutId)
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
      out: mdProto.TransferOutCommand,
  )(implicit
      loggingContext: NamedLoggingContext
  ): Try[ApiTransferCommand] = {
    for {
      contractId <- validateContractIdField("contract_id")(out.contractId)
      sourceDomain <- validateDomainIdField("source")(out.source).map(SourceDomainId(_))
      targetDomain <- validateDomainIdField("target")(out.target).map(TargetDomainId(_))
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
      offset: LedgerOffset
  )(implicit loggingContext: NamedLoggingContext): Try[GlobalOffset] =
    offset.value match {
      case LedgerOffset.Value.Absolute(absoluteOffset) =>
        val globalOffsetE = for {
          ledgerSyncOffset <- UpstreamOffsetConvert.toLedgerSyncOffset(absoluteOffset)
          globalOffset <- UpstreamOffsetConvert.toGlobalOffset(ledgerSyncOffset)
        } yield globalOffset
        globalOffsetE.toFieldError(fieldName)

      case LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN) =>
        Success(0)

      case LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END) =>
        Success(Long.MaxValue)

      case LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.Unrecognized(value)) =>
        Failure(fieldError(fieldName, s"Boundary offset field is not recognized: $value"))

      case LedgerOffset.Value.Empty =>
        Failure(fieldError(fieldName, "Offset field cannot be empty"))
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
      e.left.map(fieldError(fieldName, _)).toTry
  }
}
