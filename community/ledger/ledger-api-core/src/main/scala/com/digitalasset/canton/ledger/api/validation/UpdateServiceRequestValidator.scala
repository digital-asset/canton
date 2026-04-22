// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import cats.implicits.{catsSyntaxTuple2Semigroupal, toTraverseOps}
import com.daml.ledger.api.v2.update_service.{
  GetUpdateByIdRequest,
  GetUpdateByOffsetRequest,
  GetUpdatesPageRequest,
  GetUpdatesRequest,
}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.messages.update
import com.digitalasset.canton.ledger.api.messages.update.UpdatesPageToken
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.invalidArgument
import com.digitalasset.canton.ledger.api.validation.ValueValidator.*
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.platform.config.UpdateServiceConfig
import com.digitalasset.canton.protocol.UpdateId
import com.digitalasset.daml.lf.data.Ref.ParticipantId
import io.grpc.StatusRuntimeException

object UpdateServiceRequestValidator {
  type Result[X] = Either[StatusRuntimeException, X]

  import FieldValidator.*

  final case class PartialValidation(
      begin: Option[Offset],
      end: Option[Offset],
      descendingOrder: Boolean,
  )

  private def commonValidations(
      req: GetUpdatesRequest
  )(implicit errorLoggingContext: ErrorLoggingContext): Result[PartialValidation] =
    for {
      begin <- ParticipantOffsetValidator
        .validateNonNegative(req.beginExclusive, "begin_exclusive")
      endAndDescendingOrder <-
        if (req.descendingOrder) {
          (req.endInclusive match {
            case Some(value) => ParticipantOffsetValidator.validatePositive(value, "end_inclusive")
            case None => Left(RequestValidationErrors.DescendingOrderMissingEnd.Error().asGrpcError)
          }).map(offset => (Option(offset), true))
        } else {
          ParticipantOffsetValidator
            .validateOptionalPositive(req.endInclusive, "end_inclusive")
            .map((_, false))
        }
    } yield PartialValidation(
      begin,
      endAndDescendingOrder._1,
      endAndDescendingOrder._2,
    )

  def validate(
      req: GetUpdatesRequest,
      ledgerEnd: Option[Offset],
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Result[update.GetUpdatesRequest] =
    for {
      partial <- commonValidations(req)
      _ <- ParticipantOffsetValidator.offsetIsBeforeEnd(
        "Begin",
        partial.begin,
        ledgerEnd,
      )
      _ <- ParticipantOffsetValidator.offsetIsBeforeEnd(
        "End",
        partial.end,
        ledgerEnd,
      )
      updateFormatProto <- requirePresence(req.updateFormat, "update_format")
      updateFormat <- FormatValidator.validate(updateFormatProto)
    } yield {
      update.GetUpdatesRequest(
        partial.begin,
        partial.end,
        updateFormat,
        partial.descendingOrder,
      )
    }

  def validateUpdateByOffset(
      req: GetUpdateByOffsetRequest
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Result[update.GetUpdateByOffsetRequest] =
    for {
      offset <- ParticipantOffsetValidator.validatePositive(req.offset, "offset")
      updateFormatProto <- requirePresence(req.updateFormat, "update_format")
      updateFormat <- FormatValidator.validate(updateFormatProto)
    } yield {
      update.GetUpdateByOffsetRequest(
        offset = offset,
        updateFormat = updateFormat,
      )
    }

  def validateUpdateById(
      req: GetUpdateByIdRequest
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Result[update.GetUpdateByIdRequest] =
    for {
      _ <- requireNonEmptyString(req.updateId, "update_id")
      updateIdStr <- requireLedgerString(req.updateId)
      updateId <- UpdateId.fromLedgerString(updateIdStr).left.map(e => invalidArgument(e.message))
      updateFormatProto <- requirePresence(req.updateFormat, "update_format")
      updateFormat <- FormatValidator.validate(updateFormatProto)
    } yield {
      update.GetUpdateByIdRequest(
        updateId = updateId,
        updateFormat = updateFormat,
      )
    }

  def validateUpdatesPageRequest(
      req: GetUpdatesPageRequest,
      ledgerEnd: Option[Offset],
      participantId: ParticipantId,
      updateServiceConfig: UpdateServiceConfig,
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Result[update.GetUpdatesPageRequest] = for {
    token <- UpdatesPageToken.validateToken(req, participantId)
    endOffsetInRequest <- req.endOffsetInclusive.traverse(
      ParticipantOffsetValidator.validatePositive(_, "endOffsetInclusive")
    )
    beginExclInRequest <- req.beginOffsetExclusive.traverse(
      ParticipantOffsetValidator
        .validateNonNegative(_, "beginOffsetExclusive")
    )
    maxPageSize <- FieldValidator.validatePageSize(
      updateServiceConfig.maxUpdatesPageSize.value,
      updateServiceConfig.defaultUpdatesPageSize.value,
      req.maxPageSize,
    )
    _ <- ParticipantOffsetValidator.offsetIsBeforeEnd(
      "endOffsetInclusive",
      endOffsetInRequest,
      ledgerEnd,
    )
    _ <- ParticipantOffsetValidator.offsetIsBeforeEnd(
      "beginOffsetInclusive",
      endOffsetInRequest,
      ledgerEnd,
    )
    _ <- (beginExclInRequest, endOffsetInRequest).tupled.traverse { case (begin, end) =>
      Either.cond(
        begin.forall(_ <= end),
        (),
        RequestValidationErrors.InvalidArgument
          .Reject(s"beginOffsetExclusive is after endOffsetInclusive")
          .asGrpcError,
      )
    }
    updateFormatProto <- requirePresence(req.updateFormat, "update_format")
    updateFormat <- FormatValidator.validate(updateFormatProto)
    continueStreamFromIncl <- token.traverse(t =>
      if (req.descendingOrder) {
        t.lowestPageOffsetExclusive.toRight(
          RequestValidationErrors.InvalidUpdatesPageToken
            .Reject("Page token not from descendingOrder=true request")
            .asGrpcError
        )
      } else {
        Either.cond(
          endOffsetInRequest.forall(t.highestPageOffsetInclusive < Some(_)),
          t.highestPageOffsetInclusive.fold(Offset.firstOffset)(_.increment),
          RequestValidationErrors.InvalidUpdatesPageToken
            .Reject("Page token not from descendingOrder=false request")
            .asGrpcError,
        )
      }
    )
  } yield update.GetUpdatesPageRequest(
    startExclusive = beginExclInRequest,
    endInclusive = endOffsetInRequest,
    continueStreamFromIncl = continueStreamFromIncl,
    maxPageSize = maxPageSize,
    updateFormat = updateFormat,
    descendingOrder = req.descendingOrder,
    requestChecksum = token.map(_.requestChecksum).getOrElse(UpdatesPageToken.requestChecksum(req)),
    participantChecksum = token
      .map(_.participantIdChecksum)
      .getOrElse(UpdatesPageToken.participantChecksum(participantId)),
  )

}
