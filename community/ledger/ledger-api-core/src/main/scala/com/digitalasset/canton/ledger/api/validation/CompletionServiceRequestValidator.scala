// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.command_completion_service.{
  CompletionStreamRequest as GrpcCompletionStreamRequest,
  GetCompletionsRequest,
}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.messages.command.completion.CompletionStreamRequest
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.invalidArgument
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.ByteString
import io.grpc.StatusRuntimeException

object CompletionServiceRequestValidator {

  import FieldValidator.*

  def validateCompletionByHash(
      hash: ByteString
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, ByteString] =
    Either.cond(
      !hash.isEmpty,
      hash,
      invalidArgument("Missing field: transaction_hash"),
    )

  def validateGrpcCompletionStreamRequest(
      request: GrpcCompletionStreamRequest
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, CompletionStreamRequest] =
    for {
      userId <- requireUserId(request.userId, "user_id")
      parties <- requireParties(request.parties.toSet)
      offsetO <- ParticipantOffsetValidator.validateNonNegative(
        request.beginExclusive,
        "begin_exclusive",
      )
    } yield CompletionStreamRequest(
      userId,
      parties,
      offsetO,
    )

  def validateCompletionStreamRequest(
      request: CompletionStreamRequest,
      ledgerEnd: Option[Offset],
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, CompletionStreamRequest] =
    for {
      _ <- ParticipantOffsetValidator.offsetIsBeforeEnd(
        "Begin",
        request.offset,
        ledgerEnd,
      )
      _ <- requireNonEmpty(request.parties, "parties")
    } yield request

  final case class GetCompletionsStreamRequest(
      parties: Set[Ref.Party],
      offset: Option[Offset],
  )

  def validateGetCompletionsRequest(
      request: GetCompletionsRequest,
      ledgerEnd: Option[Offset],
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, GetCompletionsStreamRequest] =
    for {
      parties <- requireParties(request.parties.toSet)
      offsetO <- ParticipantOffsetValidator.validateNonNegative(
        request.beginExclusive,
        "begin_exclusive",
      )
      _ <- ParticipantOffsetValidator.offsetIsBeforeEnd("Begin", offsetO, ledgerEnd)
    } yield GetCompletionsStreamRequest(parties, offsetO)

}
