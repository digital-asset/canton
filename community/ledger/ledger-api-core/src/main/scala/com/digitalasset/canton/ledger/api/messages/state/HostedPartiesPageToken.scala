// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.state

import cats.syntax.either.*
import com.daml.ledger.api.v2.state_service.GetHostedPartiesPageRequest
import com.daml.platform.v1.page_tokens.HostedPartiesPageTokenPayload
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.util.PageTokenUtils
import com.digitalasset.canton.ledger.api.validation.{FieldValidator, LedgerApiSizeLimitsConfig}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.ByteString
import io.grpc.StatusRuntimeException

import scala.util.chaining.scalaUtilChainingOps

object HostedPartiesPageToken {
  private val TokenVersion = 1

  @SuppressWarnings(Array("com.digitalasset.canton.ProtobufToByteString"))
  def encode(
      request: GetHostedPartiesPageRequest,
      partyLowerBoundExcl: Ref.Party,
      hostedAtOffset: Offset,
      participantId: Ref.ParticipantId,
  ): ByteString =
    HostedPartiesPageTokenPayload(
      continuationToken = HostedPartiesContinuationToken.encode(
        partyLowerBoundExcl,
        hostedAtOffset,
        PageTokenUtils.emptyChecksum,
      ),
      version = TokenVersion,
      participantIdChecksum = calcParticipantChecksum(participantId),
      requestChecksum = calcRequestChecksum(request),
    ).toByteString

  def calcRequestChecksum(request: GetHostedPartiesPageRequest): ByteString =
    Hash
      .build(HashPurpose.HostedPartiesToken, HashAlgorithm.Sha256)
      .addOptional(request.hostedAtOffset, _.addLong)
      .addOptional(request.filter, HostedPartiesContinuationToken.hashFilter)
      .finish()
      .pipe(PageTokenUtils.toChecksum)

  def calcParticipantChecksum(participantId: Ref.ParticipantId): ByteString =
    PageTokenUtils.calcParticipantChecksum(HashPurpose.HostedPartiesToken, participantId)

  def decodeAndValidate(
      expectedRequestChecksum: ByteString,
      participantIdChecksum: ByteString,
      token: ByteString,
  )(implicit
      errorLoggingContext: ErrorLoggingContext,
      sizeLimitsConfig: LedgerApiSizeLimitsConfig,
  ): Either[StatusRuntimeException, HostedPartiesContinuation] = {
    def invalidPageToken(message: String): StatusRuntimeException =
      RequestValidationErrors.InvalidHostedPartiesPageToken.Reject(message).asGrpcError
    val limit =
      sizeLimitsConfig.publicLapiSizeConfig.getHostedPartiesPageRequest.pageTokenSizeLimit

    for {
      _ <- FieldValidator.validateByteSizeLimit(bytes = token, fieldName = "page_token", limit)
      proto <- FieldValidator
        .validateProtobufEncodedField(
          token,
          HostedPartiesPageTokenPayload,
          fieldName = "page_token",
          errorMessage = "Invalid page token for GetHostedPartiesPageRequest",
        )
        .ensure(
          invalidPageToken("The page token was prepared with a different page API version.")
        )(_.version == TokenVersion)
        .ensure(invalidPageToken("The page token was prepared by a different participant."))(
          _.participantIdChecksum == participantIdChecksum
        )
        .ensure(invalidPageToken("The page token was prepared with a different request."))(
          _.requestChecksum == expectedRequestChecksum
        )
      continuation <- HostedPartiesContinuationToken
        .decodeAndValidate(
          expectedChecksum = PageTokenUtils.emptyChecksum,
          token = proto.continuationToken,
          fieldName = "page_token",
          limit = limit,
        )
        .leftMap(_ => invalidPageToken("Invalid token contents."))
    } yield continuation
  }
}
