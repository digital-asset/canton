// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import cats.syntax.either.*
import com.daml.ledger.api.v2.state_service.GetActiveContractsPageRequest
import com.daml.platform.v1.acs_page_token.AcsPageTokenPayload
import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.util.UpdateFormatHashUtils
import com.digitalasset.canton.ledger.api.validation.ValidationErrors
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.google.protobuf.ByteString
import io.grpc.StatusRuntimeException

import scala.util.Try
import scala.util.chaining.scalaUtilChainingOps

object AcsPageToken {
  private val TokenVersion = 1

  @SuppressWarnings(Array("com.digitalasset.canton.ProtobufToByteString"))
  def encode(
      request: GetActiveContractsPageRequest,
      continuationToken: ByteString,
      activeAtOffset: Long,
      participantId: LedgerParticipantId,
  ): ByteString =
    AcsPageTokenPayload(
      continuationToken = continuationToken,
      activeAtOffset = activeAtOffset,
      version = TokenVersion,
      participantIdChecksum = calcParticipantChecksum(participantId),
      requestChecksum = calcRequestChecksum(request),
    ).toByteString

  def calcRequestChecksum(request: GetActiveContractsPageRequest): ByteString =
    Hash
      .build(HashPurpose.AcsContinuationToken, HashAlgorithm.Sha256)
      .addOptional(request.activeAtOffset, _.addLong)
      .addOptional(request.eventFormat, UpdateFormatHashUtils.hashEventFormat)
      .finish()
      .pipe(UpdateFormatHashUtils.toChecksum)

  def calcParticipantChecksum(participantId: LedgerParticipantId): ByteString =
    Hash
      .build(HashPurpose.AcsContinuationToken, HashAlgorithm.Sha256)
      .addString(participantId)
      .finish()
      .pipe(UpdateFormatHashUtils.toChecksum)

  @SuppressWarnings(Array("com.digitalasset.canton.ProtobufToByteString"))
  def decodeAndValidate(
      expectedRequestChecksum: ByteString,
      participantIdChecksum: ByteString,
      token: ByteString,
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, (Offset, AcsContinuationPointer)] = for {
    proto <- Try(AcsPageTokenPayload.parseFrom(token.toByteArray)).toEither.left
      .map(_ =>
        ValidationErrors.invalidField(
          fieldName = "page_token",
          message = "Invalid page token for GetActiveContractsPageRequest",
        )
      )
      .ensure(
        ValidationErrors.invalidAcsPageToken(
          "The page token was prepared by a different participant."
        )
      )(proto => proto.participantIdChecksum == participantIdChecksum)
      .ensure(
        ValidationErrors.invalidAcsPageToken(
          "The page token was prepared with different page API version."
        )
      )(proto => proto.version == TokenVersion)
      .ensure(
        ValidationErrors.invalidAcsPageToken(
          "The page token was prepared with different event_format or active_at_offset."
        )
      )(proto => proto.requestChecksum == expectedRequestChecksum)
    offset <- Offset
      .fromLong(proto.activeAtOffset)
      .left
      .map(_ => ValidationErrors.invalidAcsPageToken("Invalid token contents."))
    pointerToTheFirstElem <- AcsContinuationToken
      .decodeAndValidate(AcsContinuationToken.emptyChecksum, proto.continuationToken)
  } yield (offset, pointerToTheFirstElem.decrease)
}
