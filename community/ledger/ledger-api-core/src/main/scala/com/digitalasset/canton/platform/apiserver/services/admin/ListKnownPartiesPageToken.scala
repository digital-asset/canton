// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import cats.syntax.either.*
import com.daml.platform.v1.page_tokens.ListPartiesPageTokenPayload
import com.digitalasset.canton.crypto.HashPurpose
import com.digitalasset.canton.ledger.api.util.PageTokenUtils
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.ByteString
import io.grpc.StatusRuntimeException

import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.util.Try

/** Decoded page token of `PartyManagementService.ListKnownParties`.
  *
  * @param partyIdLowerBoundExcl
  *   The next page starts with parties strictly larger than this party.
  * @param validAtEventSeqId
  *   The topology event sequence ID at which the first page was computed. Subsequent pages are
  *   computed at the same ID, so that the `is_local` property of the parties is stable across
  *   pages.
  */
final case class ListKnownPartiesPageToken(
    partyIdLowerBoundExcl: Ref.Party,
    validAtEventSeqId: Long,
)

object ListKnownPartiesPageToken {

  /** Token version used to detect format mismatch. Must be bumped every time the protobuf token
    * structure is changed. Tokens without a version (issued before a snapshot was pinned) are
    * rejected.
    */
  val Version: Int = 1

  def participantChecksum(participantId: Ref.ParticipantId): ByteString =
    PageTokenUtils.calcParticipantChecksum(HashPurpose.ListPartiesPageToken, participantId)

  def encode(
      lastParty: Ref.Party,
      validAtEventSeqId: Long,
      participantId: Ref.ParticipantId,
  ): String = {
    val payload = ListPartiesPageTokenPayload(
      partyIdLowerBoundExcl = lastParty,
      validAtEventSeqId = validAtEventSeqId,
      version = Version,
      participantIdChecksum = participantChecksum(participantId),
    )
    new String(Base64.getUrlEncoder.encode(payload.toByteArray), StandardCharsets.UTF_8)
  }

  /** Decodes the page token.
    *
    * @return
    *   None if the token is empty (first page), otherwise the decoded payload. Fails if the token
    *   is not a base64-encoded protobuf payload.
    */
  def decode(pageToken: String)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, Option[ListPartiesPageTokenPayload]] =
    if (pageToken.isEmpty) Right(None)
    else
      for {
        decodedBytes <- Try(
          Base64.getUrlDecoder.decode(pageToken.getBytes(StandardCharsets.UTF_8))
        ).toEither
          .leftMap(_ => invalidPageToken("failed base64 decoding"))
        payload <- Try(ListPartiesPageTokenPayload.parseFrom(decodedBytes)).toEither
          .leftMap(_ => invalidPageToken("failed proto decoding"))
      } yield Some(payload)

  /** Validates a decoded page token payload.
    *
    * Fails if the payload was issued by an incompatible version or by a different participant, or
    * if it contains an invalid party or event sequence ID.
    */
  def validate(payload: ListPartiesPageTokenPayload, participantId: Ref.ParticipantId)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, ListKnownPartiesPageToken] =
    for {
      _ <- Either.cond(
        payload.version == Version,
        (),
        invalidPageToken(
          s"the page token was issued by an incompatible Canton version (token version ${payload.version})"
        ),
      )
      _ <- Either.cond(
        payload.participantIdChecksum == participantChecksum(participantId),
        (),
        invalidPageToken("the page token was issued by a different participant"),
      )
      party <- Ref.Party
        .fromString(payload.partyIdLowerBoundExcl)
        .leftMap(_ => invalidPageToken("invalid party in the token"))
      _ <- Either.cond(
        payload.validAtEventSeqId >= 0L,
        (),
        invalidPageToken("invalid event sequence ID in the token"),
      )
    } yield ListKnownPartiesPageToken(party, payload.validAtEventSeqId)

  private def invalidPageToken(reason: String)(implicit
      errorLogger: ErrorLoggingContext
  ): StatusRuntimeException =
    RequestValidationErrors.InvalidArgument.Reject(s"Invalid page token: $reason").asGrpcError
}
