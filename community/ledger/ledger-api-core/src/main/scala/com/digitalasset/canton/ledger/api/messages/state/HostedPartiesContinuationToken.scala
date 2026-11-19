// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.state

import cats.syntax.either.*
import com.daml.ledger.api.v2.state_service.{GetHostedPartiesRequest, HostedPartiesFilter}
import com.daml.platform.v1.page_tokens.HostedPartiesContinuationTokenPayload
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashBuilder, HashPurpose}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.util.PageTokenUtils
import com.digitalasset.canton.ledger.api.util.PageTokenUtils.Checksum
import com.digitalasset.canton.ledger.api.validation.{FieldValidator, ValidationErrors}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.ByteString
import io.grpc.StatusRuntimeException

import scala.util.chaining.scalaUtilChainingOps

final case class HostedPartiesContinuation(hostedAt: Offset, partyLowerBoundExcl: Ref.Party)

/** Serialized HostedPartiesContinuationTokenPayload, defined in page_tokens.proto. The State
  * Service uses it to resume a hosted parties stream after the token's party.
  */
object HostedPartiesContinuationToken {
  private val TokenVersion = 1

  def calcChecksum(
      request: GetHostedPartiesRequest,
      participantId: Ref.ParticipantId,
  ): Checksum =
    Hash
      .build(HashPurpose.HostedPartiesToken, HashAlgorithm.Sha256)
      .addOptional(request.hostedAtOffset, _.addLong)
      .addOptional(request.filter, hashFilter)
      .addInt(TokenVersion)
      .addString(participantId)
      .finish()
      .pipe(PageTokenUtils.toChecksum)
      .pipe(Checksum(_))

  /** Repeated filters are hashed as sets, so their order and duplicates do not matter. */
  private[state] def hashFilter[T <: HashBuilder](builder: T)(filter: HostedPartiesFilter): T =
    builder
      .addStringSet(filter.partyFilter.toSet)
      .addBool(filter.allParticipants)
      .addStringSet(filter.synchronizerIdFilter.toSet)

  def decodeAndValidate(
      expectedChecksum: Checksum,
      token: ByteString,
      fieldName: String,
      limit: PositiveInt,
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, HostedPartiesContinuation] =
    FieldValidator
      .validateByteSizeLimit(bytes = token, fieldName = fieldName, limit = limit)
      .flatMap(_ =>
        FieldValidator.validateProtobufEncodedField(
          token,
          HostedPartiesContinuationTokenPayload,
          fieldName = fieldName,
          errorMessage = "Invalid continuation token for GetHostedPartiesRequest",
        )
      )
      .ensure(ValidationErrors.invalidHostedPartiesContinuationToken)(proto =>
        proto.checksum == expectedChecksum.bytes
      )
      .flatMap(proto =>
        for {
          hostedAt <- Offset
            .fromLong(proto.hostedAtOffset)
            .leftMap(_ => ValidationErrors.invalidHostedPartiesContinuationToken)
          partyLowerBoundExcl <- Ref.Party
            .fromString(proto.partyLowerBoundExcl)
            .leftMap(_ => ValidationErrors.invalidHostedPartiesContinuationToken)
        } yield HostedPartiesContinuation(hostedAt, partyLowerBoundExcl)
      )

  @SuppressWarnings(Array("com.digitalasset.canton.ProtobufToByteString"))
  def encode(
      partyLowerBoundExcl: Ref.Party,
      hostedAtOffset: Offset,
      checksum: Checksum,
  ): ByteString =
    HostedPartiesContinuationTokenPayload(
      partyLowerBoundExcl = partyLowerBoundExcl,
      checksum = checksum.bytes,
      hostedAtOffset = hostedAtOffset.unwrap,
    ).toByteString
}
