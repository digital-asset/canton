// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import cats.syntax.either.*
import com.daml.ledger.api.v2.state_service.GetActiveContractsRequest
import com.daml.platform.v1.acs_continuation.{
  AcsContinuationPointerPayload,
  AcsContinuationTokenPayload,
}
import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.ledger.api.util.UpdateFormatHashUtils
import com.digitalasset.canton.ledger.api.validation.ValidationErrors
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.google.protobuf.ByteString
import io.grpc.StatusRuntimeException

import scala.util.Try
import scala.util.chaining.scalaUtilChainingOps

final case class AcsRangeInfo(
    continuationPointer: Option[AcsContinuationPointer],
    requestChecksum: AcsContinuationToken.Checksum,
    limit: Option[Long],
)

object AcsRangeInfo {
  def empty: AcsRangeInfo = AcsRangeInfo(None, AcsContinuationToken.emptyChecksum, None)
}

/** ADT representation of AcsContinuationTokenPayload defined in acs_continuation.proto serialized
  * form of the proto is used in State Service API for continuations
  */
sealed trait AcsContinuationPointer extends Product with Serializable {
  def toPayload: AcsContinuationPointerPayload
  def decrease: AcsContinuationPointer
}

final case class AcsContinuationPointerActiveContracts(
    sequentialId: Long
) extends AcsContinuationPointer {
  def toPayload: AcsContinuationPointerPayload = AcsContinuationPointerPayload(sequentialId, None)
  def decrease: AcsContinuationPointerActiveContracts =
    copy(sequentialId = sequentialId - 1)
}
final case class AcsContinuationPointerIncompleteReassignments(
    sequentialId: Long,
    offset: Long,
) extends AcsContinuationPointer {
  def toPayload: AcsContinuationPointerPayload =
    AcsContinuationPointerPayload(sequentialId, Some(offset))
  def decrease: AcsContinuationPointerIncompleteReassignments =
    copy(sequentialId = sequentialId - 1)
}

object AcsContinuationToken {
  private val TokenVersion = 1

  final case class Checksum(bytes: ByteString)

  def emptyChecksum: Checksum = Checksum(ByteString.copyFrom(Array.fill(4)(0.toByte)))

  def calcChecksum(
      request: GetActiveContractsRequest,
      participantId: LedgerParticipantId,
  ): Checksum =
    Hash
      .build(HashPurpose.AcsContinuationToken, HashAlgorithm.Sha256)
      .addLong(request.activeAtOffset)
      .addOptional(request.eventFormat, UpdateFormatHashUtils.hashEventFormat)
      .addInt(TokenVersion)
      .addString(participantId)
      .finish()
      .pipe(UpdateFormatHashUtils.toChecksum)
      .pipe(Checksum(_))

  def decodeAndValidate(expectedChecksum: Checksum, token: ByteString)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, AcsContinuationPointer] =
    Try(AcsContinuationTokenPayload.parseFrom(token.toByteArray)).toEither.left
      .map(_ =>
        ValidationErrors.invalidField(
          fieldName = "stream_continuation_token",
          message = "Invalid continuation token for GetActiveContractsRequest",
        )
      )
      .ensure(ValidationErrors.invalidContinuationToken)(proto =>
        proto.checksum == expectedChecksum.bytes
      )
      .flatMap(proto =>
        proto.pointer match {
          case Some(ptr) => Right(decodePointerPayload(ptr))
          case None => Left(ValidationErrors.invalidContinuationToken)
        }
      )

  private def decodePointerPayload(payload: AcsContinuationPointerPayload) =
    payload.offsetForIncompleteReassignments match {
      case None => AcsContinuationPointerActiveContracts(payload.sequentialId)
      case Some(offset) =>
        AcsContinuationPointerIncompleteReassignments(payload.sequentialId, offset)
    }

  @SuppressWarnings(Array("com.digitalasset.canton.ProtobufToByteString"))
  def activeContracts(sequentialId: Long, checksum: Checksum): ByteString =
    AcsContinuationTokenPayload(
      pointer = Some(AcsContinuationPointerActiveContracts(sequentialId).toPayload),
      checksum = checksum.bytes,
    ).toByteString

  @SuppressWarnings(Array("com.digitalasset.canton.ProtobufToByteString"))
  def incompleteReassignments(sequentialId: Long, offset: Long, checksum: Checksum): ByteString =
    AcsContinuationTokenPayload(
      pointer = Some(AcsContinuationPointerIncompleteReassignments(sequentialId, offset).toPayload),
      checksum = checksum.bytes,
    ).toByteString
}
