// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.ledger.api.v2.state_service.GetActiveContractsRequest
import com.daml.ledger.api.v2.transaction_filter.EventFormat as GrpcEventFormat
import com.daml.platform.v1.acs_continuation.{
  AcsContinuationTokenPayload,
  AcsContinuationTokenPointer,
}
import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.ledger.api.validation.ValidationErrors
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.google.protobuf.ByteString
import io.grpc.StatusRuntimeException

import scala.util.Try

// ADT representation of AcsContinuationTokenPayload defined in acs_continuation.proto
// serialized form of the proto is used in State Service API for continuations
sealed trait AcsContinuationToken extends Product with Serializable {
  def checksum: AcsContinuationToken.Checksum

  @SuppressWarnings(Array("com.digitalasset.canton.ProtobufToByteString"))
  def encode: ByteString = {
    val (sequentialId, incompleteOffset) = this match {
      case AcsContinuationTokenActive(_, sequentialId) => (sequentialId, None)
      case AcsContinuationTokenIncomplete(_, sequentialId, offset) => (sequentialId, Some(offset))
    }
    AcsContinuationTokenPayload(
      pointer = Some(
        AcsContinuationTokenPointer(
          sequentialId = sequentialId,
          offsetForIncompleteReassignments = incompleteOffset,
        )
      ),
      checksum = checksum.bytes,
    ).toByteString
  }
}

final case class AcsContinuationTokenActive(
    checksum: AcsContinuationToken.Checksum,
    sequentialId: Long,
) extends AcsContinuationToken
final case class AcsContinuationTokenIncomplete(
    checksum: AcsContinuationToken.Checksum,
    sequentialId: Long,
    offset: Long,
) extends AcsContinuationToken

object AcsContinuationToken {
  private val TokenVersion = 1

  final case class Checksum(bytes: ByteString)

  @SuppressWarnings(Array("com.digitalasset.canton.ProtobufToByteString"))
  def calcChecksum(
      request: GetActiveContractsRequest,
      participantId: LedgerParticipantId,
  ): Checksum = {
    val builder = Hash.build(HashPurpose.AcsContinuationToken, HashAlgorithm.Sha256)
    builder.addLong(request.activeAtOffset)
    request.eventFormat match {
      case Some(GrpcEventFormat(filtersByParty, filtersForAnyParty, verbose)) =>
        // since EventFormat contains a Map, we need to be careful with calculating the hash
        builder.addMap(filtersByParty)(builder.addString)(v =>
          builder.addByteString(v.toByteString)
        )
        filtersForAnyParty.foreach(f => builder.addByteString(f.toByteString))
        builder.addBool(verbose)
      case None => ()
    }
    builder.addInt(TokenVersion)
    builder.addString(participantId)
    val hash = builder.finish()
    Checksum(hash.unwrap.substring(0, 4))
  }

  def decodeAndValidate(expectedChecksum: Checksum, token: Option[ByteString])(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, Option[AcsContinuationToken]] =
    token.traverse { token =>
      Try(AcsContinuationTokenPayload.parseFrom(token.toByteArray)).toEither.left
        .map(_ =>
          ValidationErrors.invalidField(
            fieldName = "stream_continuation_token",
            message = "Invalid continuation token for GetActiveContractsRequest",
          )
        )
        .ensure(ValidationErrors.invalidToken)(proto => proto.checksum == expectedChecksum.bytes)
        .flatMap(proto =>
          proto.pointer match {
            case Some(ptr) =>
              ptr.offsetForIncompleteReassignments match {
                case None => Right(AcsContinuationTokenActive(expectedChecksum, ptr.sequentialId))
                case Some(offset) =>
                  Right(AcsContinuationTokenIncomplete(expectedChecksum, ptr.sequentialId, offset))
              }
            case None => Left(ValidationErrors.invalidToken)
          }
        )
    }
}
