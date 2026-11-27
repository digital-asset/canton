// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.state

import com.daml.ledger.api.v2.state_service.{GetHostedPartiesRequest, HostedPartiesFilter}
import com.daml.platform.v1.page_tokens.HostedPartiesContinuationTokenPayload
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.util.PageTokenUtils.Checksum
import com.digitalasset.canton.ledger.api.validation.{LedgerApiSizeLimitsConfig, SizeLimitTestUtils}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class HostedPartiesContinuationTokenTest extends AnyWordSpec with SizeLimitTestUtils {
  private val filter = HostedPartiesFilter(
    partyFilter = Seq("alice", "bob"),
    allParticipants = false,
    synchronizerIdFilter = Seq("x::synchronizer1"),
  )
  private val hostedAtOffset = Offset.tryFromLong(125)
  private val originalRequest =
    GetHostedPartiesRequest(Some(hostedAtOffset.unwrap), Some(filter), None)
  private val participantId = Ref.ParticipantId.assertFromString("Ledger")
  private val originalRequestChecksum =
    HostedPartiesContinuationToken.calcChecksum(originalRequest, participantId)
  private val partyLowerBoundExcl = Ref.Party.assertFromString("alice")
  private val originalContinuation = HostedPartiesContinuation(hostedAtOffset, partyLowerBoundExcl)
  private val encodedToken =
    HostedPartiesContinuationToken.encode(
      partyLowerBoundExcl,
      hostedAtOffset,
      originalRequestChecksum,
    )

  private implicit val elc: ErrorLoggingContext =
    ErrorLoggingContext.fromTracedLogger(NamedLogging.noopLogger)(TraceContext.empty)

  private val errorString =
    "INVALID_HOSTED_PARTIES_CONTINUATION_TOKEN(8,0): The submitted request contains an invalid continuation token. " +
      "Tokens used in GetHostedParties requests must be taken from a valid GetHostedPartiesResponse and used with " +
      "the same hosted_at_offset and filter, with the same Canton participant running the same Canton version."

  private def decodeAndValidate(
      checksum: Checksum,
      token: ByteString,
      limit: PositiveInt =
        LedgerApiSizeLimitsConfig().publicLapiSizeConfig.getHostedPartiesRequest.streamContinuationTokenSizeLimit,
  ) =
    HostedPartiesContinuationToken.decodeAndValidate(
      expectedChecksum = checksum,
      token = token,
      fieldName = "stream_continuation_token",
      limit = limit,
    )

  "HostedPartiesContinuationToken" should {
    "successfully decode an untampered token from the request" in {
      val decodedToken =
        decodeAndValidate(originalRequestChecksum, encodedToken)
      decodedToken.value should equal(originalContinuation)
    }

    byteSizeLimit("stream_continuation_token", limit = encodedToken.size())(numberOfBytes =>
      decodeAndValidate(
        originalRequestChecksum,
        encodedToken.concat(bytes(numberOfBytes - encodedToken.size())),
        PositiveInt.tryCreate(encodedToken.size()),
      )
    )

    "fail if used on a different participant" in {
      val differentParticipantId = Ref.ParticipantId.assertFromString("different")
      val checksum =
        HostedPartiesContinuationToken.calcChecksum(originalRequest, differentParticipantId)
      val decodedToken =
        decodeAndValidate(checksum, encodedToken)
      decodedToken.left.value.getStatus.getDescription should equal(
        errorString
      )
    }

    "fail if not the same hostedAt is used" in {
      val requestWithToken = originalRequest.copy(
        hostedAtOffset = originalRequest.hostedAtOffset.map(_ + 1),
        streamContinuationToken = Some(encodedToken),
      )
      val checksum = HostedPartiesContinuationToken.calcChecksum(requestWithToken, participantId)
      val decodedToken =
        decodeAndValidate(checksum, encodedToken)
      decodedToken.left.value.getStatus.getDescription should equal(
        errorString
      )
    }

    "fail if hostedAt is omitted after being used" in {
      val requestWithToken = originalRequest.copy(
        hostedAtOffset = None,
        streamContinuationToken = Some(encodedToken),
      )
      val checksum = HostedPartiesContinuationToken.calcChecksum(requestWithToken, participantId)
      val decodedToken =
        decodeAndValidate(checksum, encodedToken)
      decodedToken.left.value.getStatus.getDescription should equal(
        errorString
      )
    }

    "fail if hostedAt is used after being omitted" in {
      val requestWithoutHostedAt = originalRequest.copy(hostedAtOffset = None)
      val newToken = HostedPartiesContinuationToken.encode(
        partyLowerBoundExcl,
        hostedAtOffset,
        HostedPartiesContinuationToken.calcChecksum(requestWithoutHostedAt, participantId),
      )
      val requestWithToken = originalRequest.copy(streamContinuationToken = Some(newToken))
      val checksum = HostedPartiesContinuationToken.calcChecksum(requestWithToken, participantId)
      val decodedToken =
        decodeAndValidate(checksum, newToken)
      decodedToken.left.value.getStatus.getDescription should equal(
        errorString
      )
    }

    "fail if not the same partyFilter is used" in {
      val requestWithToken = originalRequest.copy(
        filter = Some(filter.copy(partyFilter = filter.partyFilter :+ "charlie")),
        streamContinuationToken = Some(encodedToken),
      )
      val checksum = HostedPartiesContinuationToken.calcChecksum(requestWithToken, participantId)
      val decodedToken =
        decodeAndValidate(checksum, encodedToken)
      decodedToken.left.value.getStatus.getDescription should equal(
        errorString
      )
    }

    "fail if different allParticipants flag is used" in {
      val requestWithToken = originalRequest.copy(
        filter = Some(filter.copy(allParticipants = !filter.allParticipants)),
        streamContinuationToken = Some(encodedToken),
      )
      val checksum = HostedPartiesContinuationToken.calcChecksum(requestWithToken, participantId)
      val decodedToken =
        decodeAndValidate(checksum, encodedToken)
      decodedToken.left.value.getStatus.getDescription should equal(
        errorString
      )
    }

    "fail if not the same synchronizerIdFilter is used" in {
      val requestWithToken = originalRequest.copy(
        filter = Some(filter.copy(synchronizerIdFilter = Seq("x::synchronizer2"))),
        streamContinuationToken = Some(encodedToken),
      )
      val checksum = HostedPartiesContinuationToken.calcChecksum(requestWithToken, participantId)
      val decodedToken =
        decodeAndValidate(checksum, encodedToken)
      decodedToken.left.value.getStatus.getDescription should equal(
        errorString
      )
    }

    "fail if filter values are moved between filter fields" in {
      val requestWithPartyFilter = originalRequest.copy(
        filter =
          Some(filter.copy(partyFilter = Seq("x::synchronizer1"), synchronizerIdFilter = Nil))
      )
      val newToken = HostedPartiesContinuationToken.encode(
        partyLowerBoundExcl,
        hostedAtOffset,
        HostedPartiesContinuationToken.calcChecksum(requestWithPartyFilter, participantId),
      )
      val requestWithToken = originalRequest.copy(
        filter =
          Some(filter.copy(partyFilter = Nil, synchronizerIdFilter = Seq("x::synchronizer1"))),
        streamContinuationToken = Some(newToken),
      )
      val checksum = HostedPartiesContinuationToken.calcChecksum(requestWithToken, participantId)
      val decodedToken =
        decodeAndValidate(checksum, newToken)
      decodedToken.left.value.getStatus.getDescription should equal(
        errorString
      )
    }

    "reject an invalid token" in {
      val tamperedToken = encodedToken.substring(5)
      val checksum = HostedPartiesContinuationToken.calcChecksum(originalRequest, participantId)
      val decodedToken =
        decodeAndValidate(checksum, tamperedToken)
      decodedToken.left.value.getStatus.getDescription should equal(
        "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field stream_continuation_token: " +
          "Invalid continuation token for GetHostedPartiesRequest"
      )
    }

    "reject a token with an invalid party" in {
      val tokenWithInvalidParty = HostedPartiesContinuationTokenPayload(
        partyLowerBoundExcl = "invalid party!",
        checksum = originalRequestChecksum.bytes,
        hostedAtOffset = hostedAtOffset.unwrap,
      ).toByteString
      val decodedToken =
        decodeAndValidate(originalRequestChecksum, tokenWithInvalidParty)
      decodedToken.left.value.getStatus.getDescription should equal(
        errorString
      )
    }

    "reject a token with an invalid hostedAt" in {
      val tokenWithInvalidHostedAt = HostedPartiesContinuationTokenPayload(
        partyLowerBoundExcl = partyLowerBoundExcl,
        checksum = originalRequestChecksum.bytes,
        hostedAtOffset = 0,
      ).toByteString
      val decodedToken =
        decodeAndValidate(originalRequestChecksum, tokenWithInvalidHostedAt)
      decodedToken.left.value.getStatus.getDescription should equal(
        errorString
      )
    }

    "calculate the checksum correctly for unordered and duplicate filter values" in {
      val requestWithToken = originalRequest.copy(
        filter = Some(filter.copy(partyFilter = Seq("bob", "alice", "bob"))),
        streamContinuationToken = Some(encodedToken),
      )
      val checksum = HostedPartiesContinuationToken.calcChecksum(requestWithToken, participantId)
      val decodedToken =
        decodeAndValidate(checksum, encodedToken)
      decodedToken.value should equal(originalContinuation)
    }

    "calculate the same checksum all the time" in {
      originalRequestChecksum should equal(
        Checksum(ByteString.copyFrom(Array[Byte](23, -57, 116, -57)))
      )
    }
  }
}
