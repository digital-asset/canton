// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.state

import com.daml.ledger.api.v2.state_service.{
  GetHostedPartiesPageRequest,
  GetHostedPartiesRequest,
  HostedPartiesFilter,
}
import com.daml.platform.v1.page_tokens.{
  HostedPartiesContinuationTokenPayload,
  HostedPartiesPageTokenPayload,
}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.messages.state.HostedPartiesPageToken.{
  calcParticipantChecksum,
  calcRequestChecksum,
}
import com.digitalasset.canton.ledger.api.sizeconfig.v2.GetHostedPartiesPageRequestSizeConfig
import com.digitalasset.canton.ledger.api.util.PageTokenUtils
import com.digitalasset.canton.ledger.api.validation.SizeLimitTestUtils
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class HostedPartiesPageTokenTest extends AnyWordSpec with SizeLimitTestUtils {
  private val filter = HostedPartiesFilter(
    partyFilter = Seq("alice", "bob"),
    allParticipants = false,
    synchronizerIdFilter = Seq("x::synchronizer1"),
  )
  private val originalHostedAt = Offset.tryFromLong(100)
  private val originalRequest =
    GetHostedPartiesPageRequest(None, Some(filter), None, None)
  private val participantId = Ref.ParticipantId.assertFromString("Ledger")
  private val partyLowerBoundExcl = Ref.Party.assertFromString("alice")
  private val originalContinuationToken =
    HostedPartiesContinuationToken.encode(
      partyLowerBoundExcl,
      originalHostedAt,
      PageTokenUtils.emptyChecksum,
    )
  private val originalToken = HostedPartiesPageToken.encode(
    originalRequest,
    partyLowerBoundExcl,
    hostedAtOffset = originalHostedAt,
    participantId,
  )
  private val participantIdChecksum = HostedPartiesPageToken.calcParticipantChecksum(participantId)
  private val originalRequestChecksum = HostedPartiesPageToken.calcRequestChecksum(originalRequest)

  private val invalidPageTokenErrorPrefix =
    "INVALID_HOSTED_PARTIES_PAGE_TOKEN(8,0): The submitted request contains an invalid page token. Tokens used in " +
      "GetHostedPartiesPage requests must be taken from a valid GetHostedPartiesPageResponse and used with the same " +
      "hosted_at_offset and filter, with the same Canton participant running the same Canton version. "
  private val differentParticipantError =
    invalidPageTokenErrorPrefix + "The page token was prepared by a different participant."
  private val differentRequestError =
    invalidPageTokenErrorPrefix + "The page token was prepared with a different request."
  private val differentVersionError =
    invalidPageTokenErrorPrefix + "The page token was prepared with a different page API version."
  private val invalidContentsError = invalidPageTokenErrorPrefix + "Invalid token contents."

  private implicit val elc: ErrorLoggingContext =
    ErrorLoggingContext.fromTracedLogger(NamedLogging.noopLogger)(TraceContext.empty)
  private def sizeLimitsConfigWithTokenLimit(maxTokenBytes: Int) =
    sizeLimitsConfig.copy(
      publicLapiSizeConfig = sizeLimitsConfig.publicLapiSizeConfig.copy(
        getHostedPartiesPageRequest = GetHostedPartiesPageRequestSizeConfig(
          pageTokenSizeLimit = PositiveInt.tryCreate(maxTokenBytes)
        )
      )
    )

  "HostedPartiesPageToken" should {
    "successfully decode an untampered token from the request" in {
      val decoded =
        HostedPartiesPageToken.decodeAndValidate(
          originalRequestChecksum,
          participantIdChecksum,
          originalToken,
        )
      decoded.value should equal(HostedPartiesContinuation(originalHostedAt, partyLowerBoundExcl))
    }

    byteSizeLimit("page_token", limit = originalToken.size())(numberOfBytes =>
      HostedPartiesPageToken.decodeAndValidate(
        originalRequestChecksum,
        participantIdChecksum,
        originalToken.concat(bytes(numberOfBytes - originalToken.size())),
      )(elc, sizeLimitsConfigWithTokenLimit(originalToken.size()))
    )

    "fail if used on a different participant" in {
      val differentParticipantId = Ref.ParticipantId.assertFromString("different")
      val differentParticipantChecksum =
        HostedPartiesPageToken.calcParticipantChecksum(differentParticipantId)
      val decodedToken =
        HostedPartiesPageToken.decodeAndValidate(
          originalRequestChecksum,
          differentParticipantChecksum,
          originalToken,
        )
      decodedToken.left.value.getStatus.getDescription should equal(differentParticipantError)
    }

    "fail if not the same partyFilter is used" in {
      val requestWithToken = originalRequest.copy(
        filter = Some(filter.copy(partyFilter = filter.partyFilter :+ "charlie")),
        pageToken = Some(originalToken),
      )
      val checksum = HostedPartiesPageToken.calcRequestChecksum(requestWithToken)
      val decodedToken =
        HostedPartiesPageToken.decodeAndValidate(checksum, participantIdChecksum, originalToken)
      decodedToken.left.value.getStatus.getDescription should equal(differentRequestError)
    }

    "fail if different allParticipants flag is used" in {
      val requestWithToken = originalRequest.copy(
        filter = Some(filter.copy(allParticipants = !filter.allParticipants)),
        pageToken = Some(originalToken),
      )
      val checksum = HostedPartiesPageToken.calcRequestChecksum(requestWithToken)
      val decodedToken =
        HostedPartiesPageToken.decodeAndValidate(checksum, participantIdChecksum, originalToken)
      decodedToken.left.value.getStatus.getDescription should equal(differentRequestError)
    }

    "fail if not the same synchronizerIdFilter is used" in {
      val requestWithToken = originalRequest.copy(
        filter = Some(filter.copy(synchronizerIdFilter = Seq("x::synchronizer2"))),
        pageToken = Some(originalToken),
      )
      val checksum = HostedPartiesPageToken.calcRequestChecksum(requestWithToken)
      val decodedToken =
        HostedPartiesPageToken.decodeAndValidate(checksum, participantIdChecksum, originalToken)
      decodedToken.left.value.getStatus.getDescription should equal(differentRequestError)
    }

    "fail if hostedAt is used after being omitted" in {
      val requestWithToken = originalRequest.copy(
        hostedAtOffset = Some(originalHostedAt.unwrap),
        pageToken = Some(originalToken),
      )
      val checksum = HostedPartiesPageToken.calcRequestChecksum(requestWithToken)
      val decodedToken =
        HostedPartiesPageToken.decodeAndValidate(checksum, participantIdChecksum, originalToken)
      decodedToken.left.value.getStatus.getDescription should equal(differentRequestError)
    }

    "fail if not the same hostedAt is used" in {
      val requestWithHostedAt = originalRequest.copy(hostedAtOffset = Some(originalHostedAt.unwrap))
      val newToken = HostedPartiesPageToken.encode(
        requestWithHostedAt,
        partyLowerBoundExcl,
        hostedAtOffset = originalHostedAt,
        participantId,
      )
      val requestWithToken = requestWithHostedAt.copy(
        hostedAtOffset = requestWithHostedAt.hostedAtOffset.map(_ + 1),
        pageToken = Some(newToken),
      )
      val checksum = HostedPartiesPageToken.calcRequestChecksum(requestWithToken)
      val decodedToken =
        HostedPartiesPageToken.decodeAndValidate(checksum, participantIdChecksum, newToken)
      decodedToken.left.value.getStatus.getDescription should equal(differentRequestError)
    }

    "reject an invalid token" in {
      val tamperedToken = originalToken.substring(5)
      val decodedToken =
        HostedPartiesPageToken.decodeAndValidate(
          originalRequestChecksum,
          participantIdChecksum,
          tamperedToken,
        )
      decodedToken.left.value.getStatus.getDescription should equal(
        "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field page_token: " +
          "Invalid page token for GetHostedPartiesPageRequest"
      )
    }

    "reject a token with different version" in {
      val tokenWithDifferentVersion = HostedPartiesPageTokenPayload(
        continuationToken = originalContinuationToken,
        version = 999,
        participantIdChecksum = calcParticipantChecksum(participantId),
        requestChecksum = calcRequestChecksum(originalRequest),
      ).toByteString
      val decodedToken =
        HostedPartiesPageToken.decodeAndValidate(
          originalRequestChecksum,
          participantIdChecksum,
          tokenWithDifferentVersion,
        )
      decodedToken.left.value.getStatus.getDescription should equal(differentVersionError)
    }

    "reject a token with an invalid hostedAt" in {
      val continuationTokenWithInvalidHostedAt = HostedPartiesContinuationTokenPayload
        .parseFrom(originalContinuationToken.toByteArray)
        .copy(hostedAtOffset = 0)
        .toByteString
      val tokenWithInvalidHostedAt = HostedPartiesPageTokenPayload(
        continuationToken = continuationTokenWithInvalidHostedAt,
        version = 1,
        participantIdChecksum = calcParticipantChecksum(participantId),
        requestChecksum = calcRequestChecksum(originalRequest),
      ).toByteString
      val decodedToken =
        HostedPartiesPageToken.decodeAndValidate(
          originalRequestChecksum,
          participantIdChecksum,
          tokenWithInvalidHostedAt,
        )
      decodedToken.left.value.getStatus.getDescription should equal(invalidContentsError)
    }

    "reject a token with an invalid continuation token" in {
      val tokenWithInvalidContinuationToken = HostedPartiesPageTokenPayload(
        continuationToken = ByteString.copyFromUtf8("garbage"),
        version = 1,
        participantIdChecksum = calcParticipantChecksum(participantId),
        requestChecksum = calcRequestChecksum(originalRequest),
      ).toByteString
      val decodedToken =
        HostedPartiesPageToken.decodeAndValidate(
          originalRequestChecksum,
          participantIdChecksum,
          tokenWithInvalidContinuationToken,
        )
      decodedToken.left.value.getStatus.getDescription should equal(invalidContentsError)
    }

    "reject a token with a stream continuation token" in {
      val streamContinuationToken = HostedPartiesContinuationToken.encode(
        partyLowerBoundExcl,
        originalHostedAt,
        HostedPartiesContinuationToken.calcChecksum(
          GetHostedPartiesRequest(originalRequest.hostedAtOffset, originalRequest.filter, None),
          participantId,
        ),
      )
      val tokenWithStreamContinuationToken = HostedPartiesPageTokenPayload(
        continuationToken = streamContinuationToken,
        version = 1,
        participantIdChecksum = calcParticipantChecksum(participantId),
        requestChecksum = calcRequestChecksum(originalRequest),
      ).toByteString
      val decodedToken =
        HostedPartiesPageToken.decodeAndValidate(
          originalRequestChecksum,
          participantIdChecksum,
          tokenWithStreamContinuationToken,
        )
      decodedToken.left.value.getStatus.getDescription should equal(invalidContentsError)
    }

    "calculate the checksum correctly for unordered and duplicate filter values" in {
      val requestWithToken = originalRequest.copy(
        filter = Some(filter.copy(partyFilter = Seq("bob", "alice", "bob"))),
        pageToken = Some(originalToken),
      )
      val checksum = HostedPartiesPageToken.calcRequestChecksum(requestWithToken)
      val decodedToken =
        HostedPartiesPageToken.decodeAndValidate(checksum, participantIdChecksum, originalToken)
      decodedToken.value should equal(
        HostedPartiesContinuation(originalHostedAt, partyLowerBoundExcl)
      )
    }

    "calculate the same checksum all the time" in {
      originalRequestChecksum should equal(
        ByteString.copyFrom(Array[Byte](45, -16, -103, 18))
      )
    }
  }
}
