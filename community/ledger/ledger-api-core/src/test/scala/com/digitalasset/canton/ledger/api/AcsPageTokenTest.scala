// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import com.daml.ledger.api.v2.state_service.GetActiveContractsPageRequest
import com.daml.ledger.api.v2.transaction_filter.{EventFormat, Filters}
import com.daml.platform.v1.acs_page_token.AcsPageTokenPayload
import com.digitalasset.canton.ledger.api.AcsPageToken.{
  calcParticipantChecksum,
  calcRequestChecksum,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.ByteString
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AcsPageTokenTest extends AnyFlatSpec with Matchers with EitherValues {
  private val eventFormat = EventFormat(
    filtersByParty = Map("party" -> Filters(Nil)),
    filtersForAnyParty = None,
    verbose = false,
  )
  private val originalActiveAt = 100L
  private val originalRequest =
    GetActiveContractsPageRequest(Some(originalActiveAt), Some(eventFormat), Some(5), None)
  private val participantId = Ref.ParticipantId.assertFromString("Ledger")
  private val originalContinuationToken =
    AcsContinuationToken.activeContracts(42, AcsContinuationToken.emptyChecksum)
  private val originalToken = AcsPageToken.encode(
    originalRequest,
    originalContinuationToken,
    activeAtOffset = originalActiveAt,
    participantId,
  )
  private val participantIdChecksum = AcsPageToken.calcParticipantChecksum(participantId)
  private val originalRequestChecksum = AcsPageToken.calcRequestChecksum(originalRequest)

  private implicit val elc: ErrorLoggingContext =
    ErrorLoggingContext.fromTracedLogger(NamedLogging.noopLogger)(TraceContext.empty)

  "AcsPageToken" should "successfully decode an untampered token from the request" in {
    val decoded =
      AcsPageToken.decodeAndValidate(originalRequestChecksum, participantIdChecksum, originalToken)
    decoded.value._2 should equal(AcsContinuationPointerActiveContracts(42 - 1)) // decreased seq id
    decoded.value._1.unwrap should equal(originalActiveAt)
  }

  it should "fail if used on a different participant" in {
    val differentParticipantId = Ref.ParticipantId.assertFromString("different")
    val differentParticipantChecksum = AcsPageToken.calcParticipantChecksum(differentParticipantId)
    val decodedToken =
      AcsPageToken.decodeAndValidate(
        originalRequestChecksum,
        differentParticipantChecksum,
        originalToken,
      )
    decodedToken.left.value.getStatus.getDescription should equal(
      "INVALID_PAGE_TOKEN(8,0): The submitted command contains an invalid page token. Tokens used in " +
        "ACS requests must be taken from a valid GetActiveContractsPageResponse and used with the same " +
        "EventFormat settings, with the same Canton participant running the same Canton version. The page " +
        "token was prepared by a different participant."
    )
  }

  it should "fail if not the same eventformat is used" in {
    val requestWithToken = originalRequest.copy(
      eventFormat = originalRequest.eventFormat.map(_.copy(verbose = true)),
      pageToken = Some(originalToken),
    )
    val requestChecksum = AcsPageToken.calcRequestChecksum(requestWithToken)
    val decodedToken =
      AcsPageToken.decodeAndValidate(requestChecksum, participantIdChecksum, originalToken)
    decodedToken.left.value.getStatus.getDescription should equal(
      "INVALID_PAGE_TOKEN(8,0): The submitted command contains an invalid page token. Tokens used in " +
        "ACS requests must be taken from a valid GetActiveContractsPageResponse and used with the same " +
        "EventFormat settings, with the same Canton participant running the same Canton version. The page " +
        "token was prepared with different event_format or active_at_offset."
    )
  }

  it should "fail if not the same activeAt is used" in {
    val requestWithToken = originalRequest.copy(
      activeAtOffset = originalRequest.activeAtOffset.map(_ + 1),
      pageToken = Some(originalToken),
    )
    val checksum = AcsPageToken.calcRequestChecksum(requestWithToken)
    val decodedToken =
      AcsPageToken.decodeAndValidate(checksum, participantIdChecksum, originalToken)
    decodedToken.left.value.getStatus.getDescription should equal(
      "INVALID_PAGE_TOKEN(8,0): The submitted command contains an invalid page token. Tokens used in " +
        "ACS requests must be taken from a valid GetActiveContractsPageResponse and used with the same " +
        "EventFormat settings, with the same Canton participant running the same Canton version. The page " +
        "token was prepared with different event_format or active_at_offset."
    )
  }

  it should "reject an invalid token" in {
    val tamperedToken = originalToken.substring(5)
    val decodedToken =
      AcsPageToken.decodeAndValidate(originalRequestChecksum, participantIdChecksum, tamperedToken)
    decodedToken.left.value.getStatus.getDescription should equal(
      "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field page_token: " +
        "Invalid page token for GetActiveContractsPageRequest"
    )
  }

  it should "reject a token with different version" in {
    val tokenWithDifferentVersion = AcsPageTokenPayload(
      continuationToken = originalContinuationToken,
      activeAtOffset = originalActiveAt,
      version = 999,
      participantIdChecksum = calcParticipantChecksum(participantId),
      requestChecksum = calcRequestChecksum(originalRequest),
    ).toByteString
    val decodedToken =
      AcsPageToken.decodeAndValidate(
        originalRequestChecksum,
        participantIdChecksum,
        tokenWithDifferentVersion,
      )
    decodedToken.left.value.getStatus.getDescription should equal(
      "INVALID_PAGE_TOKEN(8,0): The submitted command contains an invalid page token. Tokens used in " +
        "ACS requests must be taken from a valid GetActiveContractsPageResponse and used with the same " +
        "EventFormat settings, with the same Canton participant running the same Canton version. The page " +
        "token was prepared with different page API version."
    )
  }

  it should "calculate the same checksum all the time" in {
    originalRequestChecksum should equal(
      ByteString.copyFrom(Array[Byte](-51, 65, -123, 2))
    )
  }
}
