// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import com.daml.ledger.api.v2.state_service.GetActiveContractsRequest
import com.daml.ledger.api.v2.transaction_filter.{EventFormat, Filters}
import com.digitalasset.canton.ledger.api.AcsContinuationToken.Checksum
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.ByteString
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.{HashMap, TreeMap}

class AcsContinuationTokenTest extends AnyFlatSpec with Matchers with EitherValues {
  private val eventFormat = EventFormat(
    filtersByParty = Map("party" -> Filters(Nil)),
    filtersForAnyParty = None,
    verbose = false,
  )
  private val originalRequest = GetActiveContractsRequest(125, Some(eventFormat), None)
  private val participantId = Ref.ParticipantId.assertFromString("Ledger")
  private val originalRequestChecksum =
    AcsContinuationToken.calcChecksum(originalRequest, participantId)
  private val sequentialId = 42L
  private val originalPointer = AcsContinuationPointerActiveContracts(sequentialId)
  private val encodedToken =
    AcsContinuationToken.activeContracts(sequentialId, originalRequestChecksum)

  private implicit val elc: ErrorLoggingContext =
    ErrorLoggingContext.fromTracedLogger(NamedLogging.noopLogger)(TraceContext.empty)

  "AcsContinuationToken" should "successfully decode an untampered token from the request" in {
    val checksum = AcsContinuationToken.calcChecksum(originalRequest, participantId)
    val decodedToken =
      AcsContinuationToken.decodeAndValidate(checksum, encodedToken)
    decodedToken.value should equal(originalPointer)
  }

  it should "fail if used on a different participant" in {
    val differentParticipantId = Ref.ParticipantId.assertFromString("different")
    val checksum = AcsContinuationToken.calcChecksum(originalRequest, differentParticipantId)
    val decodedToken =
      AcsContinuationToken.decodeAndValidate(checksum, encodedToken)
    decodedToken.left.value.getStatus.getDescription should equal(
      "INVALID_CONTINUATION_TOKEN(8,0): The submitted command contains an invalid continuation token. Tokens used in ACS " +
        "requests must be taken from a valid GetActiveContractsResponse and used with the same EventFormat settings, " +
        "with the same Canton participant running the same Canton version."
    )
  }

  it should "fail if not the same eventformat is used" in {
    val requestWithToken = originalRequest.copy(
      eventFormat = originalRequest.eventFormat.map(_.copy(verbose = true)),
      streamContinuationToken = Some(encodedToken),
    )
    val checksum = AcsContinuationToken.calcChecksum(requestWithToken, participantId)
    val decodedToken =
      AcsContinuationToken.decodeAndValidate(checksum, encodedToken)
    decodedToken.left.value.getStatus.getDescription should equal(
      "INVALID_CONTINUATION_TOKEN(8,0): The submitted command contains an invalid continuation token. Tokens used in " +
        "ACS requests must be taken from a valid GetActiveContractsResponse and used with the same EventFormat settings, " +
        "with the same Canton participant running the same Canton version."
    )
  }

  it should "fail if not the same activeAt is used" in {
    val requestWithToken = originalRequest.copy(
      activeAtOffset = originalRequest.activeAtOffset + 1,
      streamContinuationToken = Some(encodedToken),
    )
    val checksum = AcsContinuationToken.calcChecksum(requestWithToken, participantId)
    val decodedToken =
      AcsContinuationToken.decodeAndValidate(checksum, encodedToken)
    decodedToken.left.value.getStatus.getDescription should equal(
      "INVALID_CONTINUATION_TOKEN(8,0): The submitted command contains an invalid continuation token. Tokens used in " +
        "ACS requests must be taken from a valid GetActiveContractsResponse and used with the same EventFormat settings, " +
        "with the same Canton participant running the same Canton version."
    )
  }

  it should "reject an invalid token" in {
    val tamperedToken = encodedToken.substring(5)
    val checksum = AcsContinuationToken.calcChecksum(originalRequest, participantId)
    val decodedToken =
      AcsContinuationToken.decodeAndValidate(checksum, tamperedToken)
    decodedToken.left.value.getStatus.getDescription should equal(
      "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field stream_continuation_token: " +
        "Invalid continuation token for GetActiveContractsRequest"
    )
  }

  it should "calculate the checksum correctly for maps" in {
    val eventFormatHashMap = EventFormat(
      filtersByParty =
        HashMap("party1" -> Filters(Nil), "party2" -> Filters(Nil), "party3" -> Filters(Nil)),
      filtersForAnyParty = None,
      verbose = false,
    )
    val eventFormatTreeMap = EventFormat(
      filtersByParty =
        TreeMap("party1" -> Filters(Nil), "party2" -> Filters(Nil), "party3" -> Filters(Nil)),
      filtersForAnyParty = None,
      verbose = false,
    )
    val originalRequest = GetActiveContractsRequest(125, Some(eventFormatHashMap), None)
    val originalRequestChecksum = AcsContinuationToken.calcChecksum(originalRequest, participantId)
    val originalPointer = AcsContinuationPointerActiveContracts(sequentialId)
    val newToken = AcsContinuationToken.activeContracts(sequentialId, originalRequestChecksum)
    val requestWithToken = originalRequest.copy(
      eventFormat = Some(eventFormatTreeMap),
      streamContinuationToken = Some(newToken),
    )
    val checksum = AcsContinuationToken.calcChecksum(requestWithToken, participantId)
    val decodedToken =
      AcsContinuationToken.decodeAndValidate(checksum, newToken)
    decodedToken.value should equal(originalPointer)
  }

  it should "calculate the same checksum all the time" in {
    originalRequestChecksum should equal(
      Checksum(ByteString.copyFrom(Array[Byte](87, -103, -53, 56)))
    )
  }
}
