// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.state

import com.daml.ledger.api.v2.state_service.GetActiveContractsRequest
import com.daml.ledger.api.v2.transaction_filter.{EventFormat, Filters}
import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.ledger.api.messages.state.AcsContinuationToken.Checksum
import com.digitalasset.canton.ledger.api.util.{PageTokenUtils, UpdateFormatHashUtils}
import com.digitalasset.canton.ledger.api.validation.{LedgerApiSizeLimitsConfig, SizeLimitTestUtils}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.{HashMap, TreeMap}
import scala.util.chaining.scalaUtilChainingOps

class AcsContinuationTokenTest extends AnyWordSpec with SizeLimitTestUtils {
  private val eventFormat = EventFormat(
    filtersByParty = Map("party" -> Filters(Nil)),
    filtersForAnyParty = None,
    verbose = false,
  )
  private val originalRequest =
    GetActiveContractsRequest(125, Some(eventFormat), None, includeIncompleteReassignments = false)
  private val participantId = Ref.ParticipantId.assertFromString("Ledger")
  private val originalRequestChecksum =
    AcsContinuationToken.calcChecksum(originalRequest, participantId)
  private val sequentialId = 42L
  private val originalPointer = AcsContinuationPointerActiveContracts(sequentialId)
  private val encodedToken =
    AcsContinuationToken.activeContracts(sequentialId, originalRequestChecksum)

  private implicit val elc: ErrorLoggingContext =
    ErrorLoggingContext.fromTracedLogger(NamedLogging.noopLogger)(TraceContext.empty)

  private val errorString =
    "INVALID_CONTINUATION_TOKEN(8,0): The submitted command contains an invalid continuation token. " +
      "Tokens used in ACS requests must be taken from a valid GetActiveContractsResponse and used with the same EventFormat settings, " +
      "with the same Canton participant running the same Canton version."

  private def decodeAndValidate(
      checksum: Checksum,
      token: ByteString,
      limit: PositiveInt =
        LedgerApiSizeLimitsConfig().publicLapiSizeConfig.getActiveContractsRequest.streamContinuationTokenSizeLimit,
  ) =
    AcsContinuationToken.decodeAndValidate(
      expectedChecksum = checksum,
      token = token,
      fieldName = "stream_continuation_token",
      limit = limit,
    )

  "AcsContinuationToken" should {
    "successfully decode an untampered token from the request" in {
      val decodedToken =
        decodeAndValidate(originalRequestChecksum, encodedToken)
      decodedToken.value should equal(originalPointer)
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
      val checksum = AcsContinuationToken.calcChecksum(originalRequest, differentParticipantId)
      val decodedToken =
        decodeAndValidate(checksum, encodedToken)
      decodedToken.left.value.getStatus.getDescription should equal(
        errorString
      )
    }

    "fail if different includeIncompleteReassignments flag is used" in {
      val differentRequest = originalRequest.copy(
        includeIncompleteReassignments = !originalRequest.includeIncompleteReassignments
      )
      val checksum = AcsContinuationToken.calcChecksum(differentRequest, participantId)
      val decodedToken =
        decodeAndValidate(checksum, encodedToken)
      decodedToken.left.value.getStatus.getDescription should equal(
        errorString
      )
    }

    "fail if not the same eventformat is used" in {
      val requestWithToken = originalRequest.copy(
        eventFormat = originalRequest.eventFormat.map(_.copy(verbose = true)),
        streamContinuationToken = Some(encodedToken),
      )
      val checksum = AcsContinuationToken.calcChecksum(requestWithToken, participantId)
      val decodedToken =
        decodeAndValidate(checksum, encodedToken)
      decodedToken.left.value.getStatus.getDescription should equal(
        errorString
      )
    }

    "fail if not the same activeAt is used" in {
      val requestWithToken = originalRequest.copy(
        activeAtOffset = originalRequest.activeAtOffset + 1,
        streamContinuationToken = Some(encodedToken),
      )
      val checksum = AcsContinuationToken.calcChecksum(requestWithToken, participantId)
      val decodedToken =
        decodeAndValidate(checksum, encodedToken)
      decodedToken.left.value.getStatus.getDescription should equal(
        errorString
      )
    }

    "reject an invalid token" in {
      val tamperedToken = encodedToken.substring(5)
      val checksum = AcsContinuationToken.calcChecksum(originalRequest, participantId)
      val decodedToken =
        decodeAndValidate(checksum, tamperedToken)
      decodedToken.left.value.getStatus.getDescription should equal(
        "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field stream_continuation_token: " +
          "Invalid continuation token for GetActiveContractsRequest"
      )
    }

    "calculate the checksum correctly for maps" in {
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
      val originalRequest = GetActiveContractsRequest(
        125,
        Some(eventFormatHashMap),
        None,
        includeIncompleteReassignments = false,
      )
      val originalRequestChecksum =
        AcsContinuationToken.calcChecksum(originalRequest, participantId)
      val originalPointer = AcsContinuationPointerActiveContracts(sequentialId)
      val newToken = AcsContinuationToken.activeContracts(sequentialId, originalRequestChecksum)
      val requestWithToken = originalRequest.copy(
        eventFormat = Some(eventFormatTreeMap),
        streamContinuationToken = Some(newToken),
      )
      val checksum = AcsContinuationToken.calcChecksum(requestWithToken, participantId)
      val decodedToken =
        decodeAndValidate(checksum, newToken)
      decodedToken.value should equal(originalPointer)
    }

    "reject a token issued with a previous token version" in {
      // checksum of originalRequest as calculated by TokenVersion = 1
      // (which did not include the includeIncompleteReassignments flag)
      val previousVersionChecksum = calcChecksumV1(originalRequest, participantId)
      val previousVersionToken =
        AcsContinuationToken.activeContracts(sequentialId, previousVersionChecksum)
      val checksum = AcsContinuationToken.calcChecksum(originalRequest, participantId)
      val decodedToken =
        decodeAndValidate(checksum, previousVersionToken)
      decodedToken.left.value.getStatus.getDescription should equal(
        errorString
      )
    }

    "calculate the same checksum all the time" in {
      originalRequestChecksum should equal(
        Checksum(ByteString.copyFrom(Array[Byte](111, -68, -89, 79)))
      )
    }
  }

  // Capture the previous version of the checksum calculation for testing purposes. It differs from
  // the current implementation by token version and the absence of includeIncompleteReassignments flag.
  private def calcChecksumV1(
      request: GetActiveContractsRequest,
      participantId: LedgerParticipantId,
  ) =
    Hash
      .build(HashPurpose.AcsContinuationToken, HashAlgorithm.Sha256)
      .addLong(request.activeAtOffset)
      .addOptional(request.eventFormat, UpdateFormatHashUtils.hashEventFormat)
      .addInt(1)
      .addString(participantId)
      .finish()
      .pipe(PageTokenUtils.toChecksum)
      .pipe(Checksum(_))

}
