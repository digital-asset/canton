// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.state_service.{
  GetHostedPartiesPageRequest,
  GetHostedPartiesRequest,
  HostedPartiesFilter,
}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.messages.state
import com.digitalasset.canton.ledger.api.messages.state.{
  HostedPartiesContinuation,
  HostedPartiesContinuationToken,
  HostedPartiesPageToken,
}
import com.digitalasset.canton.ledger.api.sizeconfig.v2.HostedPartiesFilterSizeConfig
import com.digitalasset.canton.ledger.api.util.PageTokenUtils
import com.digitalasset.canton.ledger.participant.state.index.IndexerHostedPartyFilter
import com.digitalasset.canton.logging.{ErrorLoggingContext, NoLogging}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString
import io.grpc.Status.Code.*
import org.scalatest.wordspec.AnyWordSpec

class StateServiceRequestValidatorTest extends AnyWordSpec with SizeLimitTestUtils {

  private implicit val noLogging: ErrorLoggingContext = NoLogging

  private val participantId = Ref.ParticipantId.assertFromString("participant")
  private val maxPageSize = PositiveInt.tryCreate(10)
  private val defaultPageSize = PositiveInt.tryCreate(3)
  private val requestFilter = HostedPartiesFilter(
    partyFilter = Seq(party, party2, party),
    allParticipants = true,
    synchronizerIdFilter = Seq(
      "synchronizer1::namespace",
      "synchronizer2::namespace",
      "synchronizer1::namespace",
    ),
  )
  private val streamRequest = GetHostedPartiesRequest(
    hostedAtOffset = None,
    filter = Some(requestFilter),
    streamContinuationToken = None,
  )
  private val pageRequest = GetHostedPartiesPageRequest(
    hostedAtOffset = None,
    filter = Some(requestFilter),
    pageToken = None,
    maxPageSize = None,
  )
  private val continuation = HostedPartiesContinuation(Offset.tryFromLong(42L), party)
  private val streamToken = HostedPartiesContinuationToken.encode(
    continuation.partyLowerBoundExcl,
    continuation.hostedAt,
    HostedPartiesContinuationToken.calcChecksum(streamRequest, participantId),
  )
  private val pageToken = HostedPartiesPageToken.encode(
    pageRequest,
    HostedPartiesContinuationToken.encode(
      continuation.partyLowerBoundExcl,
      continuation.hostedAt,
      PageTokenUtils.emptyChecksum,
    ),
    participantId,
  )

  private def validateStream(request: GetHostedPartiesRequest) =
    StateServiceRequestValidator.validateGetHostedPartiesRequest(request, participantId)

  private def validatePage(request: GetHostedPartiesPageRequest) =
    StateServiceRequestValidator.validateGetHostedPartiesPageRequest(
      request,
      participantId,
      maxPageSize,
      defaultPageSize,
    )

  private def assertRejectedByBothApis(
      hostedAtOffset: Long,
      filter: HostedPartiesFilter,
      expectedDescription: String,
  ): Unit = {
    requestMustFailWith(
      validateStream(
        streamRequest.copy(hostedAtOffset = Some(hostedAtOffset), filter = Some(filter))
      ),
      INVALID_ARGUMENT,
      expectedDescription,
    )
    requestMustFailWith(
      validatePage(pageRequest.copy(hostedAtOffset = Some(hostedAtOffset), filter = Some(filter))),
      INVALID_ARGUMENT,
      expectedDescription,
    )
  }

  private def synchronizerIds(numberOfSynchronizerIds: Int): Seq[String] =
    (1 to numberOfSynchronizerIds).map(i => s"synchronizer$i::namespace")

  "StateServiceRequestValidator" when {

    "validating hosted parties requests" should {
      "normalize filters and preserve an omitted snapshot offset" in {
        val expectedFilter = IndexerHostedPartyFilter(
          parties = Some(NonEmpty(Set, party, party2)),
          allParticipants = true,
          synchronizerIds = Some(
            NonEmpty(
              Set,
              SynchronizerId.tryFromString("synchronizer1::namespace"),
              SynchronizerId.tryFromString("synchronizer2::namespace"),
            )
          ),
        )
        validateStream(streamRequest).value shouldBe state.GetHostedPartiesRequest(
          filter = expectedFilter,
          requestedHostedAt = None,
          continuation = None,
          continuationChecksum =
            HostedPartiesContinuationToken.calcChecksum(streamRequest, participantId),
        )
        validatePage(pageRequest).value shouldBe state.GetHostedPartiesPageRequest(
          filter = expectedFilter,
          requestedHostedAt = None,
          continuation = None,
          pageSize = defaultPageSize.unwrap,
        )
      }

      "use the default filter when none is given" in {
        validateStream(streamRequest.copy(filter = None)).value.filter shouldBe
          IndexerHostedPartyFilter()
        validatePage(pageRequest.copy(filter = None)).value.filter shouldBe
          IndexerHostedPartyFilter()
      }

      "treat an empty party filter as no party restriction" in {
        val filter = HostedPartiesFilter(
          partyFilter = Seq.empty,
          allParticipants = false,
          synchronizerIdFilter = Seq.empty,
        )
        validateStream(streamRequest.copy(filter = Some(filter))).value.filter shouldBe
          IndexerHostedPartyFilter()
        validatePage(pageRequest.copy(filter = Some(filter))).value.filter shouldBe
          IndexerHostedPartyFilter()
      }

      "preserve explicit ledger begin and positive snapshot offsets" in {
        validateStream(
          streamRequest.copy(hostedAtOffset = Some(0L))
        ).value.requestedHostedAt shouldBe
          Some(None)
        validatePage(pageRequest.copy(hostedAtOffset = Some(0L))).value.requestedHostedAt shouldBe
          Some(None)
        validateStream(
          streamRequest.copy(hostedAtOffset = Some(42L))
        ).value.requestedHostedAt shouldBe
          Some(Some(continuation.hostedAt))
        validatePage(pageRequest.copy(hostedAtOffset = Some(42L))).value.requestedHostedAt shouldBe
          Some(Some(continuation.hostedAt))
      }

      "treat empty tokens as absent" in {
        validateStream(
          streamRequest.copy(streamContinuationToken = Some(ByteString.EMPTY))
        ) shouldBe
          validateStream(streamRequest)
        validatePage(pageRequest.copy(pageToken = Some(ByteString.EMPTY))) shouldBe
          validatePage(pageRequest)
      }

      "decode tokens without replacing the originally requested offset" in {
        validateStream(
          streamRequest.copy(streamContinuationToken = Some(streamToken))
        ).value shouldBe
          validateStream(streamRequest).value.copy(continuation = Some(continuation))
        validatePage(pageRequest.copy(pageToken = Some(pageToken))).value shouldBe
          validatePage(pageRequest).value.copy(continuation = Some(continuation))
      }

      "reject tokens when the request changes" in {
        validateStream(
          streamRequest.copy(
            hostedAtOffset = Some(42L),
            streamContinuationToken = Some(streamToken),
          )
        ).left.value.getStatus.getDescription should startWith(
          "INVALID_HOSTED_PARTIES_CONTINUATION_TOKEN"
        )
        validatePage(
          pageRequest.copy(hostedAtOffset = Some(42L), pageToken = Some(pageToken))
        ).left.value.getStatus.getDescription should startWith("INVALID_HOSTED_PARTIES_PAGE_TOKEN")
      }

      "reject tokens from another participant" in {
        val otherParticipant = Ref.ParticipantId.assertFromString("other-participant")
        StateServiceRequestValidator
          .validateGetHostedPartiesRequest(
            streamRequest.copy(streamContinuationToken = Some(streamToken)),
            otherParticipant,
          )
          .left
          .value
          .getStatus
          .getDescription should startWith("INVALID_HOSTED_PARTIES_CONTINUATION_TOKEN")
        StateServiceRequestValidator
          .validateGetHostedPartiesPageRequest(
            pageRequest.copy(pageToken = Some(pageToken)),
            otherParticipant,
            maxPageSize,
            defaultPageSize,
          )
          .left
          .value
          .getStatus
          .getDescription should startWith("INVALID_HOSTED_PARTIES_PAGE_TOKEN")
      }

      "reject malformed tokens even at ledger begin" in {
        val malformedToken = ByteString.copyFrom(Array[Byte](0))
        requestMustFailWith(
          validateStream(
            streamRequest
              .copy(hostedAtOffset = Some(0L), streamContinuationToken = Some(malformedToken))
          ),
          INVALID_ARGUMENT,
          "INVALID_FIELD(8,0): The submitted command has a field with invalid value: " +
            "Invalid field stream_continuation_token: Invalid continuation token for GetHostedPartiesRequest",
        )
        requestMustFailWith(
          validatePage(
            pageRequest.copy(hostedAtOffset = Some(0L), pageToken = Some(malformedToken))
          ),
          INVALID_ARGUMENT,
          "INVALID_FIELD(8,0): The submitted command has a field with invalid value: " +
            "Invalid field page_token: Invalid page token for GetHostedPartiesPageRequest",
        )
      }

      "enforce stream and page token size limits" in {
        val limits = sizeLimitsConfig.copy(
          publicLapiSizeConfig = sizeLimitsConfig.publicLapiSizeConfig.copy(
            getHostedPartiesRequest = sizeLimitsConfig.publicLapiSizeConfig.getHostedPartiesRequest
              .copy(streamContinuationTokenSizeLimit =
                PositiveInt.tryCreate(streamToken.size() - 1)
              ),
            getHostedPartiesPageRequest =
              sizeLimitsConfig.publicLapiSizeConfig.getHostedPartiesPageRequest
                .copy(pageTokenSizeLimit = PositiveInt.tryCreate(pageToken.size() - 1)),
          )
        )
        requestMustFailWith(
          StateServiceRequestValidator.validateGetHostedPartiesRequest(
            streamRequest.copy(streamContinuationToken = Some(streamToken)),
            participantId,
          )(noLogging, limits),
          INVALID_ARGUMENT,
          sizeLimitExceeded("stream_continuation_token", streamToken.size(), streamToken.size() - 1),
        )
        requestMustFailWith(
          StateServiceRequestValidator.validateGetHostedPartiesPageRequest(
            pageRequest.copy(pageToken = Some(pageToken)),
            participantId,
            maxPageSize,
            defaultPageSize,
          )(noLogging, limits),
          INVALID_ARGUMENT,
          sizeLimitExceeded("page_token", pageToken.size(), pageToken.size() - 1),
        )
      }

      "reject negative offsets" in {
        assertRejectedByBothApis(
          hostedAtOffset = -1L,
          filter = requestFilter,
          expectedDescription =
            "NEGATIVE_OFFSET(8,0): Offset -1 in hosted_at_offset is a negative integer: " +
              "the offset in hosted_at_offset field has to be a non-negative integer (>=0)",
        )
      }

      "reject invalid parties at ledger begin" in {
        assertRejectedByBothApis(
          hostedAtOffset = 0L,
          filter = requestFilter.copy(partyFilter = Seq("not a party!")),
          expectedDescription =
            "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: " +
              "non expected character 0x21 in Daml-LF Party \"not a party!\"",
        )
      }

      "reject invalid synchronizers at ledger begin" in {
        assertRejectedByBothApis(
          hostedAtOffset = 0L,
          filter = requestFilter.copy(synchronizerIdFilter = Seq("no-namespace")),
          expectedDescription =
            "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field " +
              "synchronizer_id_filter: Invalid unique identifier `no-namespace` with missing namespace.",
        )
        assertRejectedByBothApis(
          hostedAtOffset = 0L,
          filter = requestFilter.copy(synchronizerIdFilter = Seq("")),
          expectedDescription =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: synchronizer_id_filter",
        )
      }
    }

    "validating hosted parties page sizes" should {
      "accept positive page sizes up to the configured maximum" in {
        validatePage(pageRequest.copy(maxPageSize = Some(1))).value.pageSize shouldBe 1
        validatePage(
          pageRequest.copy(maxPageSize = Some(maxPageSize.unwrap))
        ).value.pageSize shouldBe
          maxPageSize.unwrap
      }

      "allow the page size to change when resuming" in {
        validatePage(
          pageRequest.copy(pageToken = Some(pageToken), maxPageSize = Some(1))
        ).value shouldBe
          validatePage(pageRequest).value.copy(continuation = Some(continuation), pageSize = 1)
      }

      "reject non-positive page sizes even at ledger begin" in {
        requestMustFailWith(
          validatePage(pageRequest.copy(hostedAtOffset = Some(0L), maxPageSize = Some(0))),
          INVALID_ARGUMENT,
          "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: " +
            "Requested page size must be positive, but got 0",
        )
        requestMustFailWith(
          validatePage(pageRequest.copy(hostedAtOffset = Some(0L), maxPageSize = Some(-1))),
          INVALID_ARGUMENT,
          "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: " +
            "Requested page size must be positive, but got -1",
        )
      }

      "reject page sizes above the configured maximum" in {
        requestMustFailWith(
          validatePage(pageRequest.copy(maxPageSize = Some(maxPageSize.unwrap + 1))),
          INVALID_ARGUMENT,
          "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: " +
            "Requested page size must not exceed 10, but got 11",
        )
      }
    }

    "validating the size limits of hosted parties filters" should {
      val maxParties = 2
      val maxSynchronizerIds = 3

      val sizeLimitsConfigWithLimits = sizeLimitsConfig.copy(
        publicLapiSizeConfig = sizeLimitsConfig.publicLapiSizeConfig.copy(
          hostedPartiesFilter = HostedPartiesFilterSizeConfig(
            partyFilterSizeLimit = PositiveInt.tryCreate(maxParties),
            synchronizerIdFilterSizeLimit = PositiveInt.tryCreate(maxSynchronizerIds),
          )
        )
      )

      val emptyFilter = HostedPartiesFilter(
        partyFilter = Seq.empty,
        allParticipants = false,
        synchronizerIdFilter = Seq.empty,
      )

      cardinalityLimit("party_filter", maxParties, limitName = "stream request limit")(
        numberOfParties =>
          StateServiceRequestValidator.validateGetHostedPartiesRequest(
            streamRequest.copy(filter =
              Some(emptyFilter.copy(partyFilter = parties(numberOfParties)))
            ),
            participantId,
          )(noLogging, sizeLimitsConfigWithLimits)
      )

      cardinalityLimit(
        "synchronizer_id_filter",
        maxSynchronizerIds,
        limitName = "stream request limit",
      )(numberOfSynchronizerIds =>
        StateServiceRequestValidator.validateGetHostedPartiesRequest(
          streamRequest.copy(filter =
            Some(emptyFilter.copy(synchronizerIdFilter = synchronizerIds(numberOfSynchronizerIds)))
          ),
          participantId,
        )(noLogging, sizeLimitsConfigWithLimits)
      )

      cardinalityLimit("party_filter", maxParties, limitName = "page request limit")(
        numberOfParties =>
          StateServiceRequestValidator.validateGetHostedPartiesPageRequest(
            pageRequest.copy(filter =
              Some(emptyFilter.copy(partyFilter = parties(numberOfParties)))
            ),
            participantId,
            maxPageSize,
            defaultPageSize,
          )(noLogging, sizeLimitsConfigWithLimits)
      )

      cardinalityLimit(
        "synchronizer_id_filter",
        maxSynchronizerIds,
        limitName = "page request limit",
      )(numberOfSynchronizerIds =>
        StateServiceRequestValidator.validateGetHostedPartiesPageRequest(
          pageRequest.copy(filter =
            Some(emptyFilter.copy(synchronizerIdFilter = synchronizerIds(numberOfSynchronizerIds)))
          ),
          participantId,
          maxPageSize,
          defaultPageSize,
        )(noLogging, sizeLimitsConfigWithLimits)
      )
    }
  }
}
