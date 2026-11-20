// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.state_service.HostedPartiesFilter
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.ledger.api.sizeconfig.v2.HostedPartiesFilterSizeConfig
import com.digitalasset.canton.ledger.participant.state.index.IndexerHostedPartyFilter
import com.digitalasset.canton.logging.{ErrorLoggingContext, NoLogging}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.nonempty.NonEmpty
import io.grpc.Status.Code.*
import org.scalatest.wordspec.AnyWordSpec

class StateServiceRequestValidatorTest extends AnyWordSpec with SizeLimitTestUtils {

  private implicit val noLogging: ErrorLoggingContext = NoLogging

  private def synchronizerIds(numberOfSynchronizerIds: Int): Seq[String] =
    (1 to numberOfSynchronizerIds).map(i => s"synchronizer$i::namespace")

  "StateServiceRequestValidator" when {

    "validating hosted parties filters" should {

      val synchronizerId = SynchronizerId.tryFromString("synchronizer1::namespace")
      val synchronizerId2 = SynchronizerId.tryFromString("synchronizer2::namespace")

      val filter = HostedPartiesFilter(
        partyFilter = Seq(party, party2, party),
        allParticipants = true,
        synchronizerIdFilter = Seq(
          synchronizerId.toProtoPrimitive,
          synchronizerId2.toProtoPrimitive,
          synchronizerId.toProtoPrimitive,
        ),
      )

      "pass on valid input" in {
        StateServiceRequestValidator.validateHostedPartiesFilter(Some(filter)).value shouldBe
          IndexerHostedPartyFilter(
            parties = Some(NonEmpty(Set, party, party2)),
            allParticipants = true,
            synchronizerIds = Some(NonEmpty(Set, synchronizerId, synchronizerId2)),
          )
      }

      "pass the default filter if none is given" in {
        StateServiceRequestValidator.validateHostedPartiesFilter(None).value shouldBe
          IndexerHostedPartyFilter()
      }

      "treat an empty party filter as no party restriction" in {
        StateServiceRequestValidator
          .validateHostedPartiesFilter(
            Some(
              HostedPartiesFilter(
                partyFilter = Seq.empty,
                allParticipants = false,
                synchronizerIdFilter = Seq.empty,
              )
            )
          )
          .value shouldBe IndexerHostedPartyFilter()
      }

      "fail on an invalid party" in {
        requestMustFailWith(
          request = StateServiceRequestValidator.validateHostedPartiesFilter(
            Some(filter.copy(partyFilter = Seq("not a party!")))
          ),
          code = INVALID_ARGUMENT,
          description = "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: " +
            "non expected character 0x21 in Daml-LF Party \"not a party!\"",
        )
      }

      "fail on an empty synchronizer id" in {
        requestMustFailWith(
          request = StateServiceRequestValidator.validateHostedPartiesFilter(
            Some(filter.copy(synchronizerIdFilter = Seq("")))
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: synchronizer_id_filter",
        )
      }

      "fail on an invalid synchronizer id" in {
        requestMustFailWith(
          request = StateServiceRequestValidator.validateHostedPartiesFilter(
            Some(filter.copy(synchronizerIdFilter = Seq("no-namespace")))
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field " +
              "synchronizer_id_filter: Invalid unique identifier `no-namespace` with missing namespace.",
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

      cardinalityLimit("party_filter", maxParties)(numberOfParties =>
        StateServiceRequestValidator.validateHostedPartiesFilter(
          Some(HostedPartiesFilter.defaultInstance.copy(partyFilter = parties(numberOfParties)))
        )(noLogging, sizeLimitsConfigWithLimits)
      )

      cardinalityLimit("synchronizer_id_filter", maxSynchronizerIds)(numberOfSynchronizerIds =>
        StateServiceRequestValidator.validateHostedPartiesFilter(
          Some(
            HostedPartiesFilter.defaultInstance.copy(synchronizerIdFilter =
              synchronizerIds(numberOfSynchronizerIds)
            )
          )
        )(noLogging, sizeLimitsConfigWithLimits)
      )
    }
  }
}
