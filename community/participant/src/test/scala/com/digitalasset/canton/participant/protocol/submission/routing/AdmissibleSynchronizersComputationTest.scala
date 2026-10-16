// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.error.TransactionRoutingError
import com.digitalasset.canton.error.TransactionRoutingError.TopologyErrors
import com.digitalasset.canton.ledger.participant.state.RoutingSynchronizerState
import com.digitalasset.canton.topology.DefaultTestIdentities.{
  participant1,
  participant2,
  party1,
  physicalSynchronizerId,
}
import com.digitalasset.canton.topology.TestingTopology
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{Confirmation, Submission}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  HasExecutionContext,
  ProtocolVersionChecksAnyWordSpec,
}
import org.scalatest.wordspec.AnyWordSpec

class AdmissibleSynchronizersComputationTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown
    with ProtocolVersionChecksAnyWordSpec {

  "forParties" should {
    "admit a single Submission host at threshold 2 exactly when the effective confirmation threshold is 1 and the party is not frozen" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      def route(secondHostState: HostingState): Either[TransactionRoutingError, Unit] = {
        val snapshot = TestingTopology()
          .withThreshold(
            Map(
              party1.toLf -> (PositiveInt.two, Seq(
                participant1 -> Submission,
                participant2 -> Confirmation,
              ))
            )
          )
          .withHostingStates(Map(party1.toLf -> Map(participant2 -> secondHostState)))
          .value
          .build(loggerFactory)
          .topologySnapshot()
        val synchronizerState = mock[RoutingSynchronizerState]
        when(synchronizerState.topologySnapshots)
          .thenReturn(Map(physicalSynchronizerId -> snapshot))

        new AdmissibleSynchronizersComputation(participant1, loggerFactory)
          .forParties(
            submitters = Set(party1.toLf),
            informees = Set(party1.toLf),
            synchronizerState,
          )
          .map(_ => ())
          .value
          .futureValueUS
      }

      withClue("effective confirmation threshold = 2") {
        route(HostingState.Active).left.value shouldBe a[
          TopologyErrors.DecentralizedPartyCannotSubmit.Error
        ]
      }
      withClue("effective confirmation threshold = 1")(
        route(HostingState.Nominated) shouldBe Right(())
      )
      withClue("effective confirmation threshold = 1")(
        route(HostingState.Onboarding) shouldBe Right(())
      )
      withClue("frozen")(route(HostingState.Freezing).isLeft shouldBe true)
    }
  }
}
