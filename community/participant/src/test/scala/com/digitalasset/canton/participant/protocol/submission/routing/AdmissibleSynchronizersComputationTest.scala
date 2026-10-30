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
  party2,
  party3,
  physicalSynchronizerId,
}
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{Confirmation, Submission}
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId, TestingTopology}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  HasExecutionContext,
  LfPartyId,
  ProtocolVersionChecksAnyWordSpec,
}
import org.scalatest.wordspec.AnyWordSpec

class AdmissibleSynchronizersComputationTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown
    with ProtocolVersionChecksAnyWordSpec {

  /** Routes for submitters on participant1, connected to the synchronizers of `topologies`. */
  private def route(
      topologies: Map[PhysicalSynchronizerId, TestingTopology],
      submitters: Set[LfPartyId],
      informees: Set[LfPartyId],
      prescribedSynchronizerO: Option[PhysicalSynchronizerId] = None,
  ): Either[TransactionRoutingError, Unit] = {
    val synchronizerState = mock[RoutingSynchronizerState]
    when(synchronizerState.topologySnapshots).thenReturn(topologies.map {
      case (synchronizerId, topology) =>
        synchronizerId -> topology.build(loggerFactory).topologySnapshot()
    })

    new AdmissibleSynchronizersComputation(participant1, loggerFactory)
      .forParties(submitters, informees, prescribedSynchronizerO, synchronizerState)
      .map(_ => ())
      .value
      .futureValueUS
  }

  // party1 is hosted on participant1; party2 on participant1 only
  private lazy val party1Hosted = TestingTopology().withThreshold(
    Map(
      party1.toLf -> (PositiveInt.one, Seq(
        participant1 -> Submission,
        participant2 -> Confirmation,
      )),
      party2.toLf -> (PositiveInt.one, Seq(participant1 -> Submission)),
    )
  )
  // participant2's Freezing entry freezes party1, although participant1 hosts it
  private lazy val party1Frozen =
    party1Hosted
      .withHostingStates(Map(party1.toLf -> Map(participant2 -> HostingState.Freezing)))
      .value
  private lazy val frozen = TopologyErrors.FrozenParties.Error(Set(party1.toLf))

  "forParties" should {
    "admit a single Submission host at threshold 2 exactly when the effective confirmation threshold is 1 and the party is not frozen" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      def routeParty1(secondHostState: HostingState): Either[TransactionRoutingError, Unit] =
        route(
          Map(
            physicalSynchronizerId -> TestingTopology()
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
          ),
          submitters = Set(party1.toLf),
          informees = Set(party1.toLf),
        )

      withClue("effective confirmation threshold = 2") {
        routeParty1(HostingState.Active).left.value shouldBe a[
          TopologyErrors.DecentralizedPartyCannotSubmit.Error
        ]
      }
      withClue("effective confirmation threshold = 1")(
        routeParty1(HostingState.Nominated) shouldBe Right(())
      )
      withClue("effective confirmation threshold = 1")(
        routeParty1(HostingState.Onboarding) shouldBe Right(())
      )
      withClue("frozen")(
        routeParty1(HostingState.Freezing) shouldBe Left(
          TopologyErrors.FrozenParties.Error(Set(party1.toLf))
        )
      )
    }

    "report a frozen party only if the request could be routed were no party frozen" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      val topologies = Map(physicalSynchronizerId -> party1Frozen)

      withClue("frozen informee")(
        route(topologies, Set(party2.toLf), Set(party2.toLf, party1.toLf)) shouldBe Left(frozen)
      )
      withClue("frozen informee and an unknown informee")(
        route(
          topologies,
          Set(party2.toLf),
          Set(party2.toLf, party1.toLf, party3.toLf),
        ) shouldBe Left(
          TopologyErrors.UnknownInformees.Error(Set(party1.toLf, party3.toLf))
        )
      )
    }

    "report a party frozen on the prescribed synchronizer although another synchronizer hosts it" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      val acme = SynchronizerId.tryFromString("acme::default").toPhysical
      val topologies = Map(physicalSynchronizerId -> party1Frozen, acme -> party1Hosted)
      def routeTo(prescribedSynchronizerO: Option[PhysicalSynchronizerId]) =
        route(topologies, Set(party2.toLf), Set(party2.toLf, party1.toLf), prescribedSynchronizerO)

      withClue("prescribed where frozen")(
        routeTo(Some(physicalSynchronizerId)) shouldBe Left(frozen)
      )
      withClue("prescribed where hosted")(routeTo(Some(acme)) shouldBe Right(()))
      withClue("none prescribed")(routeTo(None) shouldBe Right(()))
    }
  }
}
