// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{InstanceReference, LocalParticipantReference}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.buildBaseEnvironmentDefinition
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.UseBftSequencer
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.util.PartiesAllocator
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{Observation, Submission}
import com.digitalasset.canton.topology.{Party, PhysicalSynchronizerId}
import com.digitalasset.canton.version.ProtocolVersion
import monocle.syntax.all.*

import java.time.Duration

/** A history with a hosting state move that protocol version 36 accepts and protocol version 37
  * rejects, across an LSU from protocol version 36 to 37.
  *
  * Test scenario:
  *   - On the old synchronizer, on protocol version 36, Alice is hosted on participant1 and
  *     participant2. Alice, whose namespace participant1 holds, moves participant2 from Active back
  *     to Onboarding by setting its onboarding flag. participant1 disables the optional topology
  *     checks for this, as the submitting node rejects the move since 3.7.
  *   - The LSU initializes the successor's sequencer with the old synchronizer's history, which
  *     contains this move.
  *   - participant3 was not connected to the old synchronizer. It joins the successor and downloads
  *     the history.
  *
  * Test goal: the successor and participant3 accept the move, as the old synchronizer did, so that
  * participant2 stays Onboarding for Alice.
  *
  * Note: Runs only on the latest protocol version, as it sets the protocol versions of both
  * synchronizers.
  */
final class LsuTopologyVersionChangeHostingStateIntegrationTest extends LsuBase {
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )

  override protected def testName: String = "lsu-topology-version-change-hosting-state"

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer2" -> "sequencer1")
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  private val oldPV = ProtocolVersion.v36
  private val newPV = ProtocolVersion.v37

  override lazy val environmentDefinition: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(numParticipants = 3, numSequencers = 2, numMediators = 2)
      .withNetworkBootstrap { implicit env =>
        import env.*
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1),
            mediators = Seq(mediator1),
            overrideStaticSynchronizerParameters = Some(
              StaticSynchronizerParameters.initialValues(environment.clock, oldPV)
            ),
          )
        )
      }
      .addConfigTransforms(configTransforms*)
      .addConfigTransforms(
        // Lets participant1 submit the move back to Onboarding, as releases before 3.7 did. Only
        // submissions run the optional checks: the successor and participant3 validate the history
        // without them.
        ConfigTransforms.updateParticipantConfig("participant1")(
          _.focus(_.topology.disableOptionalTopologyChecks).replace(true)
        )
      )
      .withSetup { implicit env =>
        defaultEnvironmentSetup(participantsOverride =
          Some(Seq(env.participant1, env.participant2))
        )
      }

  private var alice: Party = _

  private def participant2State(
      observer: LocalParticipantReference,
      psid: PhysicalSynchronizerId,
  )(implicit env: TestConsoleEnvironment): HostingState =
    observer.topology.party_to_participant_mappings
      .list(psid, filterParty = alice.filterString)
      .loneElement
      .item
      .participants
      .find(_.participantId == env.participant2.id)
      .value
      .state

  "A successor on protocol version 37" should {
    "accept a move back to Onboarding that protocol version 36 accepted" onlyRunWhen (testedProtocolVersion == ProtocolVersion.latest) in {
      implicit env =>
        import env.*

        alice = PartiesAllocator(Set(participant1, participant2))(
          newParties = Seq("Alice" -> participant1.id),
          targetTopology = Map(
            "Alice" -> Map(
              daId -> (
                PositiveInt.one,
                Set((participant1.id, Submission), (participant2.id, Observation)),
              )
            )
          ),
        ).loneElement

        // On protocol version 36, the party alone may set the onboarding flag of an Active entry
        participant1.topology.party_to_participant_mappings.propose_delta(
          alice,
          adds = Seq(participant2.id -> Observation),
          requiresPartyToBeOnboarded = true,
          store = daId,
        )
        eventually() {
          participant2State(participant1, daId) shouldBe HostingState.Onboarding
        }

        val fixture = fixtureWithDefaults(newPVOverride = Some(newPV))
        performSynchronizerNodesLsu(fixture)

        environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)
        transferTraffic()
        eventually() {
          environment.simClock.value.advance(Duration.ofSeconds(1))
          participant1.synchronizers.is_connected(fixture.newPsid) shouldBe true
          participant2.synchronizers.is_connected(fixture.newPsid) shouldBe true
        }
        oldSynchronizerNodes.all.stop()
        waitForTargetTimeOnSequencer(sequencer2, environment.clock.now, logger)

        participant3.synchronizers.connect(sequencer2, daName)
        participant3.health.ping(participant1)

        participant2State(participant1, fixture.newPsid) shouldBe HostingState.Onboarding
        participant2State(participant3, fixture.newPsid) shouldBe HostingState.Onboarding
    }
  }
}
