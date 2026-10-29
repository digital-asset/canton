// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.HasSimClockUtils.advanceClockInStepsTo
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.PartiesAllocator
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.time.PositiveFiniteDuration
import com.digitalasset.canton.topology.Party
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.version.ProtocolVersion
import monocle.macros.syntax.lens.*

/** From protocol version 37, the assignment exclusivity timeout is not enforced and the automatic
  * assignment is submitted after the participant's `automatic-assignment-delay`.
  *
  * Topology:
  *   - P1, P2 connected to both da and acme, whose assignment exclusivity timeout is 10 minutes
  *   - P1 hosts Alice and has automatic-assignment-delay configured to 1 minute
  *   - P2 hosts Bob and has automatic-assignment-delay configured to 10 minutes
  */
final class AssignmentWithoutExclusivityTimeoutIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  private val exclusivityTimeout = config.NonNegativeFiniteDuration.ofMinutes(10)
  private val p1AutomaticAssignmentDelay = config.NonNegativeFiniteDuration.ofMinutes(1)

  override def environmentDefinition: EnvironmentDefinition = EnvironmentDefinition.P2_S1M1_S1M1
    .addConfigTransforms(
      ConfigTransforms.useStaticTime,
      ConfigTransforms.enableMultiSynchronizerTopologyFeatureFlag,
      ConfigTransforms.updateParticipantConfig("participant1")(
        _.focus(_.parameters.reassignmentsConfig.automaticAssignmentDelay)
          .replace(p1AutomaticAssignmentDelay)
      ),
      ConfigTransforms.updateParticipantConfig("participant2")(
        _.focus(_.parameters.reassignmentsConfig.automaticAssignmentDelay)
          .replace(config.NonNegativeFiniteDuration.ofMinutes(10))
      ),
    )
    .withSetup { implicit env =>
      import env.*
      participants.all.synchronizers.connect_local(sequencer1, alias = daName)
      participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
      participants.all.dars.upload(CantonTestsPath, synchronizerId = daId)
      participants.all.dars.upload(CantonTestsPath, synchronizerId = acmeId)

      Seq((sequencer1, daId), (sequencer2, acmeId)).foreach { case (sequencer, synchronizerId) =>
        sequencer.topology.synchronizer_parameters.propose_update(
          synchronizerId,
          _.update(assignmentExclusivityTimeout = exclusivityTimeout),
        )
      }
      val acmeExclusivityTimeout = sequencer2.topology.synchronizer_parameters
        .get_dynamic_synchronizer_parameters(acmeId)
        .assignmentExclusivityTimeout
      acmeExclusivityTimeout shouldBe exclusivityTimeout
      acmeExclusivityTimeout.underlying should be > p1AutomaticAssignmentDelay.underlying

      def hostedOn(participant: ParticipantReference) = {
        val permissions = (
          PositiveInt.one,
          Set((participant.id, ParticipantPermission.Submission: ParticipantPermission)),
        )
        Map(daId -> permissions, acmeId -> permissions)
      }

      val parties = PartiesAllocator(Set(participant1, participant2))(
        Seq(("alice", participant1), ("bob", participant2)),
        Map("alice" -> hostedOn(participant1), "bob" -> hostedOn(participant2)),
      )

      alice = parties.find(_.toProtoPrimitive.contains("alice")).value
      bob = parties.find(_.toProtoPrimitive.contains("bob")).value
    }

  private var alice: Party = _
  private var bob: Party = _

  "An assignment" should {
    "be submitted by a non-initiator before the exclusivity timeout" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        import env.*

        val iou = IouSyntax.createIou(participant1, Some(daId))(alice, bob)

        // P1 initiates the unassignment, and P2 submits the assignment manually before the
        // exclusivity timeout (10 minutes) has elapsed
        val unassigned = participant1.ledger_api.commands.submit_unassign(
          alice,
          Seq(LfContractId.assertFromString(iou.id.contractId)),
          daId,
          acmeId,
        )
        unassigned.events.loneElement.assignmentExclusivity shouldBe None

        participant2.ledger_api.commands.submit_assign(
          bob,
          unassigned.reassignmentId,
          daId,
          acmeId,
        )

        participant2.ledger_api.state.acs.incomplete_unassigned_of_party(bob) shouldBe empty
        participant2.ledger_api.state.acs
          .active_contracts_of_party(bob)
          .filter(_.createdEvent.value.contractId == iou.id.contractId)
          .loneElement
          .synchronizerId shouldBe acmeId.logical.toProtoPrimitive
    }

    "be submitted automatically after the automatic assignment delay" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        import env.*

        val iou = IouSyntax.createIou(participant2, Some(daId))(bob, alice)

        // P2 initiates the unassignment, and P1 submits the automatic assignment 1 to 2 minutes later
        val unassigned = participant2.ledger_api.commands.submit_unassign(
          bob,
          Seq(LfContractId.assertFromString(iou.id.contractId)),
          daId,
          acmeId,
        )
        val unassignmentTs =
          CantonTimestamp.fromProtoTimestamp(unassigned.reassignment.recordTime.value).value
        val clock = environment.simClock.value

        clock.advanceTo(
          unassignmentTs
            .plus(p1AutomaticAssignmentDelay.asJava)
            .minus(java.time.Duration.ofSeconds(1))
        )
        participants.all.foreach(_.testing.fetch_synchronizer_times())
        participant1.ledger_api.state.acs.incomplete_unassigned_of_party(alice) should not be empty

        // Advance in steps below the confirmation timeouts, so that the automatic assignment is not
        // sequenced at a stale time and then timed out by the next event
        val latestSubmissionTime =
          unassignmentTs.plus(p1AutomaticAssignmentDelay.asJava.multipliedBy(2))
        advanceClockInStepsTo(
          clock,
          latestSubmissionTime,
          maxStep = PositiveFiniteDuration.tryOfSeconds(10),
        ) {
          participants.all.foreach(_.testing.fetch_synchronizer_times())
        }
        eventually() {
          participant1.ledger_api.state.acs.incomplete_unassigned_of_party(alice) shouldBe empty
        }
        participant1.ledger_api.state.acs
          .active_contracts_of_party(alice)
          .filter(_.createdEvent.value.contractId == iou.id.contractId)
          .loneElement
          .synchronizerId shouldBe acmeId.logical.toProtoPrimitive
    }
  }
}
