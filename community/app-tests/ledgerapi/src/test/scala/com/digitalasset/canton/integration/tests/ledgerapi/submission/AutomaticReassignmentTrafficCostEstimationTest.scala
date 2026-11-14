// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.submission

import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  CostEstimation,
  ReassignmentCost,
}
import com.digitalasset.canton.admin.api.client.data.TrafficControlParameters
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.damltests.java.automaticreassignmenttransactions.Single
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UseProgrammableSequencer
import com.digitalasset.canton.integration.tests.SynchronizerRouterIntegrationTestSetup.{
  createAggregate,
  createSingle,
}
import com.digitalasset.canton.integration.util.TestUtils
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
}
import com.digitalasset.canton.topology.{Party, PhysicalSynchronizerId}

import scala.annotation.nowarn
import scala.jdk.CollectionConverters.*

object Syntax {
  implicit class CostEstimationOps(val costEstimation: CostEstimation) extends AnyVal {
    def transactionCosts: Long =
      costEstimation.confirmationRequestTrafficCostEstimation +
        costEstimation.confirmationResponseTrafficCostEstimation

    def reassignmentCostsForSynchronizer(
        sourceSynchronizerId: PhysicalSynchronizerId
    ): Option[ReassignmentCost] =
      costEstimation.reassignmentCosts.find(
        _.sourceSynchronizerId == sourceSynchronizerId.toProtoPrimitive
      )
  }

  implicit class ReassignmentCostOps(val reassignmentCost: ReassignmentCost) extends AnyVal {
    def unassignmentCosts: Long =
      reassignmentCost.unassignmentRequestTrafficCostEstimation +
        reassignmentCost.unassignmentResponseTrafficCostEstimation

    def assignmentCosts: Long =
      reassignmentCost.assignmentRequestTrafficCostEstimation +
        reassignmentCost.assignmentResponseTrafficCostEstimation

    def totalReassignmentCostEstimation: Long =
      unassignmentCosts + assignmentCosts
  }

}

class AutomaticReassignmentTrafficCostEstimationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {
  import Syntax.*

  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  // Update the traffic parameters to set base event cost to 0
  // This allows for easier assertions over traffic consumed
  private val trafficControlParams = TrafficControlParameters.default.copy(
    maxBaseTrafficAmount = NonNegativeLong.zero,
    baseEventCost = NonNegativeLong.zero,
  )

  // Initialized in the environment setup
  private var party1: Party = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1_S1M1_S1M1
      .addConfigTransforms(
        ProgrammableSequencer.configOverride(this.getClass.toString, loggerFactory),
        ConfigTransforms.enableMultiSynchronizerTopologyFeatureFlag,
        ConfigTransforms.enableInteractiveSubmissionTransforms,
      )
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
        participant1.synchronizers.connect_local(sequencer3, alias = repairSynchronizerName)
        participant1.dars.upload(CantonTestsPath, synchronizerId = daId)
        participant1.dars.upload(CantonTestsPath, synchronizerId = acmeId)
        participant1.dars.upload(CantonTestsPath, synchronizerId = repairSynchronizerId)

        party1 = participant1.parties.testing.enable("party1", synchronizer = daName)

        participant1.parties.testing.also_enable(party1, synchronizer = acmeName)
        participant1.parties.testing.also_enable(party1, synchronizer = repairSynchronizerName)
      }
      .withTrafficControl(
        TestUtils.waitForTargetTimeOnSynchronizerNode(wallClock.now, logger),
        trafficControlParams,
        topUpAllMembers = true,
        disableCommitments = true,
      )

  private def collectTrafficStates(
      synchronizers: Seq[PhysicalSynchronizerId]
  )(implicit env: TestConsoleEnvironment) = {
    import env.*
    synchronizers.map(synchronizerId => participant1.traffic_control.traffic_state(synchronizerId))
  }

  private def consumedTraffic(from: TrafficState, to: TrafficState): Long = {
    require(
      from.timestamp <= to.timestamp,
      s"Traffic states must be in chronological order: $from, $to",
    )
    from.availableTraffic.toLong - to.availableTraffic.toLong
  }

  private val estimationTolerancePercent = 5.0d
  private def withinTolerance(base: Long, percent: Double = estimationTolerancePercent) =
    base +- (base * percent / 100).toLong

  private def testReAssignmentTarget(
      target: Option[PhysicalSynchronizerId]
  )(implicit env: TestConsoleEnvironment) = {
    import env.*

    // Create a bunch of contracts on synchronizer 1
    val sync1ContractIds: Seq[Single.ContractId] = Seq(
      // 3 contracts on synchronizer 1 -- these will need to be reassigned
      createSingle(party1, Some(synchronizer1Id), participant1, party1).id,
      createSingle(party1, Some(synchronizer1Id), participant1, party1).id,
      createSingle(party1, Some(synchronizer1Id), participant1, party1).id,
    )

    // Create a contract on synchronizer 2, referencing those on synchronizer 1
    val aggregate = createAggregate(participant1, party1, sync1ContractIds, Some(synchronizer2Id))
    val sync2ContractIds = Seq(aggregate.id)

    // Prepare transaction to make contracts on synchronizer 1 be reassigned to synchronizer 2
    val commands = aggregate.id.exerciseCountAll().commands.asScala.toSeq
    val prepared = participant1.ledger_api.javaapi.interactive_submission.prepare(
      actAs = Seq(party1),
      commands = commands,
      synchronizerId = target,
    )

    val actualTargetString = prepared.preparedTransaction.value.metadata.value.synchronizerId

    val targetSynchronizer = PhysicalSynchronizerId.tryFromString(actualTargetString)
    val (sourceSynchronizer, expectedContractIds) =
      if (targetSynchronizer == synchronizer1Id) (synchronizer2Id, sync2ContractIds)
      else (synchronizer1Id, sync1ContractIds)

    @nowarn
    val Seq(sourceStateBefore, targetStateBefore) =
      collectTrafficStates(Seq(sourceSynchronizer, targetSynchronizer))

    // Submit transaction
    participant1.ledger_api.javaapi.commands.submit(
      actAs = Seq(party1),
      commands = commands,
      synchronizerId = Some(targetSynchronizer),
    )

    @nowarn
    val Seq(sourceStateAfter, targetStateAfter) =
      collectTrafficStates(Seq(sourceSynchronizer, targetSynchronizer))

    val realUnassignmentCosts = consumedTraffic(sourceStateBefore, sourceStateAfter)
    val realAssignmentAndTxCosts = consumedTraffic(targetStateBefore, targetStateAfter)

    val estimatedCosts = prepared.costEstimation.value
    val estimatedReAssignmentCosts =
      estimatedCosts.reassignmentCostsForSynchronizer(sourceSynchronizer).value

    estimatedReAssignmentCosts.sourceSynchronizerId shouldBe sourceSynchronizer.toProtoPrimitive
    estimatedReAssignmentCosts.targetSynchronizerId shouldBe targetSynchronizer.toProtoPrimitive
    estimatedReAssignmentCosts.contractIds should contain theSameElementsAs expectedContractIds
      .map(_.contractId)
    estimatedReAssignmentCosts.unassignmentRequestTrafficCostEstimation should be > 0L
    estimatedReAssignmentCosts.unassignmentResponseTrafficCostEstimation should be > 0L

    estimatedReAssignmentCosts.assignmentRequestTrafficCostEstimation should be > 0L
    estimatedReAssignmentCosts.assignmentResponseTrafficCostEstimation should be > 0L

    estimatedCosts.transactionCosts + estimatedReAssignmentCosts.assignmentCosts shouldBe withinTolerance(
      realAssignmentAndTxCosts
    )

    estimatedCosts.totalTrafficCostEstimation shouldBe withinTolerance(
      realUnassignmentCosts + realAssignmentAndTxCosts
    )

    estimatedReAssignmentCosts.totalReassignmentCostEstimation shouldBe (
      estimatedReAssignmentCosts.unassignmentRequestTrafficCostEstimation +
        estimatedReAssignmentCosts.unassignmentResponseTrafficCostEstimation +
        estimatedReAssignmentCosts.assignmentRequestTrafficCostEstimation +
        estimatedReAssignmentCosts.assignmentResponseTrafficCostEstimation
    )
  }

  "Traffic cost estimation" should {
    "estimate automatic re-assignment costs with target synchronizer 1" in { implicit env =>
      import env.*
      testReAssignmentTarget(target = Some(synchronizer1Id))
    }

    "estimate automatic re-assignment costs with target synchronizer 2" in { implicit env =>
      import env.*
      testReAssignmentTarget(target = Some(synchronizer2Id))
    }

    "estimate automatic re-assignment costs with no specified target (auto select)" in {
      implicit env =>
        testReAssignmentTarget(target = None)
    }

    "estimate re-assignment costs for multiple synchronizers" in { implicit env =>
      import env.*

      // Create contracts on synchronizer 1 and synchronizer 2
      val contractIdsS1: Seq[Single.ContractId] = Seq(
        createSingle(party1, Some(synchronizer1Id), participant1, party1).id
      )
      val contractIdsS2: Seq[Single.ContractId] = Seq(
        createSingle(party1, Some(synchronizer2Id), participant1, party1).id
      )

      // Create a contract on synchronizer 3, referencing those on synchronizer 1 and synchronizer 2
      val aggregate = createAggregate(
        participant = participant1,
        submitter = party1,
        singles = contractIdsS1 ++ contractIdsS2,
        synchronizerId = Some(synchronizer3Id),
      )

      val commands = aggregate.id.exerciseCountAll().commands.asScala.toSeq

      // Submit a prepare transaction to make contracts on synchronizer 1 and synchronizer 2
      // be reassigned to synchronizer 3
      val prepared = participant1.ledger_api.javaapi.interactive_submission.prepare(
        actAs = Seq(party1),
        commands = commands,
        synchronizerId = Some(synchronizer3Id),
      )

      @nowarn
      val Seq(sync1StateBefore, sync2StateBefore, sync3StateBefore) =
        collectTrafficStates(Seq(synchronizer1Id, synchronizer2Id, synchronizer3Id))

      // Submit transaction
      participant1.ledger_api.javaapi.commands.submit(
        actAs = Seq(party1),
        commands = commands,
        synchronizerId = Some(synchronizer3Id),
      )

      @nowarn
      val Seq(sync1StateAfter, sync2StateAfter, sync3StateAfter) =
        collectTrafficStates(Seq(synchronizer1Id, synchronizer2Id, synchronizer3Id))

      val realUnassignmentCostsS1 = consumedTraffic(sync1StateBefore, sync1StateAfter)
      val realUnassignmentCostsS2 = consumedTraffic(sync2StateBefore, sync2StateAfter)
      val realAssignmentAndTxCosts = consumedTraffic(sync3StateBefore, sync3StateAfter)

      val estimatedCosts = prepared.costEstimation.value
      val estimatedCostS1 = estimatedCosts.reassignmentCostsForSynchronizer(synchronizer1Id).value
      val estimatedCostS2 = estimatedCosts.reassignmentCostsForSynchronizer(synchronizer2Id).value

      estimatedCostS1.contractIds should contain theSameElementsAs contractIdsS1.map(_.contractId)
      estimatedCostS2.contractIds should contain theSameElementsAs contractIdsS2.map(_.contractId)

      estimatedCostS1.sourceSynchronizerId shouldBe synchronizer1Id.toProtoPrimitive
      estimatedCostS1.targetSynchronizerId shouldBe synchronizer3Id.toProtoPrimitive

      estimatedCostS2.sourceSynchronizerId shouldBe synchronizer2Id.toProtoPrimitive
      estimatedCostS2.targetSynchronizerId shouldBe synchronizer3Id.toProtoPrimitive

      estimatedCostS1.unassignmentRequestTrafficCostEstimation should be > 0L
      estimatedCostS1.unassignmentResponseTrafficCostEstimation should be > 0L

      estimatedCostS2.unassignmentRequestTrafficCostEstimation should be > 0L
      estimatedCostS2.unassignmentResponseTrafficCostEstimation should be > 0L

      estimatedCostS1.unassignmentCosts shouldBe withinTolerance(realUnassignmentCostsS1)
      estimatedCostS2.unassignmentCosts shouldBe withinTolerance(realUnassignmentCostsS2)

      estimatedCostS1.assignmentRequestTrafficCostEstimation should be > 0L
      estimatedCostS1.assignmentResponseTrafficCostEstimation should be > 0L
      estimatedCostS2.assignmentRequestTrafficCostEstimation should be > 0L
      estimatedCostS2.assignmentResponseTrafficCostEstimation should be > 0L

      estimatedCosts.transactionCosts +
        estimatedCostS1.assignmentCosts +
        estimatedCostS2.assignmentCosts shouldBe withinTolerance(
          realAssignmentAndTxCosts
        )

      estimatedCosts.totalTrafficCostEstimation shouldBe withinTolerance(
        realUnassignmentCostsS1 + realUnassignmentCostsS2 + realAssignmentAndTxCosts
      )

      estimatedCostS1.totalReassignmentCostEstimation shouldBe (
        estimatedCostS1.unassignmentRequestTrafficCostEstimation +
          estimatedCostS1.unassignmentResponseTrafficCostEstimation +
          estimatedCostS1.assignmentRequestTrafficCostEstimation +
          estimatedCostS1.assignmentResponseTrafficCostEstimation
      )
      estimatedCostS2.totalReassignmentCostEstimation shouldBe (
        estimatedCostS2.unassignmentRequestTrafficCostEstimation +
          estimatedCostS2.unassignmentResponseTrafficCostEstimation +
          estimatedCostS2.assignmentRequestTrafficCostEstimation +
          estimatedCostS2.assignmentResponseTrafficCostEstimation
      )
    }
  }
}
