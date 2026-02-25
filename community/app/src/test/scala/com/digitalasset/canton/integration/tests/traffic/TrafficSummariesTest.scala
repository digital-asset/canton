// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.traffic

import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.api.v2.transaction.Transaction
import com.digitalasset.canton.admin.api.client.data.TrafficControlParameters
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeLong,
  NonNegativeNumeric,
  PositiveInt,
  PositiveLong,
}
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.TrafficBalanceSupport
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.sequencing.TrafficControlParameters as InternalTrafficControlParameters
import com.digitalasset.canton.sequencing.traffic.TrafficControlErrors
import com.digitalasset.canton.{ProtocolVersionChecksFixtureAnyWordSpec, config}

/** Test the traffic inspection service on the sequencer Specifically that it can be used to
  * correlate with the mediator inspection service verdict's to obtain accurate traffic cost of
  * envelopes.
  */
trait TrafficSummariesTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with ProtocolVersionChecksFixtureAnyWordSpec
    with HasCycleUtils
    with TrafficBalanceSupport {

  // Free confirmation responses and no base cost
  // This means essentially only topology transactions and confirmation requests cost traffic
  // which makes it easier to make assertion on traffic spent by nodes
  // It does require topping them up though, which is done in the setup of the environment in this test
  private val trafficControlParameters = TrafficControlParameters(
    maxBaseTrafficAmount = NonNegativeNumeric.tryCreate(20 * 1000L),
    readVsWriteScalingFactor = InternalTrafficControlParameters.DefaultReadVsWriteScalingFactor,
    maxBaseTrafficAccumulationDuration = config.PositiveFiniteDuration.ofSeconds(1L),
    setBalanceRequestSubmissionWindowSize = config.PositiveFiniteDuration.ofMinutes(5L),
    enforceRateLimiting = true,
    baseEventCost = NonNegativeLong.zero,
    freeConfirmationResponses = true,
  )

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup { implicit env =>
        import env.*

        sequencer1.topology.synchronizer_parameters.propose_update(
          synchronizerId = daId,
          _.update(trafficControl = Some(trafficControlParameters)),
        )

        // "Deactivate" ACS commitments to not consume traffic in the background
        sequencer1.topology.synchronizer_parameters.propose_update(
          synchronizerId = daId,
          _.update(reconciliationInterval = config.PositiveDurationSeconds.ofDays(365)),
        )
        sequencer1.topology.synchronisation.await_idle()

        participants.local.foreach { participant =>
          participant.start()
          participant.health.wait_for_running()
          participant.synchronizers.connect_local(sequencer1, daName)
          participant.dars.upload(CantonExamplesPath)
          updateBalanceForMember(participant, PositiveLong.MaxValue, () => ())
        }

        updateBalanceForMember(mediator1, PositiveLong.MaxValue, () => ())

        sequencer1.topology.synchronizer_parameters.propose_update(
          synchronizerId = daId,
          // Update the traffic parameters to remove base traffic, making it easier to assert on traffic spent
          _.update(trafficControl =
            Some(trafficControlParameters.copy(maxBaseTrafficAmount = NonNegativeLong.zero))
          ),
        )

      }

  private var createRecordTime: CantonTimestamp = _

  "traffic summaries" when {
    "retrieve traffic summaries from the sequencer" in { implicit env =>
      import env.*

      // Create 3 parties on different nodes
      val alice = participant1.parties.enable("Alice")
      val bob = participant2.parties.enable("Bob")
      val charlie = participant3.parties.enable("Charlie")

      // Get the available traffic on P1 before we create the Iou
      val trafficBalanceP1BeforeCreate =
        participant1.traffic_control.traffic_state(synchronizer1Id).availableTraffic

      // Create an Iou
      val createIouCommand = Command.fromJavaProto(
        IouSyntax.testIou(alice, bob).create().commands().loneElement.toProtoCommand
      )
      val createTransaction =
        participant1.ledger_api.commands.submit(Seq(alice), Seq(createIouCommand))
      val iou = JavaDecodeUtil
        .decodeAllCreated(Iou.COMPANION)(
          com.daml.ledger.javaapi.data.Transaction
            .fromProto(Transaction.toJavaProto(createTransaction))
        )
        .loneElement
      createRecordTime =
        CantonTimestamp.fromProtoTimestamp(createTransaction.recordTime.value).value
      // Diff the old available traffic with the new one to get the cost paid by the participant for the confirmation request
      val trafficCostCreate = trafficBalanceP1BeforeCreate - participant1.traffic_control
        .traffic_state(synchronizer1Id)
        .availableTraffic

      // Get the available traffic on P2 before we exercise the Transfer
      val trafficBalanceP2BeforeExercise =
        participant2.traffic_control.traffic_state(synchronizer1Id).availableTraffic

      // And then exercise the transfer choice so we have another transaction with 2 views
      val transferCommand = Command.fromJavaProto(
        iou.id.exerciseTransfer(charlie.toProtoPrimitive).commands().loneElement.toProtoCommand
      )
      val exerciseTransaction =
        participant2.ledger_api.commands.submit(Seq(bob), Seq(transferCommand))
      val exerciseRecordTime =
        CantonTimestamp.fromProtoTimestamp(exerciseTransaction.recordTime.value).value
      // Diff the old available traffic with the new one to get the cost paid by the participant for the confirmation request
      val trafficCostExercise = trafficBalanceP2BeforeExercise - participant2.traffic_control
        .traffic_state(synchronizer1Id)
        .availableTraffic

      // Find the corresponding traffic summary
      val summaries =
        sequencer1.traffic_control.traffic_summaries(Seq(createRecordTime, exerciseRecordTime))

      // Find the corresponding verdicts
      val verdicts =
        mediator1.inspection.verdicts(createRecordTime.immediatePredecessor, PositiveInt.two)

      // Correlate traffic data with verdict data
      summaries.zip(verdicts).zip(List(trafficCostCreate, trafficCostExercise)).foreach {
        case ((summary, verdict), observedTrafficCost) =>
          // Check the record times match
          val summaryRecordTime = summary.sequencingTime
          val verdictRecordTime = verdict.recordTime
          summaryRecordTime shouldBe verdictRecordTime

          // Check the traffic costs match
          summary.totalTrafficCost shouldBe observedTrafficCost.toLong

          // Check the view hashes match
          val labelsFromSummary = summary.envelopes.flatMap(_.viewHashes)
          val viewHashesFromVerdict =
            verdict.views.transactionViews.value.views.values.map(_.viewHash).toSeq

          labelsFromSummary should not be empty
          labelsFromSummary should contain theSameElementsAs viewHashesFromVerdict
      }
    }

    "fail with a retryable error if any timestamp is above the current synchronizer time" in {
      implicit env =>
        import env.*

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          // Try with 2 timestamps, one valid, one in the future, in reverse order
          sequencer1.traffic_control.traffic_summaries(
            Seq(createRecordTime.plusSeconds(1000), createRecordTime)
          ),
          _.shouldBeCantonErrorCode(TrafficControlErrors.RequestedTimestampInTheFuture.code),
        )
    }

    "fail with a non retryable error if any timestamp does not have a corresponding event" in {
      implicit env =>
        import env.*

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          // Try with 2 timestamps, one valid, one just before which has no event, but is still below the synchronizer time
          sequencer1.traffic_control.traffic_summaries(
            Seq(createRecordTime, CantonTimestamp.MinValue)
          ),
          _.shouldBeCantonErrorCode(TrafficControlErrors.NoEventAtTimestamps.code),
        )
    }
  }
}

class TrafficSummariesTestH2 extends TrafficSummariesTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
