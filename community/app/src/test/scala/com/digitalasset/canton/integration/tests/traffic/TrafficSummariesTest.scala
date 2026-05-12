// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.traffic

import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.api.v2.transaction.Transaction
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.TrafficBalanceSupport
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{TestUtils, TrafficControlUtils}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.resource.{DbStorage, StorageSingleFactory}
import com.digitalasset.canton.sequencing.protocol.{Batch, ClosedUncompressedEnvelope, Recipients}
import com.digitalasset.canton.sequencing.traffic.TrafficControlErrors
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.PartyId
import com.google.protobuf.ByteString

/** Test the traffic inspection service on the sequencer Specifically that it can be used to
  * correlate with the mediator inspection service verdict's to obtain accurate traffic cost of
  * envelopes.
  */
trait TrafficSummariesTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils
    with TrafficBalanceSupport {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup { implicit env =>
        import env.*
        participants.local.foreach { participant =>
          participant.start()
          participant.health.wait_for_running()
          participant.synchronizers.connect_local(sequencer1, daName)
          participant.dars.upload(CantonExamplesPath)
        }
      }
      .withTrafficControl(
        TestUtils.waitForTargetTimeOnSynchronizerNode(wallClock.now, logger),
        trafficControlParameters = TrafficControlUtils.predictableTraffic,
        topUpAllMembers = true,
        disableCommitments = true,
      )

  private var alice: PartyId = _
  private var createRecordTime: CantonTimestamp = _

  "traffic summaries" when {
    "retrieve traffic summaries from the sequencer" in { implicit env =>
      import env.*

      // Create 3 parties on different nodes
      alice = participant1.parties.enable("Alice")
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
        mediator1.inspection
          .verdicts_until_complete(createRecordTime.immediatePredecessor, PositiveInt.two)
          .flatMap(_.verdict)

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
          // Try with 2 timestamps, one valid, and one below the synchronizer time but with no event
          sequencer1.traffic_control.traffic_summaries(
            Seq(createRecordTime, CantonTimestamp.MinValue)
          ),
          _.shouldBeCantonErrorCode(TrafficControlErrors.NoEventAtTimestamps.code),
        )
    }

    "fallback to no view hashes if the envelope cannot be opened" in { implicit env =>
      import env.*

      val factory = new StorageSingleFactory(sequencer1.config.storage)
      val storage = factory.tryCreate(
        connectionPoolForParticipant = false,
        logQueryCost = None,
        new SimClock(CantonTimestamp.Epoch, loggerFactory),
        scheduler = None,
        CommonMockMetrics.dbStorage,
        DefaultProcessingTimeouts.testing,
        loggerFactory,
      ) match {
        case jdbc: DbStorage => jdbc
        case _ => fail("should be db storage")
      }
      import storage.api.*
      import storage.DbStorageConverters.*

      // Create a corrupted envelope
      val corruptedBytes = ByteString.copyFromUtf8("corrupted-envelope-content")
      val corruptedEnvelope = ClosedUncompressedEnvelope.create(
        corruptedBytes,
        Recipients.cc(participant1),
        Seq.empty,
        testedProtocolVersion,
      )
      // Put it in a well-formed batch
      val corruptedBatch = Batch.fromClosed(testedProtocolVersion, corruptedEnvelope)
      val corruptedPayload = corruptedBatch.toByteString

      // Write the corrupted payload back to the DB
      storage
        .update_(
          sqlu"""update sequencer_payloads set content = $corruptedPayload where id = ${createRecordTime.toMicros}""",
          "corrupt payload",
        )
        .futureValueUS

      storage.close()

      // Evict the payload cache by restarting the sequencer
      sequencer1.stop()
      sequencer1.start()
      sequencer1.health.wait_for_running()

      // Now request traffic summaries; the envelope should fail to open but we should still get
      // a result with no view hashes
      val summaries = loggerFactory.assertLoggedWarningsAndErrorsSeq(
        sequencer1.traffic_control.traffic_summaries(Seq(createRecordTime)),
        LogEntry.assertLogSeq(
          mustContainWithClue = Seq(
            (
              _.warningMessage should include("Failed to open envelope to extract view hashes"),
              "expected failed to open envelope message",
            )
          )
        ),
      )

      summaries.loneElement.envelopes.loneElement.viewHashes shouldBe empty
      summaries.loneElement.envelopes.loneElement.envelopeTrafficCost should be > 0L
    }
  }

}

class TrafficSummariesTestH2 extends TrafficSummariesTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
