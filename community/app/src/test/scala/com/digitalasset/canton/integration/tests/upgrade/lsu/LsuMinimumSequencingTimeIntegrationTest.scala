// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.networking.grpc.GrpcError.GrpcRequestRefusedByServer
import com.digitalasset.canton.sequencing.client.SendAsyncClientError.RequestRefused
import com.digitalasset.canton.sequencing.protocol.Batch
import com.digitalasset.canton.sequencing.protocol.SendAsyncError.{
  SendAsyncErrorDirect,
  SendAsyncErrorGrpc,
}
import com.digitalasset.canton.sequencing.protocol.SequencerErrors.SubmissionRequestRefused
import com.digitalasset.canton.tracing.TraceContext
import org.slf4j.event.Level

/*
 * This test is used to test the logical synchronizer upgrade.
 * It uses 1 participants, 1 sequencer, and 1 mediator.
 * We test only after the upgrade the behavior of the minimum sequencing time (upgrade time) enforcement.
 * Minimum sequencing time is configured via the LSU announcement in the topology state.
 */
final class LsuMinimumSequencingTimeIntegrationTest
    extends LsuBase
    with SharedEnvironment
    with EntitySyntax
    with LogicalUpgradeUtils {

  override protected def testName: String = "lsu-minimum-sequencing-time"

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer2" -> "sequencer1")
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        import env.*
        defaultEnvironmentSetup(participantsOverride = Some(Seq(participant1)))
      }

  "Logical synchronizer upgrade" should {
    "initialize the nodes for the upgraded synchronizer" in { implicit env =>
      import env.*

      val fixture = fixtureWithDefaults()

      performSynchronizerNodesLsu(fixture)

      // advance the time to some point before the minimum sequencing time
      environment.simClock.value.advanceTo(upgradeTime.minusSeconds(10))

      val errorMessage =
        s"Cannot submit before or at the lower bound for sequencing time $upgradeTime; time is currently at ${environment.clock.now}"
      loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
        {
          sequencer2.underlying.value.sequencer.client
            .send(Batch(Nil, testedProtocolVersion))(TraceContext.empty, MetricsContext.Empty)
            .futureValueUS shouldBe Left(RequestRefused(SendAsyncErrorDirect(errorMessage)))

          mediator2.underlying.value.replicaManager.mediatorRuntime.value.mediator.sequencerClient
            .send(Batch(Nil, testedProtocolVersion))(TraceContext.empty, MetricsContext.Empty)
            .futureValueUS should matchPattern {
            case Left(
                  RequestRefused(
                    SendAsyncErrorGrpc(GrpcRequestRefusedByServer(_, _, _, _, Some(cantonError)))
                  )
                )
                if cantonError.code.id == SubmissionRequestRefused.id && cantonError.cause == errorMessage =>
          }

          // advance the clock to the minimum sequencing time
          environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

          eventually() {
            waitForTargetTimeOnSequencer(sequencer2, upgradeTime.immediateSuccessor)
          }
        },
        LogEntry.assertLogSeq(
          Seq(
            (
              entry => {
                entry.shouldBeCantonErrorCode(SubmissionRequestRefused)
                entry.warningMessage should include(errorMessage)
              },
              "submission request refused warning",
            )
          )
        ),
      )

      participant2.synchronizers.connect_local(sequencer2, daName)
      participant2.health.maybe_ping(participant2) should not be empty

    }
  }
}
