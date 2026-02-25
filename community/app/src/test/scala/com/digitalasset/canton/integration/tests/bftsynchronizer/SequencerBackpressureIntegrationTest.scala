// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.bftsynchronizer

import com.daml.metrics.api.MetricQualification
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.UniquePortGenerator
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DbConfig, TestSequencerClientFor}
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.examples.java.iou
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.metrics.{MetricsConfig, MetricsReporterConfig}
import com.digitalasset.canton.sequencing.client.DelayedSequencerClient
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import monocle.syntax.all.*

import scala.concurrent.Promise
import scala.jdk.CollectionConverters.*
import scala.util.Random

/** This test verifies that the gRPC backpressure mechanism in SequencerService is implemented
  * correctly, by creating traffic on one participant and then consuming the resulting traffic on
  * another participant that blocks processing in the sequencer client.
  */
trait SequencerBackpressureIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      // Enable metrics, as this is what we'll use to verify that the backpressure mechanism is working
      .addConfigTransforms(ConfigTransforms.addMonitoringEndpointForSequencers*)
      .addConfigTransform(
        _.focus(_.monitoring.metrics)
          .replace(
            MetricsConfig(
              qualifiers = MetricQualification.All,
              reporters = Seq(
                MetricsReporterConfig.Prometheus(
                  port = UniquePortGenerator.next
                )
              ),
            )
          )
      )
      // The test creates many contracts, which is slowed down by the additional consistency checks
      .addConfigTransform(
        ConfigTransforms.disableAdditionalConsistencyChecks
      )
      // Set up the delayed sequencer client for participant2
      .updateTestingConfig(
        _.focus(_.testSequencerClientFor).replace(
          Set(TestSequencerClientFor(this.getClass.getSimpleName, "participant2", "synchronizer1"))
        )
      )

  "SequencerService should backpressure" in { implicit env =>
    import env.*

    // Creating 100 contracts with a text payload of 50kb results in about 12MB of application data.
    // However, we expect that the backpressure mechanism kicks in after ~1MB.
    // Using many smaller contracts as opposed to just a few very big ones, allows us to better test
    // the boundary around the flow control window, as various buffers up and down the application stack (pekko, netty, grpc, ...)
    // could lead to more data being sent from the applications point of view.
    val numContracts = 100
    val payloadSize = 50 * 1024

    participants.local.synchronizers.connect_local(sequencer1, daName)
    participants.local.dars.upload(CantonExamplesPath)
    val alice = participant1.parties.enable("alice", synchronizeParticipants = Seq.empty)
    val bobObserver = participant2.parties.enable("bob", synchronizeParticipants = Seq.empty)

    // Disconnect participant2 so that it has to catch up later.
    participant2.synchronizers.disconnect_all()

    // Create a bunch of IOUs with a chunky payload, so that we quickly trigger the backpressure mechanism
    clue("creating IOUs") {
      (1 to numContracts).foreach { i =>
        val createCmds = new iou.Iou(
          alice.toProtoPrimitive,
          alice.toProtoPrimitive,
          new iou.Amount(i.toBigDecimal, Random.nextString(payloadSize)),
          Seq(bobObserver.toProtoPrimitive).asJava,
        ).create().commands().asScala.toSeq
        // Submit commands asynchronously, so that the IOU creation phase is a bit shorter.
        participant1.ledger_api.javaapi.commands
          .submit_async(
            Seq(alice),
            createCmds,
            synchronizer1Id,
          )
          .discard
      }

      eventually() {
        participant1.ledger_api.state.acs.of_party(alice) should have size (numContracts.toLong)
      }
    }

    // Set up the sequencer client of participant2 to block on processing the first received event.
    // This should eventually trigger the backpressure mechanism in the sequencer.
    val releaseClient = Promise[Unit]()
    val participant2SequencerClientInterceptor = DelayedSequencerClient
      .delayedSequencerClient(this.getClass.getSimpleName, daId, participant2.id.uid.toString)
      .value
    participant2SequencerClientInterceptor.setDelayPolicy { _ =>
      DelayedSequencerClient.DelayUntil(releaseClient.future)
    }

    // Helper to get the sequencer's messageSentSize metric for the subscription.
    def getMessageSentSize: Long = sequencer1.metrics
      .get_histogram(
        sequencer1.underlying.value.sequencer.metrics.grpcMetrics._1.messagesSentSize.info.name
          .toString(),
        Map(
          "grpc_service_name" -> "com.digitalasset.canton.sequencer.api.v30.SequencerService",
          "grpc_method_name" -> "Subscribe",
          "component" -> "sequencer",
        ),
      )
      .sum
      .toLong

    // Stop other nodes so that the metrics doesn't get interfered with.
    participant1.stop()
    mediator1.stop()

    // Remember the starting point of the metric before participant2 reconnects.
    val startingPoint = getMessageSentSize

    // Reconnect the participant, which starts the "blocked/slow" subscription.
    participant2.synchronizers.reconnect_all()

    eventually() {
      always() {
        val bytesSent = (getMessageSentSize - startingPoint)
        // The sequencer should send at least as many bytes as the flow control window allows.
        // If that's not the case, then the test data is too small and no actual backpressure test takes place.
        bytesSent should be >= NettyChannelBuilder.DEFAULT_FLOW_CONTROL_WINDOW.toLong
        // Exceeding the default flow control window size is okay, mostly because we're measuring the
        // bytes sent in the metrics interceptor and not what actually made it over the wire.
        // But this is actually good, because if the metric doesn't move anymore, it means that the backpressure
        // mechanism has kicked in and the application layer doesn't try to send more messages that would be buffered
        // by netty on the sequencer.
        bytesSent should be < (NettyChannelBuilder.DEFAULT_FLOW_CONTROL_WINDOW * 2L)
      }
    }

    logger.info(
      s"Bytes sent to $participant2 until backpressure kicked in: ${getMessageSentSize - startingPoint}"
    )

    // Release the client processing
    releaseClient.success(())

    // Check that participant2 eventually receives all contracts
    eventually() {
      participant2.ledger_api.state.acs
        .of_party(bobObserver, limit = PositiveInt.tryCreate(numContracts))
        .size shouldBe numContracts
    }
    // Sanity check: the payload in total should be much more than the default flow control window for the
    // test to make sense.
    val allBytesSentToParticipant2 = getMessageSentSize - startingPoint
    logger.info(s"Total bytes sent to $participant2: ${getMessageSentSize - startingPoint}")
    allBytesSentToParticipant2 should be > (NettyChannelBuilder.DEFAULT_FLOW_CONTROL_WINDOW * 2L)
  }

}

class SequencerBackpressureIntegrationTestPostgres extends SequencerBackpressureIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
