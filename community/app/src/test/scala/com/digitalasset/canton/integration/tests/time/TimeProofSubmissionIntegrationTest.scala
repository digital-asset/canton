// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.time

import com.daml.metrics.api.MetricQualification
import com.digitalasset.canton.integration.plugins.UseProgrammableSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.metrics.{MetricsConfig, MetricsReporterConfig}
import com.digitalasset.canton.sequencing.protocol.SequencerErrors.Overloaded
import com.digitalasset.canton.synchronizer.sequencer.{HasProgrammableSequencer, SendDecision}
import com.digitalasset.canton.{UniquePortGenerator, config}
import monocle.macros.syntax.lens.*

import scala.concurrent.Promise

/** Check that time proofs are correctly retried and tracked.
  *
  * In this test, we check that bouncing time proofs don't create a memory leak in the send tracker
  * and get properly cleaned up. This was a bug caused by our convoluted SequencerClient / send
  * logic with all its amplifications and retries.
  *
  * Maybe, in 2028, when the sequencer client has been rewritten, we might delete this test again.
  */
class TimeProofSubmissionIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      // required to assert for metrics
      .addConfigTransforms(
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
      .withSetup { implicit env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, daName)
        participant1.synchronizers.modify(
          daName,
          // ensure that the time tracker will retry often so we get a chance to trigger
          _.focus(_.timeTracker.timeRequest.initialRetryDelay)
            .replace(config.NonNegativeFiniteDuration.ofMillis(1))
            .focus(_.timeTracker.timeRequest.maxRetryDelay)
            .replace(config.NonNegativeFiniteDuration.ofMillis(10)),
        )
      }

  "reject time proof synchronously" in { implicit env =>
    import env.*

    val sequencer = getProgrammableSequencer(sequencer1.name)
    val sync = participant1.underlying.value.sync.connectedSynchronizerForAlias(daName).value

    val rejected = Promise[Unit]()
    sequencer.setPolicy_("reject time proofs") { submissionRequest =>
      if (submissionRequest.messageId.unwrap.startsWith("tick-")) {
        rejected.trySuccess(())
        SendDecision.Reject(
          error = Overloaded("Time proof rejected by test policy.")
        )
      } else SendDecision.Process
    }

    val res = sync.timeTracker.fetchTimeProof()

    // wait for time proof to be rejected first
    rejected.future.flatMap { _ =>
      // once rejected, reset the sequencer policy
      sequencer.resetPolicy()
      // then wait for the time proof to be sent successfully
      res.map(_ => ()).onShutdown(())
    }.futureValue // wait for the future to complete

    // once complete, check that the number of in-flight requests is zero
    // using eventually to avoid flakes in case we have some background noise
    // happening
    eventually() {
      val inFlight =
        participant1.metrics
          .get_long_point(
            "daml.sequencer-client.submissions.in-flight",
            Map("type" -> "time-proof"),
          )
          .value
      assertResult(0)(inFlight)
    }

  }

}
