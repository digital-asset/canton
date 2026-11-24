// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.reliability

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Reliability.{
  AdverseScenario,
  Component,
  ReliabilityTest,
  Remediation,
}
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.ConfigTransforms
import com.digitalasset.canton.integration.plugins.{UseConfigTransforms, UsePostgres}
import com.digitalasset.canton.integration.tests.ReplicatedNodeHelper
import com.digitalasset.canton.integration.tests.performance.BasePerformanceIntegrationTest
import com.digitalasset.canton.logging.LogEntry
import monocle.Monocle.toAppliedFocusOps
import org.scalatest.Assertion

import scala.concurrent.duration.DurationInt
import scala.util.Try

class RestartParticipantPerformanceIntegrationTest
    extends ReliabilityPerformanceIntegrationTest
    with BasePerformanceIntegrationTest
    with ReplicatedNodeHelper {

  override protected def numPermits: PositiveInt = PositiveInt.two

  "restart the participants while the runners are running".taggedAs(
    ReliabilityTest(
      Component(name = "Ping application", setting = "connected to non-replicated participant"),
      dependencyFailure = AdverseScenario(
        dependency = "Participant",
        details = "Orderly restart (`.stop()` and `.start()`)",
      ),
      Remediation(
        remediator = "Application",
        action = "Application should retry commands until successful",
      ),
      outcome =
        "transaction processing possible whenever the participant is not currently restarted",
    )
  ) in { implicit env =>
    import env.*
    runWithFault(
      participant1.name,
      participant1.config.ledgerApi.port,
      withPartyGrowth = 1, // use party growth to add topology mgmt crash
    ) { callback =>
      def restart(): Unit = {
        val (done, _) = callback()
        if (!done) {
          clue("stopping participant") {
            // As the participant is loaded, this can fail with an exception.
            // Catching the exception so that the test does not fail.
            Try(participant1.stop())
          }
          clue("starting participant") {
            participant1.start()
          }
          clue("waiting for participant to be running again") {
            participant1.health.wait_for_running()
          }
          val (_, observed) = callback()
          clue("waiting for participant to be active") {
            waitActive(participant1)
          }
          clue("reconnect to synchronizers") {
            // start without synchronisation of topology state, as the performance runner will keep on adding new
            // parties, so we'll never get a quiet topology state
            // TODO(#9367) smarter topology synchronisation
            participant1.synchronizers.reconnect_all(synchronize = None)
          }
          // wait until all runners have progressed again
          eventually(timeUntilSuccess = 200.seconds) {
            val (done, observedNew) = callback()
            if (done) succeed
            else {
              if (observed.lengthIs != observedNew.length) {
                logger.error(
                  "Observed length changed after restart. This is unexpected and may indicate a problem with the test."
                )
              }
              forAll(observedNew.zip(observed)) { case (newValue, oldValue) =>
                newValue should be > oldValue + 6 withClue s"Observed value did not progress after restart: $observedNew vs $observed"
              }
            }
          }
        }
      }

      // restart this beast 3 times
      (1 to 3).foreach(_ => restart())
    }

  }

  // Whitelisting all errors and warnings, as the list of problems that can be reported during shutdown is too long.
  override protected def acceptableErrorOrWarning(entry: LogEntry): Assertion = succeed

  setupPlugins(new UsePostgres(loggerFactory))

  registerPlugin(
    new UseConfigTransforms(
      Seq(
        ConfigTransforms.setStorageQueueSize(10000),
        _.focus(_.parameters.timeouts.processing.shutdownShort)
          .replace(NonNegativeDuration.tryFromDuration(15.seconds)), // from 3 seconds
      ),
      loggerFactory,
    )
  )
}
