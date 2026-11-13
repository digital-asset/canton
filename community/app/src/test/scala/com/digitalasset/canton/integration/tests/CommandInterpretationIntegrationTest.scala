// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.{BaseTest, config}
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.*

// This test tests that the daml engine is able to abort a submission if it takes too
// much time during interpretation.
sealed trait CommandInterpretationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs_(
          // Increase granularity of interruptions so that the (short) Ping command gets
          // some `Result.Need.Interruption`s during interpretation before completing.
          // We have had to previously lower this setting to 50 after the engine had
          // become more efficient and needed fewer cycles for a ping.
          _.focus(_.parameters.engine.iterationsBetweenInterruptions).replace(50)
        )
      )

  "a submitted command" should {
    "be aborted if it takes too much time during interpretation" in { implicit env =>
      import env.*

      val previousTolerance = ledgerTimeRecordTimeTolerance()

      // Reduce time tolerance such that the Ping has no chance to compute within limits
      setLedgerTimeRecordTimeTolerance(config.NonNegativeFiniteDuration.Zero)

      participant1.start()
      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant2.start()
      participant2.synchronizers.connect_local(sequencer1, alias = daName)

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          participant1.health.maybe_ping(
            participant2,
            timeout = 10.seconds,
          ) shouldBe empty
          // As in ping-service the timeout and the retries are parallel we need to wait until participant1 is stopped,
          // so for sure no submission failures escape the log-suppression boundary.
          participant1.stop()
        },
        LogEntry.assertLogSeq(
          Seq(
            (
              _.warningMessage should (include("INTERPRETATION_TIME_EXCEEDED") and include(
                "tolerance (0s)"
              )),
              "ping",
            )
          )
        ),
      )

      // The environment is shared: restore the tolerance so that later setups can succeed
      setLedgerTimeRecordTimeTolerance(previousTolerance)
    }
  }

  "an infinite loop" should {
    "be aborted in due time and not cause a stack overflow" in { implicit env =>
      import env.*

      // This test uses a Daml template with a choice that runs an infinite loop

      participant1.start()
      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant1.dars.upload(BaseTest.CantonTestsPath)

      val alice = participant1.parties.testing.enable("Alice")

      val moduleName = "Loop"
      val templateName = "Helper"
      val loopPackageId =
        participant1.packages.find_by_module(moduleName).headOption.value.packageId

      // Create a Helper contract
      val createHelperCmd =
        ledger_api_utils.create(loopPackageId, moduleName, templateName, Map("p" -> alice))

      participant1.ledger_api.commands.submit(Seq(alice), Seq(createHelperCmd))
      val helperId = participant1.testing
        .acs_search(daName, filterTemplate = s"$moduleName:$templateName")
        .loneElement
        .contractId

      // Set time tolerance to 3 seconds so that an infinite loop will trigger a stack overflow if not handled properly.
      // Lower it only now: the max sequencing time of a submission is its ledger time plus this tolerance,
      // so the setup above could otherwise time out before being sequenced.
      setLedgerTimeRecordTimeTolerance(config.NonNegativeFiniteDuration.tryFromDuration(3.seconds))

      // Exercise the "infinite loop" choice
      val exerciseLoopCmd =
        ledger_api_utils.exercise(
          loopPackageId,
          moduleName,
          templateName,
          "Loop",
          Map.empty,
          helperId.coid,
        )

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.ledger_api.commands.submit(Seq(alice), Seq(exerciseLoopCmd)),
        _.warningMessage should (include("INTERPRETATION_TIME_EXCEEDED") and include(
          "tolerance (3s)"
        )),
        _.errorMessage should include("Request failed for participant1"),
      )
    }
  }

  private def ledgerTimeRecordTimeTolerance()(implicit
      env: TestConsoleEnvironment
  ): config.NonNegativeFiniteDuration = {
    import env.*
    sequencer1.topology.synchronizer_parameters
      .get_dynamic_synchronizer_parameters(daId)
      .ledgerTimeRecordTimeTolerance
  }

  private def setLedgerTimeRecordTimeTolerance(
      tolerance: config.NonNegativeFiniteDuration
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    sequencer1.topology.synchronizer_parameters
      .set_ledger_time_record_time_tolerance(synchronizerId = daId, tolerance)
  }
}

//class CommandInterpretationIntegrationTestH2 extends CommandInterpretationIntegrationTest {
//  registerPlugin(new UseH2(loggerFactory))
//  registerPlugin(new UseBftSequencer(loggerFactory))
//}
class CommandInterpretationReferenceIntegrationTestPostgres
    extends CommandInterpretationIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
