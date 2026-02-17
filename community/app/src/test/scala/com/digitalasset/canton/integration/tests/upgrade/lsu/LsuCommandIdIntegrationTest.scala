// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseProgrammableSequencer}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.integration.{EnvironmentDefinition, TestEnvironment}
import com.digitalasset.canton.ledger.error.groups.ConsistencyErrors.DuplicateCommand
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality.Optional
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors
import com.digitalasset.canton.protocol.LocalRejectError.TimeRejects
import com.digitalasset.canton.sequencing.protocol.SubmissionRequest
import com.digitalasset.canton.synchronizer.sequencer.{HasProgrammableSequencer, SendDecision}
import com.digitalasset.canton.topology.PartyId
import com.google.rpc.Code

import scala.concurrent.Promise
import scala.jdk.CollectionConverters.CollectionHasAsScala

/** Test that command ids that failed during an LSU can be re-used after the LSU is complete.
  *
  * We test three cases:
  *   - Announce LSU, submit cmd, mediator drops confirmation REQUEST, LSU completes, re-use cmd id
  *   - Announce LSU, submit cmd, mediator drops confirmation RESPONSE, LSU completes, re-use cmd id
  *   - Announce LSU, upgrade time reached, submit cmd, LSU completes, re-use cmd id
  */
final class LsuCommandIdIntegrationTest extends LsuBase with HasProgrammableSequencer {
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  override protected def testName: String = "lsu-command-deduplication"

  override protected lazy val newOldSequencers = Map("sequencer2" -> "sequencer1")
  override protected lazy val newOldMediators = Map("mediator2" -> "mediator1")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(300)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S2M2_Config
      .withNetworkBootstrap(implicit env => new NetworkBootstrapper(EnvironmentDefinition.S1M1))
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        defaultEnvironmentSetup()
      }

  "Logical synchronizer upgrade" should {
    "allow the command ID of a command that failed during LSU to be used successfully post LSU" in {
      implicit env =>
        import env.*

        val fixture = fixtureWithDefaults()
        val alice = participant1.parties.enable("Alice")
        val bank = participant1.parties.enable("Bank")

        val createIouCmd = IouSyntax.testIou(bank, alice, 42).create().commands().asScala.toSeq
        val decisionTimeout =
          participant1.topology.synchronizer_parameters.latest(daId).decisionTimeout
        val progSeq = getProgrammableSequencer(sequencer1.name)

        def submitWithDropAndAwaitTimeoutFailure(
            cmdId: String,
            shouldDrop: SubmissionRequest => Boolean,
            errMsgAssertion: String => Unit,
        ): Unit =
          loggerFactory.assertLogs(
            {
              val wasDropped = Promise[Unit]()
              progSeq.setPolicy_(s"drop a message for command $cmdId") { req =>
                if (shouldDrop(req)) {
                  logger.info(s"Dropping a message for $cmdId")
                  wasDropped.success(())
                  SendDecision.Drop
                } else {
                  SendDecision.Process
                }
              }

              val offsetBeforeSubmit = participant1.ledger_api.state.end()
              participant1.ledger_api.javaapi.commands.submit_async(
                Seq(bank),
                createIouCmd,
                commandId = cmdId,
              )
              wasDropped.future.futureValue.discard // wait for the drop to actually happen

              progSeq.resetPolicy() // The damage has been done

              // Move simulated time forward far enough for the command to timeout
              environment.simClock.value.advance(decisionTimeout.plusSeconds(1).asJava)

              // Observe that the command failed
              assertCommandEventuallyFailed(
                cmdId = cmdId,
                sinceOffset = offsetBeforeSubmit,
                party = bank,
                errMsgAssertion,
              )
            },
            _.warningMessage should (
              (include regex "Response message for request .* timed out at") or
                include("Submission timed out at")
            ),
          )

        // Kick everything off
        performSynchronizerNodesLsu(fixture)

        val cmdIdDroppedConfReq = "cmd-id-dropped-conf-req"
        withClue("test with dropped confirmation request") {
          submitWithDropAndAwaitTimeoutFailure(
            cmdId = cmdIdDroppedConfReq,
            shouldDrop = _.isConfirmationRequest,
            _ should (
              include(TimeRejects.LocalTimeout.id) or
                include(SubmissionErrors.TimeoutError.id)
            ),
          )
        }

        val cmdIdDroppedConfResp = "cmd-id-dropped-conf-resp"
        withClue("test with dropped confirmation response") {
          submitWithDropAndAwaitTimeoutFailure(
            cmdId = cmdIdDroppedConfResp,
            shouldDrop = _.isConfirmationResponse,
            _ should (include(TimeRejects.LocalTimeout.id) or include(MediatorError.Timeout.id)),
          )
        }

        val cmdIdAtUpgradeTime = "cmd-id-at-upgrade-time"
        withClue("test command at upgrade time") {
          loggerFactory.assertLogsUnorderedOptional(
            {
              // Move to upgrade time so that command submissions will fail due to overlap with LSU
              environment.simClock.value.advanceTo(upgradeTime)

              val offsetBeforeSubmit = participant1.ledger_api.state.end()
              participant1.ledger_api.javaapi.commands
                .submit_async(Seq(bank), createIouCmd, commandId = cmdIdAtUpgradeTime)

              // Move forward until the new PSId is up
              environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)
              waitForTargetTimeOnSequencer(sequencer2, environment.clock.now)
              eventually() {
                participants.all.forall(_.synchronizers.is_connected(fixture.newPSId)) shouldBe true
              }
              oldSynchronizerNodes.all.stop()

              // Move further forward until this decision timeout has expired, and expect to see our submission timeout on the completion stream
              environment.simClock.value.advance(decisionTimeout.plusSeconds(1).asJava)
              participant1.health.ping(participant1) // To notify the sequencer that time has passed

              assertCommandEventuallyFailed(
                cmdIdAtUpgradeTime,
                offsetBeforeSubmit,
                bank,
                _ should (
                  include(TimeRejects.LocalTimeout.id) or
                    include(SubmissionErrors.TimeoutError.id)
                ),
              )
            },
            Optional -> (_.warningMessage should (include regex "Response message for request .* timed out at")),
            Optional -> (_.warningMessage should include("Submission timed out at")),
            Optional -> (_.warningMessage should include("Time validation has failed")),
          )
        }

        withClue(
          "now that LSU has completed, verify the failed command ids may be used once each"
        ) {
          Seq(cmdIdDroppedConfReq, cmdIdDroppedConfResp, cmdIdAtUpgradeTime).foreach { cmdId =>
            participant1.ledger_api.javaapi.commands
              .submit(Seq(bank), createIouCmd, commandId = cmdId)

            assertThrowsAndLogsCommandFailures(
              participant1.ledger_api.javaapi.commands
                .submit(Seq(bank), createIouCmd, commandId = cmdId),
              _.shouldBeCantonErrorCode(DuplicateCommand),
            )
          }
        }
    }
  }

  private def assertCommandEventuallyFailed(
      cmdId: String,
      sinceOffset: Long,
      party: PartyId,
      assertErrorMessage: String => Unit,
  )(implicit env: TestEnvironment): Unit = {
    val status = env.participant1.ledger_api.completions
      .list(
        party,
        atLeastNumCompletions = 1,
        beginOffsetExclusive = sinceOffset,
        filter = (_.commandId == cmdId),
      )
      .loneElement
      .status
      .value
    status.code should not be Code.OK
    assertErrorMessage(status.message)
  }
}
