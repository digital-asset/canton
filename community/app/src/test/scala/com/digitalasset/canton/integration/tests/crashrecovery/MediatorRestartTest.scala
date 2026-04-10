// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.crashrecovery

import com.digitalasset.canton.admin.api.client.data.DynamicSynchronizerParameters
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseProgrammableSequencer}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.synchronizer.sequencer.{HasProgrammableSequencer, SendDecision}
import com.digitalasset.canton.topology.Party
import com.digitalasset.canton.util.Mutex
import com.digitalasset.canton.{CloseableTest, HasExecutionContext}
import monocle.syntax.all.*
import org.slf4j.event.Level

import scala.concurrent.*

/** Checks the crash-recovery behavior of the mediator, in particular making sure that all verdicts
  * eventually get persisted even when the mediator is being restarted before a final response is
  * available.
  */
final class MediatorRestartTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with CloseableTest
    with HasProgrammableSequencer
    with HasExecutionContext {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  private var alice: Party = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransform(ConfigTransforms.useStaticTime)
      .addConfigTransform(
        ConfigTransforms.updateAllSequencerClientConfigs_(
          // Set a very small maximumInFlightEventBatches to make sure the mediator does not deadlock
          _.focus(_.maximumInFlightEventBatches).replace(PositiveInt.two)
        )
      )
      .withSetup { implicit env =>
        import env.*
        participants.all.foreach { p =>
          p.synchronizers.connect_local(sequencer1, daName)
          p.dars.upload(CantonExamplesPath)
        }

        alice = participant1.parties.testing.enable("alice")
      }

  "mediator" should {
    "recover from crash and provide deterministic verdict responses for all requests" in {
      implicit env =>
        import env.*

        val processCR = Promise[Unit]()
        val caughtCR = Promise[Unit]()

        val seq = getProgrammableSequencer("sequencer1")
        seq.withSendPolicy_(
          "catch confirmation response",
          {
            case message if message.isConfirmationResponse && caughtCR.trySuccess(()) =>
              SendDecision.HoldBack(processCR.future)
            case _ => SendDecision.Process
          },
        )({
          val ledgerEnd = participant1.ledger_api.state.end()
          participant1.ledger_api.javaapi.commands.submit_async(
            Seq(alice),
            Seq(IouSyntax.testIou(alice, alice).create().commands().loneElement),
          )
          caughtCR.future.futureValue
          mediator1.stop()
          mediator1.start()
          mediator1.health.wait_for_running()
          processCR.trySuccess(())
          val completion = participant1.ledger_api.completions.list(alice, 1, ledgerEnd).loneElement
          completion.status.value.code shouldBe io.grpc.Status.OK.getCode.value()
          val recordTime = CantonTimestamp
            .fromProtoTimestamp(completion.synchronizerTime.value.recordTime.value)
            .value
          mediator1.inspection.verdicts(
            recordTime.immediatePredecessor,
            PositiveInt.one,
            timeout = NonNegativeDuration.ofSeconds(5),
          ) should have size 1
        })
    }

    "handle multiple requests with out-of-order finalization" in { implicit env =>
      import env.*

      val processResp1 = Promise[Unit]()
      val processResp2 = Promise[Unit]()
      val caughtResp1 = Promise[Unit]()
      val caughtResp2 = Promise[Unit]()

      val seq = getProgrammableSequencer("sequencer1")
      seq.withSendPolicy_(
        "catch confirmation responses",
        {
          case message if message.isConfirmationResponse && caughtResp1.trySuccess(()) =>
            SendDecision.HoldBack(processResp1.future)
          case message if message.isConfirmationResponse && !caughtResp2.isCompleted =>
            caughtResp2.trySuccess(())
            SendDecision.HoldBack(processResp2.future)
          case _ => SendDecision.Process
        },
      )({
        val ledgerEnd = participant1.ledger_api.state.end()

        // Submit two commands in quick succession
        participant1.ledger_api.javaapi.commands.submit_async(
          Seq(alice),
          Seq(IouSyntax.testIou(alice, alice).create().commands().loneElement),
        )
        caughtResp1.future.futureValue

        participant1.ledger_api.javaapi.commands.submit_async(
          Seq(alice),
          Seq(IouSyntax.testIou(alice, alice).create().commands().loneElement),
        )
        caughtResp2.future.futureValue

        // Release second response first (out of order finalization)
        processResp2.trySuccess(())

        // Wait until there's only one pending request left (Req1)
        eventually()(
          mediator1.underlying.value.replicaManager.mediatorRuntime.value.mediator.state.getPendingRequestsSize shouldBe 1
        )

        // Crash - we should still not have advanced the prehead because we're missing Req1 finalization
        mediator1.stop()
        mediator1.start()
        mediator1.health.wait_for_running()

        // Release first response - both should now complete
        processResp1.trySuccess(())

        val completions = participant1.ledger_api.completions.list(alice, 2, ledgerEnd)
        completions should have size 2
        completions.foreach { c =>
          c.status.value.code shouldBe io.grpc.Status.OK.getCode.value()
        }
      })
    }

    "timeout request if crashed and restarted before response deadline expires" in { implicit env =>
      import env.*

      val caughtCR = Promise[Unit]()

      val seq = getProgrammableSequencer("sequencer1")
      seq.withSendPolicy_(
        "catch confirmation response",
        {
          case message if message.isConfirmationResponse && caughtCR.trySuccess(()) =>
            SendDecision.Drop
          case _ => SendDecision.Process
        },
      )({
        loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.WARN))(
          {
            val ledgerEnd = participant1.ledger_api.state.end()
            participant1.ledger_api.javaapi.commands.submit_async(
              Seq(alice),
              Seq(IouSyntax.testIou(alice, alice).create().commands().loneElement),
            )
            caughtCR.future.futureValue

            // Crash and restart
            mediator1.stop()
            mediator1.start()
            mediator1.health.wait_for_running()

            env.environment.simClock.value.advance(
              DynamicSynchronizerParameters
                .initialValues(testedProtocolVersion)
                .confirmationResponseTimeout
                .plusSeconds(1)
                .asJava
            )

            // Don't release the response - let it timeout
            eventually() {
              val completion =
                participant1.ledger_api.completions.list(alice, 1, ledgerEnd).loneElement
              // Should time out due to no response (mediator times out and sends rejection)
              completion.status.value.message should include(MediatorError.Timeout.id)
            }

            // Verify the timeout verdict was stored
            val recordTime = CantonTimestamp
              .fromProtoTimestamp(
                participant1.ledger_api.completions
                  .list(alice, 1, ledgerEnd)
                  .loneElement
                  .synchronizerTime
                  .value
                  .recordTime
                  .value
              )
              .value
            mediator1.inspection
              .verdicts(
                recordTime.immediatePredecessor,
                PositiveInt.one,
                timeout = NonNegativeDuration.ofSeconds(5),
              )
              .loneElement
              .verdict
              .isVerdictResultRejected shouldBe true
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.warningMessage should (include("Response message for request") and include(
                  "timed out at"
                )),
                "expected timeout warning",
              )
            )
          ),
        )

      })
    }

    "handle many concurrent unfinalized requests without deadlocking" in { implicit env =>
      import env.*

      def getNumberOfPendingRequests =
        mediator1.underlying.value.replicaManager.mediatorRuntime.value.mediator.state.getPendingRequestsSize

      val mutex = Mutex()

      // Hold back all confirmation responses to create many concurrent in-flight requests
      val responsePromises = scala.collection.mutable.ArrayBuffer[Promise[Unit]]()

      val seq = getProgrammableSequencer("sequencer1")
      seq.withSendPolicy_(
        "catch all confirmation responses",
        {
          case message if message.isConfirmationResponse =>
            val promise = Promise[Unit]()
            mutex.exclusive {
              responsePromises.append(promise)
            }
            SendDecision.HoldBack(promise.future)
          case _ => SendDecision.Process
        },
      )({
        val ledgerEnd = participant1.ledger_api.state.end()

        // Submit 20 commands concurrently, that should be plenty enough to trigger a deadlock if
        // the implementation is wrong as the parallel event bach processing is set to 2
        val numCommands = 20
        (1 to numCommands).foreach { _ =>
          participant1.ledger_api.javaapi.commands.submit_async(
            Seq(alice),
            Seq(IouSyntax.testIou(alice, alice).create().commands().loneElement),
          )
        }

        // Wait until all responses are caught
        eventually()(
          mutex.exclusive(responsePromises should have size numCommands.toLong)
        )

        // At this point, we have numCommands in-flight requests waiting for their finalizedPromise
        // Verify all requests are pending
        getNumberOfPendingRequests shouldBe numCommands

        // Now release all responses and verify all complete successfully
        mutex.exclusive {
          responsePromises.foreach(_.trySuccess(()))
        }

        val completions = participant1.ledger_api.completions.list(alice, numCommands, ledgerEnd)
        assert(completions.sizeIs == numCommands)
        completions.foreach { c =>
          c.status.value.code shouldBe io.grpc.Status.OK.getCode.value()
        }

        // Verify all requests were finalized (no longer pending)
        eventually() {
          getNumberOfPendingRequests shouldBe 0
        }
      })
    }
  }
}
