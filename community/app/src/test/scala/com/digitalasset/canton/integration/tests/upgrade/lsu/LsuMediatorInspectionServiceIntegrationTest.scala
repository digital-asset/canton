// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax.testIou
import com.digitalasset.canton.integration.{EnvironmentDefinition, TestConsoleEnvironment}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.mediator.admin.v30.VerdictResult.{
  VERDICT_RESULT_ACCEPTED,
  VERDICT_RESULT_REJECTED,
}
import com.digitalasset.canton.protocol.LocalRejectError.TimeRejects
import com.digitalasset.canton.synchronizer.sequencer.SendDecision.{HoldBack, Process}
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencerPolicies,
  SendPolicy,
}
import org.slf4j.event.Level

import java.time.Duration
import java.util.concurrent.LinkedBlockingQueue
import scala.annotation.nowarn
import scala.concurrent.{Future, Promise}

abstract class LsuMediatorInspectionServiceIntegrationTest
    extends LsuBase
    with HasProgrammableSequencer {

  override protected lazy val newOldSequencers = Map("sequencer2" -> "sequencer1")
  override protected lazy val newOldMediators = Map("mediator2" -> "mediator1")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(300)

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S2M2_Config
      .withNetworkBootstrap(implicit env => new NetworkBootstrapper(EnvironmentDefinition.S1M1))
      .addConfigTransforms(configTransforms*)
      .withSetup(implicit env => defaultEnvironmentSetup())

  /** Asynchronously submit a transaction that only involves participant1 for simpler
    * response/verdict handling.
    *
    * @return
    *   The record time in a `Future` after the processing of the transaction completed.
    */
  protected def submitSingleCreateAsync(
      amount: Double
  )(implicit env: TestConsoleEnvironment): Future[CantonTimestamp] = {
    import env.*
    val iouCommand =
      testIou(participant1.adminParty, participant1.adminParty, amount)
        .create()
        .commands()
        .loneElement
    Future(
      CantonTimestamp
        .fromInstant(
          participant1.ledger_api.javaapi.commands
            .submit(
              actAs = Seq(participant1.adminParty),
              commands = Seq(iouCommand),
            )
            .getRecordTime
        )
        .value
    )
  }

  // blocking queue used to hold back confirmation responses with the programmable sequencer
  private val confirmationResponses = new LinkedBlockingQueue[Promise[Unit]]()

  protected def holdBackConfirmationResponses()(implicit env: TestConsoleEnvironment): Unit = {
    assert(
      confirmationResponses.isEmpty,
      "There were still some leftover responses unhandled from a previous test case",
    )
    getProgrammableSequencer(env.sequencer1.name).setPolicy_("hold back confirmation responses")(
      SendPolicy.processTimeProofs_ { submissionRequest =>
        if (ProgrammableSequencerPolicies.isConfirmationResponse(submissionRequest)) {
          val p = Promise[Unit]()
          confirmationResponses.add(p)
          HoldBack(p.future)
        } else {
          Process
        }
      }
    )
  }

  private def mediator(implicit env: TestConsoleEnvironment) =
    env.mediator1.underlying.value.replicaManager.mediatorRuntime.value.mediator

  protected def takeNextResponsePromise(): Promise[Unit] = confirmationResponses.take()

  protected def numVerdictsInStore()(implicit env: TestConsoleEnvironment): Long =
    mediator.state.finalizedResponseStore.count().futureValueUS

  protected def highestRecordTime()(implicit environment: TestConsoleEnvironment): CantonTimestamp =
    mediator.state.finalizedResponseStore
      .highestRecordTime()
      .futureValueUS
      .getOrElse(CantonTimestamp.MinValue)
}

/** Tests that after upgrade time, the mediator inspection service on the old mediator sends a
  * Complete message and closes the stream.
  *
  * Scenario:
  *   - set up an LSU
  *   - create a confirmation request that is never confirmed
  *   - create another confirmation request that is confirmed
  *   - neither verdict should be returned yet by the inspection service, as the first blocks the
  *     second
  *   - pass the upgrade time, which times out the first request
  *   - query the mediator inspection service on the old mediator
  *   - observe that both requests are now returned, followed by a Complete message and the stream
  *     is closed
  */
final class LsuMediatorVerdictsUnconfirmedAndConfirmedIntegrationTest
    extends LsuMediatorInspectionServiceIntegrationTest {
  override protected def testName: String = "lsu-mediator-verdicts-unconfirmed-and-confirmed"

  "LSU upgrade" should {
    "cause the verdicts stream on the old mediator to be flushed and completed" in { implicit env =>
      import env.*

      val fixture = fixtureWithDefaults()

      clue("set up LSU")(performSynchronizerNodesLsu(fixture))

      holdBackConfirmationResponses()

      val nextFromTimestamp =
        withClue("submit single transaction, and read the verdict from the stream") {
          val preSubmitRecordTime = highestRecordTime()
          val recordTimeF = submitSingleCreateAsync(1)
          takeNextResponsePromise().success(())
          val recordTime = recordTimeF.futureValue
          val verdict =
            mediator1.inspection.verdicts_until_complete(preSubmitRecordTime, 1).loneElement
          verdict.verdict.value.recordTime.value shouldBe recordTime.toProtoTimestamp
          verdict.verdict.value.verdict shouldBe VERDICT_RESULT_ACCEPTED
          recordTime
        }

      val (unconfirmedSubmissionF, unconfirmedSubmissionP, confirmedSubmissionF) =
        withClue("create an unconfirmed request, followed by a confirmed one") {
          val numVerdictsAtStart = numVerdictsInStore()

          val unconfirmedSubmissionF = submitSingleCreateAsync(2)
          val unconfirmedSubmissionP = takeNextResponsePromise()

          // Get  nothing, as transaction hasn't been confirmed yet
          mediator1.inspection.verdicts_until_complete(
            nextFromTimestamp,
            1,
            timeout = NonNegativeDuration.ofSeconds(1),
          ) shouldBe empty

          numVerdictsInStore() shouldBe numVerdictsAtStart

          // Create another transaction, that is confirmed
          val confirmedSubmissionF = submitSingleCreateAsync(3)
          takeNextResponsePromise().success(())

          eventually() {
            numVerdictsInStore() should be > numVerdictsAtStart
          }

          // Still empty, as we're blocked behind the previous unconfirmed transaction
          mediator1.inspection.verdicts_until_complete(
            nextFromTimestamp,
            1,
            timeout = NonNegativeDuration.ofSeconds(1),
          ) shouldBe empty

          (unconfirmedSubmissionF, unconfirmedSubmissionP, confirmedSubmissionF)
        }

      val confirmedRecordTime = withClue("move past LSU upgrade time") {
        loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
          environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor),
          logs => {
            forExactly(1, logs)(_.shouldBeCantonErrorCode(TimeRejects.LocalTimeout))
            forExactly(1, logs)(
              _.warningMessage should include regex "Response message for request .* timed out at"
            )
          },
        )
        transferTraffic()

        unconfirmedSubmissionP.success(()) // Complete it to not interfere with shutdown

        eventually() {
          environment.simClock.value.advance(Duration.ofSeconds(1))
          participants.all.forall(_.synchronizers.is_connected(fixture.newPsid)) shouldBe true
        }

        // Failed due to lack of confirmation response within timeout
        unconfirmedSubmissionF.failed.value.value.toOption.value shouldBe a[CommandFailure]
        // Succeeded now
        confirmedSubmissionF.futureValue
      }

      withClue("expected the stream from the old mediator to be completed") {
        val verdicts = mediator1.inspection.verdicts_until_complete(nextFromTimestamp, 10)
        verdicts should have size 3 // Requested 10 but closed after 3
        val Seq(verdict1, verdict2, completed) = verdicts: @unchecked

        verdict1.verdict.value.verdict shouldBe VERDICT_RESULT_REJECTED // timed out without confirmation

        verdict2.verdict.value.verdict shouldBe VERDICT_RESULT_ACCEPTED
        verdict2.verdict.value.recordTime.value shouldBe confirmedRecordTime.toProtoTimestamp

        completed.complete.value.reason.passedLsuTime.value.upgradeTime.value shouldBe upgradeTime.toProtoTimestamp
      }

      withClue("new transactions are on the *new* physical synchronizer, so no more verdicts") {
        val preSubmitTs = highestRecordTime()

        submitSingleCreateAsync(4).futureValue

        // We just get the Complete message
        mediator1.inspection
          .verdicts_until_complete(
            preSubmitTs,
            2,
            timeout = NonNegativeDuration.ofSeconds(1),
          )
          .loneElement
          .complete
          .isDefined shouldBe true
      }

      withClue("deprecated .verdicts is callable but returns nothing after upgrade time") {
        (mediator1.inspection.verdicts(
          upgradeTime,
          1,
          timeout = NonNegativeDuration.ofSeconds(1),
        ): @nowarn) shouldBe empty
      }
    }
  }
}

/** Tests that the stream is closed successfully even in the case of no synchronizer activity.
  */
final class LsuMediatorVerdictsSilentSynchronizerIntegrationTest
    extends LsuMediatorInspectionServiceIntegrationTest {
  override protected def testName: String = "lsu-mediator-verdicts-silent-synchronizer"

  "LSU upgrade" should {
    "completes verdicts even when there was no other activity leading to upgrade time" in {
      implicit env =>
        import env.*

        val fixture = fixtureWithDefaults()
        clue("set up LSU")(performSynchronizerNodesLsu(fixture))

        val preLsuRecordTime = highestRecordTime()

        withClue("get verdicts is empty when nothing has happened") {
          mediator1.inspection.verdicts_until_complete(
            preLsuRecordTime,
            2,
            timeout = NonNegativeDuration.ofSeconds(1),
          ) shouldBe empty
        }

        val verdictsF = withClue("start a get_verdicts call before LSU") {
          Future(
            mediator1.inspection.verdicts_until_complete(
              preLsuRecordTime,
              2,
              timeout = NonNegativeDuration.ofSeconds(60),
            )
          )
        }

        withClue("move past LSU record time") {
          environment.simClock.foreach(_.advanceTo(upgradeTime.immediateSuccessor))
          transferTraffic()
          eventually() {
            environment.simClock.foreach(_.advance(Duration.ofSeconds(1)))
            participants.all.forall(_.synchronizers.is_connected(fixture.newPsid)) shouldBe true
          }
        }

        withClue(
          "that future started before LSU should have now returned a single completed message"
        ) {
          verdictsF.futureValue.loneElement.complete.value.reason.passedLsuTime.value.upgradeTime.value shouldBe upgradeTime.toProtoTimestamp
        }

        withClue("expected the stream from the old mediator to be completed") {
          val verdicts = mediator1.inspection.verdicts_until_complete(preLsuRecordTime, 2)
          verdicts should have size 1 // Requested 2 but closed after 1
          val completed = verdicts.head: @unchecked

          completed.complete.value.reason.passedLsuTime.value.upgradeTime.value shouldBe upgradeTime.toProtoTimestamp
        }

        withClue("new transactions are on the *new* physical synchronizer, so no more verdicts") {
          val preSubmitTs = highestRecordTime()

          submitSingleCreateAsync(4).futureValue

          // We just get the Complete message
          mediator1.inspection
            .verdicts_until_complete(
              preSubmitTs,
              2,
              timeout = NonNegativeDuration.ofSeconds(1),
            )
            .loneElement
            .complete
            .isDefined shouldBe true
        }
    }
  }
}
