// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.traffic

import cats.syntax.either.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.BigDecimalImplicits.DoubleToBigDecimal
import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnections,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{DbConfig, SynchronizerTimeTrackerConfig}
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.data.DeduplicationPeriod.DeduplicationOffset
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.examples.java.iou.{Amount, GetCash}
import com.digitalasset.canton.integration.plugins.{
  UseH2,
  UsePostgres,
  UseProgrammableSequencer,
  UseReferenceBlockSequencer,
}
import com.digitalasset.canton.integration.tests.TrafficBalanceSupport
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.security.SecurityTestLensUtils
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
  TrafficTestUtils,
}
import com.digitalasset.canton.ledger.error.groups.ConsistencyErrors.DuplicateCommand
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors
import com.digitalasset.canton.sequencing.protocol.{MediatorGroupRecipient, SequencerErrors}
import com.digitalasset.canton.synchronizer.sequencer.{HasProgrammableSequencer, SendDecision}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.{SequencerAlias, config}
import org.scalatest.Assertion
import org.slf4j.event.Level

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.annotation.nowarn
import scala.concurrent.{Await, Future}

/** Test how the traffic cost of transactions is exposed on LAPI (completion and update streams)
  */
@nowarn("msg=match may not be exhaustive")
sealed trait TrafficCostOnLAPITest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils
    with HasProgrammableSequencer
    with SecurityTestLensUtils
    with TrafficBalanceSupport {

  // Hosted on P1
  private var alice: PartyId = _
  // Hosted on P2
  private var bob: PartyId = _
  // Hosted on P1
  private var charlie: PartyId = _

  private val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()

  override def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .withSetup { implicit env =>
        import env.*
        val synchronizerConfig = SynchronizerConnectionConfig(
          daName,
          SequencerConnections.single(
            sequencer1.sequencerConnection.withAlias(SequencerAlias.tryCreate(sequencer1.name))
          ),
          manualConnect = false,
          Some(sequencer1.physical_synchronizer_id),
          // Disable time proofs to avoid messing with the traffic state in the background
          timeTracker = SynchronizerTimeTrackerConfig(
            config.NonNegativeFiniteDuration.ofDays(200L),
            config.NonNegativeFiniteDuration.ofDays(200L),
            config.NonNegativeFiniteDuration.ofDays(200L),
          ),
        )
        participants.local.foreach { participant =>
          participant.start()
          participant.health.wait_for_running()
          participant.synchronizers.connect_by_config(synchronizerConfig)
          participant.dars.upload(CantonExamplesPath)
        }

        sequencer1.topology.synchronizer_parameters.propose_update(
          synchronizerId = daId,
          _.update(
            // Lower confirmation response timeout so tests emitting a mediator verdict finish faster
            confirmationResponseTimeout = config.NonNegativeFiniteDuration.ofSeconds(2),
            // Shorten the ledgerTimeRecordTimeTolerance so that the max sequencing time is also short
            // to trigger completion timeouts quickly
            ledgerTimeRecordTimeTolerance = config.NonNegativeFiniteDuration.ofSeconds(2),
          ),
        )

        pureCryptoRef.set(sequencer1.crypto.pureCrypto)

        alice = participant1.parties.enable("alice")
        bob = participant2.parties.enable("bob")
        charlie = participant1.parties.enable("charlie")
      }
      .withTrafficControl(
        TrafficTestUtils.predictableTraffic,
        topUpAllMembers = true,
        disableCommitments = true,
      )

  // Runs test assertion, restarts all participants, and re-rerun the same assertions
  // to make sure everything still works with persistence through the DB
  private def assertWithRestart(f: String => Assertion)(implicit env: TestConsoleEnvironment) = {
    import env.*

    f("before restart")
    Await
      .result(
        participants.local.parTraverse_ { p =>
          Future {
            p.stop()
            p.start()
            p.synchronizers.reconnect_all()
            utils.retry_until_true(
              p.synchronizers.is_connected(synchronizer1Id)
            )
          }
        },
        timeouts.default.asFiniteApproximation,
      )
      .discard
    f("after restart")
  }

  "ledger API completions" should {
    "show correct transaction cost for an accepted transaction" in { implicit env =>
      import env.*

      val iou =
        IouSyntax.testIou(alice, bob, observers = List(charlie)).create().commands().loneElement
      val ledgerEndP2 = participant2.ledger_api.state.end()
      val transaction = participant1.ledger_api.javaapi.commands.submit(Seq(alice), Seq(iou))
      val recordTime = CantonTimestamp.fromInstant(transaction.getRecordTime).value

      // Get the expected cost through the sequencer traffic API
      val expectedTrafficCost =
        sequencer1.traffic_control.traffic_summaries(Seq(recordTime)).loneElement.totalTrafficCost
      expectedTrafficCost should be > 0L

      // While we're at it, check that the participant's last consumed cost also matches the expected cost
      participant1.traffic_control
        .traffic_state(synchronizer1Id)
        .lastConsumedCost
        .value shouldBe expectedTrafficCost

      assertWithRestart { cluePrefix =>
        val completion =
          participant1.ledger_api.completions.list(alice, 1, transaction.getOffset - 1).loneElement

        withClue(s"[$cluePrefix] completion has expected traffic") {
          // Completion should have the expected cost
          completion.paidTrafficCost shouldBe expectedTrafficCost
        }

        // Check that no completion is emitted on P2
        participant2.ledger_api.completions.list(
          bob,
          1,
          ledgerEndP2,
          timeout = config.NonNegativeDuration.ofSeconds(2),
        ) shouldBe empty

        // Also check that we get no completion for charlie, who was not an actAs party
        participant1.ledger_api.completions.list(
          charlie,
          1,
          transaction.getOffset - 1,
          timeout = config.NonNegativeDuration.ofSeconds(2),
        ) shouldBe empty
      }
    }

    "emit completions with cost for all actAs parties" in { implicit env =>
      import env.*

      // exercise call - has both alice and bob as signatories and actAs
      val getCashCreate = new GetCash(
        alice.toProtoPrimitive,
        charlie.toProtoPrimitive,
        new Amount(10d.toBigDecimal, "USD"),
      ).create().commands().loneElement

      val transaction =
        participant1.ledger_api.javaapi.commands.submit(Seq(alice, charlie), Seq(getCashCreate))
      val recordTime = CantonTimestamp.fromInstant(transaction.getRecordTime).value

      // Get the expected cost through the sequencer traffic API
      val expectedTrafficCost =
        sequencer1.traffic_control.traffic_summaries(Seq(recordTime)).loneElement.totalTrafficCost
      expectedTrafficCost should be > 0L

      val trafficState = participant1.traffic_control.traffic_state(synchronizer1Id)
      trafficState.lastConsumedCost.value shouldBe expectedTrafficCost

      assertWithRestart { cluePrefix =>
        Seq(alice, charlie).map { party =>
          val completion =
            participant1.ledger_api.completions
              .list(party, 1, transaction.getOffset - 1)
              .loneElement

          withClue(s"[$cluePrefix] completion for $party has expected traffic and timestamp") {
            // Completion should have the expected cost
            completion.paidTrafficCost shouldBe expectedTrafficCost
            completion.synchronizerTime.value.recordTime.value shouldBe trafficState.timestamp.toProtoTimestamp
          }
        }.last
      }
    }

    "show correct transaction cost for a transaction ordered but not sequenced (deliver error)" in {
      implicit env =>
        import env.*

        val seq1 = getProgrammableSequencer("sequencer1")

        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          seq1.withSendPolicy_(
            "replace mediator group",
            {
              case sr if sr.isConfirmationRequest =>
                SendDecision.Replace(
                  signModifiedSubmissionRequestForParticipant(
                    submissionRequestRecipients
                      .andThen(mediatorGroupRecipient)
                      // Replace the mediator group by a non-existing one
                      .modify(_ => MediatorGroupRecipient(NonNegativeInt.tryCreate(12)))(sr),
                    participant1.underlying.value,
                    daId,
                    staticSynchronizerParameters1,
                  )
                )
              case _ => SendDecision.Process
            },
          )({
            val iou = IouSyntax.testIou(alice, bob).create().commands().loneElement
            val ledgerEnd = participant1.ledger_api.state.end()

            // The submission should fail
            Either
              .catchOnly[CommandFailure](
                participant1.ledger_api.javaapi.commands.submit(Seq(alice), Seq(iou))
              )
              .isLeft shouldBe true

            // We can't get the traffic cost via the sequencer API here because the event did not get sequenced
            // successfully, so use the participant traffic API, as the participant still received a traffic receipt
            // for the failed sequenced event
            val trafficState = participant1.traffic_control.traffic_state(synchronizer1Id)
            val expectedTrafficCost = trafficState.lastConsumedCost.value
            expectedTrafficCost should be > 0L

            assertWithRestart { cluePrefix =>
              withClue(s"[$cluePrefix] completion has correct traffic cost") {
                // Observe the completion on P1, and assert it's the expected failure
                val completion =
                  participant1.ledger_api.completions.list(alice, 1, ledgerEnd).loneElement
                completion.status.value.message should include(
                  SequencerErrors.SubmissionRequestRefused.id
                )

                // Make sure the cost is correct
                completion.paidTrafficCost shouldBe expectedTrafficCost
                completion.synchronizerTime.value.recordTime.value shouldBe trafficState.timestamp.toProtoTimestamp
              }
            }
          }),
          LogEntry.assertLogSeq(
            Seq(
              (
                _.shouldBeCantonErrorCode(SequencerErrors.SubmissionRequestRefused.code),
                "expected submission request refused",
              )
            )
          ),
        )
    }

    "show transaction cost of 0 for a transaction rejected before sequencing" in { implicit env =>
      import env.*

      val cycle = createCycleCommand(alice, UUID.randomUUID().toString)
      val commandId = UUID.randomUUID().toString
      val ledgerEnd = participant1.ledger_api.state.end()
      participant1.ledger_api.commands.submit(
        Seq(alice),
        Seq(cycle),
        commandId = commandId,
        deduplicationPeriod = Some(
          DeduplicationOffset(Some(com.digitalasset.canton.data.Offset.tryFromLong(ledgerEnd)))
        ),
      )
      participant1.ledger_api.commands.submit_async(Seq(alice), Seq(cycle), commandId = commandId)

      assertWithRestart { cluePrefix =>
        withClue(s"[$cluePrefix] completion has 0 traffic cost") {
          val Seq(_, reject) = participant1.ledger_api.completions.list(alice, 2, ledgerEnd)
          reject.status.value.code shouldBe DuplicateCommand.category.grpcCode.value.value()
          reject.status.value.message should include(
            "Command submission already exists"
          )

          // The cost of the rejected transaction from command dedup should be 0 as it should not have been sequenced
          reject.paidTrafficCost shouldBe 0L
        }
      }

    }

    "show transaction cost of 0 for a transaction timing out on the sequencer" in { implicit env =>
      import env.*

      val seq1 = getProgrammableSequencer("sequencer1")

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        seq1.withSendPolicy_(
          "drop confirmation requests",
          {
            case sr if sr.isConfirmationRequest => SendDecision.Drop
            case _ => SendDecision.Process
          },
        )({
          val iou = IouSyntax.testIou(alice, bob).create().commands().loneElement
          val ledgerEnd = participant1.ledger_api.state.end()

          participant1.ledger_api.javaapi.commands.submit_async(Seq(alice), Seq(iou))

          // We disabled time proofs in this test suite but here we'll need them
          // for the participant to realize the request has been dropped
          // and it needs to timeout the transaction to emit the completion.
          // So request time proofs in the background until we receive the completion
          val requestTimeProofs = new AtomicBoolean(true)
          def requestTimeProof(): FutureUnlessShutdown[Unit] = if (requestTimeProofs.get()) {
            env.environment.clock
              .scheduleAfter(
                _ =>
                  participant1.underlying.flatTraverse(
                    _.sync
                      .connectedSynchronizerForAlias(daName)
                      .traverse(
                        _.ephemeral.timeTracker
                          .fetchTimeProof()
                      )
                  ),
                java.time.Duration.ofSeconds(1),
              )
              .flatten
              .flatMap(_ => requestTimeProof())
          } else FutureUnlessShutdown.unit

          val requestTimeProofF = requestTimeProof()

          assertWithRestart { cluePrefix =>
            withClue(s"[$cluePrefix] completion has 0 traffic cost") {
              // Observe the completion on P1, and assert it's the expected failure
              val completion =
                try {
                  participant1.ledger_api.completions.list(alice, 1, ledgerEnd).loneElement
                } finally {
                  // Stop requesting time proofs even if we didn't get a completion above
                  requestTimeProofs.set(false)
                }
              completion.status.value.message should include(SubmissionErrors.TimeoutError.id)

              // Traffic cost is 0 because we couldn't sequence the confirmation request
              completion.paidTrafficCost shouldBe 0L
            }
          }

          requestTimeProofF.futureValueUS
        }),
        LogEntry.assertLogSeq(
          Seq(
            (
              _.warningMessage should include("Submission timed out"),
              "expected submission timeout error",
            )
          )
        ),
      )
    }

    "show correct transaction cost of a transaction rejected after sequencing" in { implicit env =>
      import env.*

      val iou = IouSyntax.createIou(participant1)(alice, bob)
      val iouCall = iou.id.exerciseCall().commands().loneElement
      val ledgerEnd = participant2.ledger_api.state.end()
      // Stop p1 so that it can't confirm and the mediator issues a timeout verdict
      participant1.stop()
      // Exercise the choice through P2
      participant2.ledger_api.javaapi.commands.submit_async(Seq(bob), Seq(iouCall))

      loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.WARN))(
        assertWithRestart { cluePrefix =>
          withClue(s"[$cluePrefix] completion has correct traffic cost") {
            // Wait to observe the completion, which should be a timeout reject
            val completion = participant2.ledger_api.completions.list(bob, 1, ledgerEnd).loneElement
            completion.status.value.message should include(MediatorError.Timeout.id)

            val expectedTrafficCost =
              sequencer1.traffic_control
                .traffic_summaries(Seq(completion.synchronizerTime.value.recordTime.value))
                .loneElement
                .totalTrafficCost

            // make sure the cost is correct
            completion.paidTrafficCost should be > 0L
            completion.paidTrafficCost shouldBe expectedTrafficCost
          }
        },
        LogEntry.assertLogSeq(
          Seq.empty,
          // P1 may log a warning when it gets restarted because it missed a confirmation response
          Seq(
            (
              _.warningMessage should include("Response message for request"),
            )
          ),
        ),
      )
    }
  }
}

class TrafficCostOnLAPITestH2 extends TrafficCostOnLAPITest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}

class TrafficCostOnLAPITestPostgres extends TrafficCostOnLAPITest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
