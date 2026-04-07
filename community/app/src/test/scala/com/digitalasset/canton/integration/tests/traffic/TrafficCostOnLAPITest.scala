// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.traffic

import cats.syntax.either.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.{
  TRANSACTION_SHAPE_ACS_DELTA,
  TRANSACTION_SHAPE_LEDGER_EFFECTS,
}
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  TransactionShape,
  WildcardFilter,
}
import com.daml.ledger.javaapi.data.CreatedEvent
import com.digitalasset.canton.BigDecimalImplicits.DoubleToBigDecimal
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.UpdateWrapper
import com.digitalasset.canton.admin.api.client.data.SynchronizerConnectionConfig
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{DbConfig, SynchronizerTimeTrackerConfig}
import com.digitalasset.canton.console.{CommandFailure, ParticipantReference}
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.damltests.java.universal.UniversalContract
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
import com.digitalasset.canton.integration.util.TestUtils
import com.digitalasset.canton.integration.util.UpdateFormatHelpers.getUpdateFormat
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
import org.slf4j.event.Level

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.annotation.nowarn
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters.*

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
          sequencerConnections = sequencer1,
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
          participant.dars.upload(CantonTestsPath)
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
        TestUtils.waitForTargetTimeOnSynchronizerNode(wallClock.now, logger),
        TrafficTestUtils.predictableTraffic,
        topUpAllMembers = true,
        disableCommitments = true,
      )

  // Runs test assertion, restarts all participants, and re-rerun the same assertions
  // to make sure everything still works with persistence through the DB
  private def assertWithRestart(f: String => Unit)(implicit env: TestConsoleEnvironment) = {
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

  private def assertCostFromTransactionWrapper(update: UpdateWrapper, expectedCost: Option[Long]) =
    inside(update) { case UpdateService.TransactionWrapper(transaction) =>
      transaction.paidTrafficCost shouldBe expectedCost
    }

  private def assertUpdateCost(
      participant: ParticipantReference,
      party: Option[PartyId],
      offset: Long,
      shape: TransactionShape,
      expectedCost: Option[Long],
      updateId: String,
      cluePrefix: String,
  ) = {
    val updateFormat = getUpdateFormat(party.toList.toSet, transactionShape = shape)
    // From stream
    withClue(s"[$cluePrefix] transaction cost from $shape update stream") {
      assertCostFromTransactionWrapper(
        participant.ledger_api.updates
          .updates(
            updateFormat,
            completeAfter = PositiveInt.one,
            beginOffsetExclusive = offset,
          )
          .loneElement,
        expectedCost,
      )
    }

    // Pointwise lookup
    withClue(s"[$cluePrefix] transaction cost from pointwise lookup") {
      assertCostFromTransactionWrapper(
        participant.ledger_api.updates
          .update_by_id(
            updateId,
            updateFormat,
          )
          .value,
        expectedCost,
      )
    }
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

      // Check that the traffic cost returned from the submit command contains the correct cost
      transaction.getPaidTrafficCost shouldBe expectedTrafficCost

      // While we're at it, check that the participant's last consumed cost also matches the expected cost
      participant1.traffic_control
        .traffic_state(synchronizer1Id)
        .lastConsumedCost
        .value shouldBe expectedTrafficCost

      assertWithRestart { cluePrefix =>
        // Completion checks //
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

        // Update checks //
        // Test for both transaction shapes
        List(TRANSACTION_SHAPE_LEDGER_EFFECTS, TRANSACTION_SHAPE_ACS_DELTA).foreach { shape =>
          // Assert cost on updates from Alice's perspective on P1
          assertUpdateCost(
            participant1,
            Some(alice),
            transaction.getOffset - 1,
            shape,
            // Expect the cost to be available as alice is a submitting party and p1 is the submitting node
            Some(expectedTrafficCost),
            completion.updateId,
            s"$cluePrefix - alice",
          )

          // Assert cost on updates when reading as any party on P1
          assertUpdateCost(
            participant1,
            party = None,
            transaction.getOffset - 1,
            shape,
            // Expect the cost to be available
            Some(expectedTrafficCost),
            completion.updateId,
            s"$cluePrefix - any party",
          )

          // Assert cost on updates from Bob's perspective on P2
          assertUpdateCost(
            participant2,
            Some(bob),
            ledgerEndP2,
            shape,
            // Expect no cost as P2 is not the submitting node
            None,
            completion.updateId,
            s"$cluePrefix - bob",
          )

          // Assert cost on updates from Charlie's perspective on P1
          assertUpdateCost(
            participant1,
            Some(charlie),
            transaction.getOffset - 1,
            shape,
            None,
            completion.updateId,
            s"$cluePrefix - charlie",
          )
        }
      }
    }

    // This is tested specifically because there's a special case when grabbing the transaction from a completion
    // returns an empty transaction, which can happen for non-consuming transactions.
    // See CommandServiceImpl.fetchTransactionFromCompletion
    "show correct transaction cost for an accepted transaction with only a non-consuming choice" in {
      implicit env =>
        import env.*

        val universalCreate = new UniversalContract(
          List(alice.toProtoPrimitive).asJava,
          List.empty.asJava,
          List.empty.asJava,
          List(alice.toProtoPrimitive).asJava,
        ).create.commands.loneElement

        val universal = participant1.ledger_api.javaapi.commands
          .submit(Seq(alice), Seq(universalCreate), includeCreatedEventBlob = true)
        val nonConsumingExercise = inside(universal.getEvents.loneElement) {
          case created: CreatedEvent =>
            UniversalContract.Contract
              .fromCreatedEvent(created)
              .id
              .exerciseTouch(List(bob.toProtoPrimitive).asJava)
              .commands()
              .loneElement
        }
        val transaction =
          participant1.ledger_api.javaapi.commands.submit(Seq(alice), Seq(nonConsumingExercise))
        val recordTime = CantonTimestamp.fromInstant(transaction.getRecordTime).value
        val expectedTrafficCost =
          sequencer1.traffic_control.traffic_summaries(Seq(recordTime)).loneElement.totalTrafficCost
        expectedTrafficCost should be > 0L

        // Check that the traffic cost returned from the submit command contains the correct cost
        transaction.getPaidTrafficCost shouldBe expectedTrafficCost
    }

    // This is an odd case but it's technically possible to call submitAndWaitForTransaction while passing an event filter
    // that filters for a non-submitting party (or any party for that matter).
    // In order to keep the desired semantics that the returned traffic cost is set to 0 when filtering for non submitting
    // parties, we test specifically for it here, as there's special handling for this scenario in CommandServiceImpl.fetchTransactionFromCompletion
    // TODO(i31269): This currently does not work as we can't easily differentiate between the fallback being called because
    // the transaction has only non-consuming events (see test above), or because the party filter excludes submitting parties
    // For now, the traffic cost is reported in both cases
    "show empty transaction cost for a submitAndWait with a party filter that does not contain the submitting party" ignore {
      implicit env =>
        import env.*

        val iou =
          IouSyntax.testIou(alice, bob).create().commands().loneElement
        val transaction = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          Seq(iou),
          customEventFormat = Some(
            EventFormat(
              filtersByParty = Map(
                // Filter for charlie, who's not even involved in the transaction
                charlie.toProtoPrimitive -> Filters(
                  Seq(
                    CumulativeFilter.defaultInstance.withWildcardFilter(
                      WildcardFilter(includeCreatedEventBlob = false)
                    )
                  )
                )
              ),
              filtersForAnyParty = None,
              verbose = false,
            )
          ),
        )

        // The paid traffic cost from charlie's perspective should be 0
        transaction.getPaidTrafficCost shouldBe 0L
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
        Seq(alice, charlie).foreach { party =>
          val completion =
            participant1.ledger_api.completions
              .list(party, 1, transaction.getOffset - 1)
              .loneElement

          withClue(s"[$cluePrefix] completion for $party has expected traffic and timestamp") {
            // Completion should have the expected cost
            completion.paidTrafficCost shouldBe expectedTrafficCost
            completion.synchronizerTime.value.recordTime.value shouldBe trafficState.timestamp.toProtoTimestamp
          }
        }
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
