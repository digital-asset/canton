// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.traffic

import anorm.SqlStringInterpolation
import com.daml.ledger.api.v2.transaction_filter.{EventFormat, Filters}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{AuthServiceConfig, PositiveFiniteDuration}
import com.digitalasset.canton.console.{
  CommandFailure,
  ExternalLedgerApiClient,
  LocalParticipantReference,
  ParticipantReference,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java.cycle.Cycle
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.http.json.v2.JsTrafficServiceCodecs.*
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.ledgerapi.DbLockingSupport
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.AuthStartupConfigSuppressionRule
import com.digitalasset.canton.integration.util.{PartiesAllocator, TestUtils, TrafficControlUtils}
import com.digitalasset.canton.ledger.error.CommonErrors.ServiceNotRunning
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.ledger.api.JwtTokenUtilities
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.platform.apiserver.services.command.TrafficEnforcementBackend
import com.digitalasset.canton.platform.config.{
  TrafficAccountingConfig,
  TrafficEnforcementServerConfig,
}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.tea.TrafficEnforcementErrors.{
  CorrelationConflict,
  InsufficientBalance,
  MultiPartySubmissionRejected,
  TrafficUpdateOutOfBound,
  TransientFailure,
  UnsafeToPrune,
}
import com.digitalasset.canton.tea.daml.java.canton.traffic.credit.TrafficCreditIssuer
import com.digitalasset.canton.tea.projection.{ActAsCategory, UnchargedReason}
import com.digitalasset.canton.tea.v1.{
  GetAccountResponse,
  PruneRequest,
  PruneResponse,
  UpdateAccountRequest,
  UpdateAccountResponse,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ExternalParty, PartyId}
import com.digitalasset.canton.util.ShowUtil.*
import io.circe.parser.decode
import io.circe.syntax.*
import io.grpc.Status
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion
import org.slf4j.event.Level.{DEBUG, INFO, WARN}

import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.{URI, URLEncoder}
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.DurationConverters.*

trait ParticipantTrafficEnforcementTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils
    with TeaDbTestHelpers {

  protected val teaServerName = "tea-server"

  protected var aliceE: ExternalParty = _
  protected var bobE: ExternalParty = _
  protected var eveE: ExternalParty = _

  // Charlie and Dan are on-purpose local parties to test traffic enforcement behavior of local submissions
  protected var charlie: PartyId = _
  protected var dan: PartyId = _

  /** Default participant config transforms to enable TEA with cost enforcement on. Subclasses can
    * add focused transforms on top for this.
    */
  protected def defaultTrafficEnforcementConfigTransforms: Seq[ConfigTransform] = Seq(
    enableTrafficEnforcement("participant1"),
    // Shorten network timeout so retries to the non-existent traffic service give up quickly
    _.focus(_.parameters.timeouts.processing.network)
      .replace(config.NonNegativeDuration.tryFromDuration(5.seconds)),
  )

  protected final def enableTrafficEnforcement(participantName: String): ConfigTransform =
    ConfigTransforms.updateParticipantConfig(participantName)(
      _.focus(_.trafficAccounting)
        .replace(
          TrafficAccountingConfig(
            enabled = true,
            enforceCostOnSubmissions = true,
            trafficEnforcementServer = TrafficEnforcementServerConfig.Internal(teaServerName),
          )
        )
        // TEA uses this as the pruning retention too; zero means tests can prune right up to now.
        .focus(_.init.ledgerApi.maxDeduplicationDuration)
        .replace(config.NonNegativeFiniteDuration.Zero)
    )

  protected def extraTrafficEnforcementConfigTransforms: Seq[ConfigTransform] = Seq.empty

  protected final def participantConfigTransforms: Seq[ConfigTransform] =
    defaultTrafficEnforcementConfigTransforms ++ extraTrafficEnforcementConfigTransforms

  protected final def configTransforms: Seq[ConfigTransform] =
    participantConfigTransforms ++ Seq(
      ConfigTransforms.enableInteractiveSubmissionTransforms,
      ConfigTransforms.useStaticTime,
      ConfigTransforms.updateAllSequencerConfigs_(
        _.focus(_.trafficConfig.pruningRetentionWindow)
          .replace(config.NonNegativeFiniteDuration.ofSeconds(5))
          .focus(_.trafficConfig.trafficPurchasedCacheSizePerMember)
          .replace(PositiveInt.one)
      ),
    )

  protected final def withPredictableTraffic(
      definition: EnvironmentDefinition
  ): EnvironmentDefinition =
    definition.withTrafficControl(
      TestUtils.waitForTargetTimeOnSynchronizerNode(wallClock.now, logger),
      trafficControlParameters = TrafficControlUtils.predictableTraffic,
      topUpAllMembers = true,
      disableCommitments = true,
    )

  override def environmentDefinition: EnvironmentDefinition =
    withPredictableTraffic(
      EnvironmentDefinition.P2_S1M1
        .addConfigTransforms(configTransforms*)
        .withSetup { implicit env =>
          import env.*
          participants.local.foreach { participant =>
            participant.synchronizers.connect_local(sequencer1, alias = daName)
            participant.dars.upload(CantonExamplesPath, synchronizerId = daId)
          }

          aliceE = participant1.parties.testing.external.enable("Alice")
          bobE = participant1.parties.testing.external.enable("Bob")
          eveE = participant1.parties.testing.external.enable("Eve")

          charlie = participant1.parties.enable("Charlie")
          dan = participant1.parties.enable("Dan")
        }
    )

  protected def assertUnimplemented(entry: LogEntry): Assertion =
    entry.message should include(Status.Code.UNIMPLEMENTED.toString)

  protected def balance(participant: LocalParticipantReference, accountId: String): Long =
    participant.ledger_api.traffic.get_account(accountId).balance

  protected def balance(participant: LocalParticipantReference, party: PartyId): Long =
    balance(participant, party.toProtoPrimitive)

  /** TEA deletes the correlation in the same transaction that charges the completion. */
  protected def awaitCharged(
      participant: LocalParticipantReference,
      commandId: String,
      actAs: Set[PartyId],
  ): Unit =
    eventually() {
      correlationsFromDb(participant, changeIdHash(commandId, actAs)) shouldBe empty
    }
}

final class ParticipantTrafficEnforcementDisabledTest extends ParticipantTrafficEnforcementTest {
  override protected def extraTrafficEnforcementConfigTransforms: Seq[ConfigTransform] = Seq(
    ConfigTransforms.updateParticipantConfig("participant1")(
      _.focus(_.trafficAccounting.enabled).replace(false)
    )
  )

  "Participant" when {
    "traffic enforcement is disabled" should {
      "not expose the traffic service endpoints on the Ledger API" in { implicit env =>
        import env.*

        assertThrowsAndLogsCommandFailures(
          participant1.ledger_api.traffic.get_account(aliceE.partyId.toProtoPrimitive),
          assertUnimplemented,
        )

        assertThrowsAndLogsCommandFailures(
          participant1.ledger_api.traffic.update_account(
            aliceE.partyId.toProtoPrimitive,
            None,
          ),
          assertUnimplemented,
        )

        assertThrowsAndLogsCommandFailures(
          participant1.ledger_api.traffic.prune(wallClock.now),
          assertUnimplemented,
        )
      }
    }

    "support interactive submissions" in { implicit env =>
      import env.*

      // Pass some time to allow traffic re-fill the submission below
      environment.simClock.value.advance(Duration.ofSeconds(5L))

      // Prepare and execute should work seamlessly
      val prepared = participant1.ledger_api.interactive_submission.prepare(
        actAs = Seq(aliceE),
        commands = Seq(createCycleCommand(aliceE.partyId, "traffic")),
        hashingSchemeVersion = testedApiHashingSchemeVersion,
      )

      participant1.ledger_api.interactive_submission.execute_and_wait(
        prepared.getPreparedTransaction,
        Map(aliceE.partyId -> global_secret.sign(prepared.preparedTransactionHash, aliceE)),
        UUID.randomUUID().toString,
        prepared.hashingSchemeVersion,
      )
    }
  }
}

final class ParticipantTrafficEnforcementEnabledTest extends ParticipantTrafficEnforcementTest {
  registerPlugin(new UsePostgres(loggerFactory))

  "Participant" when {
    "traffic enforcement is enabled" should {
      "serve traffic service operations" in { implicit env =>
        import env.*

        val clock = env.environment.simClock.value
        val startTime = clock.now

        val alice = aliceE.partyId.toProtoPrimitive

        // Initially Alice has no balance
        balance(participant1, alice) shouldBe 0L

        val aliceBalance = 1_000_000L

        // Update Alice's balance
        participant1.ledger_api.traffic.update_account(alice, balanceDelta = Some(aliceBalance))

        // Check correct balance for Alice
        balance(participant1, alice) shouldBe aliceBalance

        clock.advance(1.microseconds.toJava)

        // Now deduct some traffic from Alice's account
        val deductAmount = 100_000L
        participant1.ledger_api.traffic.update_account(alice, balanceDelta = Some(-deductAmount))

        // Check correct balance for Alice after deduction
        balance(participant1, alice) shouldBe (aliceBalance - deductAmount)

        // Prune the first event
        val rsp = participant1.ledger_api.traffic
          .prune(startTime)

        rsp.prunedEventCount shouldBe 1

        val rsp2 = participant1.ledger_api.traffic
          .prune(startTime)

        rsp2.prunedEventCount shouldBe 0
      }

      "reject a prune inside the minimum pruning retention, with its own error code" in {
        implicit env =>
          import env.*

          // The retention limit is zero in this test, so only a future value will fail.
          val future = env.environment.simClock.value.now.plusSeconds(1)

          loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
            participant1.ledger_api.traffic.prune(future),
            entries =>
              inside(entries) { case Seq(clientEntry, consoleEntry) =>
                clientEntry.warningMessage should include(UnsafeToPrune.id)
                consoleEntry.shouldBeCantonErrorCode(UnsafeToPrune)
              },
          )
      }

      "reject an update_account with a delta that would take the balance out of bound, with its own error code" in {
        implicit env =>
          import env.*

          val charlieId = charlie.toProtoPrimitive
          val initialBalance = balance(participant1, charlieId)
          initialBalance shouldBe 0L

          loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
            participant1.ledger_api.traffic.update_account(charlieId, balanceDelta = Some(-1L)),
            entries =>
              inside(entries) { case Seq(clientEntry, consoleEntry) =>
                clientEntry.warningMessage should include(TrafficUpdateOutOfBound.id)
                consoleEntry.shouldBeCantonErrorCode(TrafficUpdateOutOfBound)

                clientEntry.mdc.get("trace-id") should not be empty
                clientEntry.mdc.get("span-parent-id") should not be empty
                clientEntry.mdc.get("span-name") shouldBe Some(
                  "com.digitalasset.canton.tea.v1.TrafficService/UpdateAccount"
                )
              },
          )

          // The rejected update must not have mutated the balance
          balance(participant1, charlieId) shouldBe initialBalance
      }

      "debit traffic from submitting party account" in { implicit env =>
        import env.*

        val aliceBalance = balance(participant1, aliceE.partyId)

        val iouCmd = IouSyntax.testIou(aliceE, aliceE, 10L).create().commands().asScala.toSeq
        val commandId = UUID.randomUUID().toString
        val transaction = participant1.ledger_api.javaapi.commands
          .submit(Seq(aliceE), iouCmd, commandId = commandId)

        // aliceE is external, so this covers the interactive execute path.
        awaitCharged(participant1, commandId, Set(aliceE.partyId))
        balance(participant1, aliceE.partyId) shouldBe aliceBalance - transaction.getPaidTrafficCost

        // Charlie is local, so this covers the classic path, with a generated submission id.
        participant1.ledger_api.traffic
          .update_account(charlie.toProtoPrimitive, balanceDelta = Some(1_000_000L))
        val charlieBalance = balance(participant1, charlie)
        val classicCommandId = UUID.randomUUID().toString
        val classicTransaction = participant1.ledger_api.commands
          .submit(
            actAs = Seq(charlie),
            commands = Seq(createCycleCommand(charlie, "traffic")),
            commandId = classicCommandId,
          )
        awaitCharged(participant1, classicCommandId, Set(charlie))
        balance(participant1, charlie) shouldBe
          charlieBalance - classicTransaction.paidTrafficCost.value
      }

      "reject submission for submitting party with not enough account balance" in { implicit env =>
        import env.*

        // Eve's initial balance should be 0
        balance(participant1, eveE.partyId) shouldBe 0L

        // Then, Eve's submission attempt fails
        val iouCreateCmds = IouSyntax.testIou(eveE, eveE, 10L).create().commands().asScala.toSeq
        assertThrowsAndLogsCommandFailures(
          participant1.ledger_api.javaapi.commands
            .submit(Seq(eveE), iouCreateCmds),
          entry => {
            entry.shouldBeCantonErrorCode(InsufficientBalance)
            entry.message should include regex raw".*Insufficient balance \(0\) for actual traffic cost \([1-9][0-9]*\) for account ${eveE.partyId.toProtoPrimitive}"
          },
        )

        // Top up Eve's account minimally
        participant1.ledger_api.traffic.update_account(
          eveE.partyId.toProtoPrimitive,
          balanceDelta = Some(1L),
        )

        // Check that Eve's balance is now 1
        balance(participant1, eveE.partyId) shouldBe 1L

        // Then, Eve's submission attempt fails again
        assertThrowsAndLogsCommandFailures(
          participant1.ledger_api.javaapi.commands
            .submit(Seq(eveE), iouCreateCmds),
          entry => {
            entry.shouldBeCantonErrorCode(InsufficientBalance)
            entry.message should include regex raw".*Insufficient balance \(1\) for actual traffic cost \([1-9][0-9]*\) for account ${eveE.partyId.toProtoPrimitive}"
          },
        )

        // Now let's assert that the submission also fails during 'execute' specifically
        // Give Eve some traffic
        participant1.ledger_api.traffic.update_account(
          eveE.partyId.toProtoPrimitive,
          balanceDelta = Some(100000L),
        )

        // Prepare should pass
        val prepared =
          participant1.ledger_api.javaapi.interactive_submission
            .prepare(Seq(eveE), iouCreateCmds, hashingSchemeVersion = testedApiHashingSchemeVersion)

        // Take the traffic away
        participant1.ledger_api.traffic.update_account(
          eveE.partyId.toProtoPrimitive,
          balanceDelta = Some(-100000L),
        )

        // execute_and_wait should fail
        // Note: the async 'execute' method does NOT fail because the balance check is performed in the async
        // part of the processing. Which means the clien only gets notified of the failure when receiving the completion
        assertThrowsAndLogsCommandFailures(
          participant1.ledger_api.interactive_submission.execute_and_wait(
            prepared.getPreparedTransaction,
            Map(eveE.partyId -> global_secret.sign(prepared.preparedTransactionHash, eveE)),
            UUID.randomUUID().toString,
            testedApiHashingSchemeVersion,
          ),
          entry => {
            entry.shouldBeCantonErrorCode(InsufficientBalance)
            entry.message should include regex raw".*Insufficient balance \(1\) for actual traffic cost \([1-9][0-9]*\) for account ${eveE.partyId.toProtoPrimitive}"
          },
        )
      }

      // TODO(#33681): Test the gRPC/JSON API variations in Ledger API conformance tests
      "serve traffic service operations via the JSON Ledger API" in { implicit env =>
        import env.*

        val port = participant1.config.httpLedgerApi.internalPort
          .valueOrFail("JSON API must be enabled")

        def httpGet(url: String): String = {
          val request = HttpRequest.newBuilder().uri(new URI(url)).GET().build()
          val response =
            HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())
          response.statusCode() shouldBe 200
          response.body()
        }

        def httpPost(url: String, jsonBody: String): String = {
          val request = HttpRequest
            .newBuilder()
            .uri(new URI(url))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
            .build()
          val response =
            HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())
          response.statusCode() shouldBe 200
          response.body()
        }

        val accountId = bobE.toProtoPrimitive
        val encodedAccount = URLEncoder.encode(accountId, StandardCharsets.UTF_8)
        val accountUrl = s"http://localhost:$port/v2/traffic/accounts/$encodedAccount"
        val accountsUrl = s"http://localhost:$port/v2/traffic/accounts"
        val pruneUrl = s"http://localhost:$port/v2/traffic/prune"

        def balance(): Long =
          decode[GetAccountResponse](httpGet(accountUrl)).value.balance

        def update(delta: Long): Long = {
          val requestBody = UpdateAccountRequest(
            accountId = accountId,
            balanceDelta = Some(delta),
            deduplicationId = UUID.randomUUID().toString,
          ).asJson.noSpaces
          decode[UpdateAccountResponse](
            httpPost(accountsUrl, requestBody)
          ).value.response.value.balance
        }

        def prune(beforeOrAt: CantonTimestamp): Int = {
          val requestBody = PruneRequest(
            beforeOrAt = Some(beforeOrAt.toProtoTimestamp)
          ).asJson.noSpaces
          decode[PruneResponse](
            httpPost(pruneUrl, requestBody)
          ).value.prunedEventCount
        }

        val clock = env.environment.simClock.value

        // Empty the events table, these tests do not seem to be independent
        prune(clock.now)

        // Initially the account has no balance
        balance() shouldBe 0L

        val credit = 1_000_000L
        update(credit) shouldBe credit
        balance() shouldBe credit

        val deductAmount = 100_000L
        update(-deductAmount) shouldBe (credit - deductAmount)
        balance() shouldBe (credit - deductAmount)

        prune(clock.now) shouldBe 2
      }

      "traffic enforcement does not apply for multi-act-as submissions" in { implicit env =>
        import env.*

        val actAs = Seq(charlie, dan)
        val commandId = UUID.randomUUID().toString
        val submissionId = UUID.randomUUID().toString

        // Charlie and Dan can submit a multi-party transaction without having any traffic balance
        val transaction = loggerFactory.assertLogsSeq(SuppressionRule.Level(INFO))(
          participant1.ledger_api.commands
            .submit(
              actAs = actAs,
              commands = Seq(createCycleCommand(charlie, "traffic")),
              commandId = commandId,
              submissionId = submissionId,
            ),
          forAtLeast(1, _) { entry =>
            entry.infoMessage should include(
              "Skipping traffic enforcement validation due to non-singleton actAs parties:"
            )
            forEvery(actAs)(party => entry.infoMessage should include(show"$party"))
          },
        )

        eventually() {
          unchargedFromDb(participant1, changeIdHash(commandId, actAs.toSet)) shouldBe Seq(
            (
              submissionId,
              transaction.paidTrafficCost.value,
              UnchargedReason.NoAccountSelected,
              ActAsCategory.MultipleActingParties,
            )
          )
          correlationsFromDb(participant1, changeIdHash(commandId, actAs.toSet)) shouldBe empty
        }
      }

      "debit a multi-party submission with an account_id from that command" in { implicit env =>
        import env.*

        val accountId = "acme/billing"
        participant1.ledger_api.traffic.update_account(accountId, balanceDelta = Some(1_000_000L))
        val accountBalance = balance(participant1, accountId)
        val charlieBalance = balance(participant1, charlie)
        val danBalance = balance(participant1, dan)

        val actAs = Seq(charlie, dan)
        val commandId = UUID.randomUUID().toString
        val transaction = participant1.ledger_api.commands.submit(
          actAs = actAs,
          commands = Seq(createCycleCommand(charlie, "traffic")),
          commandId = commandId,
          accountId = accountId,
        )

        awaitCharged(participant1, commandId, actAs.toSet)
        unchargedFromDb(participant1, changeIdHash(commandId, actAs.toSet)) shouldBe empty
        balance(participant1, accountId) shouldBe
          accountBalance - transaction.paidTrafficCost.value
        balance(participant1, charlie) shouldBe charlieBalance
        balance(participant1, dan) shouldBe danBalance
      }

      "check the prepare account_id and debit the execute account_id" in { implicit env =>
        import env.*

        val prepareAccount = "acme/prepare"
        val executeAccount = "acme/execute"
        participant1.ledger_api.traffic
          .update_account(executeAccount, balanceDelta = Some(1_000_000L))
        val aliceBalance = balance(participant1, aliceE.partyId)
        val executeBalance = balance(participant1, executeAccount)
        val commandId = UUID.randomUUID().toString

        def prepare() = participant1.ledger_api.interactive_submission.prepare(
          actAs = Seq(aliceE),
          commands = Seq(createCycleCommand(aliceE.partyId, "traffic")),
          commandId = commandId,
          hashingSchemeVersion = testedApiHashingSchemeVersion,
          accountId = prepareAccount,
        )

        // Preparation checks only the prepare account, not Alice's.
        assertThrowsAndLogsCommandFailures(
          prepare(),
          entry => {
            entry.shouldBeCantonErrorCode(InsufficientBalance)
            entry.message should include(s"for account $prepareAccount")
          },
        )

        participant1.ledger_api.traffic
          .update_account(prepareAccount, balanceDelta = Some(1_000_000L))
        val prepareBalance = balance(participant1, prepareAccount)
        val prepared = prepare()
        correlationsFromDb(
          participant1,
          changeIdHash(commandId, Set(aliceE.partyId)),
        ) shouldBe empty

        val transaction = participant1.ledger_api.interactive_submission
          .execute_and_wait_for_transaction(
            prepared.getPreparedTransaction,
            Map(aliceE.partyId -> global_secret.sign(prepared.preparedTransactionHash, aliceE)),
            UUID.randomUUID().toString,
            prepared.hashingSchemeVersion,
            accountId = executeAccount,
          )

        awaitCharged(participant1, commandId, Set(aliceE.partyId))
        balance(participant1, executeAccount) shouldBe
          executeBalance - transaction.paidTrafficCost.value
        balance(participant1, prepareAccount) shouldBe prepareBalance
        balance(participant1, aliceE.partyId) shouldBe aliceBalance
      }

      "skip the balance check for the admin party and reject a repeated submission id" in {
        implicit env =>
          import env.*

          val adminParty = participant1.adminParty

          def submitAsAdmin(commandId: String, submissionId: String) =
            participant1.ledger_api.javaapi.commands.submit(
              Seq(adminParty),
              Seq(createCycleCommandJava(adminParty, "traffic")),
              commandId = commandId,
              submissionId = submissionId,
            )

          // The admin party should start with no balance.
          balance(participant1, adminParty) shouldBe 0L

          // The submission succeeds even though the balance check would reject any other
          // party with no balance.
          val transaction = loggerFactory.assertLogsSeq(
            SuppressionRule.forLogger[TrafficEnforcementBackend] && SuppressionRule.Level(DEBUG)
          )(
            submitAsAdmin(UUID.randomUUID().toString, UUID.randomUUID().toString),
            forAtLeast(1, _) {
              _.debugMessage should include(
                show"Skipping traffic enforcement validation for participant admin party: ${adminParty.toLf}"
              )
            },
          )
          val cost = transaction.getPaidTrafficCost

          // The admin account is still debited through the completion stream.
          eventually() {
            balance(participant1, adminParty) shouldBe 0L - cost
          }

          // Interactive preparation is not rejected either.
          val preparedAdmin = participant1.ledger_api.interactive_submission.prepare(
            actAs = Seq(adminParty),
            commands = Seq(createCycleCommand(adminParty, "traffic-prepare")),
            hashingSchemeVersion = testedApiHashingSchemeVersion,
          )
          preparedAdmin.getPreparedTransaction should not be null

          val commandId = UUID.randomUUID().toString
          val submissionId = UUID.randomUUID().toString
          insertCorrelationIntoDb(
            participant1,
            changeIdHash(commandId, Set(adminParty)),
            submissionId,
            Some(adminParty.toProtoPrimitive),
          )
          assertThrowsAndLogsCommandFailures(
            submitAsAdmin(commandId, submissionId),
            _.shouldBeCantonErrorCode(CorrelationConflict),
          )
      }
    }

    "traffic enforcement is enabled but traffic enforcement server is not available" should {
      // TODO(#33681): Re-enable this test once we can test traffic enforcement enabled with a unavailable traffic enforcement server
      "return graceful errors on traffic and interactive submission service endpoints" ignore {
        implicit env =>
          import env.*

          def assertEntriesTeaUnavailable(entries: Seq[LogEntry]): Assertion =
            entries.foldLeft(succeed) { case (_, entry) =>
              entry.message should ((include(ServiceNotRunning.id) and include(
                "User traffic service is not running"
              )) or
                include("Retry timeout has elapsed, giving up."))
            }

          // GetAccount on P1 fails due to TEA not enabled
          loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
            participant1.ledger_api.traffic.get_account(aliceE.partyId.toProtoPrimitive),
            assertEntriesTeaUnavailable,
          )

          // UpdateAccount on P1 fails due to TEA not enabled
          loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
            participant1.ledger_api.traffic.update_account(
              aliceE.partyId.toProtoPrimitive,
              None,
            ),
            assertEntriesTeaUnavailable,
          )

          loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
            participant1.ledger_api.traffic.prune(wallClock.now),
            assertEntriesTeaUnavailable,
          )

          // Preparing on P1 fails due to TEA not enabled
          loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
            participant1.ledger_api.interactive_submission.prepare(
              actAs = Seq(aliceE),
              commands = Seq(createCycleCommand(aliceE.partyId, "traffic")),
              hashingSchemeVersion = testedApiHashingSchemeVersion,
            ),
            assertEntriesTeaUnavailable,
          )

          // Prepare a transaction on P2 for Alice (P2 does not have traffic enabled so we can prepare)
          val prepared = participant2.ledger_api.interactive_submission.prepare(
            actAs = Seq(aliceE),
            commands = Seq(createCycleCommand(aliceE.partyId, "traffic")),
            hashingSchemeVersion = testedApiHashingSchemeVersion,
          )

          // Executing on P1 fails due to TEA not enabled
          loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
            participant1.ledger_api.interactive_submission.execute_and_wait(
              prepared.getPreparedTransaction,
              Map(aliceE.partyId -> global_secret.sign(prepared.preparedTransactionHash, aliceE)),
              UUID.randomUUID().toString,
              prepared.hashingSchemeVersion,
              // Short timeout to reduce test time
              optTimeout = Some(5.seconds),
            ),
            _.foldLeft(succeed) { case (_, entry) =>
              entry.message should ((include(ServiceNotRunning.id) and include(
                "User traffic service is not running"
              )) or
                include("Retry timeout has elapsed, giving up.") or
                include("Failed to submit submission") or
                include("DEADLINE_EXCEEDED"))
            },
          )
      }
    }
  }
}

final class ParticipantTrafficEnforcementSubmissionDisabledTest
    extends ParticipantTrafficEnforcementTest {
  registerPlugin(new UsePostgres(loggerFactory))

  override protected def extraTrafficEnforcementConfigTransforms: Seq[ConfigTransform] = Seq(
    ConfigTransforms.updateParticipantConfig("participant1")(
      _.focus(_.trafficAccounting.enforceCostOnSubmissions)
        .replace(false)
        .focus(_.parameters.alphaVersionSupport)
        .replace(true)
    )
  )

  "Participant" when {
    "traffic enforcement is enabled but enforce-cost-on-submissions=false" should {
      "not upload the traffic credit package when top-up is disabled" in { implicit env =>
        import env.*

        participant1.packages
          .list()
          .map(_.packageId) should not contain TrafficCreditIssuer.PACKAGE_ID
      }

      "do not enforce traffic on submissions (allow negative balances) but still debit traffic from submitting party account" in {
        implicit env =>
          import env.*

          val alice = aliceE.partyId.toProtoPrimitive
          val initialAliceBalance = balance(participant1, alice)

          // Alice should have no balance initially
          initialAliceBalance shouldBe 0L

          val transaction = participant1.ledger_api.javaapi.commands.submit(
            Seq(aliceE),
            IouSyntax.testIou(aliceE, aliceE, 10L).create().commands().asScala.toSeq,
          )

          val cost = transaction.getPaidTrafficCost

          eventually() {
            // Alice should have negative balance
            balance(participant1, alice) shouldBe initialAliceBalance - cost
          }
      }
    }
  }
}

final class ParticipantTrafficEnforcementRejectMultiPartyTest
    extends ParticipantTrafficEnforcementTest {
  registerPlugin(new UsePostgres(loggerFactory))

  override protected def extraTrafficEnforcementConfigTransforms: Seq[ConfigTransform] = Seq(
    ConfigTransforms.updateParticipantConfig("participant1")(
      _.focus(_.trafficAccounting.rejectMultiPartySubmissions).replace(true)
    )
  )

  "Participant" when {
    "traffic enforcement is enabled with reject-multi-party-submissions=true" should {
      "reject a multi-act-as submission" in { implicit env =>
        import env.*

        val actAs = Seq(charlie, dan)

        assertThrowsAndLogsCommandFailures(
          participant1.ledger_api.commands
            .submit(
              actAs = actAs,
              commands = Seq(createCycleCommand(charlie, "traffic")),
            )
            .discard,
          entry => {
            entry.shouldBeCantonErrorCode(MultiPartySubmissionRejected)
            entry.message should include(
              show"Traffic enforcement rejected submission with non-singleton actAs parties: $actAs"
            )
          },
        )
      }

      "still validate a single-actAs submission normally" in { implicit env =>
        import env.*

        val charlieId = charlie.toProtoPrimitive
        participant1.ledger_api.traffic.update_account(charlieId, balanceDelta = Some(1_000_000L))
        val initialBalance = balance(participant1, charlieId)

        val transaction = participant1.ledger_api.javaapi.commands
          .submit(Seq(charlie), Seq(createCycleCommandJava(charlie, "traffic-single")))
        val cost = transaction.getPaidTrafficCost

        eventually() {
          balance(participant1, charlieId) shouldBe (initialBalance - cost)
        }
      }
    }
  }
}

/** Like [[ParticipantTrafficEnforcementEnabledTest]] but with a real LAPI auth service configured,
  * so [[com.digitalasset.canton.auth.TeaTokenAuthService]] is on the code path.
  */
final class ParticipantTrafficEnforcementWithAuthTest extends ParticipantTrafficEnforcementTest {
  private val jwtSecret = NonEmptyString.tryCreate("tea-auth-test-secret")

  registerPlugin(new UsePostgres(loggerFactory))

  override def beforeAll(): Unit =
    loggerFactory.suppress(AuthStartupConfigSuppressionRule) {
      super.beforeAll()
    }

  override protected def extraTrafficEnforcementConfigTransforms: Seq[ConfigTransform] = Seq(
    ConfigTransforms.updateParticipantConfig("participant1")(
      _.focus(_.ledgerApi.authServices)
        .replace(
          Seq(
            AuthServiceConfig.UnsafeJwtHmac256(
              secret = jwtSecret,
              targetAudience = None,
              targetScope = None,
            )
          )
        )
        // The admin token needs ClaimAdmin for: setup-time external party allocation, update_account, and users.create.
        .focus(_.ledgerApi.adminTokenConfig.adminClaim)
        .replace(true)
    )
  )

  "Participant" when {
    "traffic enforcement is enabled with a real LAPI auth service" should {
      "debit traffic from the submitting party, proving TeaTokenAuthService authorised TEA" in {
        implicit env =>
          import env.*

          val alice = aliceE.partyId.toProtoPrimitive

          // update_account requires ClaimAdmin. The admin token (adminClaim=true) satisfies that.
          participant1.ledger_api.traffic.update_account(alice, balanceDelta = Some(1_000_000L))

          val userId = participant1.ledger_api.users
            .create("alice-traffic-test-user", actAs = Set(aliceE.partyId))
            .id
          val client = ExternalLedgerApiClient.forReference(
            participant1,
            JwtTokenUtilities.buildUnsafeToken(jwtSecret.unwrap, userId = Some(userId)),
          )

          val balance = client.ledger_api.traffic.get_account(alice).balance
          balance shouldBe 1_000_000L

          val iouCmd = IouSyntax.testIou(aliceE, aliceE, 10L).create().commands().asScala.toSeq
          // The default event format uses filtersForAnyParty (wildcard), which requires ReadAsAnyParty.
          val eventFormat = EventFormat(
            filtersByParty = Map(alice -> Filters(Nil)),
            filtersForAnyParty = None,
            verbose = true,
          )
          val transaction = client.ledger_api.javaapi.commands.submit(
            Seq(aliceE),
            iouCmd,
            customEventFormat = Some(eventFormat),
          )
          val cost = transaction.getPaidTrafficCost

          eventually() {
            client.ledger_api.traffic
              .get_account(alice)
              .balance shouldBe balance - cost
          }
      }
    }
  }
}

final class ParticipantTrafficEnforcementDegradationTest
    extends ParticipantTrafficEnforcementTest
    with DbLockingSupport {
  registerPlugin(new UsePostgres(loggerFactory))

  // These two control how long the test waits before the lookup will fail on the lock that's held below.
  override protected def extraTrafficEnforcementConfigTransforms: Seq[ConfigTransform] = Seq(
    ConfigTransforms.updateParticipantConfig("participant1")(
      _.focus(_.trafficAccounting.allowSubmissionsOnDegradation)
        .replace(true)
        .focus(_.trafficAccounting.trafficEnforcementServer)
        .replace(
          TrafficEnforcementServerConfig.Internal(
            teaServerName,
            databaseQueryTimeout = PositiveFiniteDuration.ofMillis(100),
            accountLookupTimeout = PositiveFiniteDuration.ofMillis(500),
          )
        )
    )
  )

  // `GetAccount` goes through the same client this suite runs with a much shorter lookup timeout
  //  than the default, so read the balance straight from the table instead so we can fail the lookup
  //  faster while still not risking failing the balance assertion at the end.
  private def balanceFromDb(
      participant: LocalParticipantReference,
      accountId: String,
  ): Option[Long] = {
    val storage = dbStorage(participant)
    import storage.api.*
    implicit val closeContext: CloseContext = CloseContext(storage)
    storage
      .query(
        sql"""select total_credits - total_debits
              from par_traffic_enforcement_balance
              where account_id = $accountId"""
          .as[Long]
          .headOption,
        "read tea balance",
      )
      .futureValueUS
  }

  /** Blocks reads and writes to the TEA balance table until we release them, so the lookup times
    * out and we reliably hit the degraded path.
    */
  private def lockBalanceTable(participant: LocalParticipantReference): CommitAndClose =
    withConnectionForTest(participant)(testFunction = { conn =>
      // Set a timeout just in case so we won't hold the whole suite if something goes wrong.
      SQL"SET LOCAL lock_timeout = 30000".execute()(conn).discard
      SQL"LOCK TABLE par_traffic_enforcement_balance IN ACCESS EXCLUSIVE MODE"
        .execute()(conn)
        .discard
    })

  "Participant" when {
    "the account lookup fails and degradation is allowed" should {
      "let a submission through that the balance check would have rejected" in { implicit env =>
        import env.*

        // Charlie's balance is zero, so a lookup that did complete would reject this submission.
        val degradedMessage = "allowing the submission to proceed without a balance check"
        val noCorrelationMessage = "No correlation found for completion"
        val commandId = UUID.randomUUID().toString
        val submissionId = UUID.randomUUID().toString
        val charlieBalance = balanceFromDb(participant1, charlie.toProtoPrimitive)

        loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(WARN))(
          {
            val balanceTableLock = lockBalanceTable(participant1)
            val transaction =
              try
                participant1.ledger_api.javaapi.commands.submit(
                  Seq(charlie),
                  Seq(createCycleCommandJava(charlie, "degraded")),
                  commandId = commandId,
                  submissionId = submissionId,
                )
              finally balanceTableLock.commitAndClose()

            // TEA has no stored account for the degraded submission, so it doesn't charge it.
            eventually() {
              unchargedFromDb(participant1, changeIdHash(commandId, Set(charlie))) shouldBe Seq(
                (
                  submissionId,
                  transaction.getPaidTrafficCost.longValue(),
                  UnchargedReason.MissingCorrelation,
                  ActAsCategory.SingleNonAdminActingParty,
                )
              )
            }
          },
          entries => {
            forAtLeast(1, entries)(_.warningMessage should include(degradedMessage))
            forAtLeast(1, entries)(_.warningMessage should include(noCorrelationMessage))
            // Either timeout can win: the store failure is retried, or the client hits its deadline first.
            forEvery(entries)(entry =>
              entry.warningMessage should (include(degradedMessage) or
                include(noCorrelationMessage) or
                include(TransientFailure.id) or
                include("Retry timeout has elapsed, giving up.") or
                include("DEADLINE_EXCEEDED"))
            )
          },
        )

        balanceFromDb(participant1, charlie.toProtoPrimitive) shouldBe charlieBalance
      }
    }
  }
}

final class ParticipantTrafficEnforcementReassignmentTest
    extends ParticipantTrafficEnforcementTest {
  registerPlugin(new UsePostgres(loggerFactory))

  private var bob: PartyId = _

  private val exclusivityTimeout = config.NonNegativeFiniteDuration.ofSeconds(1)

  override protected def extraTrafficEnforcementConfigTransforms: Seq[ConfigTransform] = Seq(
    enableTrafficEnforcement("participant2"),
    ConfigTransforms.updateParticipantConfig("participant2")(
      _.focus(_.parameters.reassignmentsConfig.automaticAssignmentDelay)
        .replace(config.NonNegativeFiniteDuration.Zero)
    ),
  )

  override def environmentDefinition: EnvironmentDefinition =
    withPredictableTraffic(
      EnvironmentDefinition.P2_S1M1_S1M1
        .addConfigTransforms(configTransforms*)
        .addConfigTransform(ConfigTransforms.enableMultiSynchronizerTopologyFeatureFlag)
        .withSetup { implicit env =>
          import env.*
          participants.all.synchronizers.connect_local(sequencer1, alias = daName)
          participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
          participants.all.dars.upload(CantonExamplesPath, synchronizerId = daId)
          participants.all.dars.upload(CantonExamplesPath, synchronizerId = acmeId)

          // Before protocol version 37, a zero exclusivity timeout disables automatic assignment.
          Seq((sequencer1, daId), (sequencer2, acmeId)).foreach {
            case (sequencer, synchronizerId) =>
              sequencer.topology.synchronizer_parameters.propose_update(
                synchronizerId,
                _.update(assignmentExclusivityTimeout = exclusivityTimeout),
              )
          }

          def hostedOn(participant: ParticipantReference) = {
            val permissions = (
              PositiveInt.one,
              Set((participant.id, ParticipantPermission.Submission: ParticipantPermission)),
            )
            Map(daId -> permissions, acmeId -> permissions)
          }

          // The allocator returns each party once per synchronizer, in the order given.
          val parties = PartiesAllocator(Set(participant1, participant2))(
            Seq(("charlie", participant1), ("dan", participant1), ("bob", participant2)),
            Map(
              "charlie" -> hostedOn(participant1),
              "dan" -> hostedOn(participant1),
              "bob" -> hostedOn(participant2),
            ),
          ).map(_.partyId).distinct
          inside(parties) { case Seq(newCharlie, newDan, newBob) =>
            charlie = newCharlie
            dan = newDan
            bob = newBob
          }

          participant1.ledger_api.traffic
            .update_account(charlie.toProtoPrimitive, balanceDelta = Some(1_000_000L))
        }
    )

  /** Creates an IOU on `da` with Dan as the other stakeholder, who has no balance. Waits for TEA to
    * charge the creation before returning, so later balance reads see the charge.
    */
  private def createIouWithDan()(implicit env: TestConsoleEnvironment): LfContractId = {
    import env.*
    val commandId = UUID.randomUUID().toString
    val transaction = participant1.ledger_api.javaapi.commands.submit(
      Seq(charlie),
      IouSyntax.testIou(charlie, dan).create().commands().asScala.toSeq,
      synchronizerId = Some(daId),
      commandId = commandId,
    )
    awaitCharged(participant1, commandId, Set(charlie))
    val iou = JavaDecodeUtil.decodeAllCreated(Iou.COMPANION)(transaction).loneElement
    LfContractId.assertFromString(iou.id.contractId)
  }

  "Participant" when {
    "traffic enforcement is enabled" should {
      "charge client reassignments to their account_id or the submitter and pass TEA rejections through" in {
        implicit env =>
          import env.*

          val cid = createIouWithDan()
          val accountId = "acme/reassignments"
          participant1.ledger_api.traffic.update_account(accountId, balanceDelta = Some(1_000_000L))
          val accountBalance = balance(participant1, accountId)
          val charlieBalance = balance(participant1, charlie)

          assertThrowsAndLogsCommandFailures(
            participant1.ledger_api.commands.submit_unassign_async(dan, Seq(cid), daId, acmeId),
            _.shouldBeCantonErrorCode(InsufficientBalance),
          )

          val conflictingCommandId = UUID.randomUUID().toString
          val conflictingSubmissionId = UUID.randomUUID().toString
          insertCorrelationIntoDb(
            participant1,
            changeIdHash(conflictingCommandId, Set(charlie)),
            conflictingSubmissionId,
            Some(charlie.toProtoPrimitive),
          )
          assertThrowsAndLogsCommandFailures(
            participant1.ledger_api.commands.submit_unassign_async(
              charlie,
              Seq(cid),
              daId,
              acmeId,
              commandId = conflictingCommandId,
              submissionId = conflictingSubmissionId,
            ),
            _.shouldBeCantonErrorCode(CorrelationConflict),
          )

          // Dan has no balance, so this only passes if the participant checks the account instead.
          val unassigned = participant1.ledger_api.commands.submit_unassign(
            dan,
            Seq(cid),
            daId,
            acmeId,
            accountId = accountId,
          )
          // Up to protocol version 36, only the unassignment submitter can assign before the
          // exclusivity timeout, so Dan submits the assignment too.
          participant1.ledger_api.traffic
            .update_account(dan.toProtoPrimitive, balanceDelta = Some(1_000_000L))
          val danBalance = balance(participant1, dan)
          val assigned = participant1.ledger_api.commands.submit_assign(
            dan,
            unassigned.reassignmentId,
            daId,
            acmeId,
          )

          awaitCharged(participant1, unassigned.reassignment.commandId, Set(dan))
          awaitCharged(participant1, assigned.reassignment.commandId, Set(dan))
          balance(participant1, accountId) shouldBe accountBalance -
            unassigned.reassignment.paidTrafficCost.value
          balance(participant1, dan) shouldBe danBalance -
            assigned.reassignment.paidTrafficCost.value
          balance(participant1, charlie) shouldBe charlieBalance
      }

      "register routing reassignments with the transaction's account, not the batch submitter" in {
        implicit env =>
          import env.*

          val createCommandId = UUID.randomUUID().toString
          val tx = participant1.ledger_api.javaapi.commands.submit(
            Seq(charlie),
            Seq(createCycleCommandJava(charlie, "routing")),
            synchronizerId = Some(acmeId),
            commandId = createCommandId,
          )
          val cycle = JavaDecodeUtil.decodeAllCreated(Cycle.COMPANION)(tx).loneElement
          awaitCharged(participant1, createCommandId, Set(charlie))
          val charlieBalance = balance(participant1, charlie)

          // Charlie is the only stakeholder, so routing picks Charlie as the batch submitter.
          val commandId = UUID.randomUUID().toString
          val submissionId = UUID.randomUUID().toString
          participant1.ledger_api.javaapi.commands
            .submit(
              Seq(charlie, dan),
              cycle.id.exerciseArchive().commands.asScala.toSeq,
              synchronizerId = Some(daId),
              commandId = commandId,
              submissionId = submissionId,
            )
            .discard

          // Routing reassignments use the same no-account marker as the transaction.
          eventually() {
            val rows =
              unchargedFromDb(participant1, changeIdHash(commandId, Set(charlie))).sortBy(_._1)
            rows.map { case (_, _, reason, category) => (reason, category) } shouldBe Seq.fill(2)(
              (UnchargedReason.NoAccountSelected, ActAsCategory.SingleNonAdminActingParty)
            )
            inside(rows.map(_._1)) { case Seq(assignmentId, unassignmentId) =>
              assignmentId should startWith(s"$submissionId-a")
              unassignmentId shouldBe
                s"$submissionId-u${assignmentId.stripPrefix(s"$submissionId-a")}"
            }
          }
          balance(participant1, charlie) shouldBe charlieBalance
      }

      "charge routing reassignments to the transaction's account_id" in { implicit env =>
        import env.*

        val createCommandId = UUID.randomUUID().toString
        val tx = participant1.ledger_api.javaapi.commands.submit(
          Seq(charlie),
          Seq(createCycleCommandJava(charlie, "routing")),
          synchronizerId = Some(acmeId),
          commandId = createCommandId,
        )
        val cycle = JavaDecodeUtil.decodeAllCreated(Cycle.COMPANION)(tx).loneElement
        awaitCharged(participant1, createCommandId, Set(charlie))

        val accountId = "acme/billing"
        participant1.ledger_api.traffic.update_account(accountId, balanceDelta = Some(1_000_000L))
        val accountBalance = balance(participant1, accountId)
        val charlieBalance = balance(participant1, charlie)
        val danBalance = balance(participant1, dan)

        val commandId = UUID.randomUUID().toString
        val transaction = participant1.ledger_api.javaapi.commands.submit(
          Seq(charlie, dan),
          cycle.id.exerciseArchive().commands.asScala.toSeq,
          synchronizerId = Some(daId),
          commandId = commandId,
          accountId = accountId,
        )

        // Charlie is the only stakeholder, so routing reassigns with Charlie as the submitter.
        val changeIds =
          Seq(changeIdHash(commandId, Set(charlie, dan)), changeIdHash(commandId, Set(charlie)))
        eventually() {
          forEvery(changeIds) { changeId =>
            correlationsFromDb(participant1, changeId) shouldBe empty
            unchargedFromDb(participant1, changeId) shouldBe empty
          }
        }
        balance(participant1, accountId) should be <
          accountBalance - transaction.getPaidTrafficCost
        balance(participant1, charlie) shouldBe charlieBalance
        balance(participant1, dan) shouldBe danBalance
      }

      "charge an assignment that another participant completes to that participant's admin party" in {
        implicit env =>
          import env.*

          val iou = IouSyntax.createIou(participant1, Some(daId))(charlie, bob)
          val adminBalance = balance(participant2, participant2.adminParty)
          val unassigned = participant1.ledger_api.commands.submit_unassign(
            charlie,
            Seq(LfContractId.assertFromString(iou.id.contractId)),
            daId,
            acmeId,
          )
          val assignmentChangeIdHash = changeIdHash(unassigned.reassignment.commandId, Set(bob))

          environment.simClock.value.advance(exclusivityTimeout.asJava.multipliedBy(2))
          participants.all.foreach(_.testing.fetch_synchronizer_times())

          // Participant2 hosts Bob and isn't the initiator, so it assigns automatically.
          eventually() {
            balance(participant2, participant2.adminParty) should be < adminBalance
            correlationsFromDb(participant2, assignmentChangeIdHash) shouldBe empty
          }
          correlationsFromDb(participant1, assignmentChangeIdHash) shouldBe empty
      }
    }
  }
}
