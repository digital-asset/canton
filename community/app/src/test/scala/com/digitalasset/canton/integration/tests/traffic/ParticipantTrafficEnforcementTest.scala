// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.traffic

import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.http.json.v2.JsTrafficServiceCodecs.*
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{TestUtils, TrafficControlUtils}
import com.digitalasset.canton.ledger.error.CommonErrors.ServiceNotRunning
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors.TrafficAccountValidationFailed
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.platform.config.{
  TrafficEnforcementConfig,
  TrafficEnforcementServerConfig,
}
import com.digitalasset.canton.tea.v1.{
  GetAccountResponse,
  UpdateAccountRequest,
  UpdateAccountResponse,
}
import com.digitalasset.canton.topology.{ExternalParty, PartyId}
import com.digitalasset.canton.util.ShowUtil.*
import io.circe.parser.decode
import io.circe.syntax.*
import io.grpc.Status
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion
import org.slf4j.event.Level.INFO

import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.{URI, URLEncoder}
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.CollectionHasAsScala

sealed trait ParticipantTrafficEnforcementTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils {

  protected var aliceE: ExternalParty = _
  protected var bobE: ExternalParty = _
  protected var eveE: ExternalParty = _

  // Charlie and Dan are on-purpose local parties to test traffic enforcement behavior of local submissions
  protected var charlie: PartyId = _
  protected var dan: PartyId = _

  /** Participant config transforms to enable or disable participant-local traffic enforcement.
    */
  protected def participantConfigTransforms: Seq[ConfigTransform]

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(participantConfigTransforms*)
      .addConfigTransform(ConfigTransforms.enableInteractiveSubmissionTransforms)
      .addConfigTransforms(ConfigTransforms.useStaticTime)
      .addConfigTransform(
        ConfigTransforms.updateAllSequencerConfigs_(
          _.focus(_.trafficConfig.pruningRetentionWindow)
            .replace(config.NonNegativeFiniteDuration.ofSeconds(5))
            .focus(_.trafficConfig.trafficPurchasedCacheSizePerMember)
            .replace(PositiveInt.one)
        )
      )
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
      .withTrafficControl(
        TestUtils.waitForTargetTimeOnSynchronizerNode(wallClock.now, logger),
        trafficControlParameters = TrafficControlUtils.predictableTraffic,
        topUpAllMembers = true,
        disableCommitments = true,
      )

  protected def assertUnimplemented(entry: LogEntry): Assertion =
    entry.message should include(Status.Code.UNIMPLEMENTED.toString)
}

final class ParticipantTrafficEnforcementDisabledTest extends ParticipantTrafficEnforcementTest {
  override protected def participantConfigTransforms: Seq[ConfigTransform] = Seq(
    ConfigTransforms.updateParticipantConfig("participant1")(
      _.focus(_.trafficEnforcement).replace(TrafficEnforcementConfig(enabled = false))
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
  private val teaServerName = "tea-server"

  registerPlugin(new UsePostgres(loggerFactory))

  override protected def participantConfigTransforms: Seq[ConfigTransform] = Seq(
    ConfigTransforms.updateParticipantConfig("participant1")(
      _.focus(_.trafficEnforcement)
        .replace(
          TrafficEnforcementConfig(
            enabled = true,
            enforceCostOnSubmissions = true,
            trafficEnforcementServer = TrafficEnforcementServerConfig.Internal(teaServerName),
          )
        )
        .focus(_.parameters.alphaVersionSupport)
        .replace(true)
    ),
    // Shorten network timeout so retries to the non-existent traffic service give up quickly
    _.focus(_.parameters.timeouts.processing.network)
      .replace(config.NonNegativeDuration.tryFromDuration(5.seconds)),
  )

  "Participant" when {
    "traffic enforcement is enabled" should {
      "serve traffic service operations" in { implicit env =>
        import env.*

        val alice = aliceE.partyId.toProtoPrimitive

        // Initially Alice has no balance
        participant1.ledger_api.traffic.get_account(alice).balance shouldBe 0L

        val aliceBalance = 1_000_000L

        // Update Alice's balance
        participant1.ledger_api.traffic.update_account(alice, balanceDelta = Some(aliceBalance))

        // Check correct balance for Alice
        participant1.ledger_api.traffic.get_account(alice).balance shouldBe aliceBalance

        // Now deduct some traffic from Alice's account
        val deductAmount = 100_000L
        participant1.ledger_api.traffic.update_account(alice, balanceDelta = Some(-deductAmount))

        // Check correct balance for Alice after deduction
        participant1.ledger_api.traffic
          .get_account(alice)
          .balance shouldBe (aliceBalance - deductAmount)
      }

      "debit traffic from submitting party account" in { implicit env =>
        import env.*

        val alice = aliceE.partyId.toProtoPrimitive
        val aliceBalance = participant1.ledger_api.traffic.get_account(alice).balance

        val iouCmd = IouSyntax.testIou(aliceE, aliceE, 10L).create().commands().asScala.toSeq
        val transaction = participant1.ledger_api.javaapi.commands.submit(Seq(aliceE), iouCmd)
        val cost = transaction.getPaidTrafficCost

        eventually() {
          participant1.ledger_api.traffic.get_account(alice).balance shouldBe aliceBalance - cost
        }
      }

      "reject submission for submitting party with not enough account balance" in { implicit env =>
        import env.*

        // Eve's initial balance should be 0
        participant1.ledger_api.traffic
          .get_account(eveE.partyId.toProtoPrimitive)
          .balance shouldBe 0L

        // Then, Eve's submission attempt fails
        val iouCreateCmds = IouSyntax.testIou(eveE, eveE, 10L).create().commands().asScala.toSeq
        assertThrowsAndLogsCommandFailures(
          participant1.ledger_api.javaapi.commands
            .submit(Seq(eveE), iouCreateCmds),
          entry => {
            entry.shouldBeCantonErrorCode(TrafficAccountValidationFailed)
            entry.message should include regex raw".*Insufficient balance \(0\) for actual traffic cost \([1-9][0-9]*\) for account ${eveE.partyId.toProtoPrimitive}"
          },
        )

        // Top up Eve's account minimally
        participant1.ledger_api.traffic.update_account(
          eveE.partyId.toProtoPrimitive,
          balanceDelta = Some(1L),
        )

        // Check that Eve's balance is now 1
        participant1.ledger_api.traffic
          .get_account(eveE.partyId.toProtoPrimitive)
          .balance shouldBe 1L

        // Then, Eve's submission attempt fails again
        assertThrowsAndLogsCommandFailures(
          participant1.ledger_api.javaapi.commands
            .submit(Seq(eveE), iouCreateCmds),
          entry => {
            entry.shouldBeCantonErrorCode(TrafficAccountValidationFailed)
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

        // Initially the account has no balance
        balance() shouldBe 0L

        val credit = 1_000_000L
        update(credit) shouldBe credit
        balance() shouldBe credit

        val deductAmount = 100_000L
        update(-deductAmount) shouldBe (credit - deductAmount)
        balance() shouldBe (credit - deductAmount)
      }

      "traffic enforcement does not apply for multi-act-as submissions" in { implicit env =>
        import env.*

        val actAs = Seq(charlie, dan)

        // Charlie and Dan can submit a multi-party transaction without having any traffic balance
        loggerFactory.assertLogsSeq(SuppressionRule.Level(INFO))(
          participant1.ledger_api.commands
            .submit(
              actAs = actAs,
              commands = Seq(createCycleCommand(charlie, "traffic")),
            )
            .discard,
          forAtLeast(1, _) {
            _.infoMessage should include(
              show"Skipping traffic enforcement validation due to non-singleton actAs parties: $actAs"
            )
          },
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
                (include(Status.Code.UNAVAILABLE.toString) and include(
                  s"Could not find server: $teaServerName"
                ) or
                  include("Retry timeout has elapsed, giving up.")))
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
                (include(Status.Code.UNAVAILABLE.toString) and include(
                  s"Could not find server: $teaServerName"
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
  private val teaServerName = "tea-server"

  registerPlugin(new UsePostgres(loggerFactory))

  override protected def participantConfigTransforms: Seq[ConfigTransform] = Seq(
    ConfigTransforms.updateParticipantConfig("participant1")(
      _.focus(_.trafficEnforcement)
        .replace(
          TrafficEnforcementConfig(
            enabled = true,
            enforceCostOnSubmissions = false,
            trafficEnforcementServer = TrafficEnforcementServerConfig.Internal(teaServerName),
          )
        )
        .focus(_.parameters.alphaVersionSupport)
        .replace(true)
    ),
    // Shorten network timeout so retries to the non-existent traffic service give up quickly
    _.focus(_.parameters.timeouts.processing.network)
      .replace(config.NonNegativeDuration.tryFromDuration(5.seconds)),
  )

  "Participant" when {
    "traffic enforcement is enabled but enforce-cost-on-submissions=false" should {
      "do not enforce traffic on submissions (allow negative balances) but still debit traffic from submitting party account" in {
        implicit env =>
          import env.*

          val alice = aliceE.partyId.toProtoPrimitive
          val initialAliceBalance = participant1.ledger_api.traffic.get_account(alice).balance

          // Alice should have no balance initially
          initialAliceBalance shouldBe 0L

          val transaction = participant1.ledger_api.javaapi.commands.submit(
            Seq(aliceE),
            IouSyntax.testIou(aliceE, aliceE, 10L).create().commands().asScala.toSeq,
          )

          val cost = transaction.getPaidTrafficCost

          eventually() {
            // Alice should have negative balance
            participant1.ledger_api.traffic
              .get_account(alice)
              .balance shouldBe initialAliceBalance - cost
          }
      }
    }
  }
}
