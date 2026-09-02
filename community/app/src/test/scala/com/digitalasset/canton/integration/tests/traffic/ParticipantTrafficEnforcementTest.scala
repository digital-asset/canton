// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.traffic

import com.daml.ledger.api.v2.transaction_filter.{EventFormat, Filters}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{AuthServiceConfig, PositiveFiniteDuration}
import com.digitalasset.canton.console.{
  CommandFailure,
  ExternalLedgerApiClient,
  LocalParticipantReference,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.http.json.v2.JsTrafficServiceCodecs.*
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.AuthStartupConfigSuppressionRule
import com.digitalasset.canton.integration.util.{TestUtils, TrafficControlUtils}
import com.digitalasset.canton.ledger.error.CommonErrors.ServiceNotRunning
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.ledger.api.JwtTokenUtilities
import com.digitalasset.canton.platform.apiserver.services.command.TrafficEnforcementBackend
import com.digitalasset.canton.platform.config.{
  TrafficEnforcementConfig,
  TrafficEnforcementServerConfig,
}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.tea.TrafficEnforcementErrors.{
  InsufficientBalance,
  MultiPartySubmissionRejected,
  TrafficUpdateOutOfBound,
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
import org.slf4j.event.Level.{DEBUG, INFO, WARN}

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
    ConfigTransforms.updateParticipantConfig("participant1")(
      _.focus(_.trafficEnforcement)
        .replace(
          TrafficEnforcementConfig(
            enabled = true,
            enforceCostOnSubmissions = true,
            trafficEnforcementServer = TrafficEnforcementServerConfig.Internal(teaServerName),
          )
        )
    ),
    // Shorten network timeout so retries to the non-existent traffic service give up quickly
    _.focus(_.parameters.timeouts.processing.network)
      .replace(config.NonNegativeDuration.tryFromDuration(5.seconds)),
  )

  protected def extraTrafficEnforcementConfigTransforms: Seq[ConfigTransform] = Seq.empty

  protected final def participantConfigTransforms: Seq[ConfigTransform] =
    defaultTrafficEnforcementConfigTransforms ++ extraTrafficEnforcementConfigTransforms

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
  override protected def extraTrafficEnforcementConfigTransforms: Seq[ConfigTransform] = Seq(
    ConfigTransforms.updateParticipantConfig("participant1")(
      _.focus(_.trafficEnforcement.enabled).replace(false)
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
  registerPlugin(new UsePostgres(loggerFactory))

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

      "reject an update_account with a delta that would take the balance out of bound, with its own error code" in {
        implicit env =>
          import env.*

          val charlieId = charlie.toProtoPrimitive
          val initialBalance = participant1.ledger_api.traffic.get_account(charlieId).balance
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
          participant1.ledger_api.traffic.get_account(charlieId).balance shouldBe initialBalance
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
        participant1.ledger_api.traffic
          .get_account(eveE.partyId.toProtoPrimitive)
          .balance shouldBe 1L

        // Then, Eve's submission attempt fails again
        assertThrowsAndLogsCommandFailures(
          participant1.ledger_api.javaapi.commands
            .submit(Seq(eveE), iouCreateCmds),
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

      "not enforce the balance check for the participant admin party" in { implicit env =>
        import env.*

        val adminParty = participant1.adminParty

        // The admin party should starts with no balance.
        participant1.ledger_api.traffic
          .get_account(adminParty.toProtoPrimitive)
          .balance shouldBe 0L

        // The submission succeeds even though the balance check would reject any other
        // party with no balance.
        val transaction = loggerFactory.assertLogsSeq(
          SuppressionRule.forLogger[TrafficEnforcementBackend] && SuppressionRule.Level(DEBUG)
        )(
          participant1.ledger_api.javaapi.commands.submit(
            Seq(adminParty),
            Seq(createCycleCommandJava(adminParty, "traffic")),
          ),
          forAtLeast(1, _) {
            _.debugMessage should include(
              show"Skipping traffic enforcement validation for participant admin party: ${adminParty.toLf}"
            )
          },
        )
        val cost = transaction.getPaidTrafficCost

        // The admin account is still debited through the completion stream.
        eventually() {
          participant1.ledger_api.traffic
            .get_account(adminParty.toProtoPrimitive)
            .balance shouldBe 0L - cost
        }

        // Interactive preparation is not rejected either.
        val preparedAdmin = participant1.ledger_api.interactive_submission.prepare(
          actAs = Seq(adminParty),
          commands = Seq(createCycleCommand(adminParty, "traffic-prepare")),
          hashingSchemeVersion = testedApiHashingSchemeVersion,
        )
        preparedAdmin.getPreparedTransaction should not be null
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
      _.focus(_.trafficEnforcement.enforceCostOnSubmissions)
        .replace(false)
        .focus(_.parameters.alphaVersionSupport)
        .replace(true)
    )
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

final class ParticipantTrafficEnforcementRejectMultiPartyTest
    extends ParticipantTrafficEnforcementTest {
  registerPlugin(new UsePostgres(loggerFactory))

  override protected def extraTrafficEnforcementConfigTransforms: Seq[ConfigTransform] = Seq(
    ConfigTransforms.updateParticipantConfig("participant1")(
      _.focus(_.trafficEnforcement.rejectMultiPartySubmissions).replace(true)
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
        val initialBalance = participant1.ledger_api.traffic.get_account(charlieId).balance

        val transaction = participant1.ledger_api.javaapi.commands
          .submit(Seq(charlie), Seq(createCycleCommandJava(charlie, "traffic-single")))
        val cost = transaction.getPaidTrafficCost

        eventually() {
          participant1.ledger_api.traffic
            .get_account(charlieId)
            .balance shouldBe (initialBalance - cost)
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

final class ParticipantTrafficEnforcementDegradationTest extends ParticipantTrafficEnforcementTest {
  registerPlugin(new UsePostgres(loggerFactory))

  // 1ms can't cover the gRPC call plus the DB transaction, so every lookup times out.
  override protected def extraTrafficEnforcementConfigTransforms: Seq[ConfigTransform] = Seq(
    ConfigTransforms.updateParticipantConfig("participant1")(
      _.focus(_.trafficEnforcement.allowSubmissionsOnDegradation)
        .replace(true)
        .focus(_.trafficEnforcement.trafficEnforcementServer)
        .replace(
          TrafficEnforcementServerConfig.Internal(
            teaServerName,
            databaseQueryTimeout = PositiveFiniteDuration.ofMillis(1),
            accountLookupTimeout = PositiveFiniteDuration.ofMillis(2),
          )
        )
    )
  )

  // The `GetAccount` would go through the same client with the impossible deadline, so we
  //  have to read the balance directly from the DB instead.
  private def balanceFromDb(
      participant: LocalParticipantReference,
      accountId: String,
  ): Option[Long] = {
    val storage: DbStorage = participant.underlying.value.storage match {
      case dbStorage: DbStorage => dbStorage
      case other => fail(s"expected DbStorage, got $other")
    }
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

  "Participant" when {
    "the account lookup fails and degradation is allowed" should {
      "let a submission through that the balance check would have rejected" in { implicit env =>
        import env.*

        // Charlie's balance is zero, so a lookup that did complete would reject this submission.
        val degradedMessage = "allowing the submission to proceed without a balance check"
        loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(WARN))(
          participant1.ledger_api.javaapi.commands
            .submit(Seq(charlie), Seq(createCycleCommandJava(charlie, "degraded")))
            .getUpdateId should not be empty,
          entries => {
            forAtLeast(1, entries)(_.warningMessage should include(degradedMessage))
            forEvery(entries)(entry =>
              entry.warningMessage should (include(degradedMessage) or include(
                "DEADLINE_EXCEEDED"
              ))
            )
          },
        )

        // The submission was still charged even though the balance check was bypassed.
        eventually() {
          balanceFromDb(participant1, charlie.toProtoPrimitive).value should be < 0L
        }
      }
    }
  }
}
