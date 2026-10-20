// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.traffic

import com.daml.ledger.javaapi.data.Command
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors.Interpreter.UnhandledException
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.platform.config.{TopUpConfig, TrafficEnforcementServerConfig}
import com.digitalasset.canton.tea.daml.java.canton.traffic.credit.{AccountId, TrafficCredit}
import com.digitalasset.canton.tea.tests.java.tea.trafficpurchase.{Token, TrafficPurchaser}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.nonempty.NonEmpty
import monocle.macros.syntax.lens.*

import scala.jdk.CollectionConverters.CollectionHasAsScala

/** Top-up ingestion from `TrafficCredit` contracts. The paymaster must be hosted on the
  * participant, but the config is built before any party exists, so participant1 starts with top-up
  * disabled and is restarted with Daml top-up enabled once the paymaster is allocated.
  */
final class ParticipantTrafficEnforcementDamlTopUpTest extends ParticipantTrafficEnforcementTest {
  registerPlugin(new UsePostgres(loggerFactory))

  // Without alpha version support, a config update only applies dynamic changes
  override protected def extraTrafficEnforcementConfigTransforms: Seq[ConfigTransform] =
    ConfigTransforms.setAlphaVersionSupport(true)

  private var paymaster: PartyId = _
  // Hosted on participant1 but not a configured paymaster, so its credits are not ingested
  private var untrustedPaymaster: PartyId = _

  private val account = "top-up-account"

  private lazy val teaTrafficPurchasePath =
    BaseTest.getResourcePath("TeaTrafficPurchase-1.0.0.dar")

  private def credit(paymaster: PartyId, account: String, amount: Long): Seq[Command] =
    new TrafficCredit(paymaster.toProtoPrimitive, new AccountId(account), amount)
      .createAnd()
      .exerciseArchive()
      .commands()
      .asScala
      .toSeq

  private def enableDamlTopUp(paymaster: PartyId)(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    val newConfig = ConfigTransforms.updateParticipantConfig("participant1")(
      _.focus(_.trafficAccounting.trafficEnforcementServer).replace(
        TrafficEnforcementServerConfig.Internal(
          teaServerName,
          topUp = TopUpConfig.DamlCredit(NonEmpty(Set, paymaster)),
        )
      )
    )(environment.config)
    environment.pokeOrUpdateConfig(Some(Right(newConfig)))
    participant1.stop()
    participant1.start()
    participant1.synchronizers.reconnect_all()
  }

  "Participant" when {
    "traffic enforcement is enabled with Daml top-up" should {
      "upload and vet the traffic credit package" in { implicit env =>
        import env.*

        paymaster = participant1.parties.enable("paymaster")
        untrustedPaymaster = participant1.parties.enable("untrusted-paymaster")
        // Fund the paymasters so that their submissions pass the balance check
        Seq(paymaster, untrustedPaymaster).foreach(party =>
          participant1.ledger_api.traffic
            .update_account(party.toProtoPrimitive, balanceDelta = Some(1_000_000L))
        )

        participant1.packages.list().map(_.packageId) should not contain TrafficCredit.PACKAGE_ID

        enableDamlTopUp(paymaster)

        eventually() {
          participant1.topology.vetted_packages
            .list(store = daId, filterParticipant = participant1.filterString)
            .flatMap(_.item.packages.map(_.packageId)) should contain(TrafficCredit.PACKAGE_ID)
        }
      }

      "enforce TrafficCredit account ID and amount constraints" in { implicit env =>
        import env.*

        // Submitted by the untrusted paymaster, so that the extreme amounts are not ingested
        def submit(account: String, amount: Long) =
          participant1.ledger_api.javaapi.commands
            .submit(Seq(untrustedPaymaster), credit(untrustedPaymaster, account, amount))

        val validAccounts =
          Seq("a", "a" * 255, "é" * 255, "￿" * 255, "𐀀" * 127 + "a")
        val validCredits = validAccounts.map(_ -> 1L) :+ ("a" -> Long.MaxValue)
        forAll(validCredits) { case (account, amount) => submit(account, amount).discard }
        val invalidAccounts = Seq("", "a" * 256, "é" * 256, "𐀀" * 128)
        val invalidCredits =
          invalidAccounts.map(_ -> 1L) ++ Seq(0L, -1L, Long.MinValue).map("a" -> _)
        forAll(invalidCredits) { case (account, amount) =>
          assertThrowsAndLogsCommandFailures(
            submit(account, amount),
            entry => {
              entry.shouldBeCantonErrorCode(UnhandledException)
              entry.errorMessage should include("Template precondition violated: TrafficCredit")
            },
          )
        }
      }

      "ingest top-ups from a configured paymaster" in { implicit env =>
        import env.*

        participant1.ledger_api.traffic.get_account(account).balance shouldBe 0L

        // Two credits in the same transaction
        participant1.ledger_api.javaapi.commands
          .submit(
            Seq(paymaster),
            credit(paymaster, account, 1_000L) ++ credit(paymaster, account, 2_000L),
          )
          .discard

        eventually() {
          participant1.ledger_api.traffic.get_account(account).balance shouldBe 3_000L
        }
      }

      "not ingest top-ups from a paymaster that is not configured" in { implicit env =>
        import env.*

        val balance = participant1.ledger_api.traffic.get_account(account).balance

        participant1.ledger_api.javaapi.commands
          .submit(Seq(untrustedPaymaster), credit(untrustedPaymaster, account, 10_000L))
          .discard
        participant1.ledger_api.javaapi.commands
          .submit(Seq(paymaster), credit(paymaster, account, 100L))
          .discard

        // Credits are ingested in ledger order: once the later trusted credit is visible, the
        // untrusted one would have been ingested too
        eventually() {
          participant1.ledger_api.traffic.get_account(account).balance shouldBe balance + 100L
        }
      }

      "ingest top-ups credited by a composed purchase contract" in { implicit env =>
        import env.*

        val purchaseAccount = "purchase-account"

        participant1.dars.upload(teaTrafficPurchasePath, synchronizerId = daId).discard
        val issuer = participant1.parties.testing.enable("token-issuer")
        val receiver = participant1.parties.testing.enable("paymaster-receiver")
        val purchaser = participant1.parties.testing.enable("purchaser")
        Seq(issuer, purchaser).foreach(party =>
          participant1.ledger_api.traffic
            .update_account(party.toProtoPrimitive, balanceDelta = Some(1_000_000L))
        )

        // Purchasers are not stakeholders of the purchaser contract, so the paymaster discloses it
        val purchaserTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(paymaster),
          new TrafficPurchaser(
            paymaster.toProtoPrimitive,
            receiver.toProtoPrimitive,
            issuer.toProtoPrimitive,
            new java.math.BigDecimal("1000.0"),
          ).create.commands.asScala.toSeq,
          includeCreatedEventBlob = true,
        )
        val trafficPurchaserCid =
          JavaDecodeUtil.decodeAllCreated(TrafficPurchaser.COMPANION)(purchaserTx).loneElement.id
        val trafficPurchaserDisclosure =
          JavaDecodeUtil.decodeDisclosedContracts(purchaserTx).loneElement

        val tokenTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(issuer),
          new Token(
            issuer.toProtoPrimitive,
            purchaser.toProtoPrimitive,
            new java.math.BigDecimal("2.5"),
          ).create.commands.asScala.toSeq,
        )
        val tokenCid = JavaDecodeUtil.decodeAllCreated(Token.COMPANION)(tokenTx).loneElement.id

        participant1.ledger_api.traffic.get_account(purchaseAccount).balance shouldBe 0L

        participant1.ledger_api.javaapi.commands
          .submit(
            Seq(purchaser),
            trafficPurchaserCid
              .exerciseTrafficPurchaser_PurchaseCredits(
                purchaser.toProtoPrimitive,
                purchaseAccount,
                tokenCid,
              )
              .commands
              .asScala
              .toSeq,
            disclosedContracts = Seq(trafficPurchaserDisclosure),
          )
          .discard

        eventually() {
          participant1.ledger_api.traffic.get_account(purchaseAccount).balance shouldBe 2_500L
        }
        val payment =
          participant1.ledger_api.javaapi.state.acs.filter(Token.COMPANION)(receiver).loneElement
        payment.data.owner shouldBe receiver.toProtoPrimitive
        payment.data.amount.compareTo(new java.math.BigDecimal("2.5")) shouldBe 0
      }
    }
  }
}
