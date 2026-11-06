// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.traffic

import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.api.v2.value.Value
import com.daml.ledger.javaapi.data.{Command, Value as JavaValue}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors.Interpreter.{
  AuthorizationError,
  UnhandledException,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.platform.config.{TopUpConfig, TrafficEnforcementServerConfig}
import com.digitalasset.canton.tea.tests.java.canton.api.traffic.creditv1.{
  Metadata,
  TrafficCreditReporter,
  TrafficCreditReporter_PublishCredit,
}
import com.digitalasset.canton.tea.tests.java.tea.trafficpurchase.{
  CreditIssuer,
  Token,
  TrafficPurchaser,
}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.nonempty.NonEmpty
import monocle.macros.syntax.lens.*

import java.util.List as JList
import scala.jdk.CollectionConverters.{
  CollectionHasAsScala,
  MapHasAsJava,
  MapHasAsScala,
  SeqHasAsJava,
}

/** Top-up ingestion from `TrafficCreditReporter_PublishCredit` exercises. The paymaster must be
  * hosted on the participant, but the config is built before any party exists, so participant1
  * starts with top-up disabled and is restarted with Daml top-up enabled once the paymaster is
  * allocated.
  */
final class ParticipantTrafficEnforcementDamlTopUpTest extends ParticipantTrafficEnforcementTest {
  registerPlugin(new UsePostgres(loggerFactory))

  // Without alpha version support, a config update only applies dynamic changes
  override protected def extraTrafficEnforcementConfigTransforms: Seq[ConfigTransform] =
    ConfigTransforms.setAlphaVersionSupport(true)

  private var paymaster: PartyId = _
  // Hosted on participant1 but not a configured paymaster, so its credits are not ingested
  private var untrustedPaymaster: PartyId = _
  // The CreditIssuer contract of each paymaster, on which it publishes its credits
  private var issuers: Map[PartyId, TrafficCreditReporter.ContractId] = Map.empty

  private val account = "top-up-account"
  private val trafficCreditPackageId = TrafficCreditReporter.PACKAGE_ID
  private val emptyMetadata = new Metadata(Map.empty[String, String].asJava)

  private lazy val teaTrafficPurchasePath =
    BaseTest.getResourcePath("TeaTrafficPurchase-1.0.0.dar")

  private def credit(
      paymaster: PartyId,
      account: String,
      amount: Long,
      purchaser: PartyId,
      observers: Seq[PartyId] = Seq.empty,
      metadata: Metadata = emptyMetadata,
  ): Seq[Command] =
    issuers(paymaster)
      .exerciseTrafficCreditReporter_PublishCredit(
        account,
        amount,
        purchaser.toProtoPrimitive,
        observers.map(_.toProtoPrimitive).asJava,
        metadata,
      )
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

        participant1.packages.list().map(_.packageId) should not contain trafficCreditPackageId

        enableDamlTopUp(paymaster)
        participant1.dars.upload(teaTrafficPurchasePath, synchronizerId = daId).discard
        issuers = Seq(paymaster, untrustedPaymaster).map { party =>
          val tx = participant1.ledger_api.javaapi.commands.submit(
            Seq(party),
            new CreditIssuer(party.toProtoPrimitive).create.commands.asScala.toSeq,
          )
          party -> JavaDecodeUtil
            .decodeAllCreated(CreditIssuer.COMPANION)(tx)
            .loneElement
            .id
            .toInterface(TrafficCreditReporter.INTERFACE)
        }.toMap

        eventually() {
          participant1.topology.vetted_packages
            .list(store = daId, filterParticipant = participant1.filterString)
            .flatMap(_.item.packages.map(_.packageId)) should contain(trafficCreditPackageId)
        }
      }

      "enforce TrafficCreditReporter_PublishCredit account ID and amount constraints" in {
        implicit env =>
          import env.*

          // Submitted by the untrusted paymaster, so that the extreme amounts are not ingested
          def submit(account: String, amount: Long) =
            participant1.ledger_api.javaapi.commands
              .submit(
                Seq(untrustedPaymaster),
                credit(untrustedPaymaster, account, amount, purchaser = untrustedPaymaster),
              )

          val validAccounts =
            Seq(
              "a",
              "a" * 255,
              "a/b",
              "alice::1220ab/Bob-x_y z",
              "user@example.com",
              "@^$.!`-#+'~_|:() ",
            )
          val validCredits = validAccounts.map(_ -> 1L) :+ ("a" -> Long.MaxValue)
          forAll(validCredits) { case (account, amount) => submit(account, amount).discard }
          val invalidCredits =
            Seq("", "/a", "a/", "a//b").map((_, 1L, "Account id segments must not be empty")) ++
              Seq("é", "a,b", "a\\b", "a/b,c", "a\tb", "a\nb", "𐀀" * 127 + "a").map(
                (_, 1L, "Account id segments must only contain")
              ) ++
              Seq("a" * 256, ("a" * 127 + "/") * 2 + "a")
                .map((_, 1L, "Account id must be at most 255 characters")) ++
              Seq(0L, -1L, Long.MinValue).map(("a", _, "trafficAmount must be positive"))
          forAll(invalidCredits) { case (account, amount, error) =>
            assertThrowsAndLogsCommandFailures(
              submit(account, amount),
              entry => {
                entry.shouldBeCantonErrorCode(UnhandledException)
                entry.errorMessage should include(error)
              },
            )
          }
      }

      "validate directly published credits on a purchase contract" in { implicit env =>
        import env.*

        val issuerTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(untrustedPaymaster),
          new TrafficPurchaser(
            untrustedPaymaster.toProtoPrimitive,
            untrustedPaymaster.toProtoPrimitive,
            untrustedPaymaster.toProtoPrimitive,
            java.math.BigDecimal.ONE,
          ).create.commands.asScala.toSeq,
        )
        val issuerCid = JavaDecodeUtil
          .decodeAllCreated(TrafficPurchaser.COMPANION)(issuerTx)
          .loneElement
          .id
          .toInterface(TrafficCreditReporter.INTERFACE)

        forAll(
          Seq(
            ("a,b", 1L, "Account id segments must only contain"),
            ("a" * 256, 1L, "Account id must be at most 255 characters"),
            ("a", 0L, "trafficAmount must be positive"),
          )
        ) { case (account, amount, error) =>
          assertThrowsAndLogsCommandFailures(
            participant1.ledger_api.javaapi.commands.submit(
              Seq(untrustedPaymaster),
              issuerCid
                .exerciseTrafficCreditReporter_PublishCredit(
                  account,
                  amount,
                  untrustedPaymaster.toProtoPrimitive,
                  JList.of[String](),
                  emptyMetadata,
                )
                .commands
                .asScala
                .toSeq,
            ),
            entry => {
              entry.shouldBeCantonErrorCode(UnhandledException)
              entry.errorMessage should include(error)
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
            credit(paymaster, account, 1_000L, purchaser = paymaster) ++
              credit(paymaster, account, 2_000L, purchaser = paymaster),
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
          .submit(
            Seq(untrustedPaymaster),
            credit(untrustedPaymaster, account, 10_000L, purchaser = untrustedPaymaster),
          )
          .discard
        participant1.ledger_api.javaapi.commands
          .submit(Seq(paymaster), credit(paymaster, account, 100L, purchaser = paymaster))
          .discard

        // Credits are ingested in ledger order: once the later trusted credit is visible, the
        // untrusted one would have been ingested too
        eventually() {
          participant1.ledger_api.traffic.get_account(account).balance shouldBe balance + 100L
        }
      }

      "make published credits visible to the specified observers" in { implicit env =>
        import env.*

        val observer = participant1.parties.testing.enable("credit-observer")
        val nonObserver = participant1.parties.testing.enable("credit-non-observer")
        val observedAccount = "observed-top-up-account"
        val metadata = Map("purchase-id" -> "purchase-123", "note" -> "observer-visible credit")
        val tx = participant1.ledger_api.javaapi.commands.submit(
          Seq(paymaster),
          credit(
            paymaster,
            observedAccount,
            1000L,
            purchaser = nonObserver,
            observers = Seq(observer),
            metadata = new Metadata(metadata.asJava),
          ),
        )

        def exercisesVisibleTo(party: PartyId) =
          participant1.ledger_api.updates
            .transactions(
              partyIds = Set(party.toLf),
              completeAfter = 1,
              beginOffsetExclusive = tx.getOffset - 1,
              endOffsetInclusive = Some(tx.getOffset),
              transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
            )
            .flatMap(_.transaction.events.flatMap(_.event.exercised))

        val exercise = exercisesVisibleTo(observer).loneElement
        exercise.choice shouldBe TrafficCreditReporter.CHOICE_TrafficCreditReporter_PublishCredit.name
        exercise.witnessParties shouldBe Seq(observer.toProtoPrimitive)
        exercise.actingParties shouldBe Seq(paymaster.toProtoPrimitive)
        val publishedCredit = TrafficCreditReporter_PublishCredit
          .valueDecoder()
          .decode(JavaValue.fromProto(Value.toJavaProto(exercise.getChoiceArgument)))
        publishedCredit.purchaser shouldBe nonObserver.toProtoPrimitive
        publishedCredit.metadata.values.asScala.toMap shouldBe metadata
        exercisesVisibleTo(nonObserver) shouldBe empty
        eventually() {
          participant1.ledger_api.traffic.get_account(observedAccount).balance shouldBe 1000L
        }
      }

      "not let a party without the paymaster's authority publish credits" in { implicit env =>
        import env.*

        // The disclosure lets the submission reach the authorization check
        val issuerTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(paymaster),
          new CreditIssuer(paymaster.toProtoPrimitive).create.commands.asScala.toSeq,
          includeCreatedEventBlob = true,
        )
        val issuerCid = JavaDecodeUtil
          .decodeAllCreated(CreditIssuer.COMPANION)(issuerTx)
          .loneElement
          .id
          .toInterface(TrafficCreditReporter.INTERFACE)

        assertThrowsAndLogsCommandFailures(
          participant1.ledger_api.javaapi.commands.submit(
            Seq(untrustedPaymaster),
            issuerCid
              .exerciseTrafficCreditReporter_PublishCredit(
                account,
                1_000L,
                untrustedPaymaster.toProtoPrimitive,
                JList.of(untrustedPaymaster.toProtoPrimitive),
                emptyMetadata,
              )
              .commands
              .asScala
              .toSeq,
            disclosedContracts = Seq(JavaDecodeUtil.decodeDisclosedContracts(issuerTx).loneElement),
          ),
          _.shouldBeCantonErrorCode(AuthorizationError),
        )
      }

      "ingest top-ups credited by a composed purchase contract" in { implicit env =>
        import env.*

        val purchaseAccount = "purchase-account"

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
