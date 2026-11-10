// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.explicitdisclosure

import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.javaapi.data.{DisclosedContract, ExercisedEvent, Transaction, Value}
import com.digitalasset.canton.damltestslf23.java.tokenmint.{Rules, Token, TokenFactory}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.PartyId

import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*

/** Checks the interaction of contract keys with explicit disclosure: a by-key operation, executed
  * by a party that is authorized by the key maintainers but cannot see the keyed contract, only
  * resolves the key if the keyed contract is explicitly disclosed.
  */
final class ExplicitDisclosureContractKeysIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  private val amount: Long = 7L

  override def environmentDefinition: EnvironmentDefinition = EnvironmentDefinition.P2_S1M1
    .withSetup { implicit env =>
      import env.*
      participants.all.synchronizers.connect_local(sequencer1, alias = daName)
      participants.all.dars.upload(CantonTestsLF23Path)
    }

  "Explicit disclosure" should {
    /*
    Scenario:
    - issuer (on participant1) creates Rules with key (issuer, "USD"), visible only to issuer
    - issuer creates TokenFactory with minter (on participant2) as observer
    - minter exercises TokenFactory.Mint: issuer's authority comes from TokenFactory,
      but minter cannot see Rules
    - Without disclosure, lookupNByKey finds nothing and Mint returns None
    - With Rules disclosed, lookupNByKey finds Rules: Mint creates a Token and returns Some amount
     */
    "allow a non-stakeholder to look up a disclosed contract by key" in { implicit env =>
      import env.*

      val (issuer, minter, rules, disclosedRules, tokenFactory) = setup("lookup")
      val mintCommands = tokenFactory.id.exerciseMint().commands.asScala.toSeq

      // Without disclosure, minter does not see Rules, so the lookup succeeds but finds nothing
      val mintTxWithoutDisclosure = participant2.ledger_api.javaapi.commands.submit(
        actAs = Seq(minter),
        commands = mintCommands,
        transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
      )

      exerciseResultOptionalInt64(mintTxWithoutDisclosure, "Mint") shouldBe None
      JavaDecodeUtil.decodeAllCreated(Token.COMPANION)(mintTxWithoutDisclosure) shouldBe empty

      // With disclosure, the lookup resolves to the disclosed Rules
      val mintTx = participant2.ledger_api.javaapi.commands.submit(
        actAs = Seq(minter),
        commands = mintCommands,
        disclosedContracts = Seq(disclosedRules),
        transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
      )

      exerciseResultOptionalInt64(mintTx, "Mint") shouldBe Some(amount)
      assertToken(mintTx, issuer, minter, rules)
    }

    /*
    Same scenario as above, using TokenFactory.MintF (fetchByKey) instead of TokenFactory.Mint (lookupNByKey)
     */
    "allow a non-stakeholder to fetch a disclosed contract by key" in { implicit env =>
      import env.*

      val (issuer, minter, rules, disclosedRules, tokenFactory) = setup("fetch")
      val mintFCommands = tokenFactory.id.exerciseMintF().commands.asScala.toSeq

      // Without disclosure, the key does not resolve and fetchByKey aborts the interpretation
      assertThrowsAndLogsCommandFailures(
        participant2.ledger_api.javaapi.commands.submit(
          actAs = Seq(minter),
          commands = mintFCommands,
        ),
        _.shouldBeCommandFailure(
          CommandExecutionErrors.Interpreter.LookupErrors.ContractKeyNotFound
        ),
      )

      // With disclosure, the fetch resolves to the disclosed Rules
      val mintFTx = participant2.ledger_api.javaapi.commands.submit(
        actAs = Seq(minter),
        commands = mintFCommands,
        disclosedContracts = Seq(disclosedRules),
        transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
      )

      exerciseResultInt64(mintFTx, "MintF") shouldBe amount
      assertToken(mintFTx, issuer, minter, rules)
    }

    /*
    Scenario:
    - Same setup as above: issuer creates Rules with key issuer and TokenFactory with minter
    - shadyIssuer (on participant2) creates its own Rules with key shadyIssuer and a
      different amount
    - minter exercises TokenFactory.Mint and TokenFactory.MintF, disclosing shadyIssuer's Rules
    - The key of shadyIssuer's Rules does not match the key issuer used by TokenFactory,
      so the disclosed contract is never picked up by the by-key operations:
      - Mint returns None and does not create a Token
      - MintF is rejected with CONTRACT_KEY_NOT_FOUND
    - When both Rules contracts are disclosed, only issuer's Rules is used
     */
    "not resolve a key to a disclosed contract with a different maintainer" in { implicit env =>
      import env.*

      val (issuer, minter, rules, disclosedRules, tokenFactory) = setup("shady")
      val shadyIssuer = participant2.parties.enable("shadyIssuer")
      val shadyAmount = 0L

      // shadyIssuer creates its own Rules with the same name but a different maintainer
      val shadyRulesTx = participant2.ledger_api.javaapi.commands.submit(
        actAs = Seq(shadyIssuer),
        commands =
          new Rules(shadyIssuer.toProtoPrimitive, shadyAmount).create.commands.asScala.toSeq,
        includeCreatedEventBlob = true,
      )
      val disclosedShadyRules = JavaDecodeUtil.decodeDisclosedContracts(shadyRulesTx).loneElement

      val mintCommands = tokenFactory.id.exerciseMint().commands.asScala.toSeq
      val mintFCommands = tokenFactory.id.exerciseMintF().commands.asScala.toSeq

      // Mint: the lookup by key (issuer, "USD") does not find shadyIssuer's Rules
      val mintTxWithShadyRules = participant2.ledger_api.javaapi.commands.submit(
        actAs = Seq(minter),
        commands = mintCommands,
        disclosedContracts = Seq(disclosedShadyRules),
        transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
      )

      exerciseResultOptionalInt64(mintTxWithShadyRules, "Mint") shouldBe None
      JavaDecodeUtil.decodeAllCreated(Token.COMPANION)(mintTxWithShadyRules) shouldBe empty

      // MintF: the fetch by key (issuer, "USD") does not resolve to shadyIssuer's Rules
      assertThrowsAndLogsCommandFailures(
        participant2.ledger_api.javaapi.commands.submit(
          actAs = Seq(minter),
          commands = mintFCommands,
          disclosedContracts = Seq(disclosedShadyRules),
        ),
        _.shouldBeCommandFailure(
          CommandExecutionErrors.Interpreter.LookupErrors.ContractKeyNotFound
        ),
      )

      // With both Rules disclosed, only issuer's Rules matches the key
      val mintTx = participant2.ledger_api.javaapi.commands.submit(
        actAs = Seq(minter),
        commands = mintCommands,
        disclosedContracts = Seq(disclosedShadyRules, disclosedRules),
        transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
      )

      exerciseResultOptionalInt64(mintTx, "Mint") shouldBe Some(amount)
      assertToken(mintTx, issuer, minter, rules)

      val mintFTx = participant2.ledger_api.javaapi.commands.submit(
        actAs = Seq(minter),
        commands = mintFCommands,
        disclosedContracts = Seq(disclosedShadyRules, disclosedRules),
        transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
      )

      exerciseResultInt64(mintFTx, "MintF") shouldBe amount
      assertToken(mintFTx, issuer, minter, rules)
    }
  }

  /** Returns the exercise result of type `Optional Int` of the given choice */
  private def exerciseResultOptionalInt64(tx: Transaction, choice: String): Option[Long] =
    exerciseResult(tx, choice)
      .asOptional()
      .toScala
      .value
      .getValue
      .toScala
      .map(_.asInt64().toScala.value.getValue.longValue())

  /** Returns the exercise result of type `Int` of the given choice */
  private def exerciseResultInt64(tx: Transaction, choice: String): Long =
    exerciseResult(tx, choice)
      .asInt64()
      .toScala
      .value
      .getValue

  private def exerciseResult(tx: Transaction, choice: String): Value =
    tx.getEvents.asScala
      .collect { case e: ExercisedEvent if e.getChoice == choice => e }
      .loneElement
      .getExerciseResult

  private def assertToken(
      tx: Transaction,
      issuer: PartyId,
      minter: PartyId,
      rules: Rules.Contract,
  ): Unit = {
    val token = JavaDecodeUtil.decodeAllCreated(Token.COMPANION)(tx).loneElement
    token.data.issuer shouldBe issuer.toProtoPrimitive
    token.data.minter shouldBe minter.toProtoPrimitive
    token.data.amount shouldBe amount
    token.data.rulesIssuer shouldBe rules.data.issuer
  }

  private def setup(suffix: String)(implicit
      env: TestConsoleEnvironment
  ): (PartyId, PartyId, Rules.Contract, DisclosedContract, TokenFactory.Contract) = {
    import env.*

    val issuer = participant1.parties.enable(s"issuer-$suffix")
    val minter = participant2.parties.enable(s"minter-$suffix")

    // Create Rules visible only to issuer
    val rulesTx = participant1.ledger_api.javaapi.commands.submit(
      actAs = Seq(issuer),
      commands = new Rules(issuer.toProtoPrimitive, amount).create.commands.asScala.toSeq,
      includeCreatedEventBlob = true,
    )
    val rules = JavaDecodeUtil.decodeAllCreated(Rules.COMPANION)(rulesTx).loneElement
    val disclosedRules = JavaDecodeUtil.decodeDisclosedContracts(rulesTx).loneElement

    // Create TokenFactory, which carries issuer's authority into the choices exercised by minter
    val tokenFactory = JavaDecodeUtil
      .decodeAllCreated(TokenFactory.COMPANION)(
        participant1.ledger_api.javaapi.commands.submit(
          actAs = Seq(issuer),
          commands = new TokenFactory(
            issuer.toProtoPrimitive,
            minter.toProtoPrimitive,
          ).create.commands.asScala.toSeq,
        )
      )
      .loneElement

    eventually() {
      participant2.ledger_api.state.acs
        .active_contracts_of_party(minter)
        .map(_.createdEvent.value.contractId) should contain(tokenFactory.id.contractId)
    }

    (issuer, minter, rules, disclosedRules, tokenFactory)
  }
}
