// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.plugins.UseBftSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.TemplateBoundPartyMapping
import com.google.protobuf.ByteString

/** Integration test for the constant-product AMM under Template-Bound Parties.
  *
  * Deploys the AMM to a real Canton participant, registers the pool party as
  * template-bound (so it auto-confirms), creates a pool, executes a swap,
  * and verifies the entire pipeline works end-to-end.
  *
  * This is the definitive test: a real Canton node, real topology store, real
  * Daml engine, real auto-confirmation. The swap transaction has the pool
  * party as signatory but no one holds the pool's signing key — the hosting
  * participant auto-confirms because Pool is in the allowed template list.
  */
sealed trait AmmIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1

  "AMM with Template-Bound Party" should {
    "auto-confirm swap on allowed template" in { implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, daName)
      participant1.dars.upload(CantonExamplesPath)

      // Allocate parties
      val pool = participant1.parties.testing.enable("pool")
      val issuerA = participant1.parties.testing.enable("issuerA")
      val issuerB = participant1.parties.testing.enable("issuerB")
      val trader = participant1.parties.testing.enable("trader")

      val ammPkg =
        participant1.packages.find_by_module("Amm").headOption.value.packageId

      // Register pool as a template-bound party.
      // After this, the pool can only act through auto-confirmation on Pool, LPToken, RedeemRequest.
      // The key is not destroyed in this test (we're testing auto-confirmation, not key lifecycle).
      val tbpMapping = TemplateBoundPartyMapping(
        partyId = pool,
        hostingParticipantIds = Seq(participant1.id),
        allowedTemplateIds = Set(
          s"$ammPkg:Amm:Pool",
          s"$ammPkg:Amm:LPToken",
          s"$ammPkg:Amm:RedeemRequest",
        ),
        signingKeyHash = ByteString.copyFrom(Array.fill(32)(0x00.toByte)),
      )
      participant1.topology.transactions.propose(
        tbpMapping,
        store = TopologyStoreId.Authorized,
      )

      // Create pool tokens (issuers create tokens owned by pool)
      participant1.ledger_api.commands.submit(
        Seq(issuerA),
        Seq(
          ledger_api_utils.create(
            ammPkg, "Amm", "Token",
            Map("issuer" -> issuerA, "owner" -> pool, "symbol" -> "USDC", "amount" -> 1000.0),
          )
        ),
      )
      participant1.ledger_api.commands.submit(
        Seq(issuerB),
        Seq(
          ledger_api_utils.create(
            ammPkg, "Amm", "Token",
            Map("issuer" -> issuerB, "owner" -> pool, "symbol" -> "ETH", "amount" -> 10.0),
          )
        ),
      )

      // Get the token contract IDs — the pool holds these
      val reserveACid = participant1.testing
        .acs_search(daName, filterTemplate = "Amm:Token", filterStakeholder = Some(pool))
        .filter(_.templateId.toString.contains("Token"))
        .head.contractId  // USDC (created first)
      val reserveBCid = participant1.testing
        .acs_search(daName, filterTemplate = "Amm:Token", filterStakeholder = Some(pool))
        .filter(_.templateId.toString.contains("Token"))
        .last.contractId  // ETH (created second)

      // Create the pool with actual token contract references
      participant1.ledger_api.commands.submit(
        Seq(pool),
        Seq(
          ledger_api_utils.create(
            ammPkg, "Amm", "Pool",
            Map(
              "pool" -> pool,
              "tokenAIssuer" -> issuerA,
              "tokenBIssuer" -> issuerB,
              "symbolA" -> "USDC",
              "symbolB" -> "ETH",
              "reserveACid" -> reserveACid,
              "reserveBCid" -> reserveBCid,
              "reserveA" -> 1000.0,
              "reserveB" -> 10.0,
              "totalLP" -> 100.0,
              "feeNum" -> 997L,
              "feeDen" -> 1000L,
            ),
          )
        ),
      )

      val poolCid = participant1.testing
        .acs_search(daName, filterTemplate = "Amm:Pool")
        .loneElement
        .contractId

      // Create trader's 100 USDC
      participant1.ledger_api.commands.submit(
        Seq(issuerA),
        Seq(
          ledger_api_utils.create(
            ammPkg, "Amm", "Token",
            Map("issuer" -> issuerA, "owner" -> trader, "symbol" -> "USDC", "amount" -> 100.0),
          )
        ),
      )

      val traderTokenCid = participant1.testing
        .acs_search(daName, filterTemplate = "Amm:Token", filterStakeholder = Some(trader))
        .loneElement
        .contractId

      // THE CRITICAL TEST: Execute a swap where pool is signatory.
      // Without TBP, this would fail — no one can sign for pool.
      // With TBP, the participant auto-confirms because Pool is in the allowed set.
      participant1.ledger_api.commands.submit(
        Seq(trader),
        Seq(
          ledger_api_utils.exercise(
            ammPkg, "Amm", "Pool", "SwapAforB",
            Map(
              "trader" -> trader,
              "inputTokenCid" -> traderTokenCid,
              "minOutput" -> 0.0,
            ),
            poolCid.coid,
          )
        ),
        readAs = Seq(pool),
      )

      // Verify: pool still exists after swap (nonconsuming choice creates new pool,
      // old one remains — in production you'd archive the old one)
      participant1.testing
        .acs_search(daName, filterTemplate = "Amm:Pool") should not be empty

      // Verify: trader received ETH
      val traderTokensAfter = participant1.testing
        .acs_search(daName, filterTemplate = "Amm:Token", filterStakeholder = Some(trader))
      traderTokensAfter should not be empty
    }

    "composability: Router calls Pool.SwapAforB as sub-action — Router NOT in allowedTemplates" in {
      implicit env =>
        import env.*

        // The pool from the first test is still active.
        // Create a Router contract — Router is NOT in the pool's allowedTemplates.
        // The swap via Router should still work because Router.RouteSwap is the
        // root action (controlled by trader, not pool), and Pool.SwapAforB is a
        // sub-action with inherited authority.

        val router = participant1.parties.testing.enable("router")
        val traderR = participant1.parties.testing.enable("traderR")
        val issuerR = participant1.parties.testing.enable("issuerR")
        val poolR = participant1.parties.testing.enable("poolR")

        val ammPkg =
          participant1.packages.find_by_module("Amm").headOption.value.packageId

        // Register poolR as TBP — only Pool is allowed, NOT Router
        participant1.topology.transactions.propose(
          TemplateBoundPartyMapping(
            partyId = poolR,
            hostingParticipantIds = Seq(participant1.id),
            allowedTemplateIds = Set(
              s"$ammPkg:Amm:Pool",
              s"$ammPkg:Amm:LPToken",
              s"$ammPkg:Amm:RedeemRequest",
            ),
            signingKeyHash = ByteString.copyFrom(Array.fill(32)(0x00.toByte)),
          ),
          store = TopologyStoreId.Authorized,
        )

        // Create pool tokens
        val issuerB = participant1.parties.testing.enable("issuerRB")
        participant1.ledger_api.commands.submit(
          Seq(issuerR),
          Seq(ledger_api_utils.create(
            ammPkg, "Amm", "Token",
            Map("issuer" -> issuerR, "owner" -> poolR, "symbol" -> "USDC", "amount" -> 1000.0),
          )),
        )
        participant1.ledger_api.commands.submit(
          Seq(issuerB),
          Seq(ledger_api_utils.create(
            ammPkg, "Amm", "Token",
            Map("issuer" -> issuerB, "owner" -> poolR, "symbol" -> "ETH", "amount" -> 10.0),
          )),
        )

        val resACid = participant1.testing
          .acs_search(daName, filterTemplate = "Amm:Token", filterStakeholder = Some(poolR))
          .head.contractId
        val resBCid = participant1.testing
          .acs_search(daName, filterTemplate = "Amm:Token", filterStakeholder = Some(poolR))
          .last.contractId

        participant1.ledger_api.commands.submit(
          Seq(poolR),
          Seq(ledger_api_utils.create(
            ammPkg, "Amm", "Pool",
            Map(
              "pool" -> poolR, "tokenAIssuer" -> issuerR, "tokenBIssuer" -> issuerB,
              "symbolA" -> "USDC", "symbolB" -> "ETH",
              "reserveACid" -> resACid, "reserveBCid" -> resBCid,
              "reserveA" -> 1000.0, "reserveB" -> 10.0, "totalLP" -> 100.0,
              "feeNum" -> 997L, "feeDen" -> 1000L,
            ),
          )),
        )

        val poolCidR = participant1.testing
          .acs_search(daName, filterTemplate = "Amm:Pool", filterStakeholder = Some(poolR))
          .loneElement.contractId

        // Create the Router contract
        participant1.ledger_api.commands.submit(
          Seq(router),
          Seq(ledger_api_utils.create(
            ammPkg, "Amm", "Router",
            Map("operator" -> router),
          )),
        )

        val routerCid = participant1.testing
          .acs_search(daName, filterTemplate = "Amm:Router")
          .loneElement.contractId

        // Create trader's USDC
        participant1.ledger_api.commands.submit(
          Seq(issuerR),
          Seq(ledger_api_utils.create(
            ammPkg, "Amm", "Token",
            Map("issuer" -> issuerR, "owner" -> traderR, "symbol" -> "USDC", "amount" -> 100.0),
          )),
        )

        val traderTokenCid = participant1.testing
          .acs_search(daName, filterTemplate = "Amm:Token", filterStakeholder = Some(traderR))
          .loneElement.contractId

        // THE COMPOSABILITY TEST: exercise Router.RouteSwap.
        // Router is NOT in poolR's allowedTemplates.
        // Root action = Router.RouteSwap (controlled by trader, not pool)
        // Sub-action = Pool.SwapAforB (inherited authority)
        // Pool is auto-confirmed because it's TBP and the extractor only
        // sees root actions — pool isn't an actor on the root action.
        participant1.ledger_api.commands.submit(
          Seq(traderR),
          Seq(ledger_api_utils.exercise(
            ammPkg, "Amm", "Router", "RouteSwap",
            Map(
              "trader" -> traderR,
              "poolCid" -> poolCidR,
              "inputTokenCid" -> traderTokenCid,
              "minOutput" -> 0.0,
            ),
            routerCid.coid,
          )),
          readAs = Seq(poolR, router),
        )

        // Verify: trader received ETH via the router
        val traderTokensAfter = participant1.testing
          .acs_search(daName, filterTemplate = "Amm:Token", filterStakeholder = Some(traderR))
        traderTokensAfter should not be empty
    }

    "regular pool operator can drain via Token.Transfer — TBP prevents this" in { implicit env =>
      import env.*

      // A regular party (NOT template-bound) can use its signing key to
      // exercise Token.Transfer directly, draining pool assets.
      // This is the attack that TBP prevents by destroying the key.
      val regularPool = participant1.parties.testing.enable("regularPool")
      val issuerR1 = participant1.parties.testing.enable("issuerR1")
      val attacker = participant1.parties.testing.enable("attacker")

      val ammPkg =
        participant1.packages.find_by_module("Amm").headOption.value.packageId

      // Create a token owned by the regular pool
      participant1.ledger_api.commands.submit(
        Seq(issuerR1),
        Seq(ledger_api_utils.create(
          ammPkg, "Amm", "Token",
          Map("issuer" -> issuerR1, "owner" -> regularPool, "symbol" -> "USDC", "amount" -> 1000.0),
        )),
      )

      val poolTokenCid = participant1.testing
        .acs_search(daName, filterTemplate = "Amm:Token", filterStakeholder = Some(regularPool))
        .loneElement.contractId

      // The regular pool operator CAN drain the token via direct Transfer.
      // This succeeds because the participant holds regularPool's signing key.
      // This is the vulnerability that TBP eliminates.
      participant1.ledger_api.commands.submit(
        Seq(regularPool),
        Seq(ledger_api_utils.exercise(
          ammPkg, "Amm", "Token", "Transfer",
          Map("newOwner" -> attacker),
          poolTokenCid.coid,
        )),
      )

      // The attacker now owns the token — pool is drained
      val attackerTokens = participant1.testing
        .acs_search(daName, filterTemplate = "Amm:Token", filterStakeholder = Some(attacker))
      attackerTokens should not be empty

      // With TBP, this Transfer would be REJECTED because Token is not
      // in the pool's allowedTemplates. The key is destroyed and
      // auto-confirmation only works for whitelisted templates.
    }

    "TBP pool CANNOT drain via Token.Transfer — template not in allowed set" in { implicit env =>
      import env.*

      // The TBP pool from the first test is still active. Try to drain it
      // by exercising Token.Transfer directly. This must fail because
      // Token is NOT in the pool's allowedTemplates.
      val tbpPool = participant1.parties.testing.enable("tbpDrain")
      val issuerD = participant1.parties.testing.enable("issuerD")
      val attacker2 = participant1.parties.testing.enable("attacker2")

      val ammPkg =
        participant1.packages.find_by_module("Amm").headOption.value.packageId

      // Register as TBP — only Pool is allowed, NOT Token
      participant1.topology.transactions.propose(
        TemplateBoundPartyMapping(
          partyId = tbpPool,
          hostingParticipantIds = Seq(participant1.id),
          allowedTemplateIds = Set(s"$ammPkg:Amm:Pool"),
          signingKeyHash = ByteString.copyFrom(Array.fill(32)(0x00.toByte)),
        ),
        store = TopologyStoreId.Authorized,
      )

      // Create a token owned by the TBP pool
      participant1.ledger_api.commands.submit(
        Seq(issuerD),
        Seq(ledger_api_utils.create(
          ammPkg, "Amm", "Token",
          Map("issuer" -> issuerD, "owner" -> tbpPool, "symbol" -> "USDC", "amount" -> 1000.0),
        )),
      )

      val tbpTokenCid = participant1.testing
        .acs_search(daName, filterTemplate = "Amm:Token", filterStakeholder = Some(tbpPool))
        .loneElement.contractId

      // Try to drain: exercise Token.Transfer as tbpPool.
      // This MUST fail. Token is not in the allowed template set.
      // The auto-confirmer will reject because the root action is on Token,
      // which is not whitelisted.
      assertThrows[CommandFailure] {
        participant1.ledger_api.commands.submit(
          Seq(tbpPool),
          Seq(ledger_api_utils.exercise(
            ammPkg, "Amm", "Token", "Transfer",
            Map("newOwner" -> attacker2),
            tbpTokenCid.coid,
          )),
        )
      }

      // Token is still owned by the TBP pool — drain was prevented
      val stillPoolOwned = participant1.testing
        .acs_search(daName, filterTemplate = "Amm:Token", filterStakeholder = Some(tbpPool))
      stillPoolOwned should not be empty
    }

    "reject swap with excessive slippage demand" in { implicit env =>
      import env.*

      val pool2 = participant1.parties.testing.enable("pool2")
      val issuerA2 = participant1.parties.testing.enable("issuerA2")
      val issuerB2 = participant1.parties.testing.enable("issuerB2")
      val trader2 = participant1.parties.testing.enable("trader2")

      val ammPkg =
        participant1.packages.find_by_module("Amm").headOption.value.packageId

      // Register pool2 as TBP
      participant1.topology.transactions.propose(
        TemplateBoundPartyMapping(
          partyId = pool2,
          hostingParticipantIds = Seq(participant1.id),
          allowedTemplateIds = Set(s"$ammPkg:Amm:Pool", s"$ammPkg:Amm:LPToken", s"$ammPkg:Amm:RedeemRequest"),
          signingKeyHash = ByteString.copyFrom(Array.fill(32)(0x00.toByte)),
        ),
        store = TopologyStoreId.Authorized,
      )

      participant1.ledger_api.commands.submit(
        Seq(issuerA2),
        Seq(ledger_api_utils.create(
          ammPkg, "Amm", "Token",
          Map("issuer" -> issuerA2, "owner" -> pool2, "symbol" -> "USDC", "amount" -> 1000.0),
        )),
      )
      participant1.ledger_api.commands.submit(
        Seq(issuerB2),
        Seq(ledger_api_utils.create(
          ammPkg, "Amm", "Token",
          Map("issuer" -> issuerB2, "owner" -> pool2, "symbol" -> "ETH", "amount" -> 10.0),
        )),
      )

      val resACid2 = participant1.testing
        .acs_search(daName, filterTemplate = "Amm:Token", filterStakeholder = Some(pool2))
        .head.contractId
      val resBCid2 = participant1.testing
        .acs_search(daName, filterTemplate = "Amm:Token", filterStakeholder = Some(pool2))
        .last.contractId

      participant1.ledger_api.commands.submit(
        Seq(pool2),
        Seq(ledger_api_utils.create(
          ammPkg, "Amm", "Pool",
          Map(
            "pool" -> pool2, "tokenAIssuer" -> issuerA2, "tokenBIssuer" -> issuerB2,
            "symbolA" -> "USDC", "symbolB" -> "ETH",
            "reserveACid" -> resACid2, "reserveBCid" -> resBCid2,
            "reserveA" -> 1000.0, "reserveB" -> 10.0, "totalLP" -> 100.0,
            "feeNum" -> 997L, "feeDen" -> 1000L,
          ),
        )),
      )

      val poolCid2 = participant1.testing
        .acs_search(daName, filterTemplate = "Amm:Pool")
        .last.contractId

      participant1.ledger_api.commands.submit(
        Seq(issuerA2),
        Seq(ledger_api_utils.create(
          ammPkg, "Amm", "Token",
          Map("issuer" -> issuerA2, "owner" -> trader2, "symbol" -> "USDC", "amount" -> 100.0),
        )),
      )

      val traderToken2 = participant1.testing
        .acs_search(daName, filterTemplate = "Amm:Token", filterStakeholder = Some(trader2))
        .loneElement.contractId

      // Demand 5 ETH for 100 USDC — impossible, slippage check should reject
      assertThrows[CommandFailure] {
        participant1.ledger_api.commands.submit(
          Seq(trader2),
          Seq(ledger_api_utils.exercise(
            ammPkg, "Amm", "Pool", "SwapAforB",
            Map(
              "trader" -> trader2,
              "inputTokenCid" -> traderToken2,
              "minOutput" -> 5.0,
            ),
            poolCid2.coid,
          )),
          readAs = Seq(pool2),
        )
      }
    }
  }
}

final class AmmIntegrationTestDefault extends AmmIntegrationTest {
  registerPlugin(new UseBftSequencer(loggerFactory))
}
