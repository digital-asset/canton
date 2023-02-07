// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.daml.lf.data.ImmArray
import com.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.TransactionViewDecomposition.*
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol.{
  ConfirmationPolicy,
  ExampleTransactionFactory,
  LfNodeId,
  LfTransactionVersion,
  LfVersionedTransaction,
  RollbackContext,
  TransactionMetadata,
  WellFormedTransaction,
}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{PartyId, UniqueIdentifier}
import com.digitalasset.canton.{
  BaseTest,
  ComparesLfTransactions,
  HasExecutionContext,
  LfValue,
  NeedsNewLfContractIds,
}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Await
import scala.concurrent.duration.*

class TransactionViewDecompositionTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with ComparesLfTransactions
    with NeedsNewLfContractIds {

  ConfirmationPolicy.values foreach { confirmationPolicy =>
    s"With policy $confirmationPolicy" when {

      val factory = new ExampleTransactionFactory()(confirmationPolicy = confirmationPolicy)

      factory.standardHappyCases foreach { example =>
        s"decomposing $example into views" must {
          "yield the correct views" in {
            fromTransaction(
              confirmationPolicy,
              factory.topologySnapshot,
              example.wellFormedUnsuffixedTransaction,
              RollbackContext.empty,
            ).futureValue.toList shouldEqual example.rootViewDecompositions.toList
          }
        }
      }
    }
  }

  "A view decomposition" when {
    import ExampleTransactionFactory.*
    "a view has the same informees and thresholds as its parent" can {
      "not be constructed" in {

        val node = createNode(unsuffixedId(0))
        val informees = Set[Informee](ConfirmingParty(signatory, 1))
        val rootSeed = ExampleTransactionFactory.lfHash(-1)
        val child =
          NewView(
            node,
            informees,
            NonNegativeInt.one,
            Some(rootSeed),
            LfNodeId(0),
            Seq.empty,
            RollbackContext.empty,
          )

        an[IllegalArgumentException] should be thrownBy
          NewView(
            node,
            informees,
            NonNegativeInt.one,
            Some(rootSeed),
            LfNodeId(0),
            Seq(child),
            RollbackContext.empty,
          )
      }
    }

    "the nodes in a view have different informees or thresholds" can {
      "not be constructed" in {
        an[IllegalArgumentException] should be thrownBy
          Await.result(
            createWithConfirmationPolicy(
              ConfirmationPolicy.Signatory,
              defaultTopologySnapshot,
              createNode(unsuffixedId(0), signatories = Set(signatory)),
              Some(ExampleTransactionFactory.lfHash(-1)),
              LfNodeId(0),
              Seq(
                SameView(
                  createNode(unsuffixedId(0), signatories = Set(submitter)),
                  LfNodeId(1),
                  RollbackContext.empty,
                )
              ),
            ),
            10.seconds,
          )
      }
    }

    "there are lots of top-level nodes" can {
      "be constructed without stack overflow" in {
        val flatTransactionSize = 10000

        val decomposition = timeouts.default.await("Decomposing test transaction")(
          TransactionViewDecomposition.fromTransaction(
            ConfirmationPolicy.Signatory,
            mock[TopologySnapshot],
            createWellFormedTransaction(flatTransactionSize),
            RollbackContext.empty,
          )
        )

        decomposition.size shouldBe flatTransactionSize
      }
    }
  }

  private def createWellFormedTransaction(size: Int): WellFormedTransaction[WithoutSuffixes] = {
    val alice = PartyId(UniqueIdentifier.tryFromProtoPrimitive(s"alice::party")).toLf
    val bob = PartyId(UniqueIdentifier.tryFromProtoPrimitive(s"bob::party")).toLf
    val tb = TransactionBuilder()
    val lfNodes = (0 until size).map { nodeId =>
      val lfNode = tb.create(
        id = newLfContractIdUnsuffixed(),
        templateId = ExampleTransactionFactory.templateId,
        argument = args(
          LfValue.ValueParty(alice),
          LfValue.ValueParty(bob),
          args(notUsed),
          seq(LfValue.ValueParty(bob)),
        ),
        signatories = Set(alice),
        observers = Set(bob),
        key = None,
      )

      LfNodeId(nodeId) -> lfNode
    }.toMap

    WellFormedTransaction
      .normalizeAndCheck(
        LfVersionedTransaction(LfTransactionVersion.VDev, lfNodes, ImmArray.from(lfNodes.keys)),
        TransactionMetadata(
          CantonTimestamp.Epoch,
          CantonTimestamp.Epoch,
          lfNodes.map { case (nid, _) => nid -> hasher() },
        ),
        WithoutSuffixes,
      )
      .value
  }
}
