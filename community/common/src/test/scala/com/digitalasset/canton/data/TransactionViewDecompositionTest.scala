// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.daml.lf.transaction.test.TestNodeBuilder.CreateKey
import com.daml.lf.transaction.test.TreeTransactionBuilder.NodeWrapper
import com.daml.lf.transaction.test.{TestIdFactory, TestNodeBuilder, TreeTransactionBuilder}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.TransactionViewDecomposition.*
import com.digitalasset.canton.protocol.RollbackContext.{RollbackScope, RollbackSibling}
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.TrustLevel
import com.digitalasset.canton.topology.{PartyId, UniqueIdentifier}
import com.digitalasset.canton.util.LfTransactionUtil
import com.digitalasset.canton.{
  BaseTest,
  ComparesLfTransactions,
  HasExecutionContext,
  LfPartyId,
  LfValue,
  NeedsNewLfContractIds,
}
import org.scalatest.wordspec.AnyWordSpec

class TransactionViewDecompositionTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with ComparesLfTransactions
    with NeedsNewLfContractIds {

  lazy val factory: TransactionViewDecompositionFactory = TransactionViewDecompositionFactory(
    testedProtocolVersion
  )
  s"With factory ${factory.getClass.getSimpleName}" when {

    ConfirmationPolicy.values foreach { confirmationPolicy =>
      s"With policy $confirmationPolicy" when {

        val exampleTransactionFactory =
          new ExampleTransactionFactory()(confirmationPolicy = confirmationPolicy)

        exampleTransactionFactory.standardHappyCases foreach { example =>
          s"decomposing $example into views" must {
            "yield the correct views" in {
              factory
                .fromTransaction(
                  confirmationPolicy,
                  exampleTransactionFactory.topologySnapshot,
                  example.wellFormedUnsuffixedTransaction,
                  RollbackContext.empty,
                  Some(ExampleTransactionFactory.submitter),
                )
                .futureValue
                .toList shouldEqual example.rootViewDecompositions.toList
            }
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
        val informees =
          Set[Informee](ConfirmingParty(signatory, PositiveInt.one, TrustLevel.Ordinary))
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

    "there are lots of top-level nodes" can {
      "be constructed without stack overflow" in {
        val flatTransactionSize = 10000

        val decomposition = timeouts.default.await("Decomposing test transaction")(
          TransactionViewDecompositionFactory.V2.fromTransaction(
            ConfirmationPolicy.Signatory,
            mock[TopologySnapshot],
            wftWithCreateNodes(flatTransactionSize),
            RollbackContext.empty,
            None,
          )
        )

        decomposition.size shouldBe flatTransactionSize
      }
    }

    "a transaction with nested rollbacks" can {

      import RollbackDecomposition.*
      import com.daml.lf.transaction.test.TreeTransactionBuilder.*

      object tif extends TestIdFactory

      val alice: LfPartyId = LfPartyId.assertFromString("alice::default")
      val bob: LfPartyId = LfPartyId.assertFromString("bob::default")
      val carol: LfPartyId = LfPartyId.assertFromString("carol::default")

      val embeddedRollbackExample: LfVersionedTransaction = toVersionedTransaction(
        exerciseNode(tif.newCid, signatories = Set(alice)).withChildren(
          exerciseNode(tif.newCid, signatories = Set(alice)).withChildren(
            TestNodeBuilder
              .rollback()
              .withChildren(
                exerciseNode(tif.newCid, signatories = Set(alice, carol))
              )
          ),
          exerciseNode(tif.newCid, signatories = Set(alice, bob)),
        )
      )

      val expected = List(
        RbNewTree(
          rbScope(1),
          Set(alice),
          List[RollbackDecomposition](
            RbSameTree(rbScope(1)),
            RbNewTree(rbScope(1, 1), Set(alice, carol)),
            RbNewTree(rbScope(2), Set(alice, bob)),
          ),
        )
      )

      "does not re-used rollback contexts" in {

        val decomposition = TransactionViewDecompositionFactory.V2
          .fromTransaction(
            ConfirmationPolicy.Signatory,
            defaultTopologySnapshot,
            toWellFormedUnsuffixedTransaction(embeddedRollbackExample),
            RollbackContext.empty,
            None,
          )
          .futureValue

        val actual = RollbackDecomposition.rollbackDecomposition(decomposition)

        actual shouldBe expected
      }
    }

  }

  private def wftWithCreateNodes(size: Int): WellFormedTransaction[WithoutSuffixes] = {
    val alice = PartyId(UniqueIdentifier.tryFromProtoPrimitive(s"alice::party")).toLf
    val bob = PartyId(UniqueIdentifier.tryFromProtoPrimitive(s"bob::party")).toLf

    val tx = TreeTransactionBuilder.toVersionedTransaction(
      (0 until size)
        .map[NodeWrapper] { _ =>
          TestNodeBuilder.create(
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
            key = CreateKey.NoKey,
          )
        } *
    )

    toWellFormedUnsuffixedTransaction(tx)

  }

  private def toWellFormedUnsuffixedTransaction(
      tx: LfVersionedTransaction
  ): WellFormedTransaction[WithoutSuffixes] = {
    WellFormedTransaction
      .normalizeAndCheck(
        tx,
        TransactionMetadata(
          CantonTimestamp.Epoch,
          CantonTimestamp.Epoch,
          tx.nodes.collect { case (nid, n) if LfTransactionUtil.nodeHasSeed(n) => nid -> hasher() },
        ),
        WithoutSuffixes,
      )
      .value
  }

}

sealed trait RollbackDecomposition
object RollbackDecomposition {

  final case class RbNewTree(
      rb: RollbackScope,
      informees: Set[LfPartyId],
      children: Seq[RollbackDecomposition] = Seq.empty,
  ) extends RollbackDecomposition

  final case class RbSameTree(rb: RollbackScope) extends RollbackDecomposition

  /** The purpose of this method is to map a tree [[TransactionViewDecomposition]] onto a [[RollbackDecomposition]]
    * hierarchy aid comparison. The [[RollbackContext.nextChild]] value is significant but is not available
    * for inspection or construction. For this reason we use trick of entering a rollback context and then converting
    * to a rollback scope that has as its last sibling the nextChild value.
    */
  def rollbackDecomposition(
      decompositions: Seq[TransactionViewDecomposition]
  ): List[RollbackDecomposition] = {
    decompositions
      .map[RollbackDecomposition] {
        case view: NewView =>
          RbNewTree(
            view.rbContext.enterRollback.rollbackScope.toList,
            view.informees.map(_.party),
            rollbackDecomposition(view.tailNodes),
          )
        case view: SameView =>
          RbSameTree(view.rbContext.enterRollback.rollbackScope.toList)
      }
      .toList
  }

  def rbScope(rollbackScope: RollbackSibling*): RollbackScope = rollbackScope.toList

}
