// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.daml.lf.data.ImmArray
import com.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.TransactionViewDecomposition.*
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.TrustLevel
import com.digitalasset.canton.topology.{PartyId, UniqueIdentifier}
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

        exampleTransactionFactory.standardHappyCases.filter(
          _.supportedConfirmationPolicies.contains(confirmationPolicy)
        ) foreach { example =>
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
        val informees = Set[Informee](ConfirmingParty(signatory, 1, TrustLevel.Ordinary))
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

      "fails validation" in {

        val createWithSignatory = createNode(unsuffixedId(0), signatories = Set(signatory))
        val createWithSubmitter = createNode(unsuffixedId(0), signatories = Set(submitter))

        val (viewInformees, viewThreshold) = ConfirmationPolicy.Signatory
          .informeesAndThreshold(
            createWithSignatory,
            None,
            defaultTopologySnapshot,
            testedProtocolVersion,
          )
          .futureValue

        val viewWithInconsistentInformees = NewView(
          createWithSignatory,
          viewInformees,
          viewThreshold,
          Some(ExampleTransactionFactory.lfHash(-1)),
          LfNodeId(0),
          Seq(SameView(createWithSubmitter, LfNodeId(1), RollbackContext.empty)),
          RollbackContext.empty,
        )

        val actual = viewWithInconsistentInformees
          .compliesWith(
            ConfirmationPolicy.Signatory,
            None,
            defaultTopologySnapshot,
            testedProtocolVersion,
          )
          .value
          .futureValue
        actual.isLeft shouldBe true
      }
    }

    "there are lots of top-level nodes" can {
      "be constructed without stack overflow" in {
        val flatTransactionSize = 10000

        val decomposition = timeouts.default.await("Decomposing test transaction")(
          TransactionViewDecompositionFactory.V2.fromTransaction(
            ConfirmationPolicy.Signatory,
            mock[TopologySnapshot],
            createWellFormedTransaction(flatTransactionSize),
            RollbackContext.empty,
            None,
          )
        )

        decomposition.size shouldBe flatTransactionSize
      }
    }

    "a transaction with nested rollbacks" can {

      import RollbackTransactionBuilder.*

      val alice: LfPartyId = LfPartyId.assertFromString("alice::default")
      val bob: LfPartyId = LfPartyId.assertFromString("bob::default")
      val carol: LfPartyId = LfPartyId.assertFromString("carol::default")

      val embeddedRollbackExample: LfTransaction = {
        RollbackTransactionBuilder.run(for {
          rootId <- nextExerciseWithChildren( // NewView
            n => c => exerciseNode(n, signatories = Set(alice), children = c.toList), // SameView
            Seq(
              nextExerciseWithChildren(
                n => c => exerciseNode(n, signatories = Set(alice), children = c.toList),
                Seq(
                  nextRollbackFromChildren(
                    Seq(
                      nextExercise(exerciseNode(_, signatories = Set(alice, carol)))
                    )
                  )
                ),
              ),
              nextExercise(exerciseNode(_, signatories = Set(alice, bob))),
            ),
          )
          tx <- buildAndReset(rootNodeIds = Seq(rootId))
        } yield tx)
      }

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
            RollbackTransactionBuilder.wellFormedUnsuffixedTransaction(embeddedRollbackExample),
            RollbackContext.empty,
            None,
          )
          .futureValue

        val actual = rollbackDecomposition(decomposition)

        actual shouldBe expected
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
