// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.data.State
import com.daml.lf.data.{Bytes, ImmArray}
import com.daml.lf.transaction.{Node, NodeId, TransactionVersion}
import com.digitalasset.canton.*
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.TransactionViewDecomposition.{NewView, SameView}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.RollbackContext.{RollbackScope, RollbackSibling}
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.util.LfTransactionUtil

import scala.collection.immutable.Seq

/** This builder can build uses [[cats.data.State]] based approach to building transactions
  * so the author does not need to keep track of lots of ids.
  *
  * Its primary purpose is in building transactions that contain rollback nodes.
  */
class RollbackTransactionBuilder(override protected val loggerFactory: NamedLoggerFactory)
    extends NamedLogging {

  import RollbackTransactionBuilder.*

  private val directEc = DirectExecutionContext(logger)
  private val exampleTxFactory = new ExampleTransactionFactory()()(directEc)

  val unit: State[TxBuildState, Unit] = State.pure(())
  val nextInt: State[TxBuildState, Int] = State(s => (s.withInc(), s.i))
  val nextNodeId: State[TxBuildState, NodeId] = nextInt.map(i => NodeId(i))
  val nextHash: State[TxBuildState, LfHash] = nextInt.map(DefaultDamlValues.lfhash)
  val nextContractId: State[TxBuildState, LfContractId] = nextHash.map(LfContractId.V1.apply)
  val nextSuffixedContractId: State[TxBuildState, LfContractId] = nextInt.map { i =>
    LfContractId.V1(DefaultDamlValues.lfhash(i), Bytes.assertFromString(f"$i%04x"))
  }

  def nextCreateIds(
      create: LfContractId => LfNodeCreate
  ): State[TxBuildState, CreateIds] =
    for {
      nodeId <- nextNodeId
      cId <- nextContractId
      _ <- State.modify[TxBuildState](_.withNode(nodeId -> create(cId)))
    } yield CreateIds(nodeId, cId)

  def nextCreate(
      create: LfContractId => LfNodeCreate
  ): State[TxBuildState, NodeId] = nextCreateIds(create).map(_.nodeId)

  def nextExercise(node: LfContractId => LfNodeExercises): State[TxBuildState, LfNodeId] = for {
    cId <- nextSuffixedContractId
    nodeId <- nextNode(node(cId))
  } yield nodeId

  def traverse(children: Seq[State[TxBuildState, LfNodeId]]): State[TxBuildState, Seq[LfNodeId]] = {
    children.foldLeft(State.pure[TxBuildState, Seq[LfNodeId]](Seq.empty)) { case (seqS, childS) =>
      for {
        seq <- seqS
        child <- childS
      } yield seq :+ child
    }
  }

  def nextRollbackFromChildren(
      children: Seq[State[TxBuildState, LfNodeId]]
  ): State[TxBuildState, LfNodeId] =
    for {
      childrenIds <- traverse(children)
      nodeId <- nextRollback(LfNodeRollback(ImmArray.from(childrenIds)))
    } yield nodeId

  def nextExerciseWithChildren(
      node: LfContractId => Seq[LfNodeId] => LfNodeExercises,
      children: Seq[State[TxBuildState, LfNodeId]],
  ): State[TxBuildState, LfNodeId] = for {
    cId <- nextSuffixedContractId
    childrenIds <- traverse(children)
    nodeId <- nextNode(node(cId)(childrenIds))
  } yield nodeId

  private def nextNode(node: LfNode): State[TxBuildState, LfNodeId] = for {
    nodeId <- nextNodeId
    _ <- State.modify[TxBuildState](_.withNode(nodeId -> node))
  } yield nodeId

  def nextFetch(node: LfNodeFetch): State[TxBuildState, LfNodeId] = nextNode(node)

  def nextNodeLookupByKey(node: LfNodeLookupByKey): State[TxBuildState, LfNodeId] = nextNode(node)

  def nextRollback(node: LfNodeRollback): State[TxBuildState, LfNodeId] = nextNode(node)

  def buildAndReset(rootNodeIds: Seq[NodeId]): State[TxBuildState, LfTransaction] = for {
    state <- State.get[TxBuildState]
    _ <- State.pure(TxBuildState.empty)
  } yield LfTransaction(state.nodes, ImmArray.from(rootNodeIds))

  def run[X](state: State[TxBuildState, X]): X = state.runA(TxBuildState.empty).value

  def versionedUnsuffixedTransaction(tx: LfTransaction): LfVersionedTransaction =
    LfVersionedTransaction(
      TransactionVersion.VDev,
      tx.nodes,
      tx.roots,
    )

  private def seeds(tx: LfTransaction): Map[LfNodeId, LfHash] = tx.nodes.collect({
    case (id, n) if LfTransactionUtil.nodeHasSeed(n) => id -> exampleTxFactory.lfTransactionSeed
  })

  private def metadata(tx: LfTransaction): TransactionMetadata =
    TransactionMetadata(exampleTxFactory.ledgerTime, exampleTxFactory.submissionTime, seeds(tx))

  def wellFormedUnsuffixedTransaction(tx: LfTransaction): WellFormedTransaction[WithoutSuffixes] =
    WellFormedTransaction.normalizeAndAssert(
      versionedUnsuffixedTransaction(tx),
      metadata(tx),
      WithoutSuffixes,
    )

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

object RollbackTransactionBuilder {

  final case class TxBuildState(i: Int, nodes: Map[NodeId, Node]) {
    def withInc(): TxBuildState = this.copy(i + 1)

    def withNode(n: (NodeId, Node)): TxBuildState = this.copy(nodes = nodes + n)
  }

  object TxBuildState {
    def empty: TxBuildState = TxBuildState(0, Map.empty)
  }

  final case class CreateIds(nodeId: NodeId, contractId: LfContractId)

  sealed trait RollbackDecomposition

  final case class RbNewTree(
      rb: RollbackScope,
      informees: Set[LfPartyId],
      children: Seq[RollbackDecomposition] = Seq.empty,
  ) extends RollbackDecomposition

  final case class RbSameTree(rb: RollbackScope) extends RollbackDecomposition
}
