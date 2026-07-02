// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.Monoid
import cats.data.NonEmptyChainImpl.*
import cats.data.{Chain, NonEmptyChain}
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import com.digitalasset.canton.data.{
  NoPathRollbackContextFactory,
  PathRollbackContextFactory,
  RollbackContextFactory,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.protocol.PathRollbackContext.RollbackSibling
import com.digitalasset.canton.protocol.TransactionMerge.NodesRootsSeeds
import com.digitalasset.canton.protocol.WellFormedTransaction.{
  WithAbsoluteSuffixes,
  WithSuffixesAndMerged,
}
import com.digitalasset.canton.util.{Checked, ErrorUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{checked, protocol}
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.transaction.Transaction
import com.digitalasset.nonempty.NonEmpty

import scala.collection.immutable.HashMap
import scala.collection.mutable

object TransactionMerge {

  def apply(protocolVersion: ProtocolVersion): TransactionMerge =
    if (protocolVersion >= ProtocolVersion.v36) NoPathTransactionMerge
    else PathTransactionMerge

  final case class NodesRootsSeeds(
      nodes: Map[LfNodeId, LfNode],
      roots: Chain[LfNodeId],
      seeds: Map[LfNodeId, LfHash],
  ) {

    def rollback: NodesRootsSeeds = {
      val rollbackNodeId = LfNodeId(
        nodes.keys.map(_.index).maxOption.fold(0)(max => Math.addExact(max, 1))
      )
      val rollbackNode = LfNodeRollback(roots.toList.to(ImmArray))
      copy(
        nodes = nodes + (rollbackNodeId -> rollbackNode),
        roots = Chain(rollbackNodeId),
      )
    }

    def offsetFrom(offset: Int): NodesRootsSeeds = {
      def tx = Transaction(nodes, roots.toList.to(ImmArray))
      val offsetNodeIds = tx.nodeIdNormalization.fmap(n => LfNodeId(Math.addExact(n.index, offset)))
      val offsetTx = tx.mapNodeId(offsetNodeIds)
      val offsetSeeds = seeds.view.map { case (nodeId, hash) =>
        offsetNodeIds(nodeId) -> hash
      }.toMap
      copy(offsetTx.nodes, Chain.fromSeq(offsetTx.roots.toList), seeds = offsetSeeds)
    }

  }

  object NodesRootsSeeds extends Monoid[NodesRootsSeeds] {
    val empty: NodesRootsSeeds = NodesRootsSeeds(Map.empty, Chain.empty, Map.empty)
    def combine(x: NodesRootsSeeds, y: NodesRootsSeeds): NodesRootsSeeds = {
      val offsetB = y.offsetFrom(x.nodes.size)
      NodesRootsSeeds(
        nodes = x.nodes ++ offsetB.nodes,
        roots = x.roots ++ offsetB.roots,
        seeds = x.seeds ++ offsetB.seeds,
      )
    }
  }

}

sealed abstract class TransactionMerge {

  protected val rollbackContextFactory: RollbackContextFactory

  /** Merges a list of well-formed transactions into one, adjusting node IDs as necessary.
    *
    * Root-level LfActionNodes with an associated rollback scope are embedded in rollback node
    * ancestors according to the [[buildNodesRootsSeeds]] implementation:
    *
    * @throws java.lang.IllegalArgumentException
    *   if transactions have non-unique ledger times or preparation times
    * @throws java.lang.IllegalStateException
    *   on internal programming errors
    * @throws java.lang.ArithmeticException
    *   on integer overflows
    * @return
    *   the merged WellFormedTransaction as well as any recoverable error encountered during
    *   merging; in case of errors, the request should be rejected. If the mediator approves
    *   nevertheless, the transaction should be committed.
    */
  def merge(
      transactionsWithRollbackScopes: NonEmpty[
        Seq[WithRollbackScope[WellFormedTransaction[WithAbsoluteSuffixes]]]
      ]
  )(implicit
      loggingContext: ErrorLoggingContext
  ): (WellFormedTransaction[WithSuffixesAndMerged], Option[String]) = {

    val transactions = transactionsWithRollbackScopes.map(_.unwrap)
    val ledgerTimes = transactions.map(_.metadata.ledgerTime).distinct
    val preparationTimes = transactions.map(_.metadata.preparationTime).distinct
    val versions = transactions.map(_.tx.version).distinct

    // Should not happen, as all views have the same root hash.
    ErrorUtil.requireArgument(
      ledgerTimes.sizeIs == 1,
      s"Different ledger times: ${ledgerTimes.mkString(", ")}",
    )
    val ledgerTime = ledgerTimes.head1

    // Should not happen, as all views have the same root hash.
    ErrorUtil.requireArgument(
      preparationTimes.sizeIs == 1,
      s"Different preparation times: ${preparationTimes.mkString(", ")}",
    )
    val preparationTime = preparationTimes.head1

    val version = protocol.maxSerializationVersion(versions)

    val nodesRootsSeeds = buildNodesRootsSeeds(transactionsWithRollbackScopes)

    val (mergedTx, mergedMetadata) = WellFormedTransaction.normalizeNodeIds(
      LfVersionedTransaction(
        version,
        nodesRootsSeeds.nodes,
        nodesRootsSeeds.roots.toList.to(ImmArray),
      ),
      TransactionMetadata(ledgerTime, preparationTime, nodesRootsSeeds.seeds),
    )

    // Finally, we repeat some of the checks from `check` to assert that the result is really well-formed.
    // Firstly, we repeat checks that would fail in case of programming errors.
    // We throw in case of failures.

    def throwOnError(result: Checked[NonEmptyChain[String], String, Unit]): Unit =
      result.toEitherMergeNonaborts.valueOr { err =>
        val details = err.toList.sorted.mkString(", ")
        ErrorUtil.internalError(
          new IllegalStateException(s"Transaction is malformed after merging: $details")
        )
      }

    throwOnError(WellFormedTransaction.checkForest(mergedTx))
    throwOnError(WellFormedTransaction.checkSeeds(mergedTx, mergedMetadata.seeds))
    throwOnError(WellFormedTransaction.checkRollbackNodes(mergedTx, WithSuffixesAndMerged))

    // checkCreatedContracts is not entirely redundant.
    // If such a check fails, the error is propagated to the caller.
    val error =
      NonEmpty
        .from(rollbackContextFactory.checkCreatedContracts(mergedTx).nonaborts.toList)
        .map(_.sorted.mkString(", "))

    val wellFormedTransaction = WellFormedTransaction(mergedTx, mergedMetadata)

    (wellFormedTransaction, error)
  }

  protected[protocol] def buildNodesRootsSeeds(
      transactionsWithRollbackScopes: NonEmpty[
        Seq[WithRollbackScope[WellFormedTransaction[WithAbsoluteSuffixes]]]
      ]
  )(implicit
      loggingContext: ErrorLoggingContext
  ): NodesRootsSeeds

}

object PathTransactionMerge extends TransactionMerge {

  val rollbackContextFactory: RollbackContextFactory = PathRollbackContextFactory

  /** Root-level LfActionNodes with an associated rollback scope are embedded in rollback node
    * ancestors according to this scheme:
    *
    *   1. Every root node is embedded in as many rollback nodes as level appear in its rollback
    *      scope.
    *   1. Nodes with shared, non-empty rollback scope prefixes (or full matches) share the same
    *      rollback nodes (or all on fully matching rollback scopes).
    *   1. While the lf-engine "collapses" away some rollback nodes as part of normalization,
    *      merging does not perform any normalization as the daml indexer/ReadService-consumer does
    *      not require rollback-normalized lf-transactions.
    */
  override protected[protocol] def buildNodesRootsSeeds(
      transactionsWithRollbackScopes: NonEmpty[
        Seq[WithRollbackScope[WellFormedTransaction[WithAbsoluteSuffixes]]]
      ]
  )(implicit
      loggingContext: ErrorLoggingContext
  ): NodesRootsSeeds = {

    val transactionsWithRbScopes
        : Seq[(PathRollbackScope, WellFormedTransaction[WithAbsoluteSuffixes])] =
      transactionsWithRollbackScopes.map(wrs =>
        (checked(PathRollbackScope.tryToPathRollbackScope(wrs.rbScope)), wrs.unwrap)
      )

    val mergedNodes = HashMap.newBuilder[LfNodeId, LfNode]
    val mergedRoots = Iterable.newBuilder[LfNodeId]
    val mergedSeeds = Map.newBuilder[LfNodeId, LfHash]

    val mutableRbNodes =
      mutable.HashMap[LfNodeId, mutable.ArrayDeque[LfNodeId]]() // mutable so we can append children

    (transactionsWithRbScopes
      .foldLeft(
        // start after node-id 0 with empty rollback scope
        (
          0,
          List.empty[(RollbackSibling, LfNodeId)],
        )
      ) { case ((freeNodeId, rbScopeWithNodeIds), (rbScope, wfTx)) =>
        val headNodeIds = wfTx.unwrap.nodes.keys

        val (rbPops, rbPushes) =
          PathRollbackScope.popsAndPushes(
            PathRollbackScope(rbScopeWithNodeIds.map { case (rbSibling, _) =>
              rbSibling
            }),
            rbScope,
          )

        val maxNodeIdHead = headNodeIds.map(_.index).maxOption.getOrElse(0)
        val nextFresh =
          // freeNodeId + maxNodeIdHead + rbPushes + 1
          Math.incrementExact(Math.addExact(Math.addExact(freeNodeId, maxNodeIdHead), rbPushes))
        // Addition can overflow, but only in the following cases:
        // - The total number of nodes exceeds Int.MaxValue. This is tolerated, as such transactions are not supported.
        // - The total number of nodes is below Int.MaxValue, but some transactions have max(nodeId) >> count(nodeId).
        //   This is prevented by calling normalizeNodeIds before constructing a WellFormedTransaction.

        val rbScopeCommon = rbScopeWithNodeIds.dropRight(rbPops)
        val rbScopeToPush = rbScope.path.drop(rbScopeCommon.length).zipWithIndex

        // Create new rollback nodes and connect rollback parents and children
        val newRbScope = rbScopeToPush.foldLeft(rbScopeCommon) {
          case (rbScopeParent, (rbSiblingIndex, nodeIdIndexIncrement)) =>
            val childNodeId = LfNodeId(freeNodeId + nodeIdIndexIncrement)
            rbScopeParent.lastOption match {
              case Some((_, parentNodeId)) =>
                mutableRbNodes(parentNodeId) += childNodeId
              case None =>
                mergedRoots += childNodeId
            }
            mutableRbNodes += childNodeId -> new mutable.ArrayDeque[LfNodeId]()
            rbScopeParent :+ (rbSiblingIndex, childNodeId)
        }

        ErrorUtil.requireState(
          rbScope.path == newRbScope.map(_._1),
          s"Unexpected change of rollback scope. Actual: ${newRbScope.map(_._1)}, expected: $rbScope",
        )

        // Add regular transaction nodes
        val (adjustedTx, adjustedMetadata) = checked(wfTx.tryAdjustNodeIds(freeNodeId + rbPushes))
        mergedNodes ++= adjustedTx.nodes

        // Record regular transaction root as roots in the absence of rollback nodes.
        newRbScope.lastOption.fold {
          val _ = mergedRoots ++= adjustedTx.roots.toSeq
        } {
          // Otherwise place transaction roots under innermost rollback node.
          case (_, rbParentNodeId) =>
            val _ = mutableRbNodes(rbParentNodeId) ++= adjustedTx.roots.toSeq
        }
        mergedSeeds ++= adjustedMetadata.seeds
        (nextFresh, newRbScope)
      })
      .discard

    // Build actual rollback nodes now that we know all their children
    val rollbackNodes = mutableRbNodes.map { case (nid, children) =>
      nid -> LfNodeRollback(children.to(ImmArray))
    }

    NodesRootsSeeds(
      nodes = mergedNodes.result() ++ rollbackNodes,
      roots = Chain.fromIterableOnce(mergedRoots.result()),
      seeds = mergedSeeds.result(),
    )
  }

}

object NoPathTransactionMerge extends TransactionMerge {

  val rollbackContextFactory: RollbackContextFactory = NoPathRollbackContextFactory

  /** Root-level LfActionNodes that have been rolled back are nested in the merged transaction under
    * a single rollback node that contains all rolled back root nodes.
    */
  override protected[protocol] def buildNodesRootsSeeds(
      transactionsWithRollbackScopes: NonEmpty[
        Seq[WithRollbackScope[WellFormedTransaction[WithAbsoluteSuffixes]]]
      ]
  )(implicit
      loggingContext: ErrorLoggingContext
  ): NodesRootsSeeds =
    transactionsWithRollbackScopes
      .map { wrs =>
        val plain = NodesRootsSeeds(
          wrs.unwrap.tx.nodes,
          Chain.fromSeq(wrs.unwrap.tx.roots.toList),
          wrs.unwrap.metadata.seeds,
        )
        if (wrs.rbScope.inRollback) plain.rollback else plain
      }
      .fold(NodesRootsSeeds.empty) { (acc, nrs) =>
        val offsetNrs = nrs.offsetFrom(acc.nodes.size)
        NodesRootsSeeds.combine(acc, offsetNrs)
      }

}
