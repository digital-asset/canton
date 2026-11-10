// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.Monoid
import cats.data.NonEmptyChainImpl.*
import cats.data.{Chain, NonEmptyChain}
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.protocol
import com.digitalasset.canton.protocol.WellFormedTransaction.{
  WithAbsoluteSuffixes,
  WithSuffixesAndMerged,
}
import com.digitalasset.canton.util.{Checked, ErrorUtil}
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.transaction.Transaction
import com.digitalasset.nonempty.NonEmpty

object TransactionMerge {

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

  private object NodesRootsSeeds extends Monoid[NodesRootsSeeds] {
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

  /** Merges a list of well-formed transactions into one, adjusting node IDs as necessary.
    *
    * Root-level LfActionNodes with an associated rollback scope are embedded in rollback node
    * ancestors according to the `buildNodesRootsSeeds` implementation:
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
        .from(WellFormedTransaction.checkCreatedContracts(mergedTx).nonaborts.toList)
        .map(_.sorted.mkString(", "))

    val wellFormedTransaction = WellFormedTransaction(mergedTx, mergedMetadata)

    (wellFormedTransaction, error)
  }

  /** Root-level LfActionNodes that have been rolled back are nested in the merged transaction under
    * a single rollback node that contains all rolled back root nodes.
    */
  private[protocol] def buildNodesRootsSeeds(
      transactionsWithRollbackScopes: NonEmpty[
        Seq[WithRollbackScope[WellFormedTransaction[WithAbsoluteSuffixes]]]
      ]
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
