// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.daml.ledger.api.v1.value.{Identifier => ApiIdentifier}
import com.daml.ledger.client.binding
import com.daml.lf.data.{FrontStack, ImmArray}
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.test.TransactionBuilder.Implicits.{toIdentifier, toPackageId}
import com.digitalasset.canton.ComparesLfTransactions.{TbContext, TxTree}
import com.digitalasset.canton.logging.pretty.PrettyTestInstances._
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{
  LfContractId,
  LfLeafOnlyActionNode,
  LfNode,
  LfNodeCreate,
  LfNodeExercises,
  LfNodeFetch,
  LfNodeId,
  LfNodeLookupByKey,
  LfNodeRollback,
  LfTransaction,
  LfVersionedTransaction,
}
import org.scalatest.{Assertion, Suite}

/** Test utility to compare actual and expected lf transactions using a human-readable, hierarchical serialization of lf
  * nodes.
  */
trait ComparesLfTransactions {

  this: Suite =>

  /** Main compare entry point of two lf transactions that asserts that the "nested" representations of lf transactions
    * match relying on pretty-printing to produce a human-readable, multiline and hierarchical serialization.
    */
  def assertTransactionsMatch(
      expectedTx: LfVersionedTransaction,
      actualTx: LfVersionedTransaction,
  ): Assertion = {

    // Nest transaction nodes and eliminate redundant node-id child references.
    def nestedLfNodeFromFlat(tx: LfVersionedTransaction): Seq[TxTree] = {
      def go(nid: LfNodeId): TxTree = tx.nodes(nid) match {
        case en: LfNodeExercises =>
          TxTree(en.copy(children = ImmArray.empty), en.children.toSeq.map(go): _*)
        case rn: LfNodeRollback =>
          TxTree(rn.copy(children = ImmArray.empty), rn.children.toSeq.map(go): _*)
        case leafNode: LfLeafOnlyActionNode => TxTree(leafNode)
      }
      tx.roots.toSeq.map(go)
    }

    // Compare the nested transaction structure, easier to reason about in case of diffs - also not sensitive to node-ids.
    val expectedNested = nestedLfNodeFromFlat(expectedTx)
    val actualNested = nestedLfNodeFromFlat(actualTx)

    assert(actualNested == expectedNested)
  }

  /** Helper to initialize an lf transaction builder along with a preorder sequence of contract ids of a provided
    * lf transaction.
    */
  def txBuilderContextFrom[A](tx: LfVersionedTransaction)(code: TbContext => A): A =
    code(TbContext(new TransactionBuilder(_ => tx.version), contractIdsInPreorder(tx)))

  def txBuilderContextFromEmpty[A](code: TbContext => A): A =
    code(TbContext(TransactionBuilder(), Seq.empty))

  /** Helper to extract the contract-ids of a transaction in pre-order.
    *
    * Useful to build expected transaction in terms of "expect the second contract id of the actual transaction in this
    * create node".
    */
  private def contractIdsInPreorder(tx: LfVersionedTransaction): Seq[LfContractId] = {
    val contractIds = scala.collection.mutable.ListBuffer.empty[LfContractId]

    def add(coid: LfContractId): Unit = if (!contractIds.contains(coid)) contractIds += coid

    tx.foldInExecutionOrder(())(
      (_, _, en) => (add(en.targetCoid), LfTransaction.ChildrenRecursion.DoRecurse),
      (_, _, _) => ((), LfTransaction.ChildrenRecursion.DoRecurse),
      {
        case (_, _, cn: LfNodeCreate) => add(cn.coid)
        case (_, _, fn: LfNodeFetch) => add(fn.coid)
        case (_, _, ln: LfNodeLookupByKey) => ln.result.foreach(add)
      },
      (_, _, _) => (),
      (_, _, _) => (),
    )

    contractIds.result()
  }

  // Various helpers that help "hide" lf value boilerplate from transaction representations for improved readability.
  def args(values: LfValue*): LfValue.ValueRecord =
    LfValue.ValueRecord(None, values.map(None -> _).to(ImmArray))

  def seq(values: LfValue*): LfValue.ValueList = LfValue.ValueList(FrontStack.from(values))

  val notUsed: LfValue = LfValue.ValueUnit

  protected def templateIdFromTemplate[T](template: binding.Primitive.TemplateId[T]) = {
    import scalaz.syntax.tag._
    template.unwrap match {
      case ApiIdentifier(packageId, moduleName, entityName) =>
        toIdentifier(s"${moduleName}:${entityName}")(toPackageId(packageId))
    }
  }
}

object ComparesLfTransactions {

  /** The TxTree class adds the ability to arrange LfTransaction nodes in a tree structure rather than the flat
    * node-id-based arrangement.
    */
  case class TxTree(lfNode: LfNode, childNodes: TxTree*) extends PrettyPrinting {
    override lazy val pretty: Pretty[TxTree] = prettyOfClass(
      unnamedParam(_.lfNode),
      unnamedParamIfNonEmpty(_.childNodes),
    )

    def addToBuilder(parentNodeId: Option[LfNodeId] = None)(implicit tbContext: TbContext): Unit = {
      val nid = parentNodeId.fold(tbContext.tb.add(lfNode))(tbContext.tb.add(lfNode, _))
      childNodes.foreach(_.addToBuilder(Some(nid)))
    }

    def lfTransaction(implicit tbContext: TbContext): LfVersionedTransaction = {
      addToBuilder()
      tbContext.tb.build()
    }
  }

  /** TransactionBuilder context combines multiple implicits: transaction builder and contract ids
    */
  case class TbContext(tb: TransactionBuilder, contractIds: Seq[LfContractId])

}
