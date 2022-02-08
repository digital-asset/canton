// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.data.{EitherT, NonEmptyList}
import cats.syntax.either._
import cats.syntax.traverse._
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.util.LfTransactionUtil

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

/** Wrapper type for elements of a view decomposition
  */
sealed trait TransactionViewDecomposition extends Product with Serializable {
  def lfNode: LfActionNode
  def nodeId: LfNodeId
  def rbContext: RollbackContext
}

object TransactionViewDecomposition {

  /** Encapsulates a new view.
    *
    * @param rootNode the node constituting the view
    * @param informees the informees of rootNode
    * @param threshold the threshold of rootNode
    * @param rootSeed the seed of the rootNode
    * @param tailNodes all core nodes except `rootNode` and all child views, sorted in pre-order traversal order
    *
    * @throws java.lang.IllegalArgumentException if a subview has the same `informees` and `threshold`
    */
  case class NewView(
      rootNode: LfActionNode,
      informees: Set[Informee],
      threshold: Int,
      rootSeed: Option[LfHash],
      override val nodeId: LfNodeId,
      tailNodes: Seq[TransactionViewDecomposition],
      override val rbContext: RollbackContext,
  ) extends TransactionViewDecomposition {

    childViews.foreach { sv =>
      require(
        (sv.informees, sv.threshold) != ((informees, threshold)),
        s"Children must have different informees or thresholds than parent. " +
          s"Found threshold $threshold and informees $informees",
      )
    }

    override def lfNode: LfActionNode = rootNode

    /** All nodes of this view, i.e. core nodes and subviews, in execution order */
    def allNodes: NonEmptyList[TransactionViewDecomposition] =
      NonEmptyList.of(SameView(rootNode, nodeId, rbContext), tailNodes: _*)

    def childViews: Seq[NewView] = tailNodes.collect { case v: NewView => v }

    def coreNodes: NonEmptyList[LfActionNode] =
      NonEmptyList.of(
        rootNode,
        tailNodes.collect { case sameView: SameView => sameView.lfNode }: _*
      )

    /** Checks whether the core nodes of this view have informees [[informees]] and threshold [[threshold]] under
      * the given confirmation policy and identity snapshot.
      *
      * @return `()` or an error messages
      */
    def compliesWith(confirmationPolicy: ConfirmationPolicy, topologySnapshot: TopologySnapshot)(
        implicit ec: ExecutionContext
    ): EitherT[Future, String, Unit] = {

      EitherT(
        coreNodes.toList
          .traverse(coreNode =>
            confirmationPolicy.informeesAndThreshold(coreNode, topologySnapshot)
          )
          .map { nodeList =>
            nodeList
              .traverse { case (nodeInformees, nodeThreshold) =>
                Either.cond(
                  (nodeInformees, nodeThreshold) == ((informees, threshold)),
                  (),
                  (nodeInformees, nodeThreshold),
                )
              }
              .bimap(
                { case (nodeInformees, nodeThreshold) =>
                  "The nodes in the core of the view have different informees or thresholds.\n" +
                    s"The view has informees $informees and threshold $threshold.\n" +
                    s"Some core node has informees $nodeInformees and threshold $nodeThreshold."
                },
                _ => (),
              )
          }
      )
    }
  }

  /** Encapsulates a node that belongs to core of some [[com.digitalasset.canton.data.TransactionViewDecomposition.NewView]]. */
  case class SameView(
      lfNode: LfActionNode,
      override val nodeId: LfNodeId,
      override val rbContext: RollbackContext,
  ) extends TransactionViewDecomposition

  /** Converts `transaction: Transaction` into the corresponding `ViewDecomposition`s.
    */
  def fromTransaction(
      confirmationPolicy: ConfirmationPolicy,
      topologySnapshot: TopologySnapshot,
      transaction: WellFormedTransaction[WithoutSuffixes],
      viewRbContext: RollbackContext,
  )(implicit ec: ExecutionContext): Future[Seq[NewView]] = {

    def idAndNode(id: LfNodeId): (LfNodeId, LfNode) = id -> transaction.unwrap.nodes(id)

    def createNewView: ((LfNodeId, LfActionNode, RollbackContext)) => Future[NewView] = {
      case (rootNodeId, rootNode, rbContext) =>
        confirmationPolicy.informeesAndThreshold(rootNode, topologySnapshot).flatMap {
          case (informees, threshold) =>
            val rootSeed = transaction.seedFor(rootNodeId)
            val tailNodesF = collectTailNodes(rootNode, informees, threshold, rbContext)
            tailNodesF.map(tailNodes =>
              NewView(
                LfTransactionUtil.lightWeight(rootNode),
                informees,
                threshold,
                rootSeed,
                rootNodeId,
                tailNodes,
                rbContext,
              )
            )
        }
    }

    def collectTailNodes(
        rootNode: LfActionNode,
        viewInformees: Set[Informee],
        viewThreshold: Int,
        rbContext: RollbackContext,
    )(implicit ec: ExecutionContext): Future[Seq[TransactionViewDecomposition]] = {

      val children = LfTransactionUtil.children(rootNode).map(idAndNode)
      val actionNodeChildren = peelAwayTopLevelRollbackNodes(children, rbContext)
      actionNodeChildren
        .traverse { case (childNodeId, childNode, childRbContext) =>
          confirmationPolicy.informeesAndThreshold(childNode, topologySnapshot).flatMap {
            case (childInformees, childThreshold) =>
              if (childInformees == viewInformees && childThreshold == viewThreshold) {
                // childNode belongs to the core of the view, as informees and threshold are the same

                val nodeAsSameView =
                  SameView(LfTransactionUtil.lightWeight(childNode), childNodeId, childRbContext)

                val otherTailNodesF =
                  collectTailNodes(childNode, viewInformees, viewThreshold, childRbContext)
                otherTailNodesF.map(nodeAsSameView +: _)
              } else {
                // Node is the root of a direct subview, as informees or threshold are different
                createNewView((childNodeId, childNode, childRbContext)).map(Seq(_))
              }
          }
        }
        .map(_.flatten)
    }

    @nowarn("msg=match may not be exhaustive")
    def peelAwayTopLevelRollbackNodes(
        nodes: Seq[(LfNodeId, LfNode)],
        rbContext: RollbackContext,
    ): Seq[(LfNodeId, LfActionNode, RollbackContext)] =
      nodes match {
        case Seq() => Seq()
        case (nodeId, an: LfActionNode) +: remainingChildren =>
          (nodeId, an, rbContext) +: peelAwayTopLevelRollbackNodes(remainingChildren, rbContext)
        case (_nodeId, rn: LfNodeRollback) +: remainingChildren =>
          val rollbackChildren = rn.children.map(idAndNode).toSeq
          val rollbackChildrenRbContext = rbContext.enterRollback
          val remainingChildrenRbContext = rollbackChildrenRbContext.exitRollback
          peelAwayTopLevelRollbackNodes(rollbackChildren, rollbackChildrenRbContext) ++
            peelAwayTopLevelRollbackNodes(remainingChildren, remainingChildrenRbContext)
      }

    val rootNodes =
      peelAwayTopLevelRollbackNodes(transaction.unwrap.roots.toSeq.map(idAndNode), viewRbContext)
    rootNodes.traverse(createNewView)
  }

  /** Convenience method to create a [[TransactionViewDecomposition]] with a
    * [[com.digitalasset.canton.protocol.ConfirmationPolicy]].
    *
    * @throws java.lang.IllegalArgumentException if `rootNode` has a different informees or thresholds as one of
    *                                            the [[SameView]]s in `tailNodes`, or
    *                                            if `rootNode` has the same informees and threshold as one of
    *                                            the [[NewView]]s in `tailNodes`.
    */
  def createWithConfirmationPolicy(
      confirmationPolicy: ConfirmationPolicy,
      topologySnapshot: TopologySnapshot,
      rootNode: LfActionNode,
      rootSeed: Option[LfHash],
      rootNodeId: LfNodeId,
      tailNodes: Seq[TransactionViewDecomposition],
  )(implicit ec: ExecutionContext): Future[NewView] = {

    val rootRbContext = RollbackContext.empty

    confirmationPolicy.informeesAndThreshold(rootNode, topologySnapshot).flatMap {
      case (viewInformees, viewThreshold) =>
        val result = NewView(
          rootNode,
          viewInformees,
          viewThreshold,
          rootSeed,
          rootNodeId,
          tailNodes,
          rootRbContext,
        )
        result
          .compliesWith(confirmationPolicy, topologySnapshot)
          .map(_ => result)
          .valueOr(err => throw new IllegalArgumentException(err))
    }
  }

}
