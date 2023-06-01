// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

/** Wrapper type for elements of a view decomposition
  */
sealed trait TransactionViewDecomposition extends Product with Serializable with PrettyPrinting {
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
  final case class NewView(
      rootNode: LfActionNode,
      informees: Set[Informee],
      threshold: NonNegativeInt,
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
    def allNodes: NonEmpty[Seq[TransactionViewDecomposition]] =
      NonEmpty(Seq, SameView(rootNode, nodeId, rbContext), tailNodes: _*)

    def childViews: Seq[NewView] = tailNodes.collect { case v: NewView => v }

    def coreTailNodes: Seq[LfActionNode] =
      tailNodes.collect { case sameView: SameView => sameView.lfNode }

    /** Checks whether the core nodes of this view have informees [[informees]] and threshold [[threshold]] under
      * the given confirmation policy and identity snapshot.
      *
      * @param submittingAdminPartyO the admin party of the submitting participant; should only be defined for top-level views
      *
      * @return `()` or an error messages
      */
    def compliesWith(
        confirmationPolicy: ConfirmationPolicy,
        submittingAdminPartyO: Option[LfPartyId],
        topologySnapshot: TopologySnapshot,
        protocolVersion: ProtocolVersion,
    )(implicit
        ec: ExecutionContext
    ): EitherT[Future, String, Unit] = {

      val rootNodeInformeesAndThresholdF = confirmationPolicy
        .informeesAndThreshold(rootNode, submittingAdminPartyO, topologySnapshot, protocolVersion)
      val coreNodesInformeesAndThresholdF = coreTailNodes
        .map(coreNode =>
          confirmationPolicy
            .informeesAndThreshold(
              coreNode,
              submittingAdminPartyO = None,
              topologySnapshot,
              protocolVersion,
            )
        )

      EitherT(
        (rootNodeInformeesAndThresholdF +: coreNodesInformeesAndThresholdF).sequence
          .map { nodes =>
            nodes
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

    override def pretty: Pretty[NewView] = prettyOfClass(
      param("root node template", _.rootNode.templateId),
      param("informees", _.informees),
      param("threshold", _.threshold),
      param("node ID", _.nodeId),
      param("rollback context", _.rbContext),
      param("tail nodes", _.tailNodes),
    )
  }

  /** Encapsulates a node that belongs to core of some [[com.digitalasset.canton.data.TransactionViewDecomposition.NewView]]. */
  final case class SameView(
      lfNode: LfActionNode,
      override val nodeId: LfNodeId,
      override val rbContext: RollbackContext,
  ) extends TransactionViewDecomposition {

    override def pretty: Pretty[SameView] = prettyOfClass(
      param("lf node template", _.lfNode.templateId),
      param("node ID", _.nodeId),
      param("rollback context", _.rbContext),
    )
  }
}
