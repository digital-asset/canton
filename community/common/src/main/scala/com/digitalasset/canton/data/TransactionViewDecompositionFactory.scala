// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.data.Chain
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.TransactionViewDecomposition.{NewView, SameView}
import com.digitalasset.canton.data.TransactionViewDecompositionFactory.RollbackState.firstChild
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.LfTransactionUtil
import com.digitalasset.canton.{LfPartyId, checked}
import com.digitalasset.daml.lf.transaction.NodeId

import scala.concurrent.ExecutionContext

case object TransactionViewDecompositionFactory {

  object RollbackState {
    private val firstChild: PositiveInt = PositiveInt.one
    val empty: RollbackState = RollbackState(Vector.empty, firstChild)
  }

  final case class RollbackState(path: Vector[PositiveInt], nextChild: PositiveInt) {

    def enterRollback: RollbackState = RollbackState(path :+ nextChild, firstChild)

    def tryExitRollback: RollbackState = {
      val lastChild =
        path.lastOption.getOrElse(
          throw new IllegalStateException("Attempt to exit rollback on empty rollback context")
        )

      RollbackState(path.dropRight(1), lastChild.increment)
    }

    def inRollback: Boolean = path.nonEmpty
  }

  /** Keeps track of the state of the transaction view tree.
    *
    * @param views
    *   chains `NewView` and `SameView` as they get created to construct a transaction view tree
    * @param informees
    *   is used to aggregate the informees' partyId until a NewView is created
    * @param quorums
    *   is used to aggregate the different quorums (originated from the different ActionNodes) until
    *   a NewView is created
    */
  final private case class BuildState[V](
      views: Chain[V] = Chain.empty,
      informees: Set[LfPartyId] = Set.empty,
      quorums: Chain[Quorum] = Chain.empty,
      rollbackState: RollbackState,
  ) {

    def withViews(
        views: Chain[V],
        informees: Set[LfPartyId],
        quorums: Chain[Quorum],
        rollbackState: RollbackState,
    ): BuildState[V] =
      BuildState[V](
        this.views ++ views,
        this.informees ++ informees,
        this.quorums ++ quorums,
        rollbackState,
      )

    def withNewView(view: V, rollbackContext: RollbackState): BuildState[V] =
      BuildState[V](this.views :+ view, this.informees, this.quorums, rollbackContext)

    def childState: BuildState[TransactionViewDecomposition] =
      BuildState(Chain.empty, Set.empty, Chain.empty, rollbackState)

    def enterRollback(): BuildState[V] = copy(rollbackState = rollbackState.enterRollback)

    def exitRollback(): BuildState[V] = copy(rollbackState = checked(rollbackState.tryExitRollback))
  }

  final private case class ActionNodeInfo(
      informees: Map[LfPartyId, Set[ParticipantId]],
      quorum: Quorum,
      children: Seq[LfNodeId],
      seed: Option[LfHash],
  ) {
    lazy val participants: Set[ParticipantId] = informees.values.flatten.toSet
  }

  final private case class Builder(
      nodesM: Map[LfNodeId, LfNode],
      actionNodeInfoM: Map[LfNodeId, ActionNodeInfo],
      factory: RollbackContextFactory,
  ) {

    private def node(nodeId: LfNodeId): LfNode = nodesM.getOrElse(
      nodeId,
      throw new IllegalStateException(s"Did not find $nodeId in node map"),
    )

    @SuppressWarnings(Array("org.wartremover.warts.PartialFunctionApply"))
    private def build(
        nodeId: LfNodeId,
        state: BuildState[NewView],
    ): BuildState[NewView] =
      node(nodeId) match {
        case actionNode: LfActionNode =>
          val info = actionNodeInfoM(nodeId)
          buildNewView[NewView](nodeId, actionNode, info, state)
        case rollbackNode: LfNodeRollback =>
          builds(rollbackNode.children.toSeq, state.enterRollback()).exitRollback()
      }

    def builds(
        nodeIds: Seq[LfNodeId],
        state: BuildState[NewView],
    ): BuildState[NewView] =
      nodeIds.foldLeft(state)((s, nid) => build(nid, s))

    private def buildNewView[V >: NewView](
        nodeId: LfNodeId,
        actionNode: LfActionNode,
        info: ActionNodeInfo,
        state: BuildState[V],
    ): BuildState[V] = {

      val childState = info.children.foldLeft(state.childState) { (bs, nId) =>
        buildChildView(nId, info.participants, bs)
      }

      val newView = NewView(
        LfTransactionUtil.lightWeight(actionNode),
        /* We can use tryCreate here because at this point we only have one quorum
         * that is generated directly from the informees of the action node.
         * Only later in the process (after the tree is built) do we aggregate all the children
         * unique quorums together into a list held by the `parent` view.
         */
        ViewConfirmationParameters
          .tryCreate(
            info.informees.keySet ++ childState.informees,
            (info.quorum +: childState.quorums.toList).distinct,
          ),
        info.seed,
        nodeId,
        childState.views.toList,
        factory.fromRollbackState(state.rollbackState),
      )

      state.withNewView(newView, childState.rollbackState)
    }

    @SuppressWarnings(Array("org.wartremover.warts.PartialFunctionApply"))
    private def buildChildView(
        nodeId: LfNodeId,
        currentParticipants: Set[ParticipantId],
        state: BuildState[TransactionViewDecomposition],
    ): BuildState[TransactionViewDecomposition] = {

      /* The recipients of a transaction node are all participants that
       * host a witness of the node. So we should look at the participant recipients of
       * a node to decide when a new view is needed. In particular, a change in the informees triggers a new view only if
       * a new informee participant enters the game.
       */
      def needNewView(
          node: ActionNodeInfo,
          currentParticipants: Set[ParticipantId],
      ): Boolean = !node.participants.subsetOf(currentParticipants)

      node(nodeId) match {
        case actionNode: LfActionNode =>
          val info = actionNodeInfoM(nodeId)
          if (!needNewView(info, currentParticipants)) {
            val sameView = SameView(
              LfTransactionUtil.lightWeight(actionNode),
              nodeId,
              factory.fromRollbackState(state.rollbackState),
            )
            val childState = info.children.foldLeft(state.childState) { (bs, nId) =>
              buildChildView(nId, currentParticipants, bs)
            }

            state
              .withViews(
                sameView +: childState.views,
                info.informees.keySet ++ childState.informees,
                info.quorum +: childState.quorums,
                childState.rollbackState,
              )
          } else
            buildNewView(nodeId, actionNode, info, state)
        case rollbackNode: LfNodeRollback =>
          rollbackNode.children
            .foldLeft(state.enterRollback()) { (bs, nId) =>
              buildChildView(nId, currentParticipants, bs)
            }
            .exitRollback()
      }
    }
  }

  def fromTransaction(
      topologySnapshot: TopologySnapshot,
      transaction: WellFormedTransaction[WithoutSuffixes],
      viewRbContext: RollbackContext,
      submittingAdminPartyO: Option[LfPartyId],
      factory: RollbackContextFactory,
  )(implicit ec: ExecutionContext, tc: TraceContext): FutureUnlessShutdown[Seq[NewView]] = {

    val tx: LfVersionedTransaction = transaction.unwrap
    val rootNodes = tx.roots.toSeq
    val rollbackState = factory.toRollbackState(viewRbContext)

    val policyMapF: Iterable[FutureUnlessShutdown[(NodeId, ActionNodeInfo)]] =
      tx.nodes.collect { case (nodeId, node: LfActionNode) =>
        val childNodeIds = node match {
          case e: LfNodeExercises => e.children.toSeq
          case _ => Seq.empty
        }

        /* A submittingAdminParty is passed and added (if defined) as an extra confirming party.
         * This is only called for the root action nodes (and respective views) to guarantee proper authorization.
         * Its subsequent quorum will include the submitting party.
         */
        createActionNodeInfo(
          topologySnapshot,
          if (rootNodes.contains(nodeId)) submittingAdminPartyO else None,
          node,
          nodeId,
          childNodeIds,
          transaction,
        )
      }

    FutureUnlessShutdown.sequence(policyMapF).map(_.toMap).map { policyMap =>
      Builder(tx.nodes, policyMap, factory)
        .builds(rootNodes, BuildState[NewView](rollbackState = rollbackState))
        .views
        .toList
    }
  }

  private def createActionNodeInfo(
      topologySnapshot: TopologySnapshot,
      submittingAdminPartyO: Option[LfPartyId],
      node: LfActionNode,
      nodeId: LfNodeId,
      childNodeIds: Seq[LfNodeId],
      transaction: WellFormedTransaction[WithoutSuffixes],
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): FutureUnlessShutdown[(LfNodeId, ActionNodeInfo)] = {
    def createQuorum(
        informeesMap: Map[LfPartyId, (Set[ParticipantId], NonNegativeInt)],
        threshold: NonNegativeInt,
    ): Quorum =
      Quorum(
        informeesMap.mapFilter { case (_, weight) =>
          Option.when(weight.unwrap > 0)(
            PositiveInt.tryCreate(weight.unwrap)
          )
        },
        threshold,
      )

    val itF = informeesParticipantsAndThreshold(node, topologySnapshot, submittingAdminPartyO)
    itF.map { case (i, t) =>
      nodeId -> ActionNodeInfo(
        informees = i.fmap { case (participants, _) => participants },
        quorum = createQuorum(i, t),
        children = childNodeIds,
        seed = transaction.seedFor(nodeId),
      )
    }
  }

  /** Returns informees, participants hosting those informees, and corresponding threshold for a
    * given action node.
    */
  def informeesParticipantsAndThreshold(
      node: LfActionNode,
      topologySnapshot: TopologySnapshot,
      submittingAdminPartyO: Option[LfPartyId] = None,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[
    (Map[LfPartyId, (Set[ParticipantId], NonNegativeInt)], NonNegativeInt)
  ] = {
    val confirmingParties =
      submittingAdminPartyO.fold[Set[LfPartyId]](Set.empty)(Set(_)) |
        LfTransactionUtil.signatoriesOrMaintainers(node) | LfTransactionUtil.actingParties(node)
    require(
      confirmingParties.nonEmpty,
      "There must be at least one confirming party, as every node must have at least one signatory.",
    )

    val plainInformees = node.informeesOfNode -- confirmingParties

    val threshold = NonNegativeInt.tryCreate(confirmingParties.size)
    val informees = plainInformees ++ confirmingParties

    topologySnapshot
      .activeParticipantsOfPartiesWithInfo(informees.toSeq)
      .map(informeesMap =>
        informeesMap.map { case (partyId, partyInfo) =>
          // confirming party
          if (confirmingParties.contains(partyId))
            partyId -> (partyInfo.participants.keySet, NonNegativeInt.one)
          // plain informee
          else partyId -> (partyInfo.participants.keySet, NonNegativeInt.zero)
        }
      )
      .map(informeesMap => (informeesMap, threshold))
  }

}
