// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.SequencerBftAdminData
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.SequencerBftAdminData.{
  PeerConnectionStatus,
  PeerEndpointHealth,
  PeerEndpointHealthStatus,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.topology.SequencerNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.{
  DefaultBlockingDbReadTimeout,
  DefaultSendBlacklistTtl,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.AvailabilityModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.P2PMetrics.{
  emitAuthenticatedCount,
  emitConnectedCount,
  emitSendStats,
  sendMetricsContext,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.data.P2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  WorkflowId,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology.strongQuorumSize
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.P2PNetworkOut.{
  Admin,
  BftOrderingNetworkMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.P2PNetworkOutModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  Env,
  ModuleRef,
  P2PAddress,
  P2PConnectionEventListener,
  P2PNetworkManager,
  P2PNetworkRef,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.BftNodeShuffler
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.{
  BftOrderingMessage,
  BftOrderingMessageBody,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.timestamp.Timestamp

import java.time.Instant
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.{Failure, Random, Success}

final class P2PNetworkOutModule[
    E <: Env[E],
    P2PNetworkManagerT <: P2PNetworkManager[E, BftOrderingMessage],
](
    thisBftNodeId: BftNodeId,
    isGenesis: Boolean,
    @VisibleForTesting private[bftordering] val state: P2PNetworkOutModule.State,
    random: Random,
    clock: Clock,
    @VisibleForTesting private[bftordering] val p2pEndpointsStore: P2PEndpointsStore[E],
    metrics: BftOrderingMetrics,
    override val dependencies: P2PNetworkOutModuleDependencies[E, P2PNetworkManagerT],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
    blockingDbReadTimeout: FiniteDuration = DefaultBlockingDbReadTimeout,
    sendBlacklistExpirationDuration: FiniteDuration = DefaultSendBlacklistTtl,
)(implicit mc: MetricsContext)
    extends P2PNetworkOut[E, P2PNetworkManagerT]
    with P2PConnectionEventListener {

  private val connectedP2PEndpointIds = mutable.Set.empty[P2PEndpoint.Id]

  private val nodeShuffler = new BftNodeShuffler(random)

  val p2pNetworkManager: P2PNetworkManagerT =
    dependencies.createP2PNetworkManager(this, dependencies.p2pNetworkIn)

  override def ready(
      self: ModuleRef[P2PNetworkOut.Message]
  )(implicit traceContext: TraceContext): Unit = {
    state.maybeSelf = Some(self)
    self.asyncSend(P2PNetworkOut.Start)
  }

  override def onSequencerId(bftNodeId: BftNodeId, maybeP2PEndpoint: Option[P2PEndpoint])(implicit
      traceContext: TraceContext
  ): Unit =
    state.maybeSelf.foreach(
      _.asyncSend(P2PNetworkOut.Network.Authenticated(bftNodeId, maybeP2PEndpoint))
    )

  override def onConnect(maybeP2pEndpointId: Option[P2PEndpoint.Id])(implicit
      traceContext: TraceContext
  ): Unit =
    state.maybeSelf.foreach(_.asyncSend(P2PNetworkOut.Network.Connected(maybeP2pEndpointId)))

  override def onDisconnect(p2pEndpointId: P2PEndpoint.Id)(implicit
      traceContext: TraceContext
  ): Unit =
    state.maybeSelf.foreach(_.asyncSend(P2PNetworkOut.Network.Disconnected(p2pEndpointId)))

  import state.*

  override def receiveInternal(
      message: P2PNetworkOut.Message
  )(implicit context: E#ActorContextT[P2PNetworkOut.Message], traceContext: TraceContext): Unit =
    message match {
      case P2PNetworkOut.Start =>
        val p2pEndpoints =
          context.blockingAwait(p2pEndpointsStore.listEndpoints(), blockingDbReadTimeout)
        connectInitialNodes(p2pEndpoints)
        startModulesIfNeeded()

      case P2PNetworkOut.Internal.Connect(p2pEndpoint) =>
        logger.info("Connecting to operator-added endpoint " + p2pEndpoint.id)
        ensureSendingEnabledTo(P2PAddress.Endpoint(p2pEndpoint))

      case P2PNetworkOut.Internal.Disconnect(p2pEndpointId) =>
        logger.info("Disconnecting from operator-removed endpoint " + p2pEndpointId)
        disconnect(p2pEndpointId)

      case P2PNetworkOut.Network.Connected(maybeP2pEndpointId) =>
        if (maybeP2pEndpointId.forall(connectedP2PEndpointIds.add)) {
          logger.info(
            s"P2P endpoint ${maybeP2pEndpointId.map(_.toString).getOrElse("<incoming connection>")} " +
              s"is now connected"
          )
          emitConnectionStateMetricsAndLogEndpointsStatus(notifyMempool = false)
        }

      case P2PNetworkOut.Network.Disconnected(p2pEndpointId) =>
        if (connectedP2PEndpointIds.remove(p2pEndpointId)) {
          logger.info(s"P2P endpoint $p2pEndpointId is now disconnected")
          emitConnectionStateMetricsAndLogEndpointsStatus(notifyMempool = true)
        }

      case P2PNetworkOut.Network.Authenticated(bftNodeId, maybeP2PEndpoint) =>
        val maybeP2PEndpointId = maybeP2PEndpoint.map(_.id)
        val p2pEndpointIdString = maybeP2PEndpointId.map(_.toString).getOrElse("<unknown>")
        logger.info(
          s"Authenticated node $bftNodeId at $p2pEndpointIdString, marking endpoint (if known) as connected " +
            "and ensuring connectivity to it"
        )
        maybeP2PEndpointId.foreach(connectedP2PEndpointIds.add(_).discard)
        ensureSendingEnabledTo(P2PAddress.NodeId(bftNodeId, maybeP2PEndpoint))
        emitConnectionStateMetricsAndLogEndpointsStatus(notifyMempool = true)
        maxNodesContemporarilyAuthenticated = Math.max(
          maxNodesContemporarilyAuthenticated,
          getAuthenticatedCountIncludingSelf(),
        )
        startModulesIfNeeded()

      case P2PNetworkOut.Network.TopologyUpdate(newMembership) =>
        membership = newMembership
        sendConnectivityUpdateToMempool()

      case P2PNetworkOut.Multicast(message, recipientBftNodeIds) =>
        recipientBftNodeIds.toSeq.sorted // For determinism
          .foreach(sendIfKnown(_, message))

      case P2PNetworkOut.SendToRandomAuthenticated(
            message,
            possibleRecipients,
            workflowIdO,
            nodeThatFailedO,
            onRecipientDecision,
          ) =>
        val now = clock.now.toInstant
        updateBlacklists(
          workflowIdO,
          nodeThatFailedO,
          now,
          possibleRecipients,
          message,
        )
        workflowIdO.foreach(expireWorkflowBlacklist(now, _))
        val blackListed = getBlacklisted(workflowIdO, possibleRecipients, message)
        val authenticatedNodeIds = getAuthenticatedNodeIds(getStatus())
        val recipientNodeIdO =
          selectRecipient(
            workflowIdO,
            possibleRecipients,
            authenticatedNodeIds,
            blackListed,
            message,
          )
        try onRecipientDecision.foreach(_(recipientNodeIdO))
        catch {
          case scala.util.control.NonFatal(e) =>
            logger.warn("`onRecipientDecision` callback failed", e)
        }

      case P2PNetworkOut.EndWorkflow(workflowId) =>
        logger.debug(
          s"Ending workflow $workflowId, clearing any associated workflow blacklist info"
        )
        workflowBlacklists.remove(workflowId).discard

      case admin: P2PNetworkOut.Admin =>
        processAdminMessage(admin)
    }

  private def updateBlacklists(
      workflowIdO: Option[WorkflowId],
      nodeThatFailedO: Option[BftNodeId],
      now: Instant,
      possibleRecipients: Seq[BftNodeId],
      message: BftOrderingNetworkMessage,
  )(implicit traceContext: TraceContext): Unit =
    workflowIdO.foreach { workflowId =>
      workflowBlacklists.updateWith(workflowId) { (blacklistO: Option[Map[BftNodeId, Instant]]) =>
        if (blacklistO.isEmpty)
          logger.info(s"New workflow $workflowId started")
        nodeThatFailedO.fold[Option[Map[BftNodeId, Instant]]] {
          logger.debug(
            s"Sending message `${message.getClass.getSimpleName}` to random authenticated " +
              s"node among $possibleRecipients with workflow ID $workflowId (not a retry), " +
              s"keeping blacklist $blacklistO"
          )
          blacklistO.fold(Some(Map.empty[BftNodeId, Instant]))(blacklist => Some(blacklist))
        } { nodeThatFailed =>
          logger.info(
            s"Retrying to send message `${message.getClass.getSimpleName}` to random authenticated " +
              s"node among $possibleRecipients with workflow ID $workflowId, " +
              s"adding last node used $nodeThatFailed to blacklist $blacklistO (or refreshing " +
              "its last failure time if already present)"
          )
          blacklistO.fold(Some(Map(nodeThatFailed -> now)))(blacklist =>
            Some(blacklist + (nodeThatFailed -> now))
          )
        }
      }
    }

  private def expireWorkflowBlacklist(
      now: Instant,
      workflowId: WorkflowId,
  )(implicit traceContext: TraceContext): Unit =
    workflowBlacklists.get(workflowId).foreach { blacklist =>
      val unexpiredBlacklist =
        blacklist.filter { case (_, instant) =>
          instant.plus(sendBlacklistExpirationDuration.toJava).isAfter(now)
        }
      if (unexpiredBlacklist.sizeIs != blacklist.size) {
        logger.info(
          s"Expiring workflow blacklist info for workflow ID $workflowId, " +
            s"blacklist before expiration: $blacklist, " +
            s"blacklist after expiration: $unexpiredBlacklist"
        )
        workflowBlacklists.update(workflowId, unexpiredBlacklist)
      }
    }

  private def getBlacklisted(
      workflowIdO: Option[WorkflowId],
      possibleRecipients: Seq[BftNodeId],
      message: BftOrderingNetworkMessage,
  )(implicit traceContext: TraceContext): Set[BftNodeId] =
    workflowIdO.fold[Set[BftNodeId]] {
      logger.debug(
        s"Asked to send message `${message.getClass.getSimpleName}` to random authenticated " +
          s"node among $possibleRecipients with no workflow ID"
      )
      Set.empty
    } { workflowId =>
      val blacklist =
        workflowBlacklists
          .get(workflowId)
          .fold[Set[BftNodeId]] {
            Set.empty
          } { blacklist =>
            blacklist.keys.toSet
          }
      logger.debug(
        s"Asked to send message `${message.getClass.getSimpleName}` to random authenticated " +
          s"node among $possibleRecipients with workflow ID $workflowId, current blacklist: $blacklist"
      )
      blacklist
    }

  private def selectRecipient(
      workflowIdO: Option[WorkflowId],
      possibleRecipients: Seq[BftNodeId],
      authenticatedNodeIds: Seq[BftNodeId],
      blackListed: Set[BftNodeId],
      message: BftOrderingNetworkMessage,
  )(implicit traceContext: TraceContext): Option[BftNodeId] = {
    val candidatesBeforeBlacklist = authenticatedNodeIds.intersect(possibleRecipients)
    nodeShuffler
      .shuffle(candidatesBeforeBlacklist.diff(blackListed.toSeq))
      .headOption
      .fold[Option[BftNodeId]] {
        // Falling back to a blacklisted node if all possible recipients are blacklisted
        nodeShuffler
          .shuffle(candidatesBeforeBlacklist)
          .headOption
          .fold[Option[BftNodeId]] {
            logger.info(
              s"No authenticated nodes available among $possibleRecipients " +
                s"(even not excluding blacklisted $blackListed for workflow $workflowIdO) " +
                s"to send random unicast message `${message.getClass.getSimpleName}` to (yet?)"
            )
            None
          } { recipientNodeId =>
            logger.debug(
              s"Sending message `${message.getClass.getSimpleName}` to random authenticated node $recipientNodeId " +
                s"among $possibleRecipients (all of which are blacklisted $blackListed for workflow $workflowIdO, " +
                s"all authenticated nodes: $authenticatedNodeIds)"
            )
            sendIfKnown(recipientNodeId, message)
            Some(recipientNodeId)
          }
      } { recipientNodeId =>
        logger.debug(
          s"Sending message `${message.getClass.getSimpleName}` " +
            s"to random authenticated node $recipientNodeId among $possibleRecipients " +
            s"(excluding $blackListed for workflow $workflowIdO, " +
            s"all authenticated nodes: $authenticatedNodeIds)"
        )
        sendIfKnown(recipientNodeId, message)
        Some(recipientNodeId)
      }
  }

  private def sendIfKnown(
      bftNodeId: BftNodeId,
      message: BftOrderingNetworkMessage,
  )(implicit traceContext: TraceContext): Unit =
    if (bftNodeId != thisBftNodeId)
      networkSendIfKnown(bftNodeId, message)
    else
      dependencies.p2pNetworkIn.asyncSend(
        messageToSend(message.toProto, maybeNetworkSendInstant = None)
      )

  private def networkSendIfKnown(
      recipientBftNodeId: BftNodeId,
      message: BftOrderingNetworkMessage,
  )(implicit traceContext: TraceContext): Unit = {
    val serializedMessage = message.toProto
    p2pConnectionState
      .getNetworkRef(recipientBftNodeId)
      .fold {
        val mc1 =
          sendMetricsContext(
            metrics,
            serializedMessage,
            recipientBftNodeId,
            droppedAsUnauthenticated = true,
          )
        locally {
          implicit val mc: MetricsContext = mc1
          emitSendStats(metrics, serializedMessage)
        }
        logger.info(
          s"Dropping network message to unknown $recipientBftNodeId (possibly unauthenticated as of yet)"
        )
        logger.trace(s"Dropped message to $recipientBftNodeId is: $message")
      } { ref =>
        val mc1: MetricsContext =
          sendMetricsContext(
            metrics,
            serializedMessage,
            recipientBftNodeId,
            droppedAsUnauthenticated = false,
          )
        locally {
          logger.trace(s"Sending network message to $recipientBftNodeId: $message")
          implicit val mc: MetricsContext = mc1
          networkSend(ref, serializedMessage)
          emitSendStats(metrics, serializedMessage)
        }
      }
  }

  private def processAdminMessage(
      admin: P2PNetworkOut.Admin
  )(implicit context: E#ActorContextT[P2PNetworkOut.Message], traceContext: TraceContext): Unit =
    admin match {

      case Admin.AddEndpoint(p2pEndpoint, callback) =>
        if (p2pConnectionState.isDefined(p2pEndpoint.id)) {
          logger.info(s"Operator requested adding P2P endpoint $p2pEndpoint but it already exists")
          callback(false)
        } else {
          logger.info(s"Adding missing P2P endpoint $p2pEndpoint as requested by operator")
          context.pipeToSelf(p2pEndpointsStore.addEndpoint(p2pEndpoint)) {
            case Success(additionSuccess) =>
              callback(additionSuccess)
              if (additionSuccess)
                Some(P2PNetworkOut.Internal.Connect(p2pEndpoint))
              else
                None
            case Failure(exception) =>
              abort(s"Failed to P2P add endpoint $p2pEndpoint", exception)
          }
        }

      case Admin.RemoveEndpoint(p2pEndpointId, callback) =>
        if (p2pConnectionState.isDefined(p2pEndpointId)) {
          logger.info(s"Removing existing P2P endpoint $p2pEndpointId as requested by operator")
          context.pipeToSelf(p2pEndpointsStore.removeEndpoint(p2pEndpointId)) {
            case Success(hasBeenRemoved) =>
              callback(hasBeenRemoved)
              if (hasBeenRemoved)
                Some(P2PNetworkOut.Internal.Disconnect(p2pEndpointId))
              else
                None
            case Failure(exception) =>
              abort(s"Failed to remove P2P endpoint $p2pEndpointId", exception)
          }
        } else {
          logger.info(
            s"Operator requested removing P2P endpoint $p2pEndpointId but it does not exist"
          )
          callback(false)
        }

      case Admin.ListConfiguredEndpoints(callback) =>
        context.pipeToSelf(p2pEndpointsStore.listEndpoints()) {
          case Success(endpoints) =>
            callback(endpoints.sortBy(_.id)) // For output determinism and easier testing
            None
          case Failure(exception) =>
            abort(s"Failed to list P2P endpoints", exception)
        }

      case Admin.GetStatus(callback, p2pEndpointIds) =>
        logger.info(
          s"Operator requested P2P status for endpoints ${p2pEndpointIds.getOrElse("<all>")}"
        )
        callback(getStatus(p2pEndpointIds))
    }

  private def getStatus(
      p2pEndpointIds: Option[Iterable[P2PEndpoint.Id]] = None
  )(implicit
      context: E#ActorContextT[P2PNetworkOut.Message],
      traceContext: TraceContext,
  ): SequencerBftAdminData.PeerNetworkStatus =
    SequencerBftAdminData.PeerNetworkStatus(
      p2pEndpointIds
        .map(
          _.map(p2pEndpointId =>
            Some(p2pEndpointId) -> p2pConnectionState.getBftNodeId(p2pEndpointId)
          )
        )
        .getOrElse(
          p2pConnectionState.connections
        )
        .toSeq
        .sorted // For output determinism and easier testing
        .map { case (maybeP2PEndpointId, maybeBftNodeId) =>
          (
            maybeP2PEndpointId,
            maybeP2PEndpointId.exists(p2pConnectionState.isOutgoing),
            maybeBftNodeId,
            maybeP2PEndpointId.exists(connectedP2PEndpointIds.contains),
            p2pEndpointIds.isEmpty || maybeP2PEndpointId.exists(p2pConnectionState.isDefined),
          )
        }
        .map {
          case (
                maybeP2PEndpointId,
                isEndpointOutgoing,
                maybeBftNodeId,
                isEndpointConnected,
                isEndpointDefined,
              ) =>
            maybeP2PEndpointId match {
              case Some(p2pEndpointId) =>
                PeerConnectionStatus.PeerEndpointStatus(
                  p2pEndpointId,
                  isEndpointOutgoing,
                  health = (maybeBftNodeId, isEndpointConnected, isEndpointDefined) match {
                    case (Some(nodeId), _, _) =>
                      PeerEndpointHealth(
                        PeerEndpointHealthStatus.Authenticated(
                          SequencerNodeId
                            .fromBftNodeId(nodeId)
                            .getOrElse(abort(s"Node ID '$nodeId' is not a valid sequencer ID"))
                        ),
                        None,
                      )
                    case (None, true, _) =>
                      PeerEndpointHealth(PeerEndpointHealthStatus.Unauthenticated, None)
                    case (None, false, true) =>
                      PeerEndpointHealth(PeerEndpointHealthStatus.Disconnected, None)
                    case _ =>
                      PeerEndpointHealth(PeerEndpointHealthStatus.UnknownEndpoint, None)
                  },
                )
              case _ =>
                // Only reported for incoming connections without a known endpoint, which are considered authenticated
                PeerConnectionStatus.PeerIncomingConnection(
                  SequencerNodeId
                    .fromBftNodeId(
                      maybeBftNodeId.getOrElse(
                        abort(s"A known connection cannot miss both endpoint and node information")
                      )
                    )
                    .getOrElse(abort(s"Cannot convert '$maybeBftNodeId' to a sequencer ID"))
                )
            }
        }
    )

  private lazy val p2pEndpointThresholdForAvailabilityStart =
    AvailabilityModule.quorum(state.bootstrapMembership.orderingTopology.size)

  private lazy val p2pEndpointThresholdForConsensusStart =
    strongQuorumSize(state.bootstrapMembership.orderingTopology.size)

  private def startModulesIfNeeded()(implicit traceContext: TraceContext): Unit = {
    if (!mempoolStarted) {
      logger.info(s"Starting mempool module")
      dependencies.mempool.asyncSend(Mempool.Start)
      mempoolStarted = true
    }
    // Waiting for just a quorum (minus self) of nodes to be authenticated assumes that they are not faulty
    if (!availabilityStarted) {
      if (
        !isGenesis || maxNodesContemporarilyAuthenticated >= p2pEndpointThresholdForAvailabilityStart
      ) {
        logger.info(
          s"Starting availability module (genesis=$isGenesis, " +
            s"maxNodesContemporarilyAuthenticated=$maxNodesContemporarilyAuthenticated, " +
            s"p2pEndpointThresholdForAvailabilityStart=$p2pEndpointThresholdForAvailabilityStart)"
        )
        dependencies.availability.asyncSend(Availability.Start)
        availabilityStarted = true
      }
    }
    if (!consensusStarted) {
      if (
        !isGenesis || maxNodesContemporarilyAuthenticated >= p2pEndpointThresholdForConsensusStart
      ) {
        logger.info(
          s"Starting consensus module (genesis=$isGenesis, " +
            s"maxNodesContemporarilyAuthenticated=$maxNodesContemporarilyAuthenticated, " +
            s"p2pEndpointThresholdForConsensusStart=$p2pEndpointThresholdForConsensusStart)"
        )
        dependencies.consensus.asyncSend(Consensus.Start)
        consensusStarted = true
      }
    }
    if (!outputStarted) {
      logger.info(s"Starting output module")
      dependencies.output.asyncSend(Output.Start)
      outputStarted = true
    }
    if (!pruningStarted) {
      logger.info(s"Starting pruning module")
      dependencies.pruning.asyncSend(Pruning.Start)
      pruningStarted = true
    }
  }

  private def networkSend(
      ref: P2PNetworkRef[BftOrderingMessage],
      message: BftOrderingMessageBody,
  )(implicit traceContext: TraceContext, mc: MetricsContext): Unit =
    ref.asyncP2PSend(maybeNetworkSendInstant => messageToSend(message, maybeNetworkSendInstant))

  private def messageToSend(
      message: BftOrderingMessageBody,
      maybeNetworkSendInstant: Option[Instant],
  )(implicit traceContext: TraceContext): BftOrderingMessage =
    BftOrderingMessage(
      traceContext.asW3CTraceContext.map(_.parent).getOrElse(""),
      Some(message),
      thisBftNodeId,
      maybeNetworkSendInstant.map(networkSendInstant =>
        Timestamp(networkSendInstant.getEpochSecond, networkSendInstant.getNano)
      ),
    )

  private def connectInitialNodes(
      otherInitialP2PEndpoints: Seq[P2PEndpoint]
  )(implicit context: E#ActorContextT[P2PNetworkOut.Message], traceContext: TraceContext): Unit =
    if (!initialNodesConnecting) {
      logger.info(s"Connecting to initial P2P endpoints: $otherInitialP2PEndpoints")
      otherInitialP2PEndpoints.foreach(initialP2PEndpoint =>
        ensureSendingEnabledTo(P2PAddress.Endpoint(initialP2PEndpoint)).discard
      )
      initialNodesConnecting = true
    }

  private def ensureSendingEnabledTo(
      p2pAddress: P2PAddress
  )(implicit
      context: E#ActorContextT[P2PNetworkOut.Message],
      traceContext: TraceContext,
  ): Unit =
    p2pConnectionState.associateP2PEndpointIdToBftNodeId(p2pAddress).foreach { _ =>
      p2pConnectionState.addNetworkRefIfMissing(p2pAddress.id) { () =>
        logger.info(
          s"Not creating new network ref for '$p2pAddress' as it already exists"
        )
      } { () =>
        logger.info(s"Creating new network ref for '$p2pAddress'")
        p2pNetworkManager.createNetworkRef(context, p2pAddress)
      }
    }

  private def disconnect(
      p2pEndpointId: P2PEndpoint.Id
  )(implicit context: E#ActorContextT[P2PNetworkOut.Message], traceContext: TraceContext): Unit = {
    logger.info(
      s"Disconnecting P2P endpoint $p2pEndpointId ('${p2pConnectionState.getBftNodeId(p2pEndpointId).getOrElse("<unknown node ID>")}'"
    )
    p2pNetworkManager.shutdownOutgoingConnection(p2pEndpointId)
    connectedP2PEndpointIds.remove(p2pEndpointId).discard
    emitConnectionStateMetricsAndLogEndpointsStatus(notifyMempool = true)
  }

  private def emitConnectionStateMetricsAndLogEndpointsStatus(notifyMempool: Boolean)(implicit
      context: E#ActorContextT[P2PNetworkOut.Message],
      mc: MetricsContext,
      traceContext: TraceContext,
  ): Unit = {
    val status = getStatus()
    val authenticatedCount = getAuthenticatedNodeIds(status).size
    val connectedCount = getConnectedPeersCount(status)
    emitConnectedCount(metrics, connectedCount)
    emitAuthenticatedCount(metrics, authenticatedCount)
    logEmitForwardP2PStatus(status, notifyMempool)
  }

  private def getConnectedPeersCount(status: SequencerBftAdminData.PeerNetworkStatus) =
    status.endpointStatuses.count {
      case PeerConnectionStatus.PeerEndpointStatus(
            _,
            _,
            PeerEndpointHealth(
              PeerEndpointHealthStatus.Authenticated(_) | PeerEndpointHealthStatus.Unauthenticated,
              _,
            ),
          ) =>
        true
      case PeerConnectionStatus.PeerIncomingConnection(_) => true
      case _ => false
    }

  private def getAuthenticatedNodeIds(
      status: SequencerBftAdminData.PeerNetworkStatus
  ): Seq[BftNodeId] =
    status.endpointStatuses
      .collect {
        case PeerConnectionStatus.PeerEndpointStatus(
              _,
              _,
              PeerEndpointHealth(
                PeerEndpointHealthStatus.Authenticated(sequencerNodeId),
                _,
              ),
            ) =>
          SequencerNodeId.toBftNodeId(sequencerNodeId)
        case PeerConnectionStatus.PeerIncomingConnection(sequencerNodeId) =>
          SequencerNodeId.toBftNodeId(sequencerNodeId)
      }
      .distinct
      .sorted // For output determinism and easier testing

  private def getAuthenticatedCountIncludingSelf()(implicit
      context: E#ActorContextT[P2PNetworkOut.Message],
      traceContext: TraceContext,
  ): Int =
    getAuthenticatedNodeIds(getStatus()).size + 1

  private def logEmitForwardP2PStatus(
      status: SequencerBftAdminData.PeerNetworkStatus,
      notifyMempool: Boolean,
  )(implicit context: E#ActorContextT[P2PNetworkOut.Message], traceContext: TraceContext): Unit = {
    if (notifyMempool)
      sendConnectivityUpdateToMempool()
    metrics.p2p.update(status)
    logger.info(s"P2P endpoints status: $status")
  }

  private def sendConnectivityUpdateToMempool()(implicit
      context: E#ActorContextT[P2PNetworkOut.Message],
      traceContext: TraceContext,
  ): Unit =
    dependencies.mempool.asyncSend(
      Mempool.P2PConnectivityUpdate(membership, getAuthenticatedCountIncludingSelf())
    )
}

private[bftordering] object P2PNetworkOutModule {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  final class State(
      val p2pConnectionState: P2PConnectionState,
      val bootstrapMembership: Membership,
  ) {

    var maybeSelf: Option[ModuleRef[P2PNetworkOut.Message]] = None
    var membership: Membership = bootstrapMembership
    var initialNodesConnecting = false
    var mempoolStarted = false
    var availabilityStarted = false
    var consensusStarted = false
    var outputStarted = false
    var pruningStarted = false

    // We want to track the maximum number of contemporarily authenticated nodes,
    //  because the threshold actions will be used by protocol modules to know when
    //  there are enough connections to start, so we don't want to consider
    //  nodes that disconnected afterward. For example, when node N1 connects
    //  to other nodes:
    //
    //  - N2 authenticates.
    //  - N3 authenticates.
    //  - N2 gets disconnected (e.g. by an admin) slightly before processing the request of
    //    consensus to be started when 2 nodes are authenticated.
    //
    //  In this case, we want to start consensus anyway.
    var maxNodesContemporarilyAuthenticated = 1 // i.e., self

    // For each workflow ID, the set of nodes we have blacklisted for that workflow ID
    //  due to the workflow being retried when they were used, together with their failure instant.
    val workflowBlacklists: mutable.Map[WorkflowId, Map[BftNodeId, Instant]] =
      mutable.Map.empty
  }
}
