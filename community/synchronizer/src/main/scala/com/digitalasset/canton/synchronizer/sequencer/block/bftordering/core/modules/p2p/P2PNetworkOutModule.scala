// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.SequencerBftAdminData
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.SequencerBftAdminData.{
  PeerConnectionStatus,
  PeerEndpointHealth,
  PeerEndpointHealthStatus,
  PeerNetworkStatus,
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

  val p2pNetworkManager: P2PNetworkManagerT =
    dependencies.createP2PNetworkManager(this, dependencies.p2pNetworkIn)

  override def ready(
      self: ModuleRef[P2PNetworkOut.Message]
  )(implicit traceContext: TraceContext): Unit = {
    state.maybeSelf = Some(self)
    self.asyncSend(P2PNetworkOut.Start)
  }

  override def onNodeId(bftNodeId: BftNodeId, maybeP2PEndpoint: Option[P2PEndpoint])(implicit
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
        // Seed the module-local cache of configured endpoints.
        p2pEndpoints.foreach { case (p2pEndpoint, _) =>
          configuredP2PEndpoints.put(p2pEndpoint.id, p2pEndpoint).discard
        }
        connectInitialNodes(p2pEndpoints)
        startModulesIfNeeded()

      case P2PNetworkOut.Internal.EndpointAdded(p2pEndpoint) =>
        val p2pEndpointId = p2pEndpoint.id
        // The P2P endpoints store is insert-only w.r.t. the endpoint set, i.e. adding an endpoint whose ID is
        //  already present is a no-op that doesn't overwrite its TLS client material. The cache mirrors that
        //  semantics via `getOrElseUpdate`, so the effective (i.e. persisted) endpoint is the one it returns,
        //  which may differ from the just-added one when the ID was already configured.
        //
        //  Dialing must then use the effective endpoint, else a duplicate `AddEndpoint` carrying different TLS
        //  material could create a connection based on configuration that was never persisted; this is
        //  observable, because the runtime state may have already been cleared, e.g. by a teardown whose
        //  disconnection event hasn't been processed yet, so the connectivity check below can pass.
        val effectiveP2PEndpoint =
          configuredP2PEndpoints.getOrElseUpdate(p2pEndpointId, p2pEndpoint)
        if (p2pConnectionState.isDefined(p2pEndpointId)) {
          logger.info(
            s"P2P endpoint $p2pEndpointId that the operator just added is already known at runtime, " +
              "so not trying to connect to it"
          )
        } else {
          logger.info(
            s"Ensuring outgoing connectivity to P2P endpoint $p2pEndpointId because the operator added it"
          )
          ensureSendingEnabledTo(P2PAddress.Endpoint(effectiveP2PEndpoint))
        }

      case P2PNetworkOut.Internal.EndpointRemoved(p2pEndpointId) =>
        configuredP2PEndpoints.remove(p2pEndpointId).discard
        if (p2pConnectionState.isDefined(p2pEndpointId)) {
          logger.info(
            s"Operator removed P2P endpoint $p2pEndpointId, disconnecting from it if outgoing"
          )
          disconnect(p2pEndpointId)
        } else {
          logger.info(
            s"P2P endpoint $p2pEndpointId that the operator just removed is not known at runtime, " +
              "so not trying to disconnect from it"
          )
        }

      case P2PNetworkOut.Network.Connected(maybeP2pEndpointId) =>
        if (maybeP2pEndpointId.forall(connectedP2PEndpointIds.add)) {
          logger.info(
            s"P2P endpoint ${maybeP2pEndpointId.map(_.toString).getOrElse("<incoming connection>")} " +
              s"is now connected"
          )
          emitConnectionStateMetricsAndLogEndpointsStatus(
            getPeerNetworkStatus(),
            notifyMempool = false,
          )
        }

      case P2PNetworkOut.Network.Disconnected(p2pEndpointId) =>
        if (connectedP2PEndpointIds.remove(p2pEndpointId)) {
          logger.info(s"P2P endpoint $p2pEndpointId is now disconnected")
          emitConnectionStateMetricsAndLogEndpointsStatus(
            getPeerNetworkStatus(),
            notifyMempool = true,
          )
        }
        // A disconnection may wipe out all runtime knowledge of the endpoint; this happens in particular when an
        //  incoming connection that won connection deduplication is torn down by the peer, because incoming
        //  connections are not managed by this node, so their state, including the network ref, is fully cleaned up.
        //
        //  If the endpoint is still configured on this node, i.e. its operator did not remove it, this node
        //  is responsible for (re-)establishing an outgoing connection to it; else, the two nodes could remain
        //  disconnected forever even though this node is configured to connect to the peer, e.g. when the peer's
        //  operator removed this node's endpoint from the peer's configuration, so the peer will not redial.
        reconnectIfStillConfigured(p2pEndpointId)

      case P2PNetworkOut.Network.Authenticated(bftNodeId, maybeP2PEndpoint) =>
        val maybeP2PEndpointId = maybeP2PEndpoint.map(_.id)
        val p2pEndpointIdString = maybeP2PEndpointId.map(_.toString).getOrElse("<unknown>")
        logger.info(
          s"Authenticated node $bftNodeId at $p2pEndpointIdString, marking P2P endpoint (if known) as connected " +
            "and ensuring connectivity to it"
        )
        maybeP2PEndpointId.foreach(connectedP2PEndpointIds.add(_).discard)
        ensureSendingEnabledTo(P2PAddress.NodeId(bftNodeId, maybeP2PEndpoint))
        val peerNetworkStatus = getPeerNetworkStatus()
        emitConnectionStateMetricsAndLogEndpointsStatus(peerNetworkStatus, notifyMempool = true)
        maxNodesContemporarilyAuthenticated = Math.max(
          maxNodesContemporarilyAuthenticated,
          getAuthenticatedCountIncludingSelf(peerNetworkStatus),
        )
        startModulesIfNeeded()

      case P2PNetworkOut.Network.TopologyUpdate(newMembership) =>
        membership = newMembership
        val peerNetworkStatus = getPeerNetworkStatus()
        sendConnectivityUpdateToMempool(peerNetworkStatus)
        logQuorumInfo(peerNetworkStatus)

      case P2PNetworkOut.Multicast(message, recipientBftNodeIds) =>
        if (recipientBftNodeIds.nonEmpty) {
          val serializedMessage = message.toProto
          checkForOversizedNetworkMessage(message, serializedMessage, isMulticast = true)
          recipientBftNodeIds.toSeq.sorted // For determinism
            .foreach(sendIfKnown(_, serializedMessage))
        }

      case P2PNetworkOut.SendToRandomAuthenticated(
            message,
            firstChoiceRecipientsPool,
            secondChoiceRecipientsPool,
            workflowIdO,
            nodesThatFailed,
            onRecipientsDecision,
            howManyRecipients,
          ) =>
        val now = clock.now.toInstant
        updateBlacklists(
          workflowIdO,
          nodesThatFailed,
          now,
          firstChoiceRecipientsPool,
          message,
        )
        workflowIdO.foreach(expireWorkflowBlacklist(now, _))
        val blackListed = getBlacklisted(workflowIdO, firstChoiceRecipientsPool, message)
        val authenticatedNodeIds = getAuthenticatedNodeIds(getPeerNetworkStatus())
        val recipientNodeIds =
          selectRecipients(
            workflowIdO,
            firstChoiceRecipientsPool,
            secondChoiceRecipientsPool,
            authenticatedNodeIds,
            blackListed,
            message,
            howManyRecipients,
          )
        if (recipientNodeIds.isEmpty)
          metrics.p2p.send.sendsDropped.inc()(
            mc.withExtraLabels(
              metrics.p2p.send.failure.labels.reason.Key ->
                metrics.p2p.send.failure.labels.reason.values.NoAuthenticatedRecipientCandidates
            )
          )
        else {
          val serializedMessage = message.toProto
          checkForOversizedNetworkMessage(message, serializedMessage, isMulticast = false)
          recipientNodeIds.foreach(sendIfKnown(_, serializedMessage))
        }
        try onRecipientsDecision.foreach(_(recipientNodeIds))
        catch {
          case scala.util.control.NonFatal(e) =>
            logger.warn("`onRecipientsDecision` callback failed", e)
        }

      case P2PNetworkOut.EndWorkflow(workflowId) =>
        logger.debug(
          s"Ending workflow $workflowId, clearing any associated workflow blacklist info"
        )
        workflowBlacklists.remove(workflowId).discard

      case admin: P2PNetworkOut.Admin =>
        processAdminMessage(admin)
    }

  // Checks whether the message body is larger than the currently acceptable maxRequestSize.
  // This currently only counts the body of the `BftOrderingMessage`, and not also the `sentBy` and `sentAt`
  // fields from the BftOrderingMessage. However, this should be more than enough since the instances we
  // observe for large messages are at least several MB above the max request size.
  private def checkForOversizedNetworkMessage(
      message: BftOrderingNetworkMessage,
      serializedMessage: BftOrderingMessageBody,
      isMulticast: Boolean,
  )(implicit traceContext: TraceContext): Unit = {
    val permittedSize = membership.orderingTopology.maxRequestPayloadBytes.value
    val messageSize = serializedMessage.serializedSize

    if (messageSize > permittedSize) {
      val messageType = typeOfInnerMessage(message)
      logger.warn(
        s"Sending $messageType w/ size $messageSize is larger than max allowed $permittedSize, " +
          s"multicast: $isMulticast; message will likely be dropped by the receiving peer."
      )
    }
  }

  private def typeOfInnerMessage(message: BftOrderingNetworkMessage): String = message match {
    case BftOrderingNetworkMessage.AvailabilityMessage(signedMessage) =>
      signedMessage.message.getClass.getSimpleName
    case BftOrderingNetworkMessage.ConsensusMessage(signedMessage) =>
      val inner = signedMessage.message
      s"${inner.getClass.getSimpleName} w/ ${inner.blockMetadata}"
    case BftOrderingNetworkMessage.RetransmissionMessage(message) => message.getClass.getSimpleName
    case BftOrderingNetworkMessage.StateTransferMessage(signedMessage) =>
      signedMessage.message.getClass.getSimpleName
    case BftOrderingNetworkMessage.Empty => "Empty"
  }

  private def logQuorumInfo(
      peerNetworkStatus: PeerNetworkStatus
  )(implicit traceContext: TraceContext): Unit = {
    val weakQuorum = membership.orderingTopology.weakQuorum
    val strongQuorum = membership.orderingTopology.strongQuorum
    val authenticatedNodesCount = getAuthenticatedCountIncludingSelf(peerNetworkStatus)
    if (authenticatedNodesCount < weakQuorum) {
      logWhenQuorumOK = true
      logger.info(
        s"Authenticated P2P nodes count (including this node) $authenticatedNodesCount is currently below " +
          s"weak quorum size $weakQuorum, dissemination and ordering may not be able to proceed " +
          "until more nodes are authenticated"
      )
    } else if (authenticatedNodesCount < strongQuorum) {
      logWhenQuorumOK = true
      logger.info(
        s"Authenticated P2P nodes count (including this node) $authenticatedNodesCount is currently below " +
          s"strong quorum size $strongQuorum, ordering may not be able to proceed " +
          "until more nodes are authenticated"
      )
    } else if (logWhenQuorumOK) {
      logWhenQuorumOK = false
      logger.info(
        s"Authenticated P2P nodes count (including this node) $authenticatedNodesCount is now again above " +
          s"strong quorum size $strongQuorum"
      )
    }
  }

  private def updateBlacklists(
      workflowIdO: Option[WorkflowId],
      nodesThatFailed: Seq[BftNodeId],
      now: Instant,
      possibleRecipients: Seq[BftNodeId],
      message: BftOrderingNetworkMessage,
  )(implicit traceContext: TraceContext): Unit =
    workflowIdO.foreach { workflowId =>
      workflowBlacklists.updateWith(workflowId) { (blacklistO: Option[Map[BftNodeId, Instant]]) =>
        if (blacklistO.isEmpty)
          logger.info(s"New workflow $workflowId started")
        if (nodesThatFailed.isEmpty) {
          logger.debug(
            s"Sending message `${message.getClass.getSimpleName}` to random authenticated " +
              s"node among $possibleRecipients with workflow ID $workflowId (not a retry), " +
              s"keeping blacklist $blacklistO"
          )
          blacklistO.fold(Some(Map.empty[BftNodeId, Instant]))(blacklist => Some(blacklist))
        } else {
          logger.info(
            s"Retrying to send message `${message.getClass.getSimpleName}` to random authenticated " +
              s"node among $possibleRecipients with workflow ID $workflowId, " +
              s"adding last node used $nodesThatFailed to blacklist $blacklistO (or refreshing " +
              "its last failure time if already present)"
          )
          val nodesThatFailedWithFailureInstant = nodesThatFailed.map(_ -> now)
          blacklistO.fold(Some(Map.from(nodesThatFailedWithFailureInstant)))(blacklist =>
            Some(blacklist ++ nodesThatFailedWithFailureInstant)
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

  @SuppressWarnings(Array("org.wartremover.warts.Return", "org.wartremover.warts.Var"))
  private def selectRecipients(
      workflowIdO: Option[WorkflowId],
      firstChoiceRecipientsPool: Seq[BftNodeId],
      secondChoiceRecipientsPoolO: Option[Seq[BftNodeId]],
      authenticatedNodeIds: Seq[BftNodeId],
      blackListed: Set[BftNodeId],
      message: BftOrderingNetworkMessage,
      howManyRecipients: PositiveInt,
  )(implicit traceContext: TraceContext): Seq[BftNodeId] = {
    val firstChoiceAuthenticatedOnlyCandidates =
      authenticatedNodeIds
        .intersect(firstChoiceRecipientsPool)
    val blacklistedSeq = blackListed.toSeq
    val firstChoiceAuthenticatedAndNotBlacklistedCandidates =
      firstChoiceAuthenticatedOnlyCandidates.diff(blacklistedSeq)
    val howManyRecipientsInt = howManyRecipients.value
    val firstChoiceSelected =
      random
        .shuffle(firstChoiceAuthenticatedAndNotBlacklistedCandidates)
        .take(howManyRecipientsInt)
    if (firstChoiceSelected.sizeIs == howManyRecipientsInt)
      return firstChoiceSelected.sorted

    var remainingHowMany = howManyRecipientsInt - firstChoiceSelected.size
    val secondChoiceRecipientsPool = secondChoiceRecipientsPoolO.getOrElse(Seq.empty)
    val secondChoiceAuthenticatedOnlyCandidates =
      authenticatedNodeIds
        .intersect(secondChoiceRecipientsPool)
    val secondChoiceAuthenticatedAndNotBlacklistedCandidates =
      secondChoiceAuthenticatedOnlyCandidates.diff(blacklistedSeq)
    val secondChoiceSelected =
      random
        .shuffle(secondChoiceAuthenticatedAndNotBlacklistedCandidates)
        .take(remainingHowMany)
    val firstOrSecondChoiceSelected = firstChoiceSelected ++ secondChoiceSelected
    if (firstOrSecondChoiceSelected.sizeIs == howManyRecipientsInt)
      return firstOrSecondChoiceSelected.sorted

    remainingHowMany = howManyRecipientsInt - firstOrSecondChoiceSelected.size
    val messageType = message.getClass.getSimpleName

    def logPartialRecipientSelection(recipients: Seq[BftNodeId]): Unit =
      logger.info(
        "Not enough authenticated and whitelisted nodes available " +
          s"($howManyRecipients requested, $remainingHowMany remaining) " +
          s"among $firstChoiceRecipientsPool nor " +
          s"$secondChoiceRecipientsPoolO to send message of type `$messageType` to, " +
          s"(send blacklist for workflow $workflowIdO = $blackListed) " +
          s"falling back to $recipients among authenticated candidate in either list, " +
          s"(all authenticated nodes = $authenticatedNodeIds)"
      )

    val anyOtherAuthenticatedCandidates =
      (firstChoiceAuthenticatedOnlyCandidates ++ secondChoiceAuthenticatedOnlyCandidates).diff(
        firstOrSecondChoiceSelected
      )
    if (anyOtherAuthenticatedCandidates.nonEmpty) {
      val recipients =
        (firstOrSecondChoiceSelected ++ random
          .shuffle(anyOtherAuthenticatedCandidates)
          .take(remainingHowMany)).sorted
      logPartialRecipientSelection(recipients)
      return recipients
    }

    val recipients = firstOrSecondChoiceSelected.sorted
    logPartialRecipientSelection(recipients)
    firstOrSecondChoiceSelected
  }

  private def sendIfKnown(
      bftNodeId: BftNodeId,
      serializedMessage: BftOrderingMessageBody,
  )(implicit traceContext: TraceContext): Unit =
    if (bftNodeId != thisBftNodeId)
      networkSendIfKnown(bftNodeId, serializedMessage)
    else
      dependencies.p2pNetworkIn.asyncSend(
        messageToSend(serializedMessage, maybeNetworkSendInstant = None)
      )

  private def networkSendIfKnown(
      recipientBftNodeId: BftNodeId,
      serializedMessage: BftOrderingMessageBody,
  )(implicit traceContext: TraceContext): Unit =
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
          emitSendStats(metrics, serializedMessage, droppedAsUnauthenticated = true)
        }
        logger.info(
          s"Dropping network message to unknown $recipientBftNodeId (possibly unauthenticated as of yet)"
        )
      } { ref =>
        val mc1: MetricsContext =
          sendMetricsContext(
            metrics,
            serializedMessage,
            recipientBftNodeId,
            droppedAsUnauthenticated = false,
          )
        locally {
          implicit val mc: MetricsContext = mc1
          networkSend(recipientBftNodeId, ref, serializedMessage)
          emitSendStats(metrics, serializedMessage)
        }
      }

  private def processAdminMessage(
      admin: P2PNetworkOut.Admin
  )(implicit context: E#ActorContextT[P2PNetworkOut.Message], traceContext: TraceContext): Unit =
    admin match {

      case Admin.AddEndpoint(p2pEndpoint, callback) =>
        logger.info(s"Adding P2P endpoint $p2pEndpoint as requested by operator")
        context.pipeToSelf(p2pEndpointsStore.addEndpoint(p2pEndpoint)) {
          case Success(hasBeenAdded) =>
            try {
              callback(hasBeenAdded)
            } catch {
              case scala.util.control.NonFatal(e) =>
                logger.warn("callback for `AddEndpoint` failed", e)
            }
            if (hasBeenAdded)
              logger.info(s"P2P endpoint $p2pEndpoint successfully inserted into store")
            else
              logger.info(
                s"P2P endpoint $p2pEndpoint was already present in store, so it was not inserted"
              )
            // Always notify the module, so that it can update the cache of configured endpoints
            //  and decide whether to connect based on the runtime connection state.
            Some(P2PNetworkOut.Internal.EndpointAdded(p2pEndpoint))
          case Failure(exception) =>
            abort(s"Failed to add P2P endpoint $p2pEndpoint", exception)
        }

      case Admin.RemoveEndpoint(p2pEndpointId, callback) =>
        logger.info(s"Removing P2P endpoint $p2pEndpointId as requested by operator")
        context.pipeToSelf(p2pEndpointsStore.removeEndpoint(p2pEndpointId)) {
          case Success(hasBeenRemoved) =>
            try {
              callback(hasBeenRemoved)
            } catch {
              case scala.util.control.NonFatal(e) =>
                logger.warn("callback for `RemoveEndpoint` failed", e)
            }
            if (hasBeenRemoved)
              logger.info(s"P2P endpoint $p2pEndpointId successfully removed from store")
            else
              logger.info(
                s"P2P endpoint $p2pEndpointId was not present in store, so it was not removed"
              )
            // Always notify the module, so that it can update the cache of configured endpoints
            //  and decide whether to disconnect based on the runtime connection state.
            Some(P2PNetworkOut.Internal.EndpointRemoved(p2pEndpointId))
          case Failure(exception) =>
            abort(s"Failed to remove P2P endpoint $p2pEndpointId", exception)
        }

      case Admin.ListConfiguredEndpoints(callback) =>
        context.pipeToSelf(p2pEndpointsStore.listEndpoints()) {
          case Success(endpoints) =>
            callback(endpoints.sortBy(_._1.id)) // For output determinism and easier testing
            None
          case Failure(exception) =>
            abort(s"Failed to list P2P endpoints", exception)
        }

      case Admin.GetStatus(callback, p2pEndpointIds) =>
        callback(getPeerNetworkStatus(p2pEndpointIds))
    }

  private def getPeerNetworkStatus(
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
            // TODO(#34391): handle more connectivity status changes from this module, esp. authentication actions
            // Takes precise connectivity status from the connection state directly, but support notification-based
            //  connectivity status for simulation testing
            maybeP2PEndpointId.exists(connectedP2PEndpointIds.contains) ||
              P2PAddress
                .maybeId(maybeBftNodeId, maybeP2PEndpointId)
                .exists(p2pConnectionState.isConnected),
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
                    case (Some(nodeId), true, _) =>
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
                    case (_, false, true) =>
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
                        abort(
                          s"A known connection cannot miss both P2P endpoint and node information"
                        )
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
      recipientBftNodeId: BftNodeId,
      ref: P2PNetworkRef[BftOrderingMessage],
      message: BftOrderingMessageBody,
  )(implicit traceContext: TraceContext, mc: MetricsContext): Unit =
    ref.asyncP2PSend(
      recipientBftNodeId,
      maybeNetworkSendInstant => messageToSend(message, maybeNetworkSendInstant),
    )

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
      otherInitialP2PEndpoints: Seq[(P2PEndpoint, Option[BftNodeId])]
  )(implicit context: E#ActorContextT[P2PNetworkOut.Message], traceContext: TraceContext): Unit =
    if (!initialNodesConnecting) {
      logger.info(s"Connecting to initial P2P endpoints: $otherInitialP2PEndpoints")
      otherInitialP2PEndpoints.foreach { case (initialP2PEndpoint, _) =>
        // The node ID possibly persisted alongside the endpoint by a previous incarnation of this node is
        //  deliberately ignored, i.e. connectivity is always ensured by endpoint, because every connection must be
        //  authenticated anew and, until it is, the peer must not be addressable nor reported as authenticated.
        //
        //  Asserting the persisted association upfront would instead register the network ref by BFT node ID, which
        //  is only correct for incoming connections, and would make the endpoint appear as an authenticated
        //  incoming connection as soon as its gRPC channel is up but before any authentication happened, which in
        //  turn would inflate `maxNodesContemporarilyAuthenticated` and could start protocol modules below the
        //  quorum of authenticated nodes they require.
        //
        //  This also means that the persisted association is not used to pin a peer's identity across
        //  restarts. Within an incarnation, `associateP2PEndpointIdToBftNodeId` currently permits a different,
        //  authenticated node ID to replace the association; hardening persisted associations is tracked
        //  separately by TODO(#34191).
        //  The persisted node ID remains reported to the operator by `Admin.ListConfiguredEndpoints`,
        //  which is what it was introduced for.
        ensureSendingEnabledTo(P2PAddress.Endpoint(initialP2PEndpoint)).discard
      }
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

  // Re-establishes an outgoing connection to a disconnected P2P endpoint if it is still configured on this node.
  //
  //  The module-local cache of configured endpoints, rather than just the disconnected endpoint ID, is consulted
  //  for two independent reasons:
  //
  //  1. It is the only runtime source of the full `P2PEndpoint` needed to dial: a disconnection only carries a
  //     `P2PEndpoint.Id`, i.e. address, port and whether TLS is in use, while dialing a TLS endpoint also needs
  //     its `endpointConfig.tlsConfig` (custom trust collection and client certificate), which the ID doesn't
  //     carry and which the runtime connection state, keyed by endpoint ID, doesn't retain either. Rebuilding an
  //     endpoint from its ID alone would thus silently drop the operator-provided TLS client material.
  //
  //  2. It mirrors the authoritative set of endpoints this node's operator wants outgoing connectivity to,
  //     and disconnections are also notified for endpoints that are not in it, so it must be consulted to avoid:
  //
  //     - Resurrecting an endpoint the operator just removed.
  //     - Dialing a node that merely connected to us: incoming connections advertise their endpoint (used for
  //       connection deduplication) and their teardown notifies a disconnection for it, but this node was never
  //       configured to connect to such a peer, so it must not start doing so.
  //
  //  The cache, rather than the P2P endpoints store, is used to make this decision synchronous, so that
  //  it cannot race with a concurrent endpoint removal by an operator: reading the store would instead complete
  //  asynchronously, so a removal processed in between could be undone by a reconnection based on a stale
  //  snapshot of the store.
  //
  //  The cache is safe to keep, because this module is the only writer that can change the endpoint **set**
  //  after the bootstrap performed by `BftBlockOrderer`: the only other writer, `P2PGrpcConnectionManager`,
  //  calls `associate` upon successful authentication, which only ever sets the node ID of an already-present
  //  entry and can neither insert nor remove one, nor alter its TLS settings.
  //
  //  In addition, the node ID possibly stored alongside the endpoint is deliberately ignored, i.e. the
  //  reconnection is performed by endpoint only: the peer must authenticate again
  //  before it is addressable by node ID, so re-asserting a stored association here would bind an endpoint to a
  //  node ID that this node hasn't (re-)verified.
  //
  //  This is idempotent and cheap when connectivity is already ensured, because `ensureSendingEnabledTo`
  //  doesn't touch the state when a network ref already exists for the endpoint (or for the BFT node ID
  //  it is associated to).
  private def reconnectIfStillConfigured(
      p2pEndpointId: P2PEndpoint.Id
  )(implicit context: E#ActorContextT[P2PNetworkOut.Message], traceContext: TraceContext): Unit =
    configuredP2PEndpoints.get(p2pEndpointId) match {
      case Some(p2pEndpoint) =>
        logger.info(
          s"Disconnected P2P endpoint $p2pEndpointId is still configured on this node, " +
            "ensuring an outgoing connection to it"
        )
        ensureSendingEnabledTo(P2PAddress.Endpoint(p2pEndpoint))
      case None =>
        logger.info(
          s"Disconnected P2P endpoint $p2pEndpointId is not configured on this node, " +
            "so not reconnecting to it"
        )
    }

  private def disconnect(
      p2pEndpointId: P2PEndpoint.Id
  )(implicit context: E#ActorContextT[P2PNetworkOut.Message], traceContext: TraceContext): Unit = {
    logger.info(
      s"Disconnecting P2P endpoint $p2pEndpointId " +
        s"('${p2pConnectionState.getBftNodeId(p2pEndpointId).getOrElse("<unknown node ID>")}')"
    )
    p2pNetworkManager.shutdownOutgoingConnection(p2pEndpointId)
    connectedP2PEndpointIds.remove(p2pEndpointId).discard
    emitConnectionStateMetricsAndLogEndpointsStatus(getPeerNetworkStatus(), notifyMempool = true)
  }

  private def emitConnectionStateMetricsAndLogEndpointsStatus(
      peerNetworkStatus: PeerNetworkStatus,
      notifyMempool: Boolean,
  )(implicit
      mc: MetricsContext,
      traceContext: TraceContext,
  ): Unit = {
    if (notifyMempool)
      sendConnectivityUpdateToMempool(peerNetworkStatus)
    val authenticatedCount = getAuthenticatedNodeIds(peerNetworkStatus).size
    val connectedCount = getConnectedPeersCount(peerNetworkStatus)
    emitConnectedCount(metrics, connectedCount)
    emitAuthenticatedCount(metrics, authenticatedCount)
    metrics.p2p.update(peerNetworkStatus)
    logger.info(s"New P2P network status: $peerNetworkStatus")
    logQuorumInfo(peerNetworkStatus)
  }

  private def getConnectedPeersCount(peerNetworkStatus: PeerNetworkStatus) =
    peerNetworkStatus.endpointStatuses.count {
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

  private def getAuthenticatedNodeIds(peerNetworkStatus: PeerNetworkStatus): Seq[BftNodeId] =
    peerNetworkStatus.endpointStatuses
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

  private def getAuthenticatedCountIncludingSelf(peerNetworkStatus: PeerNetworkStatus): Int =
    getAuthenticatedNodeIds(peerNetworkStatus).size + 1

  private def sendConnectivityUpdateToMempool(peerNetworkStatus: PeerNetworkStatus)(implicit
      traceContext: TraceContext
  ): Unit =
    dependencies.mempool.asyncSend(
      Mempool.P2PConnectivityUpdate(
        membership,
        getAuthenticatedCountIncludingSelf(peerNetworkStatus),
      )
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
    var logWhenQuorumOK = false

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

    // Module-local cache of the P2P endpoints configured by this node's operator, i.e. a mirror of the
    //  P2P endpoints store (minus the associated node IDs, which this module doesn't write).
    //
    //  It exists so that the decision of whether a disconnected endpoint must be reconnected to can be taken
    //  synchronously on the module's thread, avoiding races with concurrent endpoint removals by an operator.
    //
    //  It must thus only be read and written on the module's thread, i.e. from `receiveInternal`.
    val configuredP2PEndpoints: mutable.Map[P2PEndpoint.Id, P2PEndpoint] =
      mutable.Map.empty
  }
}
