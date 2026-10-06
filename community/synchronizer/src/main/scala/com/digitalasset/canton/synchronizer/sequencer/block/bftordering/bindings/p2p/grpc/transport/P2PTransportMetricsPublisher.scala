// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.transport

import com.daml.metrics.api.MetricHandle.Meter
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.transport.P2PGrpcTransportStatsReader.TransportSnapshot
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.time.{
  Clock,
  NonNegativeFiniteDuration,
  PeriodicAction,
  PositiveFiniteDuration,
}
import io.grpc.ManagedChannel

import java.net.SocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

/** Periodically samples HTTP/2- and TCP-level transport statistics for the registered P2P gRPC
  * connections and publishes them as peer-labeled metrics (see
  * [[BftOrderingMetrics.P2PMetrics.TransportMetrics]]).
  *
  * Both directions are covered:
  *   - outgoing connections are sampled from the client `ManagedChannel`'s Channelz stats,
  *     including TCP wire bytes from the Netty channel customizer (see
  *     [[P2PNettyChannelInstrumentation]]);
  *   - incoming connections are sampled from the server socket matching the peer's remote address
  *     (see [[P2PGrpcTransportStatsReader.indexServerSocketsByRemote]]); wire bytes are not
  *     available server-side.
  *
  * To keep sampling cheap enough to run continuously, per-tick work is kept proportional to the
  * number of P2P connections: the client channel's Channelz log id is resolved once and cached, and
  * the server sockets are indexed once per tick and scanned only for the gRPC server(s) known to
  * carry P2P connections (rather than re-scanning every gRPC socket in the process for every peer).
  *
  * On each tick, for every registered peer, it:
  *   - updates the per-peer gauges (active streams, flow control windows, and, when the epoll
  *     transport is in use, TCP RTT, window scales and current retransmitted segments) to their
  *     latest sampled value;
  *   - marks the meters (streams opened/closed, gRPC messages sent/received, keepalive pings, flow
  *     control stalls, TCP wire bytes) with the delta since the previous sample, since the
  *     underlying Channelz counters are monotonic per connection; the first sample of a connection
  *     is counted as a delta from zero, so counts accumulated before registration (e.g. the
  *     already-open long-lived P2P stream and its initial messages) are not discarded.
  *
  * Peers that are no longer registered have their per-peer gauges cleaned up.
  *
  * Note on coverage: gRPC message counts are not raw HTTP/2 DATA-frame counts (a single message may
  * span several DATA frames), and HTTP/2 ping round-trip time and actual per-HTTP/2-frame-type
  * counts are not exposed by Channelz; obtaining them would require parsing HTTP/2 frames in the
  * Netty pipeline, which is only feasible on plaintext connections and is not attempted here.
  */
private[bftordering] final class P2PTransportMetricsPublisher(
    pollInterval: PositiveFiniteDuration,
    instrumentation: Option[P2PNettyChannelInstrumentation],
    metrics: BftOrderingMetrics,
    clock: Clock,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, metricsContext: MetricsContext)
    extends NamedLogging
    with FlagCloseable {

  import P2PTransportMetricsPublisher.*

  private val transport = metrics.p2p.transport

  // Outgoing connections: sampled from the client ManagedChannel's Channelz stats (+ wire bytes).
  private val clientRegistry = new ConcurrentHashMap[BftNodeId, ClientPeerState]()
  // Incoming connections: sampled from the server socket matching the peer's remote address.
  //  Server sockets have no wire-byte counters (no per-child pipeline hook on the shaded server).
  private val serverRegistry = new ConcurrentHashMap[BftNodeId, ServerPeerState]()

  // The gRPC server(s) observed to carry P2P connections, so server-socket scans can be scoped to
  //  them instead of every gRPC server in the process. Refreshed on each tick; falls back to a full
  //  scan when empty or when a registered peer is not found among the cached servers.
  private val cachedServerLogIds = new AtomicReference[Set[Long]](Set.empty)

  private val periodicAction =
    new PeriodicAction(
      clock,
      // A positive interval is required to avoid an immediate rescheduling loop that can consume a CPU core
      NonNegativeFiniteDuration.tryCreate(pollInterval.duration),
      loggerFactory,
      timeouts,
      "p2p-transport-metrics-sampling",
    )(_ => FutureUnlessShutdown.outcomeF(Future(sample())))

  /** Registers a P2P client channel (outgoing connection) for a peer to be sampled.
    *
    * The registration is tied to the specific winning connection via its channel. Only the winning
    * sender (the one that becomes the active connection) is registered, and a subsequent teardown
    * unregisters the entry only if it still belongs to that same channel (see
    * [[unregisterChannel]]). This prevents a losing duplicate connection to the same peer from
    * removing the active connection's entry.
    */
  def register(peer: BftNodeId, channel: ManagedChannel): Unit =
    clientRegistry.put(peer, new ClientPeerState(channel)).discard

  /** Stops sampling whichever peer is served by the given channel. Used when a channel is shut down
    * but only the channel (not the peer node ID) is known.
    *
    * The removal is guarded by channel identity, so tearing down a losing duplicate connection does
    * not remove the active connection's entry (which is registered under a different channel).
    */
  def unregisterChannel(channel: ManagedChannel): Unit =
    clientRegistry.asScala
      .collectFirst {
        case (peer, state) if state.channel eq channel => peer -> state
      }
      .foreach { case (peer, state) =>
        // Take a final sample before removal, otherwise every counter increment since the last poll
        //  (e.g. the closing of the single long-lived P2P stream and trailing messages/bytes) is lost.
        //  This is called before channel shutdown, so the channel's Channelz stats are still readable.
        sampleClient(peer, state)
        // Value-guarded removal so a losing duplicate's teardown does not drop the active entry.
        clientRegistry.remove(peer, state).discard
      }

  /** Registers an incoming connection for a peer, identified by its remote (peer) socket address,
    * so that the matching server socket's transport metrics get sampled.
    *
    * The registration is tied to the specific winning connection via an opaque `connection` token.
    * Only the winning sender (the one that becomes the active connection) is registered, and a
    * subsequent teardown unregisters the entry only if it still belongs to that same connection
    * (see [[unregisterServerPeer]]). This prevents a losing duplicate connection to the same peer
    * from removing the active connection's entry.
    */
  def registerServerPeer(peer: BftNodeId, remoteAddress: SocketAddress, connection: AnyRef): Unit =
    serverRegistry.put(peer, new ServerPeerState(remoteAddress, connection)).discard

  /** Stops sampling the incoming connection from a peer, but only if the currently registered entry
    * still belongs to the given `connection`. Tearing down a losing duplicate therefore does not
    * remove the active connection's entry.
    */
  def unregisterServerPeer(peer: BftNodeId, connection: AnyRef): Unit =
    Option(serverRegistry.get(peer))
      .filter(_.connection eq connection)
      .foreach { state =>
        // Take a final sample before removal, otherwise every counter increment since the last poll
        //  (e.g. the closing of the single long-lived P2P stream and trailing messages) is lost.
        //  This is called before connection shutdown, so the server socket's stats are still
        //  readable; a full socket index is built since the per-tick cache may not be populated.
        val index = P2PGrpcTransportStatsReader.indexServerSocketsByRemote(
          P2PGrpcTransportStatsReader.serverLogIds()
        )
        sampleServer(peer, state, index)
        // Value-guarded removal so a losing duplicate's teardown does not drop the active entry.
        serverRegistry.remove(peer, state).discard
      }

  override protected def onClosed(): Unit = periodicAction.close()

  /** Closes and unregisters all dynamically-created per-peer transport gauges.
    *
    * This must be called during shutdown *after* all channels have been torn down (and their final
    * samples taken), because those final samples re-create gauges. Clearing them last ensures no
    * stale gauge values remain exported after the orderer stops, and that they cannot conflict with
    * gauges created by a fresh publisher after an in-process restart. It is idempotent.
    */
  def clearGauges(): Unit = transport.cleanupTransportGauges(Set.empty)

  private def sample(): Unit = {
    sampleClients()
    sampleServers()
    // Drop per-peer gauges for (peer, direction) pairs that are no longer registered on either side
    transport.cleanupTransportGauges(
      clientRegistry.asScala.keys.map(_ -> transport.labels.direction.Outgoing).toSet ++
        serverRegistry.asScala.keys.map(_ -> transport.labels.direction.Incoming).toSet
    )
    // Reap the wire-byte counters of connections that have gone inactive, now that this tick has
    //  consumed their final delta. Retaining them until here (rather than removing them in the
    //  Netty `channelInactive`) ensures the final sample around a remote disconnect is not lost;
    //  this also reaps counters of connections the publisher never sampled (e.g. losing duplicates).
    instrumentation.foreach(_.reapInactiveWireBytes())
  }

  private def sampleClients(): Unit =
    clientRegistry.asScala.foreach { case (peer, state) => sampleClient(peer, state) }

  private def sampleClient(peer: BftNodeId, state: ClientPeerState): Unit =
    channelLogIdOf(state)
      .flatMap(P2PGrpcTransportStatsReader.readByChannelLogId)
      .foreach { snapshot =>
        val peerContext = metricsContext.withExtraLabels(
          transport.labels.Peer -> peer,
          transport.labels.Direction -> transport.labels.direction.Outgoing,
        )
        val previousSnapshotO = state.previousSnapshot.getAndSet(Some(snapshot))
        emitCommon(
          peer,
          transport.labels.direction.Outgoing,
          snapshot,
          previousSnapshotO,
          peerContext,
        )
        emitWireBytes(peer, state, snapshot)
      }

  /** Resolves and caches the Channelz log id of a client channel. The log id is stable for the
    * channel's life, so this avoids re-unwrapping forwarding channel wrappers on every tick.
    */
  private def channelLogIdOf(state: ClientPeerState): Option[Long] =
    state.channelLogId.get() match {
      case some @ Some(_) => some
      case None =>
        val resolved = P2PGrpcTransportStatsReader.resolveChannelLogId(state.channel)
        resolved.foreach(id => state.channelLogId.set(Some(id)))
        resolved
    }

  private def sampleServers(): Unit = {
    val serverStates = serverRegistry.asScala.toMap
    if (serverStates.nonEmpty) {
      // Scope the (relatively expensive) server-socket scan to the servers known to carry P2P
      //  connections, falling back to scanning all gRPC servers in the process only when the cache
      //  is empty or a registered peer is not found among the cached servers (e.g. at startup, on a
      //  new connection to a not-yet-cached server, or after a server restart).
      val cached = cachedServerLogIds.get()
      val scopedServerLogIds: Iterable[Long] =
        if (cached.nonEmpty) cached else P2PGrpcTransportStatsReader.serverLogIds()
      val scopedIndex =
        P2PGrpcTransportStatsReader.indexServerSocketsByRemote(scopedServerLogIds)
      val index =
        if (serverStates.valuesIterator.forall(state => scopedIndex.contains(state.remoteAddress)))
          scopedIndex
        else
          P2PGrpcTransportStatsReader.indexServerSocketsByRemote(
            P2PGrpcTransportStatsReader.serverLogIds()
          )
      // Refresh the cache to exactly the servers that serve a currently-registered P2P peer
      cachedServerLogIds.set(
        serverStates.valuesIterator
          .flatMap(state => index.get(state.remoteAddress).map(_.serverLogId))
          .toSet
      )
      serverStates.foreach { case (peer, state) => sampleServer(peer, state, index) }
    }
  }

  private def sampleServer(
      peer: BftNodeId,
      state: ServerPeerState,
      index: Map[SocketAddress, P2PGrpcTransportStatsReader.ServerSocketSnapshot],
  ): Unit =
    index.get(state.remoteAddress).map(_.snapshot).foreach { snapshot =>
      val peerContext = metricsContext.withExtraLabels(
        transport.labels.Peer -> peer,
        transport.labels.Direction -> transport.labels.direction.Incoming,
      )
      val previousSnapshotO = state.previousSnapshot.getAndSet(Some(snapshot))
      emitCommon(
        peer,
        transport.labels.direction.Incoming,
        snapshot,
        previousSnapshotO,
        peerContext,
      )
    }

  /** Emits the HTTP/2- and TCP-level metrics common to outgoing and incoming connections. Wire-byte
    * metrics are emitted separately (client-side only) by [[emitWireBytes]].
    */
  private def emitCommon(
      peer: BftNodeId,
      direction: String,
      snapshot: TransportSnapshot,
      previousSnapshotO: Option[TransportSnapshot],
      peerContext: MetricsContext,
  ): Unit = {

    // Latest-value gauges
    transport
      .http2ActiveStreams(peer, direction)
      .updateValue(snapshot.activeStreams.toInt)(peerContext)
    transport
      .http2LocalWindowBytes(peer, direction)
      .updateValue(snapshot.localFlowControlWindow)(peerContext)
    transport
      .http2RemoteWindowBytes(peer, direction)
      .updateValue(snapshot.remoteFlowControlWindow)(peerContext)
    // Mark on every sample (0 when the flow control windows are healthy) so the meter's time series
    //  always exists, rather than only appearing once a stall is first observed.
    val stalled = snapshot.localFlowControlWindow <= 0 || snapshot.remoteFlowControlWindow <= 0
    transport.http2FlowControlStalls.mark(if (stalled) 1L else 0L)(peerContext)

    // Delta meters: Channelz counters are monotonic per connection, so the delta since the previous
    //  sample is marked. The first sample has no previous snapshot; it is treated as a delta from
    //  zero (rather than merely a discarded baseline) so that counts accumulated before registration
    //  are not lost. This matters because registration happens after the long-lived P2P stream has
    //  already opened, so its stream open and initial messages would otherwise never be recorded.
    markDelta(transport.http2StreamsOpened, snapshot, previousSnapshotO)(_.streamsStarted)(
      peerContext
    )
    markDelta(transport.http2StreamsClosed, snapshot, previousSnapshotO)(_.streamsClosed)(
      peerContext
    )
    markDelta(transport.grpcMessagesSent, snapshot, previousSnapshotO)(_.messagesSent)(
      peerContext
    )
    markDelta(transport.grpcMessagesReceived, snapshot, previousSnapshotO)(_.messagesReceived)(
      peerContext
    )
    // Keepalive pings only fire after read-inactivity and not without active calls, so under steady
    //  traffic the delta is usually 0. Mark every sample (0 included) so the meter's time series
    //  always exists rather than only appearing once a ping is first sent.
    markDeltaAlways(transport.http2KeepAlivePings, snapshot, previousSnapshotO)(_.keepAlivesSent)(
      peerContext
    )

    // TCP-level metrics (only present when the epoll transport is in use)
    snapshot.tcp.foreach { tcp =>
      transport.tcpRttMicros(peer, direction).updateValue(tcp.rttMicros)(peerContext)
      transport.tcpSendWindowScale(peer, direction).updateValue(tcp.sndWindowScale)(peerContext)
      transport
        .tcpReceiveWindowScale(peer, direction)
        .updateValue(tcp.rcvWindowScale)(peerContext)
      // `tcpi_retrans` is the current number of outstanding retransmitted segments, not a monotonic
      //  total, so it is published as a gauge (its latest sampled value) rather than differenced
      //  into a rate.
      transport
        .tcpRetransmittedSegments(peer, direction)
        .updateValue(tcp.currentRetransmittedSegments)(peerContext)
    }
  }

  /** Emits TCP wire-level byte metrics for an outgoing connection, correlated with the socket's
    * local address via the Netty channel customizer. Wire-byte counting only works on the client
    * (dialer/outgoing) side, so these meters are labeled by peer only: a direction label would
    * always be "outgoing" and carry no information.
    */
  private def emitWireBytes(
      peer: BftNodeId,
      state: ClientPeerState,
      snapshot: TransportSnapshot,
  ): Unit =
    instrumentation.foreach { instr =>
      val wireByteContext = metricsContext.withExtraLabels(transport.labels.Peer -> peer)
      val wireBytes =
        snapshot.socketEndpoints.flatMap(instr.wireBytesFor)
      val sentTotal = wireBytes.map(_.sent.get()).sum
      val receivedTotal = wireBytes.map(_.received.get()).sum
      // The wire-byte totals are cumulative since the channel was created, so the first sample is
      //  treated as a delta from zero to avoid discarding bytes transferred before the first poll.
      val previousSentO = state.previousWireSent.getAndSet(Some(sentTotal))
      markDelta(transport.tcpWireBytesSent, sentTotal, previousSentO)(identity)(wireByteContext)
      val previousReceivedO = state.previousWireReceived.getAndSet(Some(receivedTotal))
      markDelta(transport.tcpWireBytesReceived, receivedTotal, previousReceivedO)(identity)(
        wireByteContext
      )
    }

  /** Marks the meter with the positive delta of `project` between `current` and `previousO`. A
    * missing previous sample counts as zero, so the first sample emits the full cumulative value.
    * Non-positive deltas (e.g. after a counter reset) are not marked.
    */
  // The implicit parameter is named `metricsContext` so it shadows the class-level implicit of the
  //  same name, keeping a single unambiguous implicit in scope for `meter.mark`.
  private def markDelta[A](meter: Meter, current: A, previousO: Option[A])(project: A => Long)(
      implicit metricsContext: MetricsContext
  ): Unit = {
    val delta = deltaOf(current, previousO)(project)
    if (delta > 0L) meter.mark(delta)
  }

  /** Like [[markDelta]] but always emits (marking 0 when there is no positive delta), so the
    * meter's time series exists from the first sample rather than only appearing once a nonzero
    * delta occurs.
    */
  private def markDeltaAlways[A](meter: Meter, current: A, previousO: Option[A])(
      project: A => Long
  )(implicit metricsContext: MetricsContext): Unit =
    meter.mark(math.max(0L, deltaOf(current, previousO)(project)))

  private def deltaOf[A](current: A, previousO: Option[A])(project: A => Long): Long =
    project(current) - previousO.fold(0L)(project)
}

private[bftordering] object P2PTransportMetricsPublisher {

  private final class ClientPeerState(val channel: ManagedChannel) {
    // Resolved lazily and cached: the Channelz log id is stable for the channel's life.
    val channelLogId: AtomicReference[Option[Long]] = new AtomicReference(None)
    val previousSnapshot: AtomicReference[Option[TransportSnapshot]] =
      new AtomicReference(None)
    val previousWireSent: AtomicReference[Option[Long]] = new AtomicReference(None)
    val previousWireReceived: AtomicReference[Option[Long]] = new AtomicReference(None)
  }

  private final class ServerPeerState(val remoteAddress: SocketAddress, val connection: AnyRef) {
    val previousSnapshot: AtomicReference[Option[TransportSnapshot]] =
      new AtomicReference(None)
  }
}
