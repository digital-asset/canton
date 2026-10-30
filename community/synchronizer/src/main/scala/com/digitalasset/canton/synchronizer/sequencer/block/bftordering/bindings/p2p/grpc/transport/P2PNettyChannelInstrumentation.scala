// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.transport

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.transport.P2PGrpcTransportStatsReader.ConnectionEndpoints
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollSocketChannel
import io.grpc.netty.shaded.io.netty.channel.nio.NioIoHandler
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel
import io.grpc.netty.shaded.io.netty.channel.{
  Channel,
  ChannelDuplexHandler,
  ChannelFactory,
  ChannelHandlerContext,
  ChannelPromise,
  MultiThreadIoEventLoopGroup,
}
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import scala.util.control.NonFatal

/** Instruments P2P gRPC client channels at the Netty level for transport metrics.
  *
  * Two independent things are done through Netty's channel customizer ([[customize]]):
  *
  *   - A socket-head [[ChannelDuplexHandler]] is installed via a custom `channelFactory` to count
  *     raw TCP wire-level bytes (including TLS and framing overhead) sent and received. This works
  *     with both the NIO and the epoll transport. Counters are keyed by the connection's full
  *     local/remote socket-address tuple (a local address alone is not unique across simultaneous
  *     connections) so that the transport metrics publisher can correlate them with the socket
  *     reported by gRPC Channelz (see [[P2PGrpcTransportStatsReader]]).
  *   - When epoll is preferred and available, the epoll transport is selected (via the shared event
  *     loop group managed by [[P2PNettyEpollInstrumentation]]) so that TCP-level `TcpInfo`
  *     (round-trip time, retransmissions, window scales) becomes observable through Channelz.
  *     Otherwise, the default NIO transport is kept and TCP-level `TcpInfo` metrics are simply not
  *     reported (wire-byte counting still works).
  */
private[bftordering] final class P2PNettyChannelInstrumentation(
    preferEpoll: Boolean,
    loggerFactory: NamedLoggerFactory,
) extends P2PNettyEpollInstrumentation(
      preferEpoll,
      threadNamePrefix = "bft-p2p-epoll",
      subject = "P2P client channels",
      metricsQualifier = "",
      loggerFactory,
    ) {

  import P2PNettyChannelInstrumentation.*

  private val wireBytesByConnection = new ConcurrentHashMap[ConnectionEndpoints, WireBytes]()

  /** Applies the wire-byte-counting channel factory (and an event loop group matching the chosen
    * transport) to the given channel builder. gRPC's `NettyChannelBuilder` requires that a custom
    * channel type/factory always be paired with an explicit event loop group (setting only one
    * throws "Both EventLoopGroup and ChannelType should be provided or neither"), so a NIO group is
    * supplied for the NIO fallback just as the epoll group is for the epoll transport.
    */
  def customize(builder: NettyChannelBuilder): NettyChannelBuilder =
    eventLoopGroupO.fold(
      builder
        .eventLoopGroup(nioFallbackEventLoopGroup)
        .channelFactory(newChannelFactory(epoll = false))
    )(group => builder.eventLoopGroup(group).channelFactory(newChannelFactory(epoll = true)))

  // Event loop group for the NIO fallback (created only when the epoll transport is not in use). The
  //  custom wire-byte-counting channel factory requires a paired event loop group (see `customize`).
  private lazy val nioFallbackEventLoopGroup: MultiThreadIoEventLoopGroup =
    new MultiThreadIoEventLoopGroup(
      0,
      new DefaultThreadFactory("bft-p2p-nio"),
      NioIoHandler.newFactory(),
    )

  override def close(): Unit = {
    if (eventLoopGroupO.isEmpty)
      try nioFallbackEventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS).discard
      catch { case NonFatal(_) => () }
    super.close()
  }

  /** Returns the wire-byte counters for the connection with the given local/remote endpoints, if it
    * is still retained. Keying by the full endpoint tuple (rather than the local address alone)
    * avoids collisions between simultaneous connections that Linux assigned the same local address.
    *
    * Counters remain retrievable after the connection goes inactive (see the handler below) until
    * [[reapInactiveWireBytes]] is called, so the transport metrics publisher can still consume the
    * final wire-byte delta of a connection that was reset by the remote peer.
    */
  def wireBytesFor(endpoints: ConnectionEndpoints): Option[WireBytes] =
    Option(wireBytesByConnection.get(endpoints))

  /** Removes the counters of connections that have gone inactive, value-guarded by the retained
    * `inactive` flag so a later connection that reused the same endpoints (and is therefore active
    * again) is never dropped.
    *
    * Inactive counters are retained rather than removed in `channelInactive`, because on a remote
    * disconnect the Netty channel goes inactive before the downstream gRPC handler surfaces the
    * error that drives the publisher's final sample; removing them eagerly would drop up to a full
    * polling interval of traffic around the failure. The publisher calls this once per tick, after
    * sampling, so a consumed connection's counters live for at most one further interval.
    */
  def reapInactiveWireBytes(): Unit =
    wireBytesByConnection.values().removeIf(_.inactive).discard

  private def newChannelFactory(epoll: Boolean): ChannelFactory[Channel] =
    () => {
      val channel: Channel = if (epoll) new EpollSocketChannel() else new NioSocketChannel()
      channel
        .pipeline()
        .addFirst(WireByteCounterHandlerName, new WireByteCountingHandler(wireBytesByConnection))
        .discard
      channel
    }
}

private[bftordering] object P2PNettyChannelInstrumentation {

  private val WireByteCounterHandlerName = "bft-p2p-wire-byte-counter"

  /** Monotonic counters of raw TCP wire-level bytes for a single connection. */
  final class WireBytes {
    val sent = new AtomicLong(0L)
    val received = new AtomicLong(0L)
    // Set once the connection goes inactive. The counters are retained (not removed) at that point
    //  so the publisher can still consume the final delta; they are reaped by a later value-guarded
    //  sweep (see reapInactiveWireBytes).
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    @volatile var inactive: Boolean = false
  }

  /** A socket-head handler counting raw bytes written to and read from the wire. Installed closest
    * to the socket, so under TLS it observes encrypted bytes, which is the intended wire-level
    * view.
    *
    * The counters are registered under the connection's full local/remote endpoints (a local
    * address alone is not unique across simultaneous connections). On `channelInactive` they are
    * only flagged inactive (retained), not removed: on a remote disconnect the Netty channel goes
    * inactive before the downstream gRPC handler surfaces the error that triggers the publisher's
    * final sample, so removing them here would drop the last wire-byte delta. They are reaped later
    * by [[reapInactiveWireBytes]].
    */
  private final class WireByteCountingHandler(
      registry: ConcurrentHashMap[ConnectionEndpoints, WireBytes]
  ) extends ChannelDuplexHandler {

    private val counters = new WireBytes

    private def endpointsOf(ctx: ChannelHandlerContext): Option[ConnectionEndpoints] =
      for {
        local <- Option(ctx.channel().localAddress())
        remote <- Option(ctx.channel().remoteAddress())
      } yield ConnectionEndpoints(local, remote)

    override def channelActive(ctx: ChannelHandlerContext): Unit = {
      endpointsOf(ctx).foreach(registry.put(_, counters).discard)
      super.channelActive(ctx)
    }

    override def channelInactive(ctx: ChannelHandlerContext): Unit = {
      // Retain the counters (only flag them inactive) so the publisher's final sample can still find
      //  them; a later value-guarded sweep reaps them (see reapInactiveWireBytes).
      counters.inactive = true
      super.channelInactive(ctx)
    }

    override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
      msg match {
        case buf: ByteBuf => counters.received.addAndGet(buf.readableBytes().toLong).discard
        case _ => ()
      }
      super.channelRead(ctx, msg)
    }

    override def write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise): Unit = {
      msg match {
        case buf: ByteBuf => counters.sent.addAndGet(buf.readableBytes().toLong).discard
        case _ => ()
      }
      super.write(ctx, msg, promise)
    }
  }
}
