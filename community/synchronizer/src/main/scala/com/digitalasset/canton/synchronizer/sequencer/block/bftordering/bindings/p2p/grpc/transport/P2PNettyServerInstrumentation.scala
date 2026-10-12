// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.transport

import com.digitalasset.canton.logging.NamedLoggerFactory
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollServerSocketChannel

/** Optionally switches the P2P gRPC server to the Netty epoll transport so that TCP-level `TcpInfo`
  * (round-trip time, retransmissions, window scales) for incoming P2P connections becomes
  * observable through gRPC Channelz (see [[P2PGrpcTransportStatsReader]]).
  *
  * Unlike the client-side instrumentation ([[P2PNettyChannelInstrumentation]]), no wire-byte
  * counting handler is installed: the shaded `NettyServerBuilder` exposes no per-child-channel
  * pipeline hook, so server-side wire-level byte counts (and per-HTTP/2-frame-type granularity) are
  * not available. HTTP/2-level counters and, when epoll is in use, TCP-level `TcpInfo` are still
  * obtained via Channelz.
  *
  * When epoll is unavailable or not preferred, [[customize]] is a no-op and the server keeps the
  * default NIO transport, in which case TCP-level metrics are simply not reported for incoming
  * connections. The shared epoll event loop group lifecycle is managed by
  * [[P2PNettyEpollInstrumentation]].
  */
private[bftordering] final class P2PNettyServerInstrumentation(
    preferEpoll: Boolean,
    loggerFactory: NamedLoggerFactory,
) extends P2PNettyEpollInstrumentation(
      preferEpoll,
      threadNamePrefix = "bft-p2p-server-epoll",
      subject = "the P2P server",
      metricsQualifier = " for incoming connections",
      loggerFactory,
    ) {

  /** Applies the epoll transport to the given server builder when available, else leaves it
    * unchanged.
    */
  def customize(builder: NettyServerBuilder): NettyServerBuilder =
    eventLoopGroupO.fold(builder)(group =>
      builder
        .channelType(classOf[EpollServerSocketChannel])
        .bossEventLoopGroup(group)
        .workerEventLoopGroup(group)
    )
}
