// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.transport

import io.grpc.{InternalChannelz, InternalWithLogId, ManagedChannel}

import java.lang.reflect.Field
import java.net.SocketAddress
import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** Reads HTTP/2- and TCP-level transport statistics for P2P gRPC connections via gRPC Channelz
  * ([[io.grpc.InternalChannelz]]), for both directions:
  *   - outgoing connections, i.e. the sockets of a gRPC client [[ManagedChannel]] (see
  *     [[resolveChannelLogId]] and [[readByChannelLogId]]);
  *   - incoming connections, i.e. the sockets accepted by the gRPC servers in this process, indexed
  *     by the peer's remote address (see [[serverLogIds]] and [[indexServerSocketsByRemote]]).
  *
  * The HTTP/2-level counters (streams, messages, keepalive pings, flow control windows) are always
  * available. The TCP-level information ([[TcpSnapshot]]) is only populated when the underlying
  * Netty transport reports a `TcpInfo`, which in practice requires the Netty epoll transport (see
  * [[P2PNettyChannelInstrumentation]] for client channels and [[P2PNettyServerInstrumentation]] for
  * the server); otherwise it is [[scala.None]].
  *
  * All access to gRPC internals is best-effort and defensive: any failure (e.g. because the channel
  * has been shut down, or the internal shape changes across gRPC versions) results in
  * [[scala.None]] rather than an exception, so that transport metrics degrade gracefully.
  */
private[bftordering] object P2PGrpcTransportStatsReader {

  /** Identifies a single TCP connection by its full local/remote socket-address pair.
    *
    * A local socket address alone does not uniquely identify a connection: Linux may assign the
    * same local IP and port to simultaneous connections with different remote endpoints. This
    * full-tuple identity is shared between the Netty wire-byte counters
    * ([[P2PNettyChannelInstrumentation]]) and the Channelz snapshot below, so the two can be
    * correlated unambiguously.
    */
  final case class ConnectionEndpoints(local: SocketAddress, remote: SocketAddress)

  /** TCP-level information sampled from the epoll `TcpInfo` exposed through Channelz. */
  final case class TcpSnapshot(
      rttMicros: Long,
      sndWindowScale: Int,
      rcvWindowScale: Int,
      // Current number of outstanding (unacknowledged) retransmitted segments, i.e. Linux
      //  `tcpi_retrans`. This is an instantaneous value, NOT a cumulative lifetime total: gRPC
      //  Channelz does not expose `tcpi_total_retrans`, so this must be published as a gauge rather
      //  than differenced into a rate (differencing would double-count on retransmission recovery
      //  followed by a new retransmission).
      currentRetransmittedSegments: Long,
  )

  /** A point-in-time snapshot of a connection's transport counters. Counters are monotonic since
    * the connection was established, so rates are derived by the publisher as deltas between
    * snapshots.
    */
  final case class TransportSnapshot(
      streamsStarted: Long,
      streamsSucceeded: Long,
      streamsFailed: Long,
      messagesSent: Long,
      messagesReceived: Long,
      keepAlivesSent: Long,
      localFlowControlWindow: Long,
      remoteFlowControlWindow: Long,
      tcp: Option[TcpSnapshot],
      // The full local/remote endpoints of each underlying socket, used to correlate Channelz
      //  counters with the Netty wire-byte counters keyed by the same connection identity.
      socketEndpoints: Seq[ConnectionEndpoints],
  ) {
    def activeStreams: Long = math.max(0L, streamsStarted - streamsSucceeded - streamsFailed)
    def streamsClosed: Long = streamsSucceeded + streamsFailed
  }

  /** A server socket's transport snapshot together with the Channelz log id of the owning gRPC
    * server, so callers can cache which servers actually carry P2P connections.
    */
  final case class ServerSocketSnapshot(serverLogId: Long, snapshot: TransportSnapshot)

  /** Resolves the Channelz log id of the given client channel, unwrapping forwarding wrappers. The
    * log id is stable for the life of the channel, so callers should resolve it once and cache it
    * rather than unwrapping on every sample.
    */
  def resolveChannelLogId(channel: ManagedChannel): Option[Long] =
    Try(logIdOf(channel)).toOption.flatten

  /** Best-effort read of the transport snapshot for a client channel identified by its Channelz log
    * id. Returns [[scala.None]] if the channel is not (or no longer) instrumented, or has no active
    * socket yet.
    */
  def readByChannelLogId(channelLogId: Long): Option[TransportSnapshot] =
    Try(unsafeReadByChannelLogId(channelLogId)).toOption.flatten

  /** Returns the Channelz log ids of all gRPC servers currently registered in this process. */
  def serverLogIds(): Seq[Long] =
    Try {
      val channelz = InternalChannelz.instance()
      accumulatePaginatedChannelz { fromId =>
        val page = channelz.getServers(fromId, ChannelzPageSize)
        (page.servers, page.end)
      }.map(_.getLogId.getId)
    }.toOption.getOrElse(Seq.empty)

  // Channelz's paged getters preallocate `new ArrayList<>(maxPageSize)`, so requesting a huge page
  //  (e.g. Int.MaxValue) tries to allocate a ~2-billion-element array and crashes the JVM with
  //  "Requested array size exceeds VM limit" (observed with many in-JVM orderers). We therefore page
  //  through the results with a bounded page size instead of asking for everything at once.
  private val ChannelzPageSize = 1000

  /** Accumulates all entries of a paged Channelz getter. `fetch` returns a single page (the Java
    * list of entries plus Channelz's `end` flag) starting at the given inclusive log id. Paging
    * continues from one past the largest id seen until Channelz reports the last page (or returns
    * an empty page, as a defensive guard against non-termination).
    */
  private def accumulatePaginatedChannelz[A <: InternalWithLogId](
      fetchPageFromLogIdInclusive: Long => (java.util.List[A], Boolean)
  ): List[A] = {
    val acc = List.newBuilder[A]
    @tailrec
    def loop(fromId: Long): Unit = {
      val (page, end) = fetchPageFromLogIdInclusive(fromId)
      val maxId = page.asScala.foldLeft(fromId) { (currentMax, entry) =>
        acc += entry
        currentMax max entry.getLogId.getId
      }
      if (!end && !page.isEmpty) loop(maxId + 1L)
    }
    loop(0L)
    acc.result()
  }

  /** Builds an index from a peer's remote socket address to the transport snapshot of the server
    * socket serving it, scanning only the given servers. Building the index once per sampling tick
    * (instead of re-scanning every server socket, and issuing a `getsockopt` per socket, for every
    * peer) keeps the cost proportional to the number of P2P server sockets rather than to peers
    * times all gRPC sockets in the process. Each entry carries the owning server's log id so
    * callers can cache which servers actually carry P2P connections and scope subsequent scans to
    * them.
    */
  def indexServerSocketsByRemote(
      serverLogIds: Iterable[Long]
  ): Map[SocketAddress, ServerSocketSnapshot] =
    Try(unsafeIndexServerSocketsByRemote(serverLogIds)).toOption.getOrElse(Map.empty)

  private def unsafeReadByChannelLogId(channelLogId: Long): Option[TransportSnapshot] = {
    val channelz = InternalChannelz.instance()
    Option(channelz.getChannel(channelLogId)).flatMap { instrumentedChannel =>
      val channelStats = instrumentedChannel.getStats.get()
      // Client channels keep their transport sockets under subchannels; also consider any
      //  sockets attached directly to the channel for robustness across transports.
      val directSocketIds = channelStats.sockets.asScala.iterator.map(_.getLogId.getId)
      val subchannelSocketIds =
        channelStats.subchannels.asScala.iterator.flatMap { subchannel =>
          Option(channelz.getSubchannel(subchannel.getLogId.getId)).iterator.flatMap {
            instrumentedSubchannel =>
              instrumentedSubchannel.getStats.get().sockets.asScala.iterator.map(_.getLogId.getId)
          }
        }
      val socketStats =
        (directSocketIds ++ subchannelSocketIds).distinct.flatMap { socketId =>
          Option(channelz.getSocket(socketId)).flatMap(instrumented =>
            Option(instrumented.getStats.get())
          )
        }.toList
      socketStats match {
        case Nil => None
        case stats => Some(aggregate(stats))
      }
    }
  }

  private def unsafeIndexServerSocketsByRemote(
      serverLogIds: Iterable[Long]
  ): Map[SocketAddress, ServerSocketSnapshot] = {
    val channelz = InternalChannelz.instance()
    val index = mutable.Map.empty[SocketAddress, ServerSocketSnapshot]
    serverLogIds.foreach { serverLogId =>
      serverSocketStats(channelz, serverLogId).foreach { stats =>
        Option(stats.remote).foreach { remote =>
          index.getOrElseUpdate(remote, ServerSocketSnapshot(serverLogId, aggregate(List(stats))))
        }
      }
    }
    index.toMap
  }

  private def serverSocketStats(
      channelz: InternalChannelz,
      serverLogId: Long,
  ): List[InternalChannelz.SocketStats] =
    accumulatePaginatedChannelz { fromId =>
      val page = channelz.getServerSockets(serverLogId, fromId, ChannelzPageSize)
      (page.sockets, page.end)
    }.flatMap { withLogId =>
      Option(channelz.getSocket(withLogId.getLogId.getId)).flatMap(instrumented =>
        Option(instrumented.getStats.get())
      )
    }

  private def aggregate(socketStats: List[InternalChannelz.SocketStats]): TransportSnapshot = {
    def sum(project: InternalChannelz.TransportStats => Long): Long =
      socketStats.iterator.map(stats => Option(stats.data).fold(0L)(project)).sum

    val tcp =
      socketStats.iterator
        .flatMap(stats => Option(stats.socketOptions).flatMap(options => Option(options.tcpInfo)))
        .toList match {
        case Nil => None
        case tcpInfos @ (tcpInfo :: _) =>
          // Typically there is a single socket per P2P streaming connection; if there were more,
          //  we report the first socket's window scales and RTT and sum the current retransmitted
          //  segments.
          Some(
            TcpSnapshot(
              rttMicros = tcpInfo.rtt.toLong,
              sndWindowScale = tcpInfo.sndWscale,
              rcvWindowScale = tcpInfo.rcvWscale,
              currentRetransmittedSegments = tcpInfos.iterator.map(_.retrans.toLong).sum,
            )
          )
      }

    TransportSnapshot(
      streamsStarted = sum(_.streamsStarted),
      streamsSucceeded = sum(_.streamsSucceeded),
      streamsFailed = sum(_.streamsFailed),
      messagesSent = sum(_.messagesSent),
      messagesReceived = sum(_.messagesReceived),
      keepAlivesSent = sum(_.keepAlivesSent),
      localFlowControlWindow = sum(_.localFlowControlWindow),
      remoteFlowControlWindow = sum(_.remoteFlowControlWindow),
      tcp = tcp,
      socketEndpoints = socketStats.flatMap { stats =>
        for {
          local <- Option(stats.local)
          remote <- Option(stats.remote)
        } yield ConnectionEndpoints(local, remote)
      },
    )
  }

  /** The channel returned by `NettyChannelBuilder.build()` is a forwarding wrapper that does not
    * itself implement [[io.grpc.InternalWithLogId]]. We therefore unwrap the `delegate` chain
    * (walking up the class hierarchy to find a [[ManagedChannel]]-typed field) until we reach the
    * Channelz-instrumented implementation.
    */
  private def logIdOf(channel: ManagedChannel): Option[Long] = {
    @tailrec
    def unwrap(current: ManagedChannel, depthLeft: Int): Option[InternalWithLogId] =
      current match {
        case withLogId: InternalWithLogId => Some(withLogId)
        case _ if depthLeft <= 0 => None
        case _ =>
          delegateOf(current) match {
            case Some(delegate) => unwrap(delegate, depthLeft - 1)
            case None => None
          }
      }
    unwrap(channel, depthLeft = MaxChannelUnwrapDepth).map(_.getLogId.getId)
  }

  // Safety bound on the delegate chain walk in `logIdOf`, guarding against cycles or unexpectedly
  //  deep wrapping. In practice `NettyChannelBuilder.build()` yields a single wrapper
  //  (`ManagedChannelOrphanWrapper` -> `ManagedChannelImpl`), so this leaves ample headroom.
  private val MaxChannelUnwrapDepth = 8

  /** Returns the [[ManagedChannel]] wrapped by the given forwarding channel, if any.
    *
    * gRPC's forwarding wrappers (e.g. `ForwardingManagedChannel`, which
    * `ManagedChannelOrphanWrapper` extends) keep the wrapped channel in a private field and don't
    * expose it publicly. We therefore find it by reflection: walk from the runtime class up to its
    * superclasses and take the first declared field whose type is a [[ManagedChannel]], then read
    * it.
    *
    * Returns [[scala.None]] if there is no such field, if reflection access fails (e.g. blocked by
    * the module system or a security manager) or if the field is `null`.
    */
  private def delegateOf(channel: ManagedChannel): Option[ManagedChannel] = {
    @tailrec
    def findDelegateField(clazz: Class[?]): Option[Field] =
      Option(clazz) match {
        case None => None
        case Some(cls) =>
          cls.getDeclaredFields.find(field =>
            classOf[ManagedChannel].isAssignableFrom(field.getType)
          ) match {
            case some @ Some(_) => some
            case None => findDelegateField(cls.getSuperclass)
          }
      }
    findDelegateField(channel.getClass).flatMap { field =>
      Try {
        field.setAccessible(true)
        field.get(channel)
      }.toOption.collect { case managedChannel: ManagedChannel => managedChannel }
    }
  }
}
