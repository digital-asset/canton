// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.transport

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import io.grpc.InternalChannelz
import io.grpc.netty.shaded.io.grpc.netty.InternalNettySocketSupport
import io.grpc.netty.shaded.io.grpc.netty.InternalNettySocketSupport.{
  InternalHelper,
  InternalNativeSocketOptions,
}
import io.grpc.netty.shaded.io.netty.channel.epoll.{Epoll, EpollIoHandler, EpollSocketChannel}
import io.grpc.netty.shaded.io.netty.channel.{Channel, EventLoopGroup, MultiThreadIoEventLoopGroup}
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal

/** Base class managing the optional shared Netty epoll [[EventLoopGroup]] that makes TCP-level
  * `TcpInfo` (round-trip time, retransmissions, window scales) observable through gRPC Channelz
  * (see [[P2PGrpcTransportStatsReader]]).
  *
  * When [[preferEpoll]] is set and epoll is available on the platform, a single epoll event loop
  * group is created (and must be [[close]]d on shutdown); subclasses wire it into their respective
  * Netty builder via `customize`. Otherwise, the default NIO transport is kept and TCP-level
  * metrics are simply not reported.
  *
  * The `subject` and `metricsQualifier` parameters only tune the log messages: `subject` names what
  * is being instrumented (e.g. `P2P client channels`) and `metricsQualifier` is an optional suffix
  * appended to "TCP-level transport metrics" (e.g. ` for incoming connections`).
  */
private[bftordering] abstract class P2PNettyEpollInstrumentation(
    preferEpoll: Boolean,
    threadNamePrefix: String,
    subject: String,
    metricsQualifier: String,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  import com.digitalasset.canton.tracing.TraceContext.Implicits.Empty.*

  protected val eventLoopGroupO: Option[EventLoopGroup] =
    if (preferEpoll && epollAvailable) {
      try {
        val group = new MultiThreadIoEventLoopGroup(
          0,
          new DefaultThreadFactory(threadNamePrefix),
          EpollIoHandler.newFactory(),
        )
        // gRPC's default socket-options helper returns no native options, so Channelz never carries
        //  a `TcpInfo` and TCP-level metrics stay empty even under epoll. gRPC ships no epoll mapper
        //  and installs none by default, so we install one (process-wide, once) that reads the epoll
        //  channel's `TcpInfo`; without it RTT, window scales and retransmitted segments are never
        //  reported.
        P2PNettyEpollInstrumentation.installNativeSocketOptionsHelper()
        logger.info(
          s"Using the Netty epoll transport for $subject; " +
            s"TCP-level transport metrics$metricsQualifier will be available"
        )
        Some(group)
      } catch {
        case NonFatal(e) =>
          logger.info(
            s"Failed to initialize the Netty epoll transport for $subject; falling back to NIO, " +
              s"so TCP-level (TcpInfo) transport metrics$metricsQualifier will be unavailable",
            e,
          )
          None
      }
    } else {
      if (preferEpoll)
        logger.info(
          "The Netty epoll transport is not available on this platform; falling back to NIO, " +
            s"so TCP-level (TcpInfo) transport metrics$metricsQualifier will be unavailable"
        )
      None
    }

  /** Whether the epoll transport is in use, i.e. whether TCP-level `TcpInfo` metrics can be
    * reported.
    */
  def usingEpoll: Boolean = eventLoopGroupO.isDefined

  def close(): Unit =
    eventLoopGroupO.foreach { group =>
      try group.shutdownGracefully(0, 0, TimeUnit.SECONDS).discard
      catch {
        case NonFatal(e) =>
          logger.debug(s"Failed to shut down the epoll event loop group for $subject", e)
      }
    }

  private def epollAvailable: Boolean =
    try Epoll.isAvailable
    catch {
      case NonFatal(_) => false
    }
}

private[bftordering] object P2PNettyEpollInstrumentation {

  private val nativeSocketOptionsHelperInstalled = new AtomicBoolean(false)

  /** Installs, once per process, a gRPC native socket-options helper that reads the epoll `TcpInfo`
    * from an [[EpollSocketChannel]] and exposes it through Channelz. gRPC's default helper returns
    * nothing (and gRPC ships no epoll implementation), so without this the Channelz
    * `SocketOptions.tcpInfo` is always absent and TCP-level metrics (RTT, window scales,
    * retransmitted segments) are never reported, even when the epoll transport is in use.
    *
    * The helper is a process-wide, best-effort singleton: any failure to read `TcpInfo` (e.g. a
    * non-epoll or closed channel) simply yields no native options, so metrics degrade gracefully.
    */
  private def installNativeSocketOptionsHelper(): Unit =
    if (nativeSocketOptionsHelperInstalled.compareAndSet(false, true))
      InternalNettySocketSupport.setHelper(new InternalHelper {
        @SuppressWarnings(Array("org.wartremover.warts.Null"))
        override def getNativeSocketOptions(channel: Channel): InternalNativeSocketOptions =
          channel match {
            case epollChannel: EpollSocketChannel =>
              try {
                Option(epollChannel.tcpInfo()) match {
                  case Some(info) =>
                    val tcpInfo = new InternalChannelz.TcpInfo.Builder()
                      .setState(info.state())
                      .setCaState(info.caState())
                      .setRetransmits(info.retransmits())
                      .setProbes(info.probes())
                      .setBackoff(info.backoff())
                      .setOptions(info.options())
                      .setSndWscale(info.sndWscale())
                      .setRcvWscale(info.rcvWscale())
                      .setRto(info.rto().toInt)
                      .setAto(info.ato().toInt)
                      .setSndMss(info.sndMss().toInt)
                      .setRcvMss(info.rcvMss().toInt)
                      .setUnacked(info.unacked().toInt)
                      .setSacked(info.sacked().toInt)
                      .setLost(info.lost().toInt)
                      .setRetrans(info.retrans().toInt)
                      .setFackets(info.fackets().toInt)
                      .setLastDataSent(info.lastDataSent().toInt)
                      .setLastAckSent(info.lastAckSent().toInt)
                      .setLastDataRecv(info.lastDataRecv().toInt)
                      .setLastAckRecv(info.lastAckRecv().toInt)
                      .setPmtu(info.pmtu().toInt)
                      .setRcvSsthresh(info.rcvSsthresh().toInt)
                      .setRtt(info.rtt().toInt)
                      .setRttvar(info.rttvar().toInt)
                      .setSndSsthresh(info.sndSsthresh().toInt)
                      .setSndCwnd(info.sndCwnd().toInt)
                      .setAdvmss(info.advmss().toInt)
                      .setReordering(info.reordering().toInt)
                      .build()
                    new InternalNativeSocketOptions(tcpInfo, java.util.Collections.emptyMap())
                  case None => null
                }
              } catch {
                case NonFatal(_) => null
              }
            case _ => null
          }
      })
}
