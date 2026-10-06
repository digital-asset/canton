// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.transport

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup
import io.grpc.netty.shaded.io.netty.channel.epoll.{Epoll, EpollEventLoopGroup}
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory

import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal

/** Base class managing the optional shared Netty epoll [[EventLoopGroup]] that makes TCP-level
  * `TcpInfo` (round-trip time, retransmissions, window scales) observable through gRPC Channelz
  * (see [[P2PGrpcTransportStatsReader]]).
  *
  * When [[preferEpoll]] is set and epoll is available on the platform, a single epoll event loop
  * group is created (and must be [[close]]d on shutdown); subclasses wire it into their respective
  * Netty builder via `customize`. Otherwise the default NIO transport is kept and TCP-level metrics
  * are simply not reported.
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
        val group = new EpollEventLoopGroup(0, new DefaultThreadFactory(threadNamePrefix))
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
