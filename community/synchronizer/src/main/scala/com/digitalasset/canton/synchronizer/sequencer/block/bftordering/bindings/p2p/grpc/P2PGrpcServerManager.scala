// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext

final class P2PGrpcServerManager(
    maybeServerUS: Option[UnlessShutdown[LifeCycle.CloseableServer]],
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging
    with FlagCloseable { self =>

  import TraceContext.Implicits.Empty.emptyTraceContext

  def startServer(): Unit =
    if (!isClosing) {
      maybeServerUS.fold {
        logger.info("Not starting P2P gRPC server due to none configured")
      } {
        case UnlessShutdown.Outcome(closeableServer) =>
          val server = closeableServer.server.start()
          logger.info(s"Started P2P gRPC server on port ${server.getPort}")
        case UnlessShutdown.AbortedDueToShutdown =>
          logger.info("Not starting P2P gRPC server due to shutdown")
      }
    } else {
      logger.info("Not starting P2P gRPC server due to shutdown")
    }

  private def shutdownGrpcServers(): Unit =
    maybeServerUS.foreach(_.foreach { closeableServer =>
      logger.info(s"Shutting down P2P gRPC server")
      shutdownGrpcServer(closeableServer)
    })

  private def shutdownGrpcServer(server: LifeCycle.CloseableServer): Unit = {
    // https://github.com/grpc/grpc-java/issues/8770
    val serverPort = server.server.getPort
    logger.debug(s"Terminating P2P gRPC server on port $serverPort")
    server.close()
    logger.info(s"Successfully terminated the P2P gRPC server on port $serverPort")
  }

  override def onClosed(): Unit = {
    logger.debug("Closing P2P gRPC server manager")
    shutdownGrpcServers()
    logger.debug("Closed P2P gRPC server manager")
  }
}
