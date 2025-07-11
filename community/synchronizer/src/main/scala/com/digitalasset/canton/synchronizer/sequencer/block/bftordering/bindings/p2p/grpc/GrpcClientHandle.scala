// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.digitalasset.canton.lifecycle.PromiseUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.GrpcClientHandle.AuthenticationTimeout
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.GrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingServiceReceiveResponse
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.DelayUtil
import io.grpc.stub.StreamObserver

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Failure

final class GrpcClientHandle(
    server: P2PEndpoint,
    sequencerIdPromiseUS: PromiseUnlessShutdown[SequencerId],
    cleanupClientConnectionToServer: P2PEndpoint => Unit,
    authenticationEnabled: Boolean,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends StreamObserver[BftOrderingServiceReceiveResponse]
    with NamedLogging {

  private implicit val traceContext: TraceContext = TraceContext.empty

  setupFakeAuthenticationTimeout()

  override def onNext(response: BftOrderingServiceReceiveResponse): Unit = {
    logger.debug(s"in client role received initial gRPC message from '$server' in server role")
    if (!sequencerIdPromiseUS.isCompleted) {
      SequencerId.fromProtoPrimitive(response.from, "from") match {
        case Left(e) =>
          val msg = s"received unparseable sequencer ID from '$server' in server role: $e"
          logger.warn(msg)
          val error = new RuntimeException(msg)
          sequencerIdPromiseUS.complete(Failure(error))
          onError(error)
        case Right(sequencerId) => sequencerIdPromiseUS.outcome_(sequencerId)
      }
    } else {
      logger.warn(
        s"in client role received further gRPC messages from '$server' in server role"
      )
    }
  }

  override def onError(t: Throwable): Unit = {
    logger.info(
      s"in client role received error (${t.getMessage}) from '$server' in server role, " +
        "invalidating connection and shutting down the gRPC channel",
      t,
    )
    cleanupClientConnectionToServer(server)
  }

  override def onCompleted(): Unit = {
    logger.info(
      s"in client role received completion from '$server' in server role, " +
        "invalidating connection and shutting down the gRPC channel"
    )
    cleanupClientConnectionToServer(server)
  }

  private def setupFakeAuthenticationTimeout(): Unit =
    if (!authenticationEnabled)
      DelayUtil.delay(AuthenticationTimeout).onComplete { _ =>
        if (!sequencerIdPromiseUS.isCompleted) {
          val msg =
            s"client role did not receive initial gRPC message from '$server' in server role within $AuthenticationTimeout"
          logger.warn(msg)
          val error = new RuntimeException(msg)
          sequencerIdPromiseUS.complete(Failure(error))
          onError(error)
        }
      }
}

object GrpcClientHandle {

  private val AuthenticationTimeout: FiniteDuration = 5.seconds
}
