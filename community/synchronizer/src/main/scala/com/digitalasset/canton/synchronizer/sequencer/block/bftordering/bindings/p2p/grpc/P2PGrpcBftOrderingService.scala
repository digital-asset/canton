// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.digitalasset.canton.lifecycle.UnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.Miscellaneous.objId
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.*
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import io.grpc.stub.StreamObserver

class P2PGrpcBftOrderingService(
    createServerSidePeerReceiver: (
        StreamObserver[
          BftOrderingMessage
        ],
        TraceContext,
    ) => Option[UnlessShutdown[StreamObserver[
      BftOrderingMessage
    ]]],
    override val loggerFactory: NamedLoggerFactory,
) extends BftOrderingServiceGrpc.BftOrderingService
    with NamedLogging {

  override def receive(
      sendingStreamObserver: StreamObserver[BftOrderingMessage]
  ): StreamObserver[BftOrderingMessage] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    createServerSidePeerReceiver(sendingStreamObserver, traceContext) match {
      case Some(UnlessShutdown.Outcome(peerReceiver)) => peerReceiver
      case Some(UnlessShutdown.AbortedDueToShutdown) =>
        // No receiver created means that we're shutting down
        logger.debug(s"Completing peer sender ${objId(sendingStreamObserver)} due to shutdown")
        // Unsynchronized but we're shutting down, so this observer hasn't been sent anything
        sendingStreamObserver.onCompleted()
        new StreamObserver[BftOrderingMessage]() {
          override def onNext(value: BftOrderingMessage): Unit =
            logger.debug(s"Received message $value, ignoring due to shutdown")
          override def onError(t: Throwable): Unit =
            logger.debug(s"Received error, ignoring due to shutdown", t)
          override def onCompleted(): Unit =
            logger.debug(s"Received completion")
        }
      case None =>
        // Receiver setup already failed the response stream. Return a listener without closing
        //  it again or propagating an exception through gRPC's startCall.
        new StreamObserver[BftOrderingMessage]() {
          override def onNext(value: BftOrderingMessage): Unit =
            logger.debug(s"Received message $value, ignoring after receiver setup failed")
          override def onError(t: Throwable): Unit =
            logger.debug(s"Received error after receiver setup failed", t)
          override def onCompleted(): Unit =
            logger.debug(s"Received completion")
        }
    }
  }
}
