// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.server.api.services.grpc

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.{KillSwitch, KillSwitches, Materializer}
import com.daml.error.ContextualizedErrorLogger
import com.daml.error.definitions.CommonErrors
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.server.akka.ServerAdapter
import io.grpc.StatusRuntimeException
import io.grpc.stub.StreamObserver

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, blocking}

trait StreamingServiceLifecycleManagement extends AutoCloseable {
  @volatile private var _closed = false
  private val _killSwitches = TrieMap.empty[KillSwitch, Object]

  protected val contextualizedErrorLogger: ContextualizedErrorLogger

  def close(): Unit = blocking {
    synchronized {
      if (!_closed) {
        _closed = true
        _killSwitches.keySet.foreach(_.abort(closingError(contextualizedErrorLogger)))
        _killSwitches.clear()
      }
    }
  }

  private def errorHandler(throwable: Throwable) =
    CommonErrors.ServiceInternalError
      .UnexpectedOrUnknownException(throwable)(contextualizedErrorLogger)
      .asGrpcError

  protected def registerStream[RespT](
      responseObserver: StreamObserver[RespT]
  )(createSource: => Source[RespT, NotUsed])(implicit
      materializer: Materializer,
      executionSequencerFactory: ExecutionSequencerFactory,
  ): Unit = {
    def ifNotClosed(run: () => Unit): Unit =
      if (_closed) responseObserver.onError(closingError(contextualizedErrorLogger))
      else run()

    // Double-checked locking to keep the (potentially expensive)
    // by-name `source` evaluation out of the synchronized block
    ifNotClosed { () =>
      val sink = ServerAdapter.toSink(responseObserver, errorHandler)
      // Force evaluation before synchronized block
      val source = createSource

      blocking {
        synchronized {
          ifNotClosed { () =>
            val (killSwitch, doneF) = source
              .viaMat(KillSwitches.single)(Keep.right)
              .watchTermination()(Keep.both)
              .toMat(sink)(Keep.left)
              .run()

            _killSwitches += killSwitch -> NotUsed

            // This can complete outside the synchronized block
            // maintaining the need of using a concurrent collection for _killSwitches
            doneF.onComplete(_ => _killSwitches -= killSwitch)(ExecutionContext.parasitic)
          }
        }
      }
    }
  }

  def closingError(errorLogger: ContextualizedErrorLogger): StatusRuntimeException =
    CommonErrors.ServerIsShuttingDown.Reject()(errorLogger).asGrpcError
}
