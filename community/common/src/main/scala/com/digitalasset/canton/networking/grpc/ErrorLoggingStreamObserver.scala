// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.StatusRuntimeException
import io.grpc.stub.StreamObserver
import com.digitalasset.canton.util.ShowUtil._

abstract class ErrorLoggingStreamObserver[R](
    logger: TracedLogger,
    serverName: String,
    request: String,
)(implicit traceContext: TraceContext)
    extends StreamObserver[R] {
  final override def onError(t: Throwable): Unit =
    t match {
      case sre: StatusRuntimeException =>
        GrpcError(request, serverName, sre).log(logger)
      case _: Throwable =>
        logger.error(
          show"An exception has occurred while awaiting responses from ${serverName.singleQuoted} on ${request.doubleQuoted}.",
          t,
        )
    }

  override def onCompleted(): Unit = logger.info("The stream has completed.")
}
