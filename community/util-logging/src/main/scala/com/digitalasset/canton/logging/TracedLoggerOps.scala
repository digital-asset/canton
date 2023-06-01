// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import com.daml.grpc.GrpcException
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.Status

import scala.util.control.NonFatal
import scala.util.{Failure, Try}

object TracedLoggerOps {
  implicit class TracedLoggerOps(val logger: TracedLogger) extends AnyVal {
    def logErrorsOnCall[Out](implicit traceContext: TraceContext): PartialFunction[Try[Out], Unit] =
      logErrorsOnCallImpl[Out](logger)
  }

  private def internalOrUnknown(code: Status.Code): Boolean =
    code == Status.Code.INTERNAL || code == Status.Code.UNKNOWN

  private def logError(logger: TracedLogger, t: Throwable)(implicit
      traceContext: TraceContext
  ): Unit =
    logger.error("Unhandled internal error", t)

  private def logErrorsOnCallImpl[Out](logger: TracedLogger)(implicit
      traceContext: TraceContext
  ): PartialFunction[Try[Out], Unit] = {
    case Failure(e @ GrpcException(s, _)) =>
      if (internalOrUnknown(s.getCode)) {
        logError(logger, e)
      }
    case Failure(NonFatal(e)) =>
      logError(logger, e)
  }

}
