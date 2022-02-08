// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

class LoggingAlarmStreamer(logger: TracedLogger) extends AlarmStreamer {

  override def alarm(throwable: Throwable)(implicit traceContext: TraceContext): Future[Unit] =
    Future.successful(logger.error("An error happened", throwable))

  override def alarm(message: String)(implicit traceContext: TraceContext): Future[Unit] =
    Future.successful(logger.error(message))
}
