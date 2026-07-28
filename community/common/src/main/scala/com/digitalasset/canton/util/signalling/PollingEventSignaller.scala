// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.signalling

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

/** Ignores notifications and simply triggers signals periodically based on a static polling
  * interval.
  */
class PollingEventSignaller[K](
    pollingInterval: NonNegativeFiniteDuration,
    val loggerFactory: NamedLoggerFactory,
) extends EventSignaller[K, Unit]
    with NamedLogging {
  override def notify(notification: Notification[K], signal: Unit): Unit = ()

  override def readSignals(
      k: K,
      prettyK: String,
  )(implicit traceContext: TraceContext): Source[NotificationSignal[Unit], NotUsed] =
    Source
      .tick(pollingInterval.toScala, pollingInterval.toScala, NotificationSignal.unit)
      .conflate((a, _) => a)
      .mapMaterializedValue(_ => NotUsed)

  override def close(): Unit = ()
}
