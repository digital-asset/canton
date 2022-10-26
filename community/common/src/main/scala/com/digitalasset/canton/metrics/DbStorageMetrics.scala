// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.MetricHandle.{Counter, DropwizardGauge, DropwizardTimer, Gauge, Timer}
import com.daml.metrics.MetricName

import scala.concurrent.duration.*

class DbStorageMetrics(basePrefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.Factory {
  override val prefix: MetricName = basePrefix :+ "db-storage"

  def loadGaugeM(name: String): TimedLoadGauge = {
    val timerM = timer(prefix :+ name)
    loadGauge(prefix :+ name :+ "load", 1.second, timerM)
  }

  @MetricDoc.Tag(
    summary = "Timer monitoring duration and rate of accessing the given storage",
    description = """Covers both read from and writes to the storage.""",
  )
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  val timerExampleForDocs: Timer = DropwizardTimer(prefix :+ "<storage>", null)

  @MetricDoc.Tag(
    summary = "The load on the given storage",
    description =
      """The load is a factor between 0 and 1 describing how much of an existing interval
          |has been spent reading from or writing to the storage.""",
  )
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  val loadExampleForDocs: Gauge[Double] =
    DropwizardGauge(prefix :+ "<storage>" :+ "load", null)

  object alerts extends DbAlertMetrics(prefix, registry)

  object queue extends DbQueueMetrics(prefix :+ "general", registry)

  object writeQueue extends DbQueueMetrics(prefix :+ "write", registry)

  object locks extends DbQueueMetrics(prefix :+ "locks", registry)

}

class DbQueueMetrics(basePrefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.Factory {
  override val prefix: MetricName = basePrefix :+ "executor"

  @MetricDoc.Tag(
    summary = "Number of database access tasks waiting in queue",
    description =
      """Database access tasks get scheduled in this queue and get executed using one of the
        |existing asynchronous sessions. A large queue indicates that the database connection is
        |not able to deal with the large number of requests.
        |Note that the queue has a maximum size. Tasks that do not fit into the queue
        |will be retried, but won't show up in this metric.""",
  )
  val queue = counter(prefix :+ "queued")

  @MetricDoc.Tag(
    summary = "Number of database access tasks currently running",
    description = """Database access tasks run on an async executor. This metric shows
        |the current number of tasks running in parallel.""",
  )
  val running = counter(prefix :+ "running")

  @MetricDoc.Tag(
    summary = "Scheduling time metric for database tasks",
    description = """Every database query is scheduled using an asynchronous executor with a queue.
        |The time a task is waiting in this queue is monitored using this metric.""",
  )
  val waitTimer = timer(prefix :+ "waittime")

}

class DbAlertMetrics(basePrefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.Factory {
  override val prefix: MetricName = basePrefix :+ "alerts"

  @MetricDoc.Tag(
    summary = "Number of failed writes to the event log",
    description =
      """Failed writes to the single dimension event log indicate an issue requiring user intervention. In the case of
        |domain event logs, the corresponding domain no longer emits any subsequent events until domain recovery is
        |initiated (e.g. by disconnecting and reconnecting the participant from the domain). In the case of the
        |participant event log, an operation might need to be reissued. If this counter is larger than zero, check the
        |canton log for errors for details.
        |""",
  )
  val failedEventLogWrites: Counter = counter(prefix :+ "single-dimension-event-log")

  @MetricDoc.Tag(
    summary = "Number of failed writes to the multi-domain event log",
    description =
      """Failed writes to the multi domain event log indicate an issue requiring user intervention. In the case of
        |domain event logs, the corresponding domain no longer emits any subsequent events until domain recovery is
        |initiated (e.g. by disconnecting and reconnecting the participant from the domain). In the case of the
        |participant event log, an operation might need to be reissued. If this counter is larger than zero, check the
        |canton log for errors for details.
        |""",
  )
  val failedMultiDomainEventLogWrites: Counter = counter(prefix :+ "multi-domain-event-log")

}
