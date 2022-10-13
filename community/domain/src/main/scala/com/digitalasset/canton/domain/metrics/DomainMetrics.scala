// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.metrics

import com.codahale.metrics.MetricRegistry
import com.codahale.{metrics as codahale}
import com.daml.metrics.MetricHandle.{Gauge, Meter, VarGauge}
import com.daml.metrics.MetricName
import com.digitalasset.canton.metrics.{
  DbStorageMetrics,
  MetricDoc,
  MetricHandle,
  SequencerClientMetrics,
}

class SequencerMetrics(parent: MetricName, val registry: MetricRegistry)
    extends MetricHandle.NodeMetrics {
  override val prefix = MetricName(parent :+ "sequencer")

  object sequencerClient extends SequencerClientMetrics(prefix, registry)

  @MetricDoc.Tag(
    summary = "Number of active sequencer subscriptions",
    description =
      """This metric indicates the number of active subscriptions currently open and actively
        |served subscriptions at the sequencer.""",
  )
  val subscriptionsGauge: VarGauge[Int] = varGauge(MetricName(prefix :+ "subscriptions"), 0)
  @MetricDoc.Tag(
    summary = "Number of messages processed by the sequencer",
    description = """This metric measures the number of successfully validated messages processed
                    |by the sequencer since the start of this process.""",
  )
  val messagesProcessed: Meter = meter(prefix :+ "processed")

  @MetricDoc.Tag(
    summary = "Number of message bytes processed by the sequencer",
    description =
      """This metric measures the total number of message bytes processed by the sequencer.""",
  )
  val bytesProcessed: Meter = meter(prefix :+ "processed-bytes")

  @MetricDoc.Tag(
    summary = "Number of time requests received by the sequencer",
    description =
      """When a Participant needs to know the domain time it will make a request for a time proof to be sequenced.
        |It would be normal to see a small number of these being sequenced, however if this number becomes a significant
        |portion of the total requests to the sequencer it could indicate that the strategy for requesting times may
        |need to be revised to deal with different clock skews and latencies between the sequencer and participants.""",
  )
  val timeRequests: Meter = meter(prefix :+ "time-requests")

  object dbStorage extends DbStorageMetrics(prefix, registry)
}

object SequencerMetrics {
  val notImplemented = new SequencerMetrics(MetricName("todo"), new MetricRegistry())
}

class EnvMetrics(override val registry: MetricRegistry) extends MetricHandle.Factory {
  override def prefix: MetricName = MetricName("env")

  private val executionContextQueueSizeName = prefix :+ "execution-context" :+ "queue-size"
  @MetricDoc.Tag(
    summary = "Gives the number size of the global execution context queue",
    description = """This execution context is shared across all nodes running on the JVM""",
  )
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private val executionContextQueueSizeDoc: Gauge[codahale.Gauge[Int], Int] = // For docs only
    Gauge(executionContextQueueSizeName, null)

  def registerExecutionContextQueueSize(f: () => Int): Gauge[codahale.Gauge[Int], Int] = {
    gaugeWithSupplier(
      executionContextQueueSizeName,
      () =>
        new codahale.Gauge[Int] {
          override def getValue: Int = f()
        },
    )
  }

}

class DomainMetrics(override val prefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.NodeMetrics {

  object dbStorage extends DbStorageMetrics(prefix, registry)

  object sequencer extends SequencerMetrics(prefix, registry)

  object mediator extends MediatorMetrics(prefix, registry)

  object topologyManager extends IdentityManagerMetrics(prefix, registry)
}

class MediatorNodeMetrics(override val prefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.NodeMetrics {
  object dbStorage extends DbStorageMetrics(prefix, registry)

  object mediator extends MediatorMetrics(prefix, registry)
}

class MediatorMetrics(basePrefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.Factory {

  override val prefix: MetricName = basePrefix :+ "mediator"

  object sequencerClient extends SequencerClientMetrics(prefix, registry)

  @MetricDoc.Tag(
    summary = "Number of currently outstanding requests",
    description = """This metric provides the number of currently open requests registered
                    |with the mediator.""",
  )
  val outstanding: VarGauge[Int] = this.varGauge(prefix :+ "outstanding-requests", 0)

  @MetricDoc.Tag(
    summary = "Number of totally processed requests",
    description = """This metric provides the number of totally processed requests since the system
                    |has been started.""",
  )
  val requests: Meter = this.meter(prefix :+ "requests")

}

class IdentityManagerMetrics(basePrefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.Factory {
  override val prefix: MetricName = basePrefix :+ "topology-manager"

  object sequencerClient extends SequencerClientMetrics(prefix, registry)
}
