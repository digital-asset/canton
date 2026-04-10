// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import com.digitalasset.canton.tracing.TracingConfig.{Propagation, Tracer}

import scala.concurrent.duration.FiniteDuration

/** @param propagation
  *   How should trace contexts (debugging details associated with actions) be propagated between
  *   nodes.
  * @param tracer
  *   Tracer configuration
  */
final case class TracingConfig(
    propagation: Propagation = Propagation.Enabled,
    tracer: Tracer = Tracer(),
)

object TracingConfig {

  /** Configuration for when trace context should be propagated */
  sealed trait Propagation
  object Propagation {
    case object Enabled extends Propagation
    case object Disabled extends Propagation
  }

  final case class Tracer(
      exporter: Exporter = Exporter.Disabled,
      sampler: Sampler = Sampler.AlwaysOn(),
      batchSpanProcessor: BatchSpanProcessor = BatchSpanProcessor(),
  )

  sealed trait Sampler {
    def parentBased: Boolean
  }
  object Sampler {
    final case class AlwaysOn(parentBased: Boolean = true) extends Sampler
    final case class AlwaysOff(parentBased: Boolean = true) extends Sampler
    final case class TraceIdRatio(ratio: Double, parentBased: Boolean = true) extends Sampler
  }

  final case class BatchSpanProcessor(
      batchSize: Option[Int] = None,
      scheduleDelay: Option[FiniteDuration] = None,
  )

  /** Configuration for how to export spans */
  sealed trait Exporter
  object Exporter {
    case object Disabled extends Exporter

    /** Zipkin exporter is deprecated from opentelemetry, with removal planned for mid-2026. See
      * https://opentelemetry.io/blog/2025/deprecating-zipkin-exporters/ for more details.
      */
    @deprecated("Use OTLP exporter instead.", since = "3.5.0")
    final case class Zipkin(address: String = "localhost", port: Int = 9411) extends Exporter
    final case class Otlp(
        address: String = "localhost",
        port: Int = 4317,
        trustCollectionPath: Option[String] = None,
        additionalHeaders: Map[String, String] = Map.empty,
        timeout: Option[FiniteDuration] = None,
        connectTimeout: Option[FiniteDuration] = None,
    ) extends Exporter
  }
}
