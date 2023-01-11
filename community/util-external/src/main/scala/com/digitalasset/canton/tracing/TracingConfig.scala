// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import com.digitalasset.canton.tracing.TracingConfig.{Propagation, Tracer}

/** @param propagation       How should trace contexts (debugging details associated with actions) be propagated between nodes.
  * @param tracer            Tracer configuration
  */
case class TracingConfig(propagation: Propagation = Propagation.Enabled, tracer: Tracer = Tracer())

object TracingConfig {

  /** Configuration for when trace context should be propagated */
  sealed trait Propagation
  object Propagation {
    case object Enabled extends Propagation
    case object Disabled extends Propagation
  }

  case class Tracer(exporter: Exporter = Exporter.Disabled, sampler: Sampler = Sampler.AlwaysOn())

  sealed trait Sampler {
    def parentBased: Boolean
  }
  object Sampler {
    case class AlwaysOn(parentBased: Boolean = true) extends Sampler
    case class AlwaysOff(parentBased: Boolean = true) extends Sampler
    case class TraceIdRatio(ratio: Double, parentBased: Boolean = true) extends Sampler
  }

  /** Configuration for how to export spans */
  sealed trait Exporter
  object Exporter {
    case object Disabled extends Exporter
    case class Jaeger(address: String = "localhost", port: Int = 14250) extends Exporter
    case class Zipkin(address: String = "localhost", port: Int = 9411) extends Exporter
  }
}
