// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import cats.data.NonEmptyList
import com.digitalasset.canton.logging.TracedLogger

/** Utility mixin for creating a single trace context from a batch of traced items */
object BatchTracing {
  def withNelTracedBatch[A <: HasTraceContext, B](logger: TracedLogger, items: NonEmptyList[A])(
      fn: TraceContext => NonEmptyList[A] => B
  ): B =
    fn(TraceContext.ofBatch(items.toList)(logger))(items)
}
