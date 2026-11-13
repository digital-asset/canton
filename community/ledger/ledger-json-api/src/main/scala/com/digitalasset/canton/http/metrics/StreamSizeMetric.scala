// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.metrics

import com.daml.metrics.api.MetricHandle.Histogram
import com.daml.metrics.api.MetricsContext
import org.apache.pekko.stream.scaladsl.Source

private[metrics] object StreamSizeMetric {

  // Passes the elements of the source through unchanged, and records their total size
  // in the given histogram when the stream terminates.
  // The size is recorded in-line, before completion is signalled downstream, so that anyone
  // observing the completion of the stream also observes the recorded size.
  def recordTotalSize[T, Mat](
      source: Source[T, Mat],
      metric: Histogram,
  )(size: T => Long)(implicit mc: MetricsContext): Source[T, Mat] =
    source.statefulMap(() => 0L)(
      (acc, elem) => (acc + size(elem), elem),
      acc => {
        metric.update(acc)
        None
      },
    )
}
