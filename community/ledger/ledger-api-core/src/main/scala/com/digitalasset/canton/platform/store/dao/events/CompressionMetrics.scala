// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.metrics.api.MetricHandle.Histogram
import com.digitalasset.canton.metrics.Metrics

object CompressionMetrics {

  final class Field(val compressed: Histogram, val uncompressed: Histogram)

  def createArgument(metrics: Metrics): CompressionMetrics.Field =
    new Field(
      compressed = metrics.index.db.compression.createArgumentCompressed,
      uncompressed = metrics.index.db.compression.createArgumentUncompressed,
    )

  def createKeyValue(metrics: Metrics) =
    new Field(
      compressed = metrics.index.db.compression.createKeyValueCompressed,
      uncompressed = metrics.index.db.compression.createKeyValueUncompressed,
    )

  def exerciseArgument(metrics: Metrics) =
    new Field(
      compressed = metrics.index.db.compression.exerciseArgumentCompressed,
      uncompressed = metrics.index.db.compression.exerciseArgumentUncompressed,
    )

  def exerciseResult(metrics: Metrics) =
    new Field(
      compressed = metrics.index.db.compression.exerciseResultCompressed,
      uncompressed = metrics.index.db.compression.exerciseResultUncompressed,
    )
}
