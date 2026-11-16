// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.RequireTypes.PositiveNumeric

/** Shared batching settings for tests that do not depend on a particular node module. */
final case class TestBatchingConfig(
    maxItemsInBatch: PositiveNumeric[Int] = BatchingConfig.defaultMaxItemsBatch,
    maxTopologyWriteBatchSize: PositiveNumeric[Int] =
      BatchingConfig.defaultMaxTopologyWriteBatchSize,
    maxTopologyUpdateBatchSize: PositiveNumeric[Int] =
      BatchingConfig.defaultMaxTopologyUpdateBatchSize,
    parallelism: PositiveNumeric[Int] = BatchingConfig.defaultBatchingParallelism,
    aggregator: BatchAggregatorConfig = BatchingConfig.defaultAggregator,
    pruningParallelism: PositiveNumeric[Int] = BatchingConfig.defaultPruningParallelism,
    topologyCacheAggregator: BatchAggregatorConfig = BatchingConfig.defaultAggregator,
) extends BatchingConfig
