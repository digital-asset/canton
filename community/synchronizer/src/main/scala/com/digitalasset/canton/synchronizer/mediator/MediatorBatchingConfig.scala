// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.config.{BatchAggregatorConfig, BatchingConfig}

/** Configuration settings for mediator batching.
  *
  * @param mediatorFetchFinalizedResponsesAggregator
  *   batching configuration for the mediator for fetching finalized responses
  * @param mediatorStoreFinalizedResponsesAggregator
  *   batching configuration for the mediator for storing finalized responses
  */
final case class MediatorBatchingConfig(
    maxItemsInBatch: PositiveNumeric[Int] = BatchingConfig.defaultMaxItemsBatch,
    maxTopologyWriteBatchSize: PositiveNumeric[Int] =
      BatchingConfig.defaultMaxTopologyWriteBatchSize,
    maxTopologyUpdateBatchSize: PositiveNumeric[Int] =
      BatchingConfig.defaultMaxTopologyUpdateBatchSize,
    parallelism: PositiveNumeric[Int] = BatchingConfig.defaultBatchingParallelism,
    aggregator: BatchAggregatorConfig = BatchingConfig.defaultAggregator,
    mediatorFetchFinalizedResponsesAggregator: BatchAggregatorConfig =
      BatchingConfig.defaultAggregator,
    mediatorStoreFinalizedResponsesAggregator: BatchAggregatorConfig =
      BatchingConfig.defaultAggregator,
    pruningParallelism: PositiveNumeric[Int] = BatchingConfig.defaultPruningParallelism,
    topologyCacheAggregator: BatchAggregatorConfig = BatchingConfig.defaultAggregator,
) extends BatchingConfig
