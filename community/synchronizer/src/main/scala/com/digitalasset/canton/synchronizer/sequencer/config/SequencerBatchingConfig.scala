// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.config

import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.config.{
  BatchAggregatorConfig,
  BatchingConfig,
  PositiveFiniteDuration,
}

/** Configuration settings for sequencer batching.
  *
  * @param maxPruningTimeInterval
  *   Split pruning queries into intervals of this duration to avoid sequential scans.
  * @param inFlightAggregationsQueryInterval
  *   (optionally) split aggregator queries into intervals of this duration to avoid sequential
  *   scans. When too many query intervals are generated, query interval splitting does not occur.
  */
final case class SequencerBatchingConfig(
    maxItemsInBatch: PositiveNumeric[Int] = BatchingConfig.defaultMaxItemsBatch,
    maxTopologyWriteBatchSize: PositiveNumeric[Int] =
      BatchingConfig.defaultMaxTopologyWriteBatchSize,
    maxTopologyUpdateBatchSize: PositiveNumeric[Int] =
      BatchingConfig.defaultMaxTopologyUpdateBatchSize,
    parallelism: PositiveNumeric[Int] = BatchingConfig.defaultBatchingParallelism,
    aggregator: BatchAggregatorConfig = BatchingConfig.defaultAggregator,
    inFlightAggregationsQueryInterval: Option[PositiveFiniteDuration] =
      SequencerBatchingConfig.defaultInFlightAggregationsQueryInterval,
    maxPruningTimeInterval: PositiveFiniteDuration =
      SequencerBatchingConfig.defaultMaxPruningTimeInterval,
    pruningParallelism: PositiveNumeric[Int] = BatchingConfig.defaultPruningParallelism,
    topologyCacheAggregator: BatchAggregatorConfig = BatchingConfig.defaultAggregator,
) extends BatchingConfig

object SequencerBatchingConfig {
  // 30 minutes corresponds to 1440 pruning queries after 30 days downtime.
  private[canton] val defaultMaxPruningTimeInterval: PositiveFiniteDuration =
    PositiveFiniteDuration.ofMinutes(30)
  private[canton] val defaultInFlightAggregationsQueryInterval: Option[PositiveFiniteDuration] =
    None
}
