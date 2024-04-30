// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.traffic

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{BatchAggregatorConfig, NonNegativeFiniteDuration}

/** Configuration for the traffic balance manager.
  * @param maximumTrafficBalanceCacheSize The maximum number of entries (= members) to keep in the cache.
  * @param batchAggregatorConfig configures how balances are batched before being written to the store.
  * @param pruningRetentionWindow the duration for which balances are kept in the cache and the store.
  *        Balances older than this duration will be pruned at regular intervals.
  */
final case class SequencerTrafficConfig(
    maximumTrafficBalanceCacheSize: PositiveInt = PositiveInt.tryCreate(1000),
    batchAggregatorConfig: BatchAggregatorConfig = BatchAggregatorConfig.Batching(),
    pruningRetentionWindow: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofHours(2L),
)
