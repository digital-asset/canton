// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.config

import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.config.{BatchAggregatorConfig, BatchingConfig}

/** Configuration settings for participant batching.
  *
  * @param maxPruningBatchSize
  *   Maximum number of events to prune from a participant at a time, used to break up Canton
  *   participant-internal batches.
  * @param ledgerApiPruningBatchSize
  *   Number of events to prune from the ledger api server index-database at a time during automatic
  *   background pruning. Canton-internal store pruning happens at the smaller batch size of
  *   "maxPruningBatchSize" to minimize memory usage whereas ledger-api-server index-db pruning
  *   needs sufficiently large batches to amortize the database overhead of "skipping over" active
  *   contracts.
  * @param maxAcsImportBatchSize
  *   maximum number of active contracts in a batch to be imported
  * @param maxStakeholderGroupsBatchSize
  *   maximum number of stakeholder groups in a batch when reading commitments from a snapshot
  */
final case class ParticipantBatchingConfig(
    maxItemsInBatch: PositiveNumeric[Int] = BatchingConfig.defaultMaxItemsBatch,
    maxTopologyWriteBatchSize: PositiveNumeric[Int] =
      BatchingConfig.defaultMaxTopologyWriteBatchSize,
    maxTopologyUpdateBatchSize: PositiveNumeric[Int] =
      BatchingConfig.defaultMaxTopologyUpdateBatchSize,
    ledgerApiPruningBatchSize: PositiveNumeric[Int] =
      ParticipantBatchingConfig.defaultLedgerApiPruningBatchSize,
    maxAcsImportBatchSize: PositiveNumeric[Int] =
      ParticipantBatchingConfig.defaultMaxAcsImportBatchSize,
    parallelism: PositiveNumeric[Int] = BatchingConfig.defaultBatchingParallelism,
    aggregator: BatchAggregatorConfig = BatchingConfig.defaultAggregator,
    contractStoreAggregator: BatchAggregatorConfig =
      ParticipantBatchingConfig.defaultContractStoreAggregator,
    contractJournalAggregator: BatchAggregatorConfig =
      ParticipantBatchingConfig.defaultContractJournalAggregator,
    maxPruningBatchSize: PositiveNumeric[Int] =
      ParticipantBatchingConfig.defaultMaxPruningBatchSize,
    pruningParallelism: PositiveNumeric[Int] = BatchingConfig.defaultPruningParallelism,
    topologyCacheAggregator: BatchAggregatorConfig = BatchingConfig.defaultAggregator,
    maxStakeholderGroupsBatchSize: PositiveNumeric[Int] =
      ParticipantBatchingConfig.defaultMaxStakeholderGroupsBatchSize,
) extends BatchingConfig

object ParticipantBatchingConfig {
  private[canton] val defaultMaxPruningBatchSize: PositiveNumeric[Int] =
    PositiveNumeric.tryCreate(1000)
  private[canton] val defaultLedgerApiPruningBatchSize: PositiveNumeric[Int] =
    PositiveNumeric.tryCreate(50000)
  private[canton] val defaultMaxAcsImportBatchSize: PositiveNumeric[Int] =
    PositiveNumeric.tryCreate(1000)
  private[canton] val defaultContractJournalAggregator: BatchAggregatorConfig.Batching =
    BatchAggregatorConfig.Batching(
      maximumInFlight = PositiveNumeric.tryCreate(2),
      maximumBatchSize = PositiveNumeric.tryCreate(1000),
    )
  private[canton] val defaultContractStoreAggregator: BatchAggregatorConfig.Batching =
    BatchAggregatorConfig.Batching(
      maximumInFlight = PositiveNumeric.tryCreate(5),
      maximumBatchSize = PositiveNumeric.tryCreate(50),
    )
  private[canton] val defaultMaxStakeholderGroupsBatchSize: PositiveNumeric[Int] =
    PositiveNumeric.tryCreate(1000)
}
