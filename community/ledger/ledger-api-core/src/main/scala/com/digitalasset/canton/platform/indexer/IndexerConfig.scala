// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer

import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.platform.indexer.IndexerConfig.*
import com.digitalasset.canton.platform.store.DbSupport.{ConnectionPoolConfig, DataSourceProperties}
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** See com.digitalasset.canton.platform.indexer.JdbcIndexer for semantics on these configurations.
  *
  *   - enableCompression: switches on compression for both consuming and non-consuming exercises,
  *     equivalent to setting both enableCompressionConsumingExercise and
  *     enableCompressionNonConsumingExercise to true. This is to maintain backward compatibility
  *     with existing config files.
  *   - enableCompressionConsumingExercise: switches on compression for consuming exercises
  *   - enableCompressionNonConsumingExercise: switches on compression for non-consuming exercises
  */
final case class IndexerConfig(
    batchingParallelism: NonNegativeInt = NonNegativeInt.tryCreate(DefaultBatchingParallelism),
    enableCompression: Boolean = DefaultEnableCompression,
    enableCompressionConsumingExercise: Boolean = DefaultEnableCompression,
    enableCompressionNonConsumingExercise: Boolean = DefaultEnableCompression,
    ingestionParallelism: NonNegativeInt = NonNegativeInt.tryCreate(DefaultIngestionParallelism),
    inputMappingParallelism: NonNegativeInt =
      NonNegativeInt.tryCreate(DefaultInputMappingParallelism),
    dbPrepareParallelism: NonNegativeInt = NonNegativeInt.tryCreate(DefaultDbPrepareParallelism),
    maxInputBufferSize: NonNegativeInt = NonNegativeInt.tryCreate(DefaultMaxInputBufferSize),
    restartDelay: config.NonNegativeFiniteDuration =
      config.NonNegativeFiniteDuration.ofSeconds(DefaultRestartDelay.toSeconds),
    useWeightedBatching: Boolean =
      DefaultUseWeightedBatching, // feature flag to enable improved batching strategy in ingestion pipeline
    submissionBatchSize: Long = DefaultSubmissionBatchSize,
    submissionBatchInsertionSize: Long = DefaultSubmissionBatchInsertionSize,
    maxOutputBatchedBufferSize: Int = DefaultMaxOutputBatchedBufferSize,
    maxTailerBatchSize: Int = DefaultMaxTailerBatchSize,
    postProcessingParallelism: Int = DefaultPostProcessingParallelism,
    queueMaxBlockedOffer: Int = DefaultQueueMaxBlockedOffer,
    queueBufferSize: Int = DefaultQueueBufferSize,
    queueUncommittedWarnThreshold: Int = DefaultQueueUncommittedWarnThreshold,
    queueRecoveryRetryMinWaitMillis: Int = DefaultQueueRecoveryRetryMinWaitMillis,
    queueRecoveryRetryMaxWaitMillis: Int = DefaultQueueRecoveryRetryMaxWaitMillis,
    queueRecoveryRetryAttemptWarnThreshold: Int = DefaultQueueRecoveryRetryAttemptWarnThreshold,
    queueRecoveryRetryAttemptErrorThreshold: Int = DefaultQueueRecoveryRetryAttemptErrorThreshold,
    disableMonotonicityChecks: Boolean = false,
    postgresDataSource: PostgresDataSourceConfig = DefaultPostgresDataSourceConfig,
    achsConfig: Option[AchsConfig] = DefaultAchsConfig,
)

object IndexerConfig {

  // Exposed as public method so defaults can be overridden in the downstream code.
  def createDataSourcePropertiesForTesting(
      indexerConfig: IndexerConfig,
      loggerFactory: NamedLoggerFactory,
  ): DataSourceProperties = DataSourceProperties(
    // PostgresSQL specific configurations
    postgres = PostgresDataSourceConfig(
      synchronousCommit = Some(PostgresDataSourceConfig.SynchronousCommitValue.Off)
    ),
    connectionPool = createConnectionPoolConfig(indexerConfig, loggerFactory = loggerFactory),
  )

  def createConnectionPoolConfig(
      indexerConfig: IndexerConfig,
      loggerFactory: NamedLoggerFactory,
      connectionTimeout: FiniteDuration = FiniteDuration(
        // 250 millis is the lowest possible value for this Hikari configuration (see HikariConfig JavaDoc)
        250,
        "millis",
      ),
  ): ConnectionPoolConfig = {
    // Base pool: ingestion + dbPrepare + 2 (tailing ledger_end + post processing end updates)
    val basePoolSize =
      indexerConfig.ingestionParallelism.unwrap +
        indexerConfig.dbPrepareParallelism.unwrap +
        2

    // ACHS pool: population + removal + 2 (bump validAt + update last pointers)
    val achsPoolSize = indexerConfig.achsConfig.fold(0) { achsConfig =>
      achsConfig.populationParallelism.unwrap +
        achsConfig.removalParallelism.unwrap +
        2
    }

    val connectionPoolSize = basePoolSize + achsPoolSize

    indexerConfig.achsConfig.foreach { achsConfig =>
      val initParallelism = achsConfig.initParallelism.unwrap * 2

      if (initParallelism > connectionPoolSize) {
        loggerFactory
          .getLogger(getClass)
          .warn(
            s"ACHS initialization parallelism ($initParallelism) exceeds the indexing parallelism ($connectionPoolSize). " +
              "Consider decreasing the ACHS initialization parallelism."
          )
      }
    }
    ConnectionPoolConfig(
      connectionPoolSize = connectionPoolSize,
      connectionTimeout = connectionTimeout,
    )
  }

  val DefaultRestartDelay: FiniteDuration = 10.seconds
  val DefaultMaxInputBufferSize: Int = 50
  val DefaultInputMappingParallelism: Int = 16
  val DefaultDbPrepareParallelism: Int = 4
  val DefaultBatchingParallelism: Int = 4
  val DefaultIngestionParallelism: Int = 16
  val DefaultUseWeightedBatching: Boolean = false
  val DefaultSubmissionBatchSize: Long = 50L
  val DefaultSubmissionBatchInsertionSize: Long = 5000L
  val DefaultEnableCompression: Boolean = false
  val DefaultMaxOutputBatchedBufferSize: Int = 16
  val DefaultMaxTailerBatchSize: Int = 10
  val DefaultPostProcessingParallelism: Int = 8
  val DefaultQueueMaxBlockedOffer: Int = 1000
  val DefaultQueueBufferSize: Int = 50
  val DefaultQueueUncommittedWarnThreshold: Int = 5000
  val DefaultQueueRecoveryRetryMinWaitMillis: Int = 50
  val DefaultQueueRecoveryRetryMaxWaitMillis: Int = 5000
  val DefaultQueueRecoveryRetryAttemptWarnThreshold: Int = 50
  val DefaultQueueRecoveryRetryAttemptErrorThreshold: Int = 100
  val DefaultPostgresDataSourceConfig: PostgresDataSourceConfig =
    PostgresDataSourceConfig(networkTimeout = Some(config.NonNegativeFiniteDuration.ofSeconds(20)))
  val DefaultAchsConfig: Option[AchsConfig] = None

  /** Configuration for the Active Contracts Head Snapshot (ACHS). See
    * [[com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.AchsState]] for more.
    *
    * @param validAtDistanceTarget
    *   The target distance (in event sequential ids) between the current ledger end and the valid
    *   at of the ACHS.
    * @param lastPopulatedDistanceTarget
    *   The target distance (in event sequential ids) between the valid at and the last populated
    *   offset of the ACHS.
    * @param populationParallelism
    *   Parallelism for ACHS population during normal operation.
    * @param removalParallelism
    *   Parallelism for ACHS removal during normal operation.
    * @param aggregationThreshold
    *   Aggregation threshold for ACHS maintenance batches.
    * @param initParallelism
    *   Parallelism for ACHS population/removal during initialization.
    * @param initAggregationThreshold
    *   Aggregation threshold for ACHS maintenance batches during initialization.
    */
  final case class AchsConfig(
      validAtDistanceTarget: NonNegativeLong,
      lastPopulatedDistanceTarget: NonNegativeLong,
      populationParallelism: NonNegativeInt =
        NonNegativeInt.tryCreate(AchsConfig.DefaultPopulationParallelism),
      removalParallelism: NonNegativeInt =
        NonNegativeInt.tryCreate(AchsConfig.DefaultRemovalParallelism),
      aggregationThreshold: Long = AchsConfig.DefaultAggregationThreshold,
      initParallelism: NonNegativeInt = NonNegativeInt.tryCreate(AchsConfig.DefaultInitParallelism),
      initAggregationThreshold: Long = AchsConfig.DefaultInitAggregationThreshold,
  )

  object AchsConfig {
    val DefaultPopulationParallelism: Int = 4
    val DefaultRemovalParallelism: Int = 4
    val DefaultAggregationThreshold: Long = 10000L
    val DefaultInitParallelism: Int = 8
    val DefaultInitAggregationThreshold: Long = 100000L
  }

}
