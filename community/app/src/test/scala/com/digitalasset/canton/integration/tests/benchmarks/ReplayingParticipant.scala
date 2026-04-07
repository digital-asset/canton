// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.benchmarks

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.metrics.ExecutorServiceMetrics
import com.daml.metrics.api.MetricsContext
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest.{
  RichSynchronizerIdO,
  defaultStaticSynchronizerParameters,
  testedProtocolVersion,
  testedReleaseProtocolVersion,
}
import com.digitalasset.canton.FutureHelpers
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.DbConfig.Postgres
import com.digitalasset.canton.config.RequireTypes.{PositiveInt, PositiveLong}
import com.digitalasset.canton.console.LocalSequencerReference
import com.digitalasset.canton.crypto.{Crypto, SynchronizerCrypto, SynchronizerCryptoClient}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.networking.grpc.ClientChannelParams
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.replica.ReplicaManager
import com.digitalasset.canton.resource.{DbStorageSingle, Storage}
import com.digitalasset.canton.sequencing.authentication.AuthenticationTokenManagerConfig
import com.digitalasset.canton.sequencing.client.ReplayAction.SequencerSends
import com.digitalasset.canton.sequencing.client.pool.{
  GrpcSequencerConnectionPoolFactory,
  SequencerConnectionPool,
}
import com.digitalasset.canton.sequencing.client.transports.replay.{ReplayClient, ReplayClientImpl}
import com.digitalasset.canton.sequencing.client.{ReplayConfig, RequestSigner}
import com.digitalasset.canton.sequencing.{GrpcSequencerConnection, SequencerConnections}
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.synchronizer.metrics.SequencerTestMetrics
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.cache.{CacheTestMetrics, TopologyStateWriteThroughCache}
import com.digitalasset.canton.topology.client.WriteThroughCacheSynchronizerTopologyClient
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{
  DbInitializationStore,
  NoPackageDependencies,
  TopologyStore,
}
import com.digitalasset.canton.topology.{Member, ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TraceContext, TracingConfig}
import org.apache.pekko.stream.Materializer
import org.scalatest.{EitherValues, OptionValues}

import java.nio.file.Path
import scala.concurrent.ExecutionContextExecutor

/** A lightweight implementation of the Participant for replay test purposes. It's a thin wrapper
  * around the [[ReplayingSendsSequencerClientTransportPekko]] that avoids heavy read-side
  * processing.
  */
final class ReplayingParticipant private (
    val sendsConfig: SequencerSends,
    synchronizerCryptoClient: SynchronizerCryptoClient,
    replayClient: ReplayClient,
    storage: DbStorageSingle,
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
) extends NamedLogging
    with FlagCloseableAsync {

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] =
    Seq[AsyncOrSyncCloseable](
      SyncCloseable("replayClient.close()", replayClient.close()),
      SyncCloseable("synchronizerCryptoClient.close()", synchronizerCryptoClient.close()),
      SyncCloseable("storage.close()", storage.close()),
    )
}

object ReplayingParticipant extends FutureHelpers with EitherValues with OptionValues {

  def tryCreate(
      connectedToSequencer: LocalSequencerReference,
      replayDirectory: Path,
      recordingFileName: String,
      futureSupervisor: FutureSupervisor,
      postgresPlugin: UsePostgres,
      clock: Clock,
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
  )(implicit
      env: TestConsoleEnvironment,
      closeContext: CloseContext,
      traceContext: TraceContext,
  ): ReplayingParticipant = {
    import env.*

    val nodeName = recordingFileName.split("::")(1)

    val extendedLoggerFactory = loggerFactory.append("replaying-participant", nodeName)

    val dbConfig =
      postgresPlugin.generateDbConfig(
        nodeName,
        // Make it as lightweight as possible
        DbParametersConfig(maxConnections = Some(PositiveInt.one)),
        Postgres.defaultConfig,
      )
    val storage = DbStorageSingle
      .create(
        dbConfig,
        // `true` results in a smaller number of reserved connections, but we already define the number above
        connectionPoolForParticipant = false,
        logQueryCost = None,
        clock = clock,
        scheduler = None, // Used for query cost tracking (disabled above)
        metrics = ParticipantTestMetrics.storageMetrics,
        timeouts = timeouts,
        loggerFactory = extendedLoggerFactory,
      )
      .value
      .onShutdown(throw new IllegalStateException("Unexpected shutdown"))
      .value

    val initStore = new DbInitializationStore(storage, timeouts, extendedLoggerFactory)
    val member = ParticipantId(initStore.uid.futureValueUS.value)
    val psid = connectedToSequencer.synchronizer_id.toPhysical

    val crypto =
      mkCrypto(
        clock,
        futureSupervisor,
        storage,
        extendedLoggerFactory,
        timeouts,
      ).valueOrFail("create crypto").futureValueUS
    val synchronizerCrypto = SynchronizerCrypto(crypto, defaultStaticSynchronizerParameters)

    val connectionPool = mkConnectionPool(
      connectedToSequencer.sequencerConnection.toInternal,
      member,
      psid,
      synchronizerCrypto,
      clock,
      futureSupervisor,
      extendedLoggerFactory,
      timeouts,
    ).futureValueUS.valueOr(err => throw new IllegalStateException(err))

    val indexedStringStore =
      IndexedStringStore.create(
        storage,
        CacheConfig(maximumSize = PositiveLong.tryCreate(10)),
        timeouts,
        loggerFactory,
      )

    val topologyStore =
      TopologyStore
        .create(
          SynchronizerStore(psid),
          storage,
          indexedStringStore,
          testedProtocolVersion,
          timeouts,
          BatchingConfig(),
          extendedLoggerFactory,
        )
        .futureValueUS
    val synchronizerTopologyClient =
      WriteThroughCacheSynchronizerTopologyClient
        .create(
          clock,
          defaultStaticSynchronizerParameters,
          topologyStore,
          new TopologyStateWriteThroughCache(
            topologyStore,
            BatchAggregatorConfig.defaultsForTesting,
            TopologyConfig.forTesting.topologyStateCacheEvictionThreshold,
            TopologyConfig.forTesting.maxTopologyStateCacheItems,
            TopologyConfig.forTesting.enableTopologyStateCacheConsistencyChecks,
            CacheTestMetrics.metrics,
            futureSupervisor,
            timeouts,
            loggerFactory,
          ),
          synchronizerUpgradeTime = None,
          NoPackageDependencies,
          CachingConfigs(),
          // turn off consistency checks for performance tests
          enableConsistencyChecks = false,
          TopologyConfig(),
          timeouts,
          futureSupervisor,
          extendedLoggerFactory,
        )()
        .futureValueUS
    val synchronizerCryptoClient =
      SynchronizerCryptoClient.create(
        member,
        psid.logical,
        synchronizerTopologyClient,
        defaultStaticSynchronizerParameters,
        synchronizerCrypto,
        verificationParallelismLimit = PositiveInt.one,
        CachingConfigs.defaultPublicKeyConversionCache,
        timeouts,
        futureSupervisor,
        extendedLoggerFactory,
      )

    val sendsConfig = SequencerSends(extendedLoggerFactory, usePekko = true)

    val recordingConfig =
      ReplayConfig(replayDirectory, sendsConfig).recordingConfig.setFilename(recordingFileName)

    // Registers itself in the `sendsConfig`
    val replayClient = new ReplayClientImpl(
      testedProtocolVersion,
      recordingConfig.fullFilePath,
      sendsConfig,
      member,
      connectionPool,
      RequestSigner(synchronizerCryptoClient, extendedLoggerFactory),
      synchronizerCryptoClient.currentSnapshotApproximation.futureValueUS,
      clock,
      SequencerTestMetrics.sequencerClient,
      timeouts,
      extendedLoggerFactory,
    )

    new ReplayingParticipant(
      sendsConfig,
      synchronizerCryptoClient,
      replayClient,
      storage,
      extendedLoggerFactory,
      timeouts,
    )
  }

  private def mkCrypto(
      clock: Clock,
      futureSupervisor: FutureSupervisor,
      storage: Storage,
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
  )(implicit
      executionContext: ExecutionContextExecutor,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Crypto] = {
    val cryptoConfig = CryptoConfig()
    Crypto
      .create(
        cryptoConfig,
        CachingConfigs.defaultKmsMetadataCache,
        SessionEncryptionKeyCacheConfig(),
        CachingConfigs.defaultPublicKeyConversionCache,
        storage,
        Option.empty[ReplicaManager],
        testedReleaseProtocolVersion,
        futureSupervisor,
        clock,
        executionContext,
        timeouts,
        BatchingConfig(),
        loggerFactory,
        NoReportingTracerProvider,
        new ExecutorServiceMetrics(NoOpMetricsFactory),
      )
  }

  private def mkConnectionPool(
      sequencerConnection: GrpcSequencerConnection,
      member: Member,
      synchronizerId: PhysicalSynchronizerId,
      synchronizerCrypto: SynchronizerCrypto,
      clock: Clock,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
  )(implicit
      executionContext: ExecutionContextExecutor,
      executionSequencerFactory: ExecutionSequencerFactory,
      materializer: Materializer,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, SequencerConnectionPool] = {
    val connectionPoolFactory = new GrpcSequencerConnectionPoolFactory(
      clientProtocolVersions = NonEmpty(Seq, testedProtocolVersion),
      minimumProtocolVersion = Some(testedProtocolVersion),
      authConfig = AuthenticationTokenManagerConfig(),
      params = ClientChannelParams.ForTesting,
      member = member,
      clock = clock,
      crypto = synchronizerCrypto.crypto,
      seedForRandomnessO = None,
      metrics = CommonMockMetrics.sequencerClient.connectionPool,
      metricsContext = MetricsContext.Empty,
      futureSupervisor = futureSupervisor,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
    )

    for {
      connectionPool <- EitherT.fromEither[FutureUnlessShutdown](
        connectionPoolFactory
          .createFromOldConfig(
            sequencerConnections = SequencerConnections.single(sequencerConnection),
            expectedPsidO = Some(synchronizerId),
            tracingConfig = TracingConfig(propagation = TracingConfig.Propagation.Disabled),
            name = "replay",
          )
          .leftMap(_.toString)
      )
      _ <- connectionPool.start().leftMap(_.toString)
    } yield connectionPool
  }
}
