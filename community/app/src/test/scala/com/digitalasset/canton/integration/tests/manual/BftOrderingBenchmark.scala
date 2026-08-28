// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual

import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonConfig.{ConfigReaders, ConfigWriters}
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeInt,
  Port,
  PositiveInt,
  PositiveLong,
  PositiveNumeric,
}
import com.digitalasset.canton.config.{
  ActiveRequestLimitsConfig,
  BasicKeepAliveServerConfig,
  PositiveFiniteDuration,
  ReplicationConfig,
  ServerConfig,
}
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.UseBftSequencer.UseStandaloneConfig
import com.digitalasset.canton.integration.plugins.toxiproxy.UseToxiproxy.ToxiproxyConfig
import com.digitalasset.canton.integration.plugins.toxiproxy.{
  BftSequencerPeerToPeer,
  ProxyConfig,
  SequencerToPostgres,
  UseToxiproxy,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.bftsequencer.AwaitsBftSequencerAuthenticationDisseminationQuorum
import com.digitalasset.canton.integration.tests.manual.BftOrderingBenchmark.BftOrderingBenchmarkConfig
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.metrics.MetricsConfig
import com.digitalasset.canton.networking.grpc.ClientChannelParams
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.{
  DefaultAvailabilityMinProposalCreationDelay,
  DefaultConsensusEmptyBlockCreationTimeout,
  DefaultDelayedInitQueueMaxSize,
  DefaultEpochStateTransferTimeout,
  DefaultMaxBatchCreationInterval,
  DefaultMinRequestsInBatch,
  DefaultNetworkSendAttempts,
  DefaultNetworkSendRetryMaximumDelay,
  DefaultNetworkSendRetryMinimumDelay,
  DefaultOutputEnqueueMaxRetries,
  DefaultOutputEnqueueMaxRetryDelay,
  DefaultOutputFetchHowManyRecipients,
  DefaultOutputFetchMinimumDelay,
  DefaultOutputFetchTimeout,
  DefaultOutputFetchTimeoutCap,
  DefaultSendBlacklistTtl,
  DefaultSequencerCoreSubscriptionConfig,
  SequencerCoreSubscriptionConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.SequencingParameters.{
  DefaultLeaderSelectionPolicyConfig,
  DefaultMaxBatchesPerProposal,
  DefaultMaxRequestsInBatch,
  DefaultPbftViewChangeTimeout,
  DefaultSegmentLength,
  SegmentLength,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  BlacklistLeaderSelectionPolicyConfig,
  SequencingParameters,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.dabft.DaBftBindingFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.{
  BftBenchmarkConfig,
  BftBenchmarkTool,
}
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.util.BytesUnit
import eu.rekawek.toxiproxy
import eu.rekawek.toxiproxy.model.ToxicDirection
import monocle.macros.syntax.lens.*
import pureconfig.error.{
  CannotReadFile,
  CannotReadResource,
  CannotReadUrl,
  ConfigReaderException,
  ConfigReaderFailures,
}
import pureconfig.generic.ProductHint
import pureconfig.generic.semiauto.{deriveReader, deriveWriter}
import pureconfig.{ConfigObjectSource, ConfigReader, ConfigSource, ConfigWriter}

import java.net.URI
import scala.concurrent.duration.*

/** Configured by passing a typesafe config URL, which may start with "classpath:///", to the system
  * property `bft-ordering-benchmark.config`. If not provided, the default configuration file
  * `bft-ordering-benchmark.conf` is loaded from the classpath. Example script:
  *
  * #!/usr/bin/env bash
  *
  * set -euo pipefail
  *
  * if [ $# -ne 1 ]; then echo "Usage: $0 <config-file>"; exit 1; fi
  *
  * CONFIG_FILE="$1"
  *
  * if [ ! -f "$CONFIG_FILE" ]; then echo "Error: file not found: $CONFIG_FILE"; exit 1; fi
  *
  * # Convert to an absolute path so that the file:// URI is valid regardless of the working #
  * directory.
  *
  * CONFIG_FILE="$(cd "$(dirname "$CONFIG_FILE")" && pwd)/$(basename "$CONFIG_FILE")"
  *
  * export LOG_LEVEL_CANTON=INFO
  *
  * export POSTGRES_USER="*"
  *
  * export POSTGRES_PASSWORD="*"
  *
  * export POSTGRES_DB=test-user
  *
  * export POSTGRES_PORT=5432
  *
  * export POSTGRES_HOST="localhost"
  *
  * # When CI is defined, it ensures no dockerized Postgres is being used.
  *
  * # export CI=1
  *
  * # Warning: setting `-Dscala.concurrent.context.numThreads=N` with N lower than the number of ...
  * # cores available to the JVM may severely affect performance. The default is to use the number .
  * # of cores available to the JVM.
  *
  * export SBT_OPTS="-Xmx90G \
  * -Dbft-ordering-benchmark.config=file://$CONFIG_FILE"
  *
  * sbt "dumpClassPath; testOnly \
  * com.digitalasset.canton.integration.tests.manual.BftOrderingBenchmark"
  */
@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
class BftOrderingBenchmark
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AwaitsBftSequencerAuthenticationDisseminationQuorum {

  private implicit def preventAllUnknownKeys[T]: ProductHint[T] =
    ProductHint[T](allowUnknownKeys = false)

  // Semi-auto ConfigReader derivations for all nested types. Not using fully automatic derivation at all,
  //  not even just for nested types driving from a semi-auto top-level derivation, even though this approach
  //  would support hints, in order to avoid stack overflow errors in the Scala compiler (probably due to
  //  recursive PureConfig macro generation logic in combination with deep config structures).

  // Canton and CantonBFT config types

  private val configReaders: ConfigReaders = new ConfigReaders()
  import configReaders.*

  private val configWriters: ConfigWriters = new ConfigWriters(confidential = true)
  import configWriters.*

  // UseBftSequencer.TestSlowdownConfig subtypes
  private implicit lazy val postOrderingDelayConfigReader
      : ConfigReader[UseBftSequencer.PostOrderingDelayConfig] =
    deriveReader[UseBftSequencer.PostOrderingDelayConfig]
  private implicit lazy val postOrderingDelayConfigWriter
      : ConfigWriter[UseBftSequencer.PostOrderingDelayConfig] =
    deriveWriter[UseBftSequencer.PostOrderingDelayConfig]

  private implicit lazy val p2pSendDelayConfigEntryReader
      : ConfigReader[UseBftSequencer.P2PSendDelayConfigEntry] =
    deriveReader[UseBftSequencer.P2PSendDelayConfigEntry]
  private implicit lazy val p2pSendDelayConfigEntryWriter
      : ConfigWriter[UseBftSequencer.P2PSendDelayConfigEntry] =
    deriveWriter[UseBftSequencer.P2PSendDelayConfigEntry]

  private implicit lazy val p2pSendDelayConfigReader
      : ConfigReader[UseBftSequencer.P2PSendDelayConfig] =
    deriveReader[UseBftSequencer.P2PSendDelayConfig]
  private implicit lazy val p2pSendDelayConfigWriter
      : ConfigWriter[UseBftSequencer.P2PSendDelayConfig] =
    deriveWriter[UseBftSequencer.P2PSendDelayConfig]

  private implicit lazy val topologyDelayConfigReader
      : ConfigReader[UseBftSequencer.TopologyDelayConfig] =
    deriveReader[UseBftSequencer.TopologyDelayConfig]
  private implicit lazy val topologyDelayConfigWriter
      : ConfigWriter[UseBftSequencer.TopologyDelayConfig] =
    deriveWriter[UseBftSequencer.TopologyDelayConfig]

  private implicit lazy val testSlowdownConfigReader
      : ConfigReader[UseBftSequencer.TestSlowdownConfig] =
    deriveReader[UseBftSequencer.TestSlowdownConfig]
  private implicit lazy val testSlowdownConfigWriter
      : ConfigWriter[UseBftSequencer.TestSlowdownConfig] =
    deriveWriter[UseBftSequencer.TestSlowdownConfig]

  // BftBenchmarkConfig types
  private implicit lazy val transactionSizeAndWeightReader
      : ConfigReader[BftBenchmarkConfig.TransactionSizeAndWeight] =
    deriveReader[BftBenchmarkConfig.TransactionSizeAndWeight]
  private implicit lazy val transactionSizeAndWeightWriter
      : ConfigWriter[BftBenchmarkConfig.TransactionSizeAndWeight] =
    deriveWriter[BftBenchmarkConfig.TransactionSizeAndWeight]

  private implicit lazy val transactionSizesAndWeightsReader
      : ConfigReader[BftBenchmarkConfig.TransactionSizesAndWeights] =
    deriveReader[BftBenchmarkConfig.TransactionSizesAndWeights]
  private implicit lazy val transactionSizesAndWeightsWriter
      : ConfigWriter[BftBenchmarkConfig.TransactionSizesAndWeights] =
    deriveWriter[BftBenchmarkConfig.TransactionSizesAndWeights]

  private implicit lazy val testCatchupReader: ConfigReader[BftBenchmarkConfig.TestCatchup] =
    deriveReader[BftBenchmarkConfig.TestCatchup]
  private implicit lazy val testCatchupWriter: ConfigWriter[BftBenchmarkConfig.TestCatchup] =
    deriveWriter[BftBenchmarkConfig.TestCatchup]

  // Top-level config
  private implicit lazy val bftOrderingBenchmarkConfigReader
      : ConfigReader[BftOrderingBenchmarkConfig] =
    deriveReader[BftOrderingBenchmarkConfig]
  deriveWriter[BftOrderingBenchmarkConfig]

  private val BFTOrderingBenchmarkPrefix = "bft-ordering-benchmark"
  private val PostgresProxyNameSuffix = "postgres"

  private val classpathUriPrefix = "classpath:///"
  private val defaultConfigLocation = s"$classpathUriPrefix$BFTOrderingBenchmarkPrefix.conf"
  private val configSysProp: Option[String] = sys.props.get(s"$BFTOrderingBenchmarkPrefix.config")
  private val (configLocation, isDefault) =
    configSysProp.fold(defaultConfigLocation -> true)(_ -> false)
  private val configSource: ConfigObjectSource =
    if (configLocation.startsWith(classpathUriPrefix))
      ConfigSource.resources(configLocation.stripPrefix(classpathUriPrefix))
    else
      ConfigSource.url(new URI(configLocation).toURL)
  private val configWriter = deriveWriter[BftOrderingBenchmarkConfig]
  private val bftOrderingBenchmarkConfig =
    configSource.load[BftOrderingBenchmarkConfig] match {
      case Right(res) =>
        val configString = configWriter.to(res).render()
        logger.info(
          s"Successfully loaded BFT ordering benchmark config from $configLocation " +
            s"(using default location: $isDefault): $configString"
        )
        res
      case Left(
            ConfigReaderFailures(
              CannotReadUrl(_, None) | CannotReadResource(_, None) | CannotReadFile(_, None),
              tail*,
            )
          ) if isDefault && tail.isEmpty =>
        val res = BftOrderingBenchmarkConfig()
        configWriter.to(res).render()
        logger.info(
          s"No configuration source found at default config location $configLocation, using default config: $res"
        )
        res
      case Left(otherReadFailures) =>
        val errorMsg =
          s"Failed to load BFT ordering benchmark config from $configLocation (using default location: $isDefault)"
        val exception = ConfigReaderException(otherReadFailures)
        logger.error(errorMsg, exception)
        fail(errorMsg, exception)
    }

  registerPlugin(
    new UsePostgres(
      loggerFactory,
      customDbNames = Some((identity, "_bft_ordering_benchmark")),
      customMaxConnectionsByNode =
        Some(_ => Some(bftOrderingBenchmarkConfig.numberOfDbConnectionsPerNode)),
    )
  )

  private val bftSequencerPlugin =
    new UseBftSequencer(
      loggerFactory,
      shouldOverwriteStoredEndpoints = true,
      shouldUseMemoryStorageForBftOrderer =
        bftOrderingBenchmarkConfig.useInMemoryStorageForBftOrderer,
      shouldBenchmarkBftSequencer = true,
      useStandaloneConfig = Some(
        UseStandaloneConfig(
          pbftViewChangeTimeout = bftOrderingBenchmarkConfig.pbftViewChangeTimeout,
          segmentLength = bftOrderingBenchmarkConfig.segmentLength.value,
          blacklistLeaderSelectionPolicyConfig =
            bftOrderingBenchmarkConfig.blacklistLeaderSelectionPolicyConfig,
          maxRequestsInBatch = bftOrderingBenchmarkConfig.maxRequestsInBatch,
          maxBatchesPerBlockProposal = bftOrderingBenchmarkConfig.maxBatchesPerBlockProposal,
          testSlowdown = bftOrderingBenchmarkConfig.testSlowdown,
        )
      ),
      consensusEmptyBlockCreationTimeout =
        bftOrderingBenchmarkConfig.consensusEmptyBlockCreationTimeout.underlying,
      sequencingParameters = Some(
        SequencingParameters.create(
          pbftViewChangeTimeout = bftOrderingBenchmarkConfig.pbftViewChangeTimeout.toInternal,
          segmentLength = SegmentLength(bftOrderingBenchmarkConfig.segmentLength),
          blacklistLeaderSelectionPolicyConfig =
            bftOrderingBenchmarkConfig.blacklistLeaderSelectionPolicyConfig,
          maxRequestsInBatch = bftOrderingBenchmarkConfig.maxRequestsInBatch,
          maxBatchesPerBlockProposal = bftOrderingBenchmarkConfig.maxBatchesPerBlockProposal,
        )(testedProtocolVersion)
      ),
      minRequestsInBatch = bftOrderingBenchmarkConfig.minRequestsInBatch,
      maxBatchCreationInterval = bftOrderingBenchmarkConfig.maxBatchCreationInterval.underlying,
      availabilityMinProposalCreationDelay =
        bftOrderingBenchmarkConfig.availabilityMinProposalCreationDelay.underlying,
      dedicatedExecutionContextDivisor =
        bftOrderingBenchmarkConfig.dedicatedExecutionContextDivisor.map(_.value),
      sequencerCoreSubscriptionConfig = SequencerCoreSubscriptionConfig(
        pekkoQueueSourceBufferSize = bftOrderingBenchmarkConfig.pekkoQueueSourceBufferSize.value,
        pauseOrdererThresholdBufferSize =
          bftOrderingBenchmarkConfig.pauseOrdererThresholdBufferSize.value,
        resumeOrdererThresholdBufferSize =
          bftOrderingBenchmarkConfig.resumeOrdererThresholdBufferSize.value,
      ),
      delayedInitQueueMaxSize = bftOrderingBenchmarkConfig.delayedInitQueueMaxSize,
      epochStateTransferRetryTimeout = bftOrderingBenchmarkConfig.epochStateTransferRetryTimeout,
      outputFetchTimeout = bftOrderingBenchmarkConfig.outputFetchTimeout,
      outputFetchMinimumDelay = bftOrderingBenchmarkConfig.outputFetchMinimumDelay,
      outputFetchTimeoutCap = bftOrderingBenchmarkConfig.outputFetchTimeoutCap,
      outputFetchHowManyRecipients = bftOrderingBenchmarkConfig.outputFetchHowManyRecipients,
      outputEnqueueMaxRetries = bftOrderingBenchmarkConfig.outputEnqueueMaxRetries,
      outputEnqueueMaxRetryDelay = bftOrderingBenchmarkConfig.outputEnqueueMaxRetryDelay,
      sendBlacklistTtl = bftOrderingBenchmarkConfig.sendBlacklistTtl,
      networkSendAttempts = bftOrderingBenchmarkConfig.networkSendAttempts,
      networkSendRetryMinimumDelay = bftOrderingBenchmarkConfig.networkSendRetryMinimumDelay,
      networkSendRetryJitterCap = bftOrderingBenchmarkConfig.networkSendRetryJitterCap,
      p2pServerMaxInboundMessageSize = bftOrderingBenchmarkConfig.p2pServerMaxInboundMessageSize,
      p2pServerFlowControlWindow = bftOrderingBenchmarkConfig.p2pServerFlowControlWindow,
      p2pServerInitialFlowControlWindow =
        bftOrderingBenchmarkConfig.p2pServerInitialFlowControlWindow,
      p2pServerMaxConcurrentCallsPerConnection =
        bftOrderingBenchmarkConfig.p2pServerMaxConcurrentCallsPerConnection,
      p2pServerLimits = bftOrderingBenchmarkConfig.p2pServerLimits,
      p2pServerKeepAliveConfig = bftOrderingBenchmarkConfig.p2pServerKeepAliveConfig,
      p2pClientChannelParams = bftOrderingBenchmarkConfig.p2pClientChannelParams,
    )
  registerPlugin(bftSequencerPlugin)

  // We'll bootstrap one synchronizer with only one mediator (which is the minimum). We do need an environment where
  // the BFT orderer gets initialized, but we don't need it to handle submissions (the standalone mode will take care
  // of submissions).
  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 0,
        numSequencers = bftOrderingBenchmarkConfig.numberOfNodes.value,
        numMediators = 1,
      )
      .clearConfigTransforms() // to disable globally unique ports
      .addConfigTransforms(
        ReplayTestCommon.configTransforms(
          metricsConfigOverride = Some(bftOrderingBenchmarkConfig.metricsConfig)
        )*
      )
      .addConfigTransform(ConfigTransforms.enableNonStandardConfig)
      .addConfigTransform(ConfigTransforms.updateAllSequencerConfigs { case (_, sequencerConfig) =>
        bftOrderingBenchmarkConfig.batchCacheSizeMb.fold(sequencerConfig) { batchSizeInMb =>
          sequencerConfig
            .focus(_.parameters.caching.bftOrderingBatchCache)
            .modify(
              _.copy(maximumMemory = PositiveNumeric.tryCreate(BytesUnit.MB(batchSizeInMb.value)))
            )
        }
      })
      .addConfigTransforms(
        ConfigTransforms.updateAllSequencerConfigs { case (_, sequencerConfig) =>
          bftOrderingBenchmarkConfig.dbReplicationEnabled.fold(sequencerConfig) {
            replicationEnabled =>
              sequencerConfig
                .focus(_.replication)
                .replace(Some(ReplicationConfig(enabled = Some(replicationEnabled))))
          }
        }
      )
      .addConfigTransforms(
        _.focus(_.monitoring.tracing.tracer).replace(
          TracingConfig.Tracer(
            exporter =
              if (bftOrderingBenchmarkConfig.tracingEnabled)
                TracingConfig.Exporter
                  .Otlp(port = bftOrderingBenchmarkConfig.tracingReportingPort.unwrap)
              else TracingConfig.Exporter.Disabled,
            sampler = TracingConfig.Sampler
              .TraceIdRatio(ratio = bftOrderingBenchmarkConfig.tracingSamplerRatio),
          )
        )
      )
      .withNetworkBootstrap { implicit env =>
        import env.*

        logger.info(s"Actual Canton config: ${env.actualConfig.dumpString}")

        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = sequencers.all,
            synchronizerThreshold = PositiveInt.one,
            sequencers = sequencers.all,
            mediators = mediators.all,
            mediatorThreshold = PositiveInt.one,
          )
        )
      }

  val toxiProxyPlugin: Option[UseToxiproxy] =
    if (
      bftOrderingBenchmarkConfig.sequencerToSequencerLatencyMillis.isDefined || bftOrderingBenchmarkConfig.sequencerDbLatencyMillis.isDefined
    ) {
      Some({
        val sequencerToPostgresProxyConfigs: Seq[ProxyConfig] =
          (1 to bftOrderingBenchmarkConfig.numberOfNodes.value).map { sequencerIndex =>
            SequencerToPostgres(
              s"sequencer$sequencerIndex-to-$PostgresProxyNameSuffix",
              s"sequencer$sequencerIndex",
            )
          }
        val sequencerPeerToPeerProxyConfigs: Seq[ProxyConfig] =
          for {
            toSequencerIndex <- (1 to bftOrderingBenchmarkConfig.numberOfNodes.value)
          } yield {
            BftSequencerPeerToPeer(
              s"to-peer$toSequencerIndex",
              s"sequencer$toSequencerIndex",
            )
          }

        new UseToxiproxy(
          ToxiproxyConfig(proxies =
            sequencerToPostgresProxyConfigs ++ sequencerPeerToPeerProxyConfigs
          )
        )
      })
    } else None

  toxiProxyPlugin.foreach(registerPlugin)

  private def addToxics(proxy: toxiproxy.Proxy): Unit = {
    val proxyName = proxy.getName
    val isDbProxy = proxyName.endsWith(PostgresProxyNameSuffix)
    val maybeLatency =
      if (isDbProxy) {
        bftOrderingBenchmarkConfig.sequencerDbLatencyMillis
      } else {
        bftOrderingBenchmarkConfig.sequencerToSequencerLatencyMillis
      }

    maybeLatency.foreach { latency =>
      // Use only upstream (client -> server) latencies to try to avoid issues related to directions in which connections
      //  are established, i.e., we want connections to use Toxiproxy addresses before non-Toxiproxy addresses are used.
      // TODO(#28117) support two-way latencies
      proxy
        .toxics()
        .latency(s"upstream-latency-$proxyName", ToxicDirection.UPSTREAM, latency.value)
    }
  }

  "Run a BFT orderer benchmark" in { implicit env =>
    import env.*

    toxiProxyPlugin.foreach(
      _.runningToxiproxy.controllingToxiproxyClient.getProxies.forEach(addToxics)
    )
    mediators.local.foreach(_.stop())
    sequencers.local.foreach {
      _.bft.pruning
        .set_bft_schedule(
          cron = "0 * * * * ?",
          maxDuration = 15.seconds,
          retention = config.PositiveDurationSeconds.ofSeconds(
            bftOrderingBenchmarkConfig.pruningRetentionPeriodSeconds.value
          ),
          minBlocksToKeep = bftOrderingBenchmarkConfig.pruningMinBlocksToKeepHistory.value,
        )
    }

    // Use a high timeout to allow many nodes in performance testing environments

    waitUntilAllBftSequencersAuthenticateDisseminationQuorum(5.minutes)

    val nodesToStop = env.sequencers.local.zipWithIndex
      .filter(x => bftOrderingBenchmarkConfig.testCatchupConfig.nodesToStop.contains(x._2))
      .map(_._1)

    if (nodesToStop.nonEmpty) {

      nodesToStop.foreach(_.stop())

      env.actorSystem.scheduler.scheduleOnce(
        bftOrderingBenchmarkConfig.testCatchupConfig.durationNodesAreDown
      ) {
        nodesToStop.foreach(_.start())
      }
    }

    val benchmarkTool = new BftBenchmarkTool(new DaBftBindingFactory(loggerFactory), loggerFactory)
    val p2pEndpoints = bftSequencerPlugin.p2pEndpoints.getOrElse(fail("No P2P endpoints found"))
    val benchmarkToolConfig =
      BftBenchmarkConfig(
        transactionSizesAndWeights = bftOrderingBenchmarkConfig.transactionSizesAndWeights.payloads,
        testCatchup = bftOrderingBenchmarkConfig.testCatchupConfig,
        runDuration = bftOrderingBenchmarkConfig.runDuration.underlying,
        perNodeWritePeriod = bftOrderingBenchmarkConfig.perNodeWritePeriod.underlying,
        reportingInterval = bftOrderingBenchmarkConfig.reportingInterval.map(_.underlying),
        nodes = env.sequencers.local.zipWithIndex.map { case (sequencer, idx) =>
          val name = sequencer.name
          val p2pConfig = p2pEndpoints(name)

          val host = p2pConfig.address
          val port = p2pConfig.port.unwrap
          val node: BftBenchmarkConfig.Node =
            if (idx == 0) {
              BftBenchmarkConfig.NetworkedReadWriteNode(
                host = host,
                writePort = port,
                readPort = port,
              )
            } else {
              BftBenchmarkConfig.NetworkedWriteOnlyNode(
                host = host,
                writePort = port,
              )
            }
          node
        },
      )
    benchmarkTool.run(benchmarkToolConfig).discard
  }
}

private object BftOrderingBenchmark {

  /** Configuration for the BFT ordering benchmark test.
    *
    * Loaded via Typesafe Config (pureconfig). See `bft-ordering-benchmark.conf` for a ready-to-use
    * example.
    *
    * Refer to `BftBlockOrdererConfig` and ancillary classes for the meaning of parameters not
    * documented below.
    *
    * @param numberOfNodes
    *   Number of BFT sequencer nodes in the network (default: 4).
    * @param runDuration
    *   Total duration of the benchmark run (default: 1 minute, meant for quick test runs).
    * @param perNodeWritePeriod
    *   Interval between write submissions per node (default: 100ms).
    * @param reportingInterval
    *   How often to print benchmark progress metrics (default: None, i.e., disabled).
    * @param segmentLength
    *   Number of blocks per PBFT consensus segment (default: 10).
    * @param consensusEmptyBlockCreationTimeout
    *   Timeout before an empty block is proposed when there are no pending requests (default: 5s).
    * @param pbftViewChangeTimeout
    *   Timeout before triggering a PBFT view change (default: 10s).
    * @param blacklistLeaderSelectionPolicyConfig
    *   Policy controlling how slow leaders are blacklisted from future segment assignments.
    * @param dedicatedExecutionContextDivisor
    *   Optional divisor for sizing the dedicated execution context thread pool.
    * @param useInMemoryStorageForBftOrderer
    *   If true, uses in-memory storage for the BFT orderer instead of the database (default:
    *   false).
    * @param numberOfDbConnectionsPerNode
    *   Number of database connections allocated per sequencer node (default: 12).
    * @param tracingEnabled
    *   Whether OpenTelemetry tracing is enabled (default: false).
    * @param tracingReportingPort
    *   Port for the OTLP tracing exporter (default: 4317).
    * @param tracingSamplerRatio
    *   Ratio of traces to sample, must be between 0.0 and 1.0 (default: 0.5).
    * @param transactionSizesAndWeights
    *   Distribution of transaction sizes and their relative weights for load generation.
    * @param sequencerToSequencerLatencyMillis
    *   Optional simulated latency (in ms) between sequencer peers via Toxiproxy.
    * @param sequencerDbLatencyMillis
    *   Optional simulated latency (in ms) between sequencers and the database via Toxiproxy.
    * @param dbReplicationEnabled
    *   Whether database replication is enabled (default: Some(true)).
    * @param testCatchupConfig
    *   Catchup configuration used during the benchmark run (default: no catchup).
    * @param pekkoQueueSourceBufferSize
    *   Buffer size for the Pekko queue source in the sequencer core subscription.
    * @param pauseOrdererThresholdBufferSize
    *   Buffer threshold at which the orderer is paused to apply backpressure.
    * @param resumeOrdererThresholdBufferSize
    *   Buffer threshold at which the orderer is resumed after backpressure.
    * @param testSlowdown
    *   Optional configuration to introduce artificial slowdowns in the benchmark run.
    */
  private final case class BftOrderingBenchmarkConfig(
      numberOfNodes: PositiveInt = PositiveInt.tryCreate(4),
      runDuration: PositiveFiniteDuration = PositiveFiniteDuration.tryFromDuration(1.minute),
      perNodeWritePeriod: PositiveFiniteDuration =
        PositiveFiniteDuration.tryFromDuration(100.milliseconds),
      reportingInterval: Option[PositiveFiniteDuration] = None,
      metricsConfig: MetricsConfig =
        ReplayTestCommon.prometheusMetricsConfig(Port.tryCreate(19091)),
      segmentLength: PositiveLong = DefaultSegmentLength.length,
      consensusEmptyBlockCreationTimeout: PositiveFiniteDuration =
        PositiveFiniteDuration.tryFromDuration(DefaultConsensusEmptyBlockCreationTimeout),
      pbftViewChangeTimeout: PositiveFiniteDuration = DefaultPbftViewChangeTimeout.toConfig,
      blacklistLeaderSelectionPolicyConfig: BlacklistLeaderSelectionPolicyConfig =
        DefaultLeaderSelectionPolicyConfig,
      maxRequestsInBatch: Short = DefaultMaxRequestsInBatch,
      minRequestsInBatch: Short = DefaultMinRequestsInBatch,
      maxBatchCreationInterval: PositiveFiniteDuration =
        PositiveFiniteDuration.tryFromDuration(DefaultMaxBatchCreationInterval),
      maxBatchesPerBlockProposal: Short = DefaultMaxBatchesPerProposal,
      availabilityMinProposalCreationDelay: PositiveFiniteDuration =
        PositiveFiniteDuration.tryFromDuration(DefaultAvailabilityMinProposalCreationDelay),
      batchCacheSizeMb: Option[PositiveLong] = None,
      dedicatedExecutionContextDivisor: Option[PositiveInt] = None,
      pruningRetentionPeriodSeconds: PositiveLong = PositiveLong.tryCreate(300), // 5 minutes
      pruningMinBlocksToKeepHistory: PositiveInt = PositiveInt.tryCreate(500),
      useInMemoryStorageForBftOrderer: Boolean = false,
      numberOfDbConnectionsPerNode: PositiveInt = PositiveInt.tryCreate(12),
      delayedInitQueueMaxSize: PositiveInt = PositiveInt.tryCreate(DefaultDelayedInitQueueMaxSize),
      epochStateTransferRetryTimeout: PositiveFiniteDuration =
        PositiveFiniteDuration.tryFromDuration(DefaultEpochStateTransferTimeout),
      outputFetchTimeout: PositiveFiniteDuration =
        PositiveFiniteDuration.tryFromDuration(DefaultOutputFetchTimeout),
      outputFetchMinimumDelay: PositiveFiniteDuration =
        PositiveFiniteDuration.tryFromDuration(DefaultOutputFetchMinimumDelay),
      outputFetchTimeoutCap: PositiveFiniteDuration =
        PositiveFiniteDuration.tryFromDuration(DefaultOutputFetchTimeoutCap),
      outputFetchHowManyRecipients: PositiveInt = DefaultOutputFetchHowManyRecipients,
      outputEnqueueMaxRetries: NonNegativeInt =
        NonNegativeInt.tryCreate(DefaultOutputEnqueueMaxRetries),
      outputEnqueueMaxRetryDelay: PositiveFiniteDuration =
        PositiveFiniteDuration.tryFromDuration(DefaultOutputEnqueueMaxRetryDelay),
      sendBlacklistTtl: PositiveFiniteDuration =
        PositiveFiniteDuration.tryFromDuration(DefaultSendBlacklistTtl),
      networkSendAttempts: PositiveInt = DefaultNetworkSendAttempts,
      networkSendRetryMinimumDelay: PositiveFiniteDuration = DefaultNetworkSendRetryMinimumDelay,
      networkSendRetryJitterCap: PositiveFiniteDuration = DefaultNetworkSendRetryMaximumDelay,
      p2pServerMaxInboundMessageSize: NonNegativeInt = ServerConfig.defaultMaxInboundMessageSize,
      // Keep Canton defaults for P2P server-side flow control, i.e. automatic flow control
      //  with implementation defaults for the initial window size
      p2pServerFlowControlWindow: Option[PositiveInt] = ServerConfig.defaultFlowControlWindow,
      p2pServerInitialFlowControlWindow: Option[PositiveInt] =
        ServerConfig.defaultInitialFlowControlWindow,
      p2pServerMaxConcurrentCallsPerConnection: NonNegativeInt =
        ServerConfig.defaultMaxConcurrentCallsPerConnection,
      p2pServerLimits: Option[ActiveRequestLimitsConfig] = None,
      p2pServerKeepAliveConfig: Option[BasicKeepAliveServerConfig] = Some(
        BasicKeepAliveServerConfig()
      ),
      p2pClientChannelParams: ClientChannelParams =
        ClientChannelParams.Default.copy(flowControlWindow = None),
      tracingEnabled: Boolean = false,
      tracingReportingPort: Port = Port.tryCreate(4317),
      tracingSamplerRatio: Double = 0.5,
      transactionSizesAndWeights: BftBenchmarkConfig.TransactionSizesAndWeights =
        BftBenchmarkConfig.TransactionSizesAndWeights(
          BftBenchmarkConfig.TransactionSizeAndWeight(
            sizeBytes = NonNegativeInt.tryCreate(3000),
            weight = PositiveInt.tryCreate(1),
          ) :: Nil
        ),
      dbReplicationEnabled: Option[Boolean] = Some(true),
      testCatchupConfig: BftBenchmarkConfig.TestCatchup =
        BftBenchmarkConfig.TestCatchup.NoTestCatchup,
      pekkoQueueSourceBufferSize: PositiveInt = PositiveInt.tryCreate(
        DefaultSequencerCoreSubscriptionConfig.pekkoQueueSourceBufferSize
      ),
      pauseOrdererThresholdBufferSize: PositiveInt = PositiveInt.tryCreate(
        DefaultSequencerCoreSubscriptionConfig.pauseOrdererThresholdBufferSize
      ),
      resumeOrdererThresholdBufferSize: PositiveInt = PositiveInt.tryCreate(
        DefaultSequencerCoreSubscriptionConfig.resumeOrdererThresholdBufferSize
      ),
      testSlowdown: Option[UseBftSequencer.TestSlowdownConfig] = None,
      // Toxics
      sequencerToSequencerLatencyMillis: Option[PositiveLong] = None,
      sequencerDbLatencyMillis: Option[PositiveLong] = None,
  ) {
    require(
      tracingSamplerRatio >= 0.0 && tracingSamplerRatio <= 1.0,
      "tracingSamplerRatio must be between 0.0 and 1.0",
    )
    require(
      minRequestsInBatch > 0 && minRequestsInBatch <= maxRequestsInBatch,
      "minRequestsInBatch must be greater than 0 and less than or equal to maxRequestsInBatch",
    )
    require(
      maxBatchesPerBlockProposal > 0,
      "maxBatchesPerBlockProposal must be greater than 0",
    )
  }
}
