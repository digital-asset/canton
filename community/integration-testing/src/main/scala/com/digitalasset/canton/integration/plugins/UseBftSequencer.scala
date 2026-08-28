// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import com.daml.tls.TlsClientConfig
import com.digitalasset.canton
import com.digitalasset.canton.UniquePortGenerator
import com.digitalasset.canton.admin.api.client.data.SequencingParameters
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.StorageConfig.Memory
import com.digitalasset.canton.config.{
  ActiveRequestLimitsConfig,
  BasicKeepAliveServerConfig,
  CantonConfig,
  PositiveFiniteDuration,
  QueryCostMonitoringConfig,
  ServerConfig,
}
import com.digitalasset.canton.crypto.provider.jce.JcePrivateCrypto
import com.digitalasset.canton.crypto.{Fingerprint, SigningKeySpec, SigningKeyUsage}
import com.digitalasset.canton.integration.plugins.UseBftSequencer.UseStandaloneConfig
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.{
  MultiSynchronizer,
  SequencerSynchronizerGroups,
  SingleSynchronizer,
}
import com.digitalasset.canton.integration.{EnvironmentSetupPlugin, TestConsoleEnvironment}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.grpc.ClientChannelParams
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig.BftSequencer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.BftBlockOrderingP2PSendDelayConfig.DelayByRecipients
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.{
  BftBlockOrderingP2PSendDelayConfig,
  BftBlockOrderingStandalonePeerConfig,
  BftBlockOrderingStandaloneTopologyDelayConfig,
  DefaultDedicatedExecutionContextDivisor,
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
  P2PNetworkConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.BlacklistLeaderSelectionPolicyConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.FiniteDurationDistribution
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeConfig
import com.digitalasset.canton.synchronizer.sequencer.{
  BlockSequencerConfig,
  BlockSequencerStreamInstrumentationConfig,
  SequencerConfig,
}
import com.digitalasset.canton.topology.{Namespace, SequencerId}
import com.digitalasset.canton.util.SingleUseCell
import monocle.macros.GenLens
import monocle.macros.syntax.lens.*
import org.scalatest.EitherValues

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** Plugin that rewrites the configuration to use CantonBFT as sequencer implementation. Parameters
  * not mentioned below override the corresponding CantonBFT configuration.
  *
  * @param sequencerGroups
  *   Configures whether more than one synchronizer is being used and, in that case, which sequencer
  *   belongs to which synchronizer.
  * @param dynamicallyOnboardedSequencerNames
  *   Names of sequencers that are not part of the initial network config, and can be added later as
  *   part of a test.
  * @param shouldGenerateEndpointsOnly
  *   If true, replaces addresses and ports only (instead of building a full config) to avoid their
  *   clashes. Useful for config file integration tests.
  * @param shouldOverwriteStoredEndpoints
  *   Set to true to overwrite peer endpoints in the database with config, e.g., when using a
  *   database dump.
  * @param shouldUseMemoryStorageForBftOrderer
  *   Overwrites the dedicated BFT Orderer's storage to in-memory.
  * @param useStandaloneConfig
  *   Enable standalone BFT ordering nodes mode.
  */
final class UseBftSequencer(
    override protected val loggerFactory: NamedLoggerFactory,
    val sequencerGroups: SequencerSynchronizerGroups = SingleSynchronizer,
    dynamicallyOnboardedSequencerNames: Seq[InstanceName] = Seq.empty,
    shouldGenerateEndpointsOnly: Boolean = false,
    shouldOverwriteStoredEndpoints: Boolean = false,
    shouldUseMemoryStorageForBftOrderer: Boolean = false,
    shouldBenchmarkBftSequencer: Boolean = false,
    useStandaloneConfig: Option[UseStandaloneConfig] = None,
    // Use a shorter empty block creation timeout by default to speed up tests that stop sequencing
    //  and use `GetTime` to await an effective time to be reached on the synchronizer.
    consensusEmptyBlockCreationTimeout: FiniteDuration = 250.millis,
    // Use a longer topology warn timeout in tests to avoid flakes under concurrent CI load.
    consensusNewEpochTopologyWarnTimeout: FiniteDuration = 10.seconds,
    sequencingParameters: Option[topology.SequencingParameters] = None,
    minRequestsInBatch: Short = DefaultMinRequestsInBatch,
    maxBatchCreationInterval: FiniteDuration = DefaultMaxBatchCreationInterval,
    availabilityMinProposalCreationDelay: FiniteDuration = 50.millis,
    dedicatedExecutionContextDivisor: Option[Int] = DefaultDedicatedExecutionContextDivisor,
    sequencerCoreSubscriptionConfig: BftBlockOrdererConfig.SequencerCoreSubscriptionConfig =
      BftBlockOrdererConfig.DefaultSequencerCoreSubscriptionConfig,
    viewChangeTimeoutOverride: Option[FiniteDuration] = None,
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
) extends EnvironmentSetupPlugin
    with EitherValues {

  private val tmpDir = better.files.File(System.getProperty("java.io.tmpdir"))

  val p2pEndpoints: SingleUseCell[Map[InstanceName, BftBlockOrdererConfig.P2PEndpointConfig]] =
    new SingleUseCell()

  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig =
    if (shouldGenerateEndpointsOnly) generateEndpoints(config, sequencingParameters)
    else createFullConfig(config, sequencingParameters)

  override def afterEnvironmentCreated(
      config: CantonConfig,
      environment: TestConsoleEnvironment,
  ): Unit =
    sequencingParameters.foreach { sequencingParameters =>
      val sequencingParametersByteString = sequencingParameters.toByteString
      // > PV34: take the sequencer parameters from the topology.
      //  This only works if the environment is automatically initialized,
      //  else the test must do it manually.
      environment.runOnAllInitializedSynchronizersForAllOwners { case (owner, synchronizer) =>
        owner.topology.sequencing_parameters.propose(
          synchronizer.synchronizerId,
          SequencingParameters(Option(sequencingParametersByteString)),
        )
      }
    }

  // Only called if `shouldGenerateEndpointsOnly` is true
  private def generateEndpoints(
      config: CantonConfig,
      sequencingParameters: Option[topology.SequencingParameters],
  ): CantonConfig = {
    val instanceNameToPort =
      config.sequencers.keys.map(_ -> UniquePortGenerator.next).toMap

    val sequencers =
      config.sequencers.map { case (selfInstanceName, sequencerNodeConfig) =>
        val otherInitialNames = config.sequencers.keys.filterNot(_ == selfInstanceName).toSeq
        val standaloneOpt = createStandaloneConfig(selfInstanceName, otherInitialNames)
        val sequencer =
          sequencerNodeConfig.sequencer match {
            case BftSequencer(blockSequencerConfig, bftOrdererConfig) =>
              // When adding overrides from this plugin, also update `createFullConfig`
              BftSequencer(
                blockSequencerConfig,
                bftOrdererConfig
                  .copy(
                    leaderSelectionPolicyConfigForPv34 = getLeaderSelectionPolicyConfigForPv34(
                      sequencingParameters,
                      bftOrdererConfig,
                    ),
                    consensusEmptyBlockCreationTimeout = consensusEmptyBlockCreationTimeout,
                    consensusNewEpochTopologyWarnTimeout = consensusNewEpochTopologyWarnTimeout,
                    minRequestsInBatch = minRequestsInBatch,
                    maxBatchCreationInterval = maxBatchCreationInterval,
                    availabilityMinProposalCreationDelay = availabilityMinProposalCreationDelay,
                    dedicatedExecutionContextDivisor = dedicatedExecutionContextDivisor,
                    standalone = standaloneOpt,
                    storage = Option.when(shouldUseMemoryStorageForBftOrderer)(Memory()),
                    sequencerCoreSubscriptionConfig = sequencerCoreSubscriptionConfig,
                    viewChangeTimeoutOverride = viewChangeTimeoutOverride,
                    delayedInitQueueMaxSize = delayedInitQueueMaxSize.value,
                    epochStateTransferRetryTimeout = epochStateTransferRetryTimeout.underlying,
                    outputFetchTimeout = outputFetchTimeout.underlying,
                    outputFetchMinimumDelay = outputFetchMinimumDelay.underlying,
                    outputFetchTimeoutCap = outputFetchTimeoutCap.underlying,
                    outputFetchHowManyRecipients = outputFetchHowManyRecipients,
                    outputEnqueueMaxRetries = outputEnqueueMaxRetries.value,
                    outputEnqueueMaxRetryDelay = outputEnqueueMaxRetryDelay.underlying,
                    sendBlacklistTtl = sendBlacklistTtl.underlying,
                    networkSendAttempts = networkSendAttempts,
                    networkSendRetryMinimumDelay = networkSendRetryMinimumDelay,
                    networkSendRetryJitterCap = networkSendRetryJitterCap,
                  )
                  // server endpoint's lens
                  .focus(_.initialNetwork)
                  .some
                  .andThen(GenLens[P2PNetworkConfig](_.serverEndpoint))
                  .modify(
                    _.focus(_.address)
                      .replace("localhost")
                      .focus(_.internalPort)
                      .replace(Some(instanceNameToPort(selfInstanceName)))
                      .focus(_.externalAddress)
                      .replace("localhost")
                      .focus(_.externalPort)
                      .replace(instanceNameToPort(selfInstanceName))
                      .focus(_.maxInboundMessageSize)
                      .replace(p2pServerMaxInboundMessageSize)
                      .focus(_.flowControlWindow)
                      .replace(p2pServerFlowControlWindow)
                      .focus(_.initialFlowControlWindow)
                      .replace(p2pServerInitialFlowControlWindow)
                      .focus(_.maxConcurrentCallsPerConnection)
                      .replace(p2pServerMaxConcurrentCallsPerConnection)
                      .focus(_.limits)
                      .replace(p2pServerLimits)
                      .focus(_.keepAliveServer)
                      .replace(p2pServerKeepAliveConfig)
                  )
                  // peer endpoints' lens
                  .focus(_.initialNetwork)
                  .some
                  .andThen(GenLens[P2PNetworkConfig](_.peerEndpoints))
                  .modify { peerEndpoints =>
                    val otherPeerPorts =
                      instanceNameToPort.filterNot { case (name, _) => name == selfInstanceName }
                    peerEndpoints
                      .zip(otherPeerPorts.values)
                      .map { case (p2pEndpointConfig, port) =>
                        p2pEndpointConfig
                          .focus(_.address)
                          .replace("localhost")
                          .focus(_.port)
                          .replace(port)
                          .focus(_.channel)
                          .replace(p2pClientChannelParams)
                      }
                  },
              )

            case otherSequencerConfig => otherSequencerConfig
          }

        selfInstanceName -> sequencerNodeConfig.focus(_.sequencer).replace(sequencer)
      }

    config.focus(_.sequencers).replace(sequencers)
  }

  // Only called if `shouldGenerateEndpointsOnly` is false
  private def createFullConfig(
      config: CantonConfig,
      sequencingParameters: Option[topology.SequencingParameters],
  ): CantonConfig = {
    // Contains all sequencers from the environment definition. Typically, the environment definition also contains
    //  sequencers that are onboarded dynamically by tests (i.e, not initialized from the very beginning).
    val groups = sequencerGroups match {
      case MultiSynchronizer(groups) => groups
      case SingleSynchronizer => Seq(config.sequencers.keys)
    }
    val sequencersToEndpoints: mutable.Map[InstanceName, BftBlockOrdererConfig.P2PEndpointConfig] =
      mutable.Map()
    val sequencersToConfig: Map[InstanceName, SequencerConfig] =
      groups.flatMap { group =>
        val endpoints = group.map { sequencerName =>
          sequencerName -> BftBlockOrdererConfig.P2PEndpointConfig(
            "localhost",
            UniquePortGenerator.next,
            Some(TlsClientConfig(trustCollectionFile = None, clientCert = None, enabled = false)),
          )
        }.toMap
        endpoints.map { case (selfInstanceName, selfEndpoint) =>
          sequencersToEndpoints.addOne(
            selfInstanceName -> selfEndpoint.focus(_.channel).replace(p2pClientChannelParams)
          )
          val otherInitialNamesAndEndpoints =
            if (dynamicallyOnboardedSequencerNames.contains(selfInstanceName))
              // Dynamically onboarded peers' endpoints are not part of the initial network but are added later
              //  by the concrete test case.
              Seq.empty
            else
              endpoints.view.filterNot { case (name, _) =>
                name == selfInstanceName || dynamicallyOnboardedSequencerNames.contains(name)
              }.toSeq
          val (otherInitialNames, otherInitialEndpoints) = otherInitialNamesAndEndpoints.unzip
          val network = BftBlockOrdererConfig.P2PNetworkConfig(
            BftBlockOrdererConfig.P2PServerConfig(
              selfEndpoint.address,
              internalPort = Some(selfEndpoint.port),
              externalAddress = selfEndpoint.address,
              externalPort = selfEndpoint.port,
              externalTlsConfig = Some(
                TlsClientConfig(trustCollectionFile = None, clientCert = None, enabled = false)
              ),
              maxInboundMessageSize = p2pServerMaxInboundMessageSize,
              flowControlWindow = p2pServerFlowControlWindow,
              initialFlowControlWindow = p2pServerInitialFlowControlWindow,
              maxConcurrentCallsPerConnection = p2pServerMaxConcurrentCallsPerConnection,
              limits = p2pServerLimits,
              keepAliveServer = p2pServerKeepAliveConfig,
            ),
            peerEndpoints = otherInitialEndpoints.map(
              _.focus(_.channel).replace(p2pClientChannelParams)
            ),
            overwriteStoredEndpoints = shouldOverwriteStoredEndpoints,
          )
          val standaloneOpt = createStandaloneConfig(selfInstanceName, otherInitialNames)
          val blockSequencerConfig = {
            // without this config overrides (which are applied before plugins) are not preserved
            val existingBlockSequencerConfig =
              config.sequencers.get(selfInstanceName).map(_.sequencer) match {
                case Some(bft: SequencerConfig.BftSequencer) => bft.block
                case Some(external: SequencerConfig.External) => external.block
                case _ => BlockSequencerConfig()
              }
            if (shouldBenchmarkBftSequencer)
              existingBlockSequencerConfig.copy(
                circuitBreaker = BlockSequencerConfig.CircuitBreakerConfig(enabled = false),
                streamInstrumentation = BlockSequencerStreamInstrumentationConfig(isEnabled = true),
              )
            else existingBlockSequencerConfig
          }
          // When adding overrides from this plugin, also update `generateEndpoints`
          selfInstanceName -> SequencerConfig.BftSequencer(
            block = blockSequencerConfig,
            config = BftBlockOrdererConfig(
              initialNetwork = Some(network),
              leaderSelectionPolicyConfigForPv34 = getLeaderSelectionPolicyConfigForPv34(
                sequencingParameters,
                BftBlockOrdererConfig(),
              ),
              consensusEmptyBlockCreationTimeout = consensusEmptyBlockCreationTimeout,
              consensusNewEpochTopologyWarnTimeout = consensusNewEpochTopologyWarnTimeout,
              minRequestsInBatch = minRequestsInBatch,
              maxBatchCreationInterval = maxBatchCreationInterval,
              availabilityMinProposalCreationDelay = availabilityMinProposalCreationDelay,
              dedicatedExecutionContextDivisor = dedicatedExecutionContextDivisor,
              standalone = standaloneOpt,
              storage = Option.when(shouldUseMemoryStorageForBftOrderer)(Memory()),
              sequencerCoreSubscriptionConfig = sequencerCoreSubscriptionConfig,
              viewChangeTimeoutOverride = viewChangeTimeoutOverride,
              delayedInitQueueMaxSize = delayedInitQueueMaxSize.value,
              epochStateTransferRetryTimeout = epochStateTransferRetryTimeout.underlying,
              outputFetchTimeout = outputFetchTimeout.underlying,
              outputFetchMinimumDelay = outputFetchMinimumDelay.underlying,
              outputFetchTimeoutCap = outputFetchTimeoutCap.underlying,
              outputFetchHowManyRecipients = outputFetchHowManyRecipients,
              outputEnqueueMaxRetries = outputEnqueueMaxRetries.value,
              outputEnqueueMaxRetryDelay = outputEnqueueMaxRetryDelay.underlying,
              sendBlacklistTtl = sendBlacklistTtl.underlying,
              networkSendAttempts = networkSendAttempts,
              networkSendRetryMinimumDelay = networkSendRetryMinimumDelay,
              networkSendRetryJitterCap = networkSendRetryJitterCap,
            ),
          )
        }
      }.toMap

    def mapSequencerConfigs(
        kv: (InstanceName, SequencerNodeConfig)
    ): (InstanceName, SequencerNodeConfig) = kv match {
      case (name, cfg) =>
        (
          name,
          cfg.focus(_.sequencer).replace(sequencersToConfig(name)),
        )
    }

    p2pEndpoints.putIfAbsent(sequencersToEndpoints.toMap)
    config
      .focus(_.monitoring.logging.queryCost)
      .modify {
        case None =>
          Option.when(shouldBenchmarkBftSequencer)(
            QueryCostMonitoringConfig(every = canton.config.NonNegativeFiniteDuration.ofSeconds(30))
          )
        case other => other
      }
      .focus(_.sequencers)
      .modify(_.map(mapSequencerConfigs))
  }

  private def createStandaloneConfig(
      selfInstanceName: InstanceName,
      otherInitialNames: Seq[InstanceName],
  ): Option[BftBlockOrdererConfig.BftBlockOrderingStandaloneNetworkConfig] =
    useStandaloneConfig.map { standaloneConfig =>
      val keyPair = JcePrivateCrypto
        .generateSigningKeypair(SigningKeySpec.EcCurve25519, SigningKeyUsage.ProtocolOnly)
        .getOrElse(throw new RuntimeException("Failed to generate keypair"))
      val privKey = keyPair.privateKey
      val pubKey = keyPair.publicKey
      val privKeyFile = tmpDir / s"node-${selfInstanceName}_signing_private_key.bin"
      val pubKeyFile = tmpDir / s"node-${selfInstanceName}_signing_public_key.bin"
      privKeyFile.writeByteArray(privKey.toProtoV30.value.toByteArray)
      pubKeyFile.writeByteArray(pubKey.toProtoV30.value.toByteArray)
      val suffixDigits = getSuffixDigits(selfInstanceName.unwrap)
      val postOrderingDelayConfigO =
        standaloneConfig.testSlowdown.flatMap(_.postOrderingDelay).flatMap { config =>
          Option.when(
            suffixDigits.nonEmpty && config.nodesToDelay
              .contains(PositiveInt.tryCreate(suffixDigits.toInt))
          )(config.delay)
        }
      val p2pSendDelayConfigO =
        standaloneConfig.testSlowdown.flatMap(_.p2pSendDelay).flatMap { config =>
          config.entries
            .find(_.sources.contains(PositiveInt.tryCreate(suffixDigits.toInt)))
            .map(_.config)
            .map(instanceIndexToName(otherInitialNames.map(_.unwrap), _))
        }
      val topologyDelayConfig = standaloneConfig.testSlowdown
        .flatMap(_.topologyDelay)
        .flatMap { config =>
          Option.when(
            suffixDigits.nonEmpty && config.nodesToDelay
              .contains(PositiveInt.tryCreate(suffixDigits.toInt))
          )(config.delay)
        }
      val testSlowdownConfigO = if (
        postOrderingDelayConfigO.isDefined || p2pSendDelayConfigO.isDefined || topologyDelayConfig.isDefined
      )
        Some(
          BftBlockOrdererConfig.BftBlockOrderingStandaloneTestSlowdownConfig(
            postOrderingDelay = postOrderingDelayConfigO,
            sendDelay = p2pSendDelayConfigO,
            topologyDelay = topologyDelayConfig,
          )
        )
      else None
      BftBlockOrdererConfig.BftBlockOrderingStandaloneNetworkConfig(
        thisSequencerId = standaloneSequencerId(selfInstanceName),
        signingPrivateKeyProtoFile = privKeyFile.toJava,
        signingPublicKeyProtoFile = pubKeyFile.toJava,
        segmentLength = standaloneConfig.segmentLength,
        pbftViewChangeTimeout = standaloneConfig.pbftViewChangeTimeout.underlying,
        blacklistLeaderSelectionPolicyConfig =
          standaloneConfig.blacklistLeaderSelectionPolicyConfig,
        maxRequestsInBatch = standaloneConfig.maxRequestsInBatch,
        maxBatchesPerBlockProposal = standaloneConfig.maxBatchesPerBlockProposal,
        peers = otherInitialNames
          .map { otherInitialInstanceName =>
            BftBlockOrderingStandalonePeerConfig(
              sequencerId = standaloneSequencerId(otherInitialInstanceName),
              signingPublicKeyProtoFile =
                tmpDir / s"node-${otherInitialInstanceName}_signing_public_key.bin" toJava,
            )
          },
        testSlowdown = testSlowdownConfigO,
      )
    }

  private def instanceIndexToName(
      otherInstanceNames: Seq[String],
      config: BftBlockOrderingP2PSendDelayConfig,
  ): BftBlockOrderingP2PSendDelayConfig =
    config.copy(delaysByRecipients = config.delaysByRecipients.map {
      case DelayByRecipients(sources, delayDistribution) =>
        DelayByRecipients(
          sources.flatMap(idx => otherInstanceNames.find(oin => idx == getSuffixDigits(oin))),
          delayDistribution,
        )
    })

  private def getLeaderSelectionPolicyConfigForPv34(
      sequencingParameters: Option[topology.SequencingParameters],
      bftOrdererConfig: BftBlockOrdererConfig,
  ): Option[BlacklistLeaderSelectionPolicyConfig] =
    sequencingParameters
      .map(_.blacklistLeaderSelectionPolicyConfig)
      .orElse(bftOrdererConfig.leaderSelectionPolicyConfigForPv34)

  private def standaloneSequencerId(instanceName: InstanceName): String =
    SequencerId
      .tryCreate(instanceName.unwrap, Namespace(Fingerprint.tryFromString("default")))
      .toProtoPrimitive

  private def getSuffixDigits(s: String): String =
    s.reverse.takeWhile(_.isDigit).reverse
}

object UseBftSequencer {

  final case class PostOrderingDelayConfig(
      nodesToDelay: Set[PositiveInt],
      delay: FiniteDurationDistribution,
  )

  final case class P2PSendDelayConfigEntry(
      sources: Seq[PositiveInt],
      config: BftBlockOrderingP2PSendDelayConfig,
  )

  final case class P2PSendDelayConfig(entries: Seq[P2PSendDelayConfigEntry])

  final case class TopologyDelayConfig(
      nodesToDelay: Set[PositiveInt],
      delay: BftBlockOrderingStandaloneTopologyDelayConfig,
  )

  /** Configuration to simulate artificial slowdowns in standalone mode for testing purposes.
    *
    * @param postOrderingDelay
    *   Optional artificial delay applied after ordering on specified nodes.
    * @param p2pSendDelay
    *   Optional per-source P2P send delay configuration.
    */
  final case class TestSlowdownConfig(
      postOrderingDelay: Option[PostOrderingDelayConfig],
      p2pSendDelay: Option[P2PSendDelayConfig],
      topologyDelay: Option[TopologyDelayConfig],
  )

  final case class UseStandaloneConfig(
      pbftViewChangeTimeout: PositiveFiniteDuration,
      segmentLength: Long,
      blacklistLeaderSelectionPolicyConfig: BlacklistLeaderSelectionPolicyConfig,
      maxRequestsInBatch: Short,
      maxBatchesPerBlockProposal: Short,
      testSlowdown: Option[TestSlowdownConfig],
  )
}
