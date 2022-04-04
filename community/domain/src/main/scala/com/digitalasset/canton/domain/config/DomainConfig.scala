// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.config

import com.digitalasset.canton.config.RequireTypes.{ExistingFile, NonNegativeInt, Port}
import com.digitalasset.canton.config._
import com.digitalasset.canton.domain.sequencing.sequencer.CommunitySequencerConfig
import com.digitalasset.canton.domain.topology.RequestProcessingStrategyConfig
import com.digitalasset.canton.networking.grpc.CantonServerBuilder
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.time.{DomainTimeTrackerConfig, NonNegativeFiniteDuration}
import com.digitalasset.canton.tracing.TracingConfig
import io.netty.handler.ssl.SslContext
import monocle.macros.syntax.lens._

import java.io.File

/** The public server configuration ServerConfig used by the domain.
  *
  * TODO(i4056): Client authentication over TLS is currently unsupported,
  *  because there is a token based protocol to authenticate clients. This may change in the future.
  */
trait PublicServerConfig extends ServerConfig {

  def tls: Option[TlsBaseServerConfig]

  /** Expiration time for a nonce that is generated for an
    * authentication challenge. as an authentication request is
    * expected to be followed up with almost immediately to generate
    * an authentication token the nonce expiry should be short. the
    * nonce is automatically invalided on use.
    */
  def nonceExpirationTime: NonNegativeFiniteDuration

  /** Expiration time for authentication tokens. Tokens are used to authenticate participants.
    * Choose a shorter time for better security and a longer time for better performance.
    */
  def tokenExpirationTime: NonNegativeFiniteDuration

  lazy val clientConfig: ClientConfig =
    ClientConfig(address, port, tls.map(c => TlsClientConfig(Some(c.certChainFile), None)))

  override def sslContext: Option[SslContext] = tls.map(CantonServerBuilder.baseSslContext)

  override def serverCertChainFile: Option[ExistingFile] = tls.map(_.certChainFile)

  /** This setting has no effect. Therfore hardcoding it to 0.
    */
  override final def maxInboundMessageSize: NonNegativeInt = NonNegativeInt.tryCreate(0)

  def connection: String = {
    val scheme = tls.fold("http")(_ => "https")
    s"$scheme://$address:$port"
  }
}

case class CommunityPublicServerConfig(
    override val address: String = "127.0.0.1",
    override val internalPort: Option[Port] = None,
    override val tls: Option[TlsBaseServerConfig] = None,
    override val keepAliveServer: Option[KeepAliveServerConfig] = Some(KeepAliveServerConfig()),
    override val nonceExpirationTime: NonNegativeFiniteDuration =
      NonNegativeFiniteDuration.ofMinutes(1),
    override val tokenExpirationTime: NonNegativeFiniteDuration =
      NonNegativeFiniteDuration.ofHours(1),
) extends PublicServerConfig
    with CommunityServerConfig

case class DomainNodeParameters(
    tracing: TracingConfig,
    delayLoggingThreshold: NonNegativeFiniteDuration,
    loggingConfig: LoggingConfig,
    logQueryCost: Option[QueryCostMonitoringConfig],
    enableAdditionalConsistencyChecks: Boolean,
    enablePreviewFeatures: Boolean,
    processingTimeouts: ProcessingTimeout,
    sequencerClient: SequencerClientConfig,
    cachingConfigs: CachingConfigs,
    nonStandardConfig: Boolean,
) extends LocalNodeParameters

trait DomainBaseConfig extends LocalNodeConfig {

  /** determines how this node is initialized */
  def init: InitConfig

  /** if enabled, selected events will be logged by the ParticipantAuditor class */
  def auditLogging: Boolean

  /** parameters of the interface used for administrating the domain */
  def adminApi: AdminServerConfig

  /** determines how the domain stores received messages and state */
  def storage: StorageConfig

  /** determines the crypto provider used for signing, hashing, and encryption and its configuration */
  def crypto: CryptoConfig

  /** determines how the domain performs topology management */
  def topology: TopologyConfig

  /** misc global parameters */
  def domainParameters: DomainParametersConfig

  /** Configuration of parameters related to time tracking using the domain sequencer. Used by the IDM and optionally the mediator and sequencer components. */
  def timeTracker: DomainTimeTrackerConfig

  override def clientAdminApi: ClientConfig = adminApi.clientConfig
}

/** Configuration parameters for a single domain. */
trait DomainConfig extends DomainBaseConfig {

  override val nodeTypeName: String = "domain"

  /** parameters of the interface used to communicate with participants */
  def publicApi: PublicServerConfig

  /** location of the service agreement of the domain (if any) */
  def serviceAgreement: Option[File]

  def sequencerConnectionConfig: SequencerConnectionConfig.Grpc =
    publicApi.toSequencerConnectionConfig

  def toRemoteConfig: RemoteDomainConfig = RemoteDomainConfig(
    adminApi = adminApi.clientConfig,
    publicApi = sequencerConnectionConfig,
    crypto = crypto,
  )
}

final case class CommunityDomainConfig(
    override val init: InitConfig = InitConfig(),
    override val auditLogging: Boolean = false,
    override val publicApi: CommunityPublicServerConfig = CommunityPublicServerConfig(),
    override val adminApi: CommunityAdminServerConfig = CommunityAdminServerConfig(),
    override val storage: CommunityStorageConfig = CommunityStorageConfig.Memory(),
    override val crypto: CryptoConfig = CryptoConfig(),
    override val topology: TopologyConfig = TopologyConfig(),
    override val domainParameters: DomainParametersConfig = DomainParametersConfig(),
    sequencer: CommunitySequencerConfig.Database = CommunitySequencerConfig.Database(),
    override val serviceAgreement: Option[File] = None,
    override val timeTracker: DomainTimeTrackerConfig = DomainTimeTrackerConfig(),
    override val sequencerClient: SequencerClientConfig = SequencerClientConfig(),
    override val caching: CachingConfigs = CachingConfigs(),
) extends DomainConfig
    with CommunityLocalNodeConfig
    with ConfigDefaults[CommunityDomainConfig] {

  override def withDefaults: CommunityDomainConfig = {
    import ConfigDefaults._
    this
      .focus(_.publicApi.internalPort)
      .modify(domainPublicApiPort.setDefaultPort)
      .focus(_.adminApi.internalPort)
      .modify(domainAdminApiPort.setDefaultPort)
  }
}

/** Configuration parameters to connect to a domain running remotely
  *
  * @param adminApi the client settings used to connect to the admin api of the remote process.
  * @param publicApi these details are provided to other nodes to use for how they should
  *                            connect to the sequencer if the domain node has an embedded sequencer
  * @param crypto determines the algorithms used for signing, hashing, and encryption, used
  *               on the client side for serialization.
  */
final case class RemoteDomainConfig(
    adminApi: ClientConfig,
    publicApi: SequencerConnectionConfig.Grpc,
    crypto: CryptoConfig = CryptoConfig(),
) extends NodeConfig {
  override def clientAdminApi: ClientConfig = adminApi
}

/** Configuration parameters for the domain topology manager.
  *
  * @param requireParticipantCertificate requires a participant to provide a certificate of its identity before being added to the domain.
  * @param permissioning determines whether/how the topology manager approves topology transactions
  */
final case class TopologyConfig(
    requireParticipantCertificate: Boolean = false,
    permissioning: RequestProcessingStrategyConfig =
      RequestProcessingStrategyConfig.AutoApprove(true),
)
