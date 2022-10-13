// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.config

import com.daml.jwt.JwtTimestampLeeway
import com.daml.ledger.api.tls.{SecretsUrl, TlsConfiguration, TlsVersion}
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.apiserver.configuration.RateLimitingConfig
import com.daml.platform.apiserver.{ApiServerConfig as DamlApiServerConfig}
import com.daml.platform.configuration.{
  CommandConfiguration,
  IndexServiceConfig as DamlIndexServiceConfig,
}
import com.daml.platform.indexer.ha.HaConfig
import com.daml.platform.indexer.{
  IndexerConfig as DamlIndexerConfig,
  IndexerStartupMode,
  PackageMetadataViewConfig,
}
import com.daml.platform.store.DbSupport.{DataSourceProperties as DamlDataSourceProperties}
import com.daml.platform.store.backend.postgresql.{
  PostgresDataSourceConfig as DamlPostgresDataSourceConfig
}
import com.daml.platform.usermanagement.UserManagementConfig
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.DeprecatedConfigUtils.DeprecatedFieldsFor
import com.digitalasset.canton.config.LocalNodeConfig.LocalNodeConfigDeprecationImplicits
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt.*
import com.digitalasset.canton.config.RequireTypes.*
import com.digitalasset.canton.config.*
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.networking.grpc.CantonServerBuilder
import com.digitalasset.canton.participant.admin.AdminWorkflowConfig
import com.digitalasset.canton.participant.config.PostgresDataSourceConfigCanton.{
  DefaultPostgresTcpKeepalivesCount,
  DefaultPostgresTcpKeepalivesIdle,
  DefaultPostgresTcpKeepalivesInterval,
  SynchronousCommitValue,
}
import com.digitalasset.canton.participant.ledger.api.CantonLedgerApiServerWrapper.IndexerLockIds
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.time.NonNegativeFiniteDuration.*
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.version.{ParticipantProtocolVersion, ProtocolVersion}
import io.netty.handler.ssl.{ClientAuth, SslContext}
import io.scalaland.chimney.dsl.*
import monocle.macros.syntax.lens.*

import java.security.InvalidParameterException
import scala.jdk.DurationConverters.*

/** Base for all participant configs - both local and remote */
trait BaseParticipantConfig extends NodeConfig {
  def clientLedgerApi: ClientConfig
}

object LocalParticipantConfig {

  // TODO(i10108): remove when backwards compatibility can be discarded
  /** Adds deprecations specific to LocalParticipantConfig
    * We need to manually combine it with the upstream deprecations from LocalNodeConfig
    * in order to not lose them.
    */
  trait LocalParticipantDeprecationsImplicits extends LocalNodeConfigDeprecationImplicits {
    implicit def deprecatedLocalParticipantConfig[X <: LocalParticipantConfig]
        : DeprecatedFieldsFor[X] =
      new DeprecatedFieldsFor[LocalParticipantConfig] {
        override def movedFields: List[DeprecatedConfigUtils.MovedConfigPath] = List(
          DeprecatedConfigUtils.MovedConfigPath(
            "ledger-api.max-deduplication-duration",
            "init.ledger-api.max-deduplication-duration",
          ),
          DeprecatedConfigUtils.MovedConfigPath(
            "parameters.unique-contract-keys",
            "init.parameters.unique-contract-keys",
          ),
          DeprecatedConfigUtils.MovedConfigPath(
            "parameters.enable-causality-tracking",
            "init.parameters.unsafe-enable-causality-tracking",
          ),
        ) ++ deprecatedLocalNodeConfig.movedFields
      }
  }
}

/** Base for local participant configurations */
trait LocalParticipantConfig extends BaseParticipantConfig with LocalNodeConfig {
  override val nodeTypeName: String = "participant"

  /** determines how this node is initialized */
  def init: ParticipantInitConfig

  /** determines the algorithms used for signing, hashing, and encryption */
  def crypto: CryptoConfig

  /** parameters of the interfaces that applications use to change and query the ledger */
  def ledgerApi: LedgerApiServerConfig

  /** parameters of the interface used to administrate the participant */
  def adminApi: AdminServerConfig

  /** determines how the participant stores the ledger */
  def storage: StorageConfig

  /** determines whether and how to support the ledger API time service */
  def testingTime: Option[TestingTimeServiceConfig]

  /** general participant node parameters */
  def parameters: ParticipantNodeParameterConfig

  def toRemoteConfig: RemoteParticipantConfig
}

/** How eagerly participants make the ledger api aware of added (and eventually changed or removed) parties */
sealed trait PartyNotificationConfig
object PartyNotificationConfig {

  /** Publish party changes through the ledger api as soon as possible,
    * i.e., as soon as identity checks have succeeded on the participant.
    * Note that ledger API applications may not make use of the change immediately,
    * because the domains of the participant may not yet have processed the change when the notification is sent.
    */
  case object Eager extends PartyNotificationConfig

  /** Publish party changes when they have become effective on a domain.
    * This ensures that ledger API apps can immediately make use of party changes when they receive the notification.
    * If a party is changed on a participant while the participant is not connected to any domain,
    * then the party change will fail if triggered via the ledger API
    * and delayed until the participant connects to a domain if triggered via Canton's admin endpoint.
    */
  case object ViaDomain extends PartyNotificationConfig
}

case class ParticipantProtocolConfig(
    minimumProtocolVersion: Option[ProtocolVersion],
    devVersionSupport: Boolean,
    dontWarnOnDeprecatedPV: Boolean,
    initialProtocolVersion: ProtocolVersion,
)

case class ParticipantNodeParameters(
    override val tracing: TracingConfig,
    override val delayLoggingThreshold: NonNegativeFiniteDuration,
    override val loggingConfig: LoggingConfig,
    override val logQueryCost: Option[QueryCostMonitoringConfig],
    override val enableAdditionalConsistencyChecks: Boolean,
    override val enablePreviewFeatures: Boolean,
    override val processingTimeouts: ProcessingTimeout,
    override val nonStandardConfig: Boolean,
    partyChangeNotification: PartyNotificationConfig,
    adminWorkflow: AdminWorkflowConfig,
    maxUnzippedDarSize: Int,
    stores: ParticipantStoreConfig,
    override val cachingConfigs: CachingConfigs,
    override val sequencerClient: SequencerClientConfig,
    transferTimeProofFreshnessProportion: NonNegativeInt,
    protocolConfig: ParticipantProtocolConfig,
    uniqueContractKeys: Boolean,
    enableCausalityTracking: Boolean,
    unsafeEnableDamlLfDevVersion: Boolean,
    ledgerApiServerParameters: LedgerApiServerParametersConfig,
    maxDbConnections: Int,
    excludeInfrastructureTransactions: Boolean,
) extends LocalNodeParameters {
  override def devVersionSupport: Boolean = protocolConfig.devVersionSupport
  override def dontWarnOnDeprecatedPV: Boolean = protocolConfig.dontWarnOnDeprecatedPV
  override def initialProtocolVersion: ProtocolVersion = protocolConfig.initialProtocolVersion
}

/** Configuration parameters for a single participant
  *
  * Please note that any client connecting to the ledger-api of the respective participant must set his GRPC max inbound
  * message size to 2x the value defined here, as we assume that a Canton transaction of N bytes will not be bigger
  * than 2x N on the ledger-api. Though this is just an assumption.
  * Please also note that the participant will refuse to connect to a domain where its max inbound message size is not
  * sufficient to guarantee the processing of all transactions.
  */
case class CommunityParticipantConfig(
    override val init: ParticipantInitConfig = ParticipantInitConfig(),
    override val crypto: CommunityCryptoConfig = CommunityCryptoConfig(),
    override val ledgerApi: LedgerApiServerConfig = LedgerApiServerConfig(),
    override val adminApi: CommunityAdminServerConfig = CommunityAdminServerConfig(),
    override val storage: CommunityStorageConfig = CommunityStorageConfig.Memory(),
    override val testingTime: Option[TestingTimeServiceConfig] = None,
    override val parameters: ParticipantNodeParameterConfig = ParticipantNodeParameterConfig(),
    override val sequencerClient: SequencerClientConfig = SequencerClientConfig(),
    override val caching: CachingConfigs = CachingConfigs(),
) extends LocalParticipantConfig
    with CommunityLocalNodeConfig
    with ConfigDefaults[DefaultPorts, CommunityParticipantConfig] {

  override def clientAdminApi: ClientConfig = adminApi.clientConfig

  override def clientLedgerApi: ClientConfig = ledgerApi.clientConfig

  def toRemoteConfig: RemoteParticipantConfig =
    RemoteParticipantConfig(clientAdminApi, clientLedgerApi)

  override def withDefaults(ports: DefaultPorts): CommunityParticipantConfig = {
    this
      .focus(_.ledgerApi.internalPort)
      .modify(ports.ledgerApiPort.setDefaultPort)
      .focus(_.adminApi.internalPort)
      .modify(ports.participantAdminApiPort.setDefaultPort)
  }
}

/** Configuration to connect the console to a participant running remotely.
  *
  * @param adminApi the configuration to connect the console to the remote admin api
  * @param ledgerApi the configuration to connect the console to the remote ledger api
  * @param token optional bearer token to use on the ledger-api if jwt authorization is enabled
  */
case class RemoteParticipantConfig(
    adminApi: ClientConfig,
    ledgerApi: ClientConfig,
    token: Option[String] = None,
) extends BaseParticipantConfig {
  override def clientAdminApi: ClientConfig = adminApi
  override def clientLedgerApi: ClientConfig = ledgerApi
}

/** Canton configuration case class to pass-through configuration options to the ledger api server
  *
  * @param address                   ledger api server host name.
  * @param internalPort              ledger api server port.
  * @param maxEventCacheWeight       ledger api server event cache maximum weight (caffeine cache size)
  * @param maxContractCacheWeight    ledger api server contract cache maximum weight (caffeine cache size)
  * @param tls                       tls configuration setting from ledger api server.
  * @param configurationLoadTimeout  ledger api server startup delay if no timemodel has been sent by canton via ReadService
  * @param eventsPageSize            database / akka page size for batching of ledger api server index ledger events queries.
  * @param eventsProcessingParallelism parallelism for loading and decoding ledger events
  * @param activeContractsService    configurations pertaining to the ledger api server's "active contracts service"
  * @param commandService            configurations pertaining to the ledger api server's "command service"
  * @param managementServiceTimeout  ledger api server management service maximum duration. Duration has to be finite
  *                                  as the ledger api server uses java.time.duration that does not support infinite scala durations.
  * @param postgresDataSource        config for ledger api server when using postgres
  * @param authServices              type of authentication services used by ledger-api server. If empty, we use a wildcard.
  *                                  Otherwise, the first service response that does not say "unauthenticated" will be used.
  * @param keepAliveServer                 keep-alive configuration for ledger api requests
  * @param maxContractStateCacheSize       maximum caffeine cache size of mutable state cache of contracts
  * @param maxContractKeyStateCacheSize    maximum caffeine cache size of mutable state cache of contract keys
  * @param maxInboundMessageSize     maximum inbound message size
  * @param databaseConnectionTimeout database connection timeout
  * @param enableInMemoryFanOutForLedgerApi enable the "in-memory fan-out" performance optimization (default false; not tested for production yet)
  * @param maxTransactionsInMemoryFanOutBufferSize maximum number of transactions to hold in the "in-memory fanout" (if enabled)
  * @param additionalMigrationPaths Optional extra paths for the database migrations
  * @param inMemoryStateUpdaterParallelism The processing parallelism of the Ledger API server in-memory state updater
  * @param inMemoryFanOutThreadPoolSize Size of the thread-pool backing the Ledger API in-memory fan-out.
  *                                     If not set, defaults to ((number of thread)/4 + 1)
  * @param rateLimit limit the ledger api server request rates based on system metrics
  */
case class LedgerApiServerConfig(
    address: String = "127.0.0.1",
    internalPort: Option[Port] = None,
    maxEventCacheWeight: Long = 0L,
    maxContractCacheWeight: Long = 0L,
    tls: Option[TlsServerConfig] = None,
    configurationLoadTimeout: NonNegativeFiniteDuration =
      LedgerApiServerConfig.DefaultConfigurationLoadTimeout,
    eventsPageSize: Int = LedgerApiServerConfig.DefaultEventsPageSize,
    eventsProcessingParallelism: Int = LedgerApiServerConfig.DefaultEventsProcessingParallelism,
    bufferedStreamsPageSize: Int = DamlIndexServiceConfig.DefaultBufferedStreamsPageSize,
    activeContractsService: ActiveContractsServiceConfig = ActiveContractsServiceConfig(),
    commandService: CommandServiceConfig = CommandServiceConfig(),
    userManagementService: UserManagementServiceConfig = UserManagementServiceConfig(),
    managementServiceTimeout: NonNegativeFiniteDuration =
      LedgerApiServerConfig.DefaultManagementServiceTimeout,
    postgresDataSource: PostgresDataSourceConfigCanton = PostgresDataSourceConfigCanton(),
    authServices: Seq[AuthServiceConfig] = Seq.empty,
    keepAliveServer: Option[KeepAliveServerConfig] = Some(KeepAliveServerConfig()),
    maxContractStateCacheSize: Long = LedgerApiServerConfig.DefaultMaxContractStateCacheSize,
    maxContractKeyStateCacheSize: Long = LedgerApiServerConfig.DefaultMaxContractKeyStateCacheSize,
    maxInboundMessageSize: NonNegativeInt = ServerConfig.defaultMaxInboundMessageSize,
    databaseConnectionTimeout: NonNegativeFiniteDuration =
      LedgerApiServerConfig.DefaultDatabaseConnectionTimeout,
    apiStreamShutdownTimeout: NonNegativeFiniteDuration =
      LedgerApiServerConfig.DefaultApiStreamShutdownTimeout,
    maxTransactionsInMemoryFanOutBufferSize: Int =
      LedgerApiServerConfig.DefaultMaxTransactionsInMemoryFanOutBufferSize,
    enableInMemoryFanOutForLedgerApi: Boolean = false, // Not tested for production yet
    additionalMigrationPaths: Seq[String] = Seq.empty,
    inMemoryStateUpdaterParallelism: Int =
      LedgerApiServerConfig.DefaultInMemoryStateUpdaterParallelism,
    inMemoryFanOutThreadPoolSize: Option[Int] = None,
    rateLimit: Option[RateLimitingConfig] = None,
    preparePackageMetadataTimeOutWarning: NonNegativeFiniteDuration =
      LedgerApiServerConfig.DefaultPreparePackageMetadataTimeOutWarning,
) extends CommunityServerConfig // We can't currently expose enterprise server features at the ledger api anyway
    {

  lazy val clientConfig: ClientConfig =
    ClientConfig(address, port, tls.map(_.clientConfig))

  override def sslContext: Option[SslContext] =
    tls.map(CantonServerBuilder.sslContext)

  override def serverCertChainFile: Option[ExistingFile] =
    tls.map(_.certChainFile)

}

object LedgerApiServerConfig {

  // Defaults inspired by default settings in kvutils
  val DefaultEventsPageSize: Int = 1000
  val DefaultEventsProcessingParallelism: Int = 8
  val DefaultConfigurationLoadTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(10L)
  val DefaultManagementServiceTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofMinutes(2L)
  val DefaultMaxContractStateCacheSize: Long = 100000L
  val DefaultMaxContractKeyStateCacheSize: Long = 100000L
  val DefaultDatabaseConnectionTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(30)
  val DefaultMaxTransactionsInMemoryFanOutBufferSize: Int = 10000
  val DefaultApiStreamShutdownTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(5)
  val DefaultInMemoryStateUpdaterParallelism: Int = 2
  val DefaultPreparePackageMetadataTimeOutWarning: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration(DamlIndexServiceConfig.PreparePackageMetadataTimeOutWarning.toJava)

  def DefaultInMemoryFanOutThreadPoolSize(implicit loggingContext: ErrorLoggingContext): Int = {
    val numberOfThreads =
      Threading.detectNumberOfThreads(loggingContext.logger)(loggingContext.traceContext)
    numberOfThreads / 4 + 1
  }

  /** the following case class match will help us detect any additional configuration options added
    * when we upgrade the Daml code. if the below match fails because there are more config options,
    * add them to our "LedgerApiServerConfig".
    */
  private def _completenessCheck(
      apiServerConfig: DamlApiServerConfig,
      indexServiceConfig: DamlIndexServiceConfig,
  ): Unit = {

    val DamlApiServerConfig(
      address,
      apiStreamShutdownTimeout,
      _command,
      _configurationLoadTimeout, // time the ledger api submit services wait for canton to send time model configuration
      _initialLedgerConfiguration, // not used by canton - always None
      managementServiceTimeout,
      _maxInboundMessageSize, // configured via participant.maxInboundMessageSize
      port,
      _portFile,
      _rateLimitingConfig,
      _seeding,
      _timeProviderType,
      tlsConfiguration,
      _userManagement,
    ) = apiServerConfig

    val DamlIndexServiceConfig(
      _eventsPageSize, // configured via participant.eventsPageSize,
      _eventsProcessingParallelism,
      _bufferedStreamsPageSize,
      acsIdPageSize,
      _acsIdPageBufferSize,
      _acsIdPageWorkingMemoryBytes,
      acsIdFetchingParallelism,
      acsContractFetchingParallelism,
      acsGlobalParallelism,
      _maxContractStateCacheSize,
      _maxContractKeyStateCacheSize,
      _maxTransactionsInMemoryFanOutBufferSize,
      _apiStreamShutdownTimeout, // configured via LedgerApiServerConfig.apiStreamShutdownTimeout
      inMemoryStateUpdaterParallelism,
      inMemoryFanOutThreadPoolSize,
      preparePackageMetadataTimeOutWarning,
    ) = indexServiceConfig

    def fromClientAuth(clientAuth: ClientAuth): ServerAuthRequirementConfig = {
      import ServerAuthRequirementConfig.*
      clientAuth match {
        case ClientAuth.REQUIRE =>
          None // not passing "require" as we need adminClientCerts in this case which are not available here
        case ClientAuth.OPTIONAL => Optional
        case ClientAuth.NONE => None
      }
    }

    val tlsConfig = tlsConfiguration match {
      case Some(
            TlsConfiguration(
              true,
              Some(keyCertChainFile),
              Some(keyFile),
              trustCertCollectionFile,
              secretsUrl,
              authRequirement,
              enableCertRevocationChecking,
              optTlsVersion,
            )
          ) =>
        Some(
          TlsServerConfig(
            certChainFile = ExistingFile.tryCreate(keyCertChainFile),
            privateKeyFile = ExistingFile.tryCreate(keyFile),
            trustCollectionFile = trustCertCollectionFile.map(x => ExistingFile.tryCreate(x)),
            secretsUrl = secretsUrl.map(_.toString),
            clientAuth = fromClientAuth(authRequirement),
            minimumServerProtocolVersion = optTlsVersion.map(_.version),
            enableCertRevocationChecking = enableCertRevocationChecking,
          )
        )
      case _ => None
    }

    LedgerApiServerConfig(
      address = address.getOrElse("localhost"),
      internalPort = Some(Port.tryCreate(port.value)),
      tls = tlsConfig,
      activeContractsService = ActiveContractsServiceConfig(
        acsIdPageSize,
        acsIdFetchingParallelism,
        acsContractFetchingParallelism,
        acsGlobalParallelism,
      ),
      managementServiceTimeout = NonNegativeFiniteDuration(managementServiceTimeout.toJava),
      apiStreamShutdownTimeout =
        NonNegativeFiniteDuration.ofMillis(_apiStreamShutdownTimeout.toMillis),
      inMemoryStateUpdaterParallelism = inMemoryStateUpdaterParallelism,
      inMemoryFanOutThreadPoolSize = Some(inMemoryFanOutThreadPoolSize),
      preparePackageMetadataTimeOutWarning =
        NonNegativeFiniteDuration(preparePackageMetadataTimeOutWarning.toJava),
    ).discard
  }

  def ledgerApiServerTlsConfigFromCantonServerConfig(
      tlsCantonConfig: TlsServerConfig
  ): TlsConfiguration =
    TlsConfiguration(
      enabled = true,
      certChainFile = Some(tlsCantonConfig.certChainFile.unwrap),
      privateKeyFile = Some(tlsCantonConfig.privateKeyFile.unwrap),
      trustCollectionFile = tlsCantonConfig.trustCollectionFile.map(_.unwrap),
      secretsUrl = tlsCantonConfig.secretsUrl.map(SecretsUrl.fromString),
      clientAuth = tlsCantonConfig.clientAuth match {
        case ServerAuthRequirementConfig.Require(_cert) => ClientAuth.REQUIRE
        case ServerAuthRequirementConfig.Optional => ClientAuth.OPTIONAL
        case ServerAuthRequirementConfig.None => ClientAuth.NONE
      },
      enableCertRevocationChecking = tlsCantonConfig.enableCertRevocationChecking,
      minimumServerProtocolVersion = tlsCantonConfig.minimumServerProtocolVersion.map { v =>
        Seq[TlsVersion.TlsVersion](TlsVersion.V1, TlsVersion.V1_1, TlsVersion.V1_2, TlsVersion.V1_3)
          .find(_.version == v)
          .getOrElse(
            throw new IllegalArgumentException(s"Unknown TLS protocol version ${v}")
          )
      },
    )

}

/** Ledger api active contracts service specific configurations
  *
  * @param acsIdPageSize                  number of contract ids fetched from the index for every round trip when serving ACS calls
  * @param acsIdPageBufferSize            buffer size used for indexer fetching
  * @param acsIdFetchingParallelism       number of contract id pages fetched in parallel when serving ACS calls
  * @param acsContractFetchingParallelism number of event pages fetched in parallel when serving ACS calls
  * @param acsGlobalParallelism           maximum number of concurrent ACS queries to the index database
  * Note _completenessCheck performed in LedgerApiServerConfig above
  */
case class ActiveContractsServiceConfig(
    acsIdPageSize: Int = DamlIndexServiceConfig.DefaultAcsIdPageSize,
    acsIdPageBufferSize: Int = DamlIndexServiceConfig.DefaultAcsIdPageBufferSize,
    acsIdFetchingParallelism: Int = DamlIndexServiceConfig.DefaultAcsIdFetchingParallelism,
    acsContractFetchingParallelism: Int =
      DamlIndexServiceConfig.DefaultAcsContractFetchingParallelism,
    acsGlobalParallelism: Int = DamlIndexServiceConfig.DefaultAcsGlobalParallelism,
)

/** Ledger api command service specific configurations
  *
  * @param trackerRetentionPeriod maximum command tracking duration
  * @param inputBufferSize        maximum number of commands queued for submission for each distinct set of parties
  * @param maxCommandsInFlight    maximum number of submitted commands waiting to be completed for each distinct set of parties
  */
case class CommandServiceConfig(
    trackerRetentionPeriod: NonNegativeFiniteDuration = NonNegativeFiniteDuration(
      CommandConfiguration.Default.trackerRetentionPeriod
    ),
    inputBufferSize: Int = CommandConfiguration.Default.inputBufferSize,
    maxCommandsInFlight: Int = CommandConfiguration.Default.maxCommandsInFlight,
) {
  // This helps us detect if any CommandService configuration are added in the daml repo.
  private def _completenessCheck(config: CommandConfiguration): CommandServiceConfig =
    config match {
      case CommandConfiguration(inputBufferSize, maxCommandsInFlight, trackerRetentionPeriod) =>
        CommandServiceConfig(
          trackerRetentionPeriod = NonNegativeFiniteDuration(trackerRetentionPeriod),
          inputBufferSize = inputBufferSize,
          maxCommandsInFlight = maxCommandsInFlight,
        )
    }

  def damlConfig: CommandConfiguration =
    this.into[CommandConfiguration].disableDefaultValues.transform
}

object CommandServiceConfig {
  // This only serves to detect changes upstream (e.g., deletion of a field)
  def fromDaml(config: CommandConfiguration): CommandServiceConfig = {
    config
      .into[CommandServiceConfig]
      .disableDefaultValues
      .withFieldComputed(
        _.trackerRetentionPeriod,
        c => NonNegativeFiniteDuration(c.trackerRetentionPeriod),
      )
      .transform
  }
}

/** Ledger api user management service specific configurations
  *
  * @param enabled                        whether to enable participant user management
  * @param maxCacheSize                   maximum in-memory cache size for user management state
  * @param cacheExpiryAfterWriteInSeconds determines the maximum delay for propagating user management state changes
  * @param maxRightsPerUser               maximum number of rights per user
  * @param maxUsersPageSize               maximum number of users returned
  */
case class UserManagementServiceConfig(
    enabled: Boolean = true,
    maxCacheSize: Int = UserManagementConfig.DefaultMaxCacheSize,
    cacheExpiryAfterWriteInSeconds: Int =
      UserManagementConfig.DefaultCacheExpiryAfterWriteInSeconds,
    maxRightsPerUser: Int = UserManagementConfig.MaxRightsPerUser,
    maxUsersPageSize: Int = UserManagementConfig.DefaultMaxUsersPageSize,
) {

  def damlConfig: UserManagementConfig =
    this.into[UserManagementConfig].disableDefaultValues.transform
}

object UserManagementServiceConfig {
  // This only serves to detect changes upstream (e.g., deletion of a field)
  def fromDaml(config: UserManagementConfig): UserManagementServiceConfig = {
    config
      .into[UserManagementServiceConfig]
      .disableDefaultValues
      .withFieldConst(_.maxRightsPerUser, UserManagementConfig.MaxRightsPerUser)
      .transform
  }
}

final case class PostgresDataSourceConfigCanton(
    synchronousCommit: Option[String] = None,
    // TCP keepalive configuration for postgres. See https://www.postgresql.org/docs/13/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SETTINGS for details
    tcpKeepalivesIdle: Option[Int] =
      DefaultPostgresTcpKeepalivesIdle, // corresponds to: tcp_keepalives_idle
    tcpKeepalivesInterval: Option[Int] =
      DefaultPostgresTcpKeepalivesInterval, // corresponds to: tcp_keepalives_interval
    tcpKeepalivesCount: Option[Int] =
      DefaultPostgresTcpKeepalivesCount, // corresponds to: tcp_keepalives_count
) {
  val typedSynchronousCommit: Option[SynchronousCommitValue] =
    synchronousCommit.map(_.toLowerCase).map {
      case "on" => SynchronousCommitValue.On
      case "off" => SynchronousCommitValue.Off
      case "remote_write" => SynchronousCommitValue.RemoteWrite
      case "remote_apply" => SynchronousCommitValue.RemoteApply
      case "local" => SynchronousCommitValue.Local
      case other =>
        throw new InvalidParameterException(s"Unsupported value `$other` for synchronous commit.")
    }

  def damlConfig: DamlPostgresDataSourceConfig =
    this
      .into[DamlPostgresDataSourceConfig]
      .withFieldComputed(_.synchronousCommit, _.typedSynchronousCommit.map(_.damlConfig))
      .disableDefaultValues
      .transform
}

object PostgresDataSourceConfigCanton {
  // By default synchronous commit locally but don't await ACKs when replicated
  val DefaultSynchronousCommitMode: String = "LOCAL"

  private val defaultPostgresDataSourceConfig = DamlIndexerConfig
    .createDataSourceProperties(DamlIndexerConfig.DefaultIngestionParallelism)
    .postgres
  val DefaultPostgresTcpKeepalivesIdle: Option[Int] =
    defaultPostgresDataSourceConfig.tcpKeepalivesIdle
  val DefaultPostgresTcpKeepalivesInterval: Option[Int] =
    defaultPostgresDataSourceConfig.tcpKeepalivesInterval
  val DefaultPostgresTcpKeepalivesCount: Option[Int] =
    defaultPostgresDataSourceConfig.tcpKeepalivesCount

  sealed abstract class SynchronousCommitValue(val value: String) {
    def damlConfig: DamlPostgresDataSourceConfig.SynchronousCommitValue
  }
  object SynchronousCommitValue {
    case object On extends SynchronousCommitValue("on") {
      val damlConfig = DamlPostgresDataSourceConfig.SynchronousCommitValue.On
    }
    case object Off extends SynchronousCommitValue("off") {
      val damlConfig = DamlPostgresDataSourceConfig.SynchronousCommitValue.Off
    }
    case object RemoteWrite extends SynchronousCommitValue("remote_write") {
      val damlConfig = DamlPostgresDataSourceConfig.SynchronousCommitValue.RemoteWrite
    }
    case object RemoteApply extends SynchronousCommitValue("remote_apply") {
      val damlConfig = DamlPostgresDataSourceConfig.SynchronousCommitValue.RemoteApply
    }
    case object Local extends SynchronousCommitValue("local") {
      val damlConfig = DamlPostgresDataSourceConfig.SynchronousCommitValue.Local
    }
  }

  // This only serves to detect changes upstream (e.g., deletion of a field)
  def fromDaml(config: DamlPostgresDataSourceConfig): PostgresDataSourceConfigCanton =
    config
      .into[PostgresDataSourceConfigCanton]
      .disableDefaultValues
      .withFieldComputed(_.synchronousCommit, _.synchronousCommit.map(_.pgSqlName))
      .transform
}

/** Ledger api indexer specific configurations
  *
  * See com.daml.platform.indexer.JdbcIndexer for semantics on these configurations.
  */
case class IndexerConfig(
    restartDelay: NonNegativeFiniteDuration = IndexerConfig.DefaultRestartDelay,
    maxInputBufferSize: NonNegativeInt =
      NonNegativeInt.tryCreate(DamlIndexerConfig.DefaultMaxInputBufferSize),
    inputMappingParallelism: NonNegativeInt =
      NonNegativeInt.tryCreate(DamlIndexerConfig.DefaultInputMappingParallelism),
    batchingParallelism: NonNegativeInt =
      NonNegativeInt.tryCreate(DamlIndexerConfig.DefaultBatchingParallelism),
    ingestionParallelism: NonNegativeInt =
      NonNegativeInt.tryCreate(DamlIndexerConfig.DefaultIngestionParallelism),
    submissionBatchSize: Long = DamlIndexerConfig.DefaultSubmissionBatchSize,
    enableCompression: Boolean = DamlIndexerConfig.DefaultEnableCompression,
    schemaMigrationAttempts: Int = IndexerStartupMode.DefaultSchemaMigrationAttempts,
    schemaMigrationAttemptBackoff: NonNegativeFiniteDuration =
      IndexerConfig.DefaultSchemaMigrationAttemptBackoff,
    packageMetadataView: PackageMetadataViewConfig =
      DamlIndexerConfig.DefaultPackageMetadataViewConfig,
    maxOutputBatchedBufferSize: Int = DamlIndexerConfig.DefaultMaxOutputBatchedBufferSize,
    maxTailerBatchSize: Int = DamlIndexerConfig.DefaultMaxTailerBatchSize,
) {
  def damlConfig(
      indexerLockIds: Option[IndexerLockIds],
      dataSourceProperties: Option[DamlDataSourceProperties],
  ): DamlIndexerConfig = this
    .into[DamlIndexerConfig]
    .disableDefaultValues
    .withFieldConst(_.startupMode, IndexerStartupMode.MigrateAndStart(false))
    .withFieldConst(
      _.highAvailability,
      indexerLockIds.fold(HaConfig()) { case IndexerLockIds(mainLockId, workerLockId) =>
        HaConfig(indexerLockId = mainLockId, indexerWorkerLockId = workerLockId)
      },
    )
    .withFieldConst(_.dataSourceProperties, dataSourceProperties)
    .transform
}

object IndexerConfig {
  val DefaultRestartDelay: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(DamlIndexerConfig.DefaultRestartDelay.toSeconds)
  val DefaultSchemaMigrationAttemptBackoff: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(
      IndexerStartupMode.DefaultSchemaMigrationAttemptBackoff.toSeconds
    )

  /** The following case class match will help us detect any additional configuration options added
    * when we upgrade the Daml libraries. If the below match fails because there are more config options,
    * add them to the canton "IndexerConfig".
    */
  private def _completenessCheck(config: DamlIndexerConfig): Unit = {
    val DamlIndexerConfig(
      batchingParallelism,
      _dataSourceProperties, // this is used, via the LedgerApiServerConfig
      enableCompression,
      _highAvailability, // consider making a subset of these settings configurable once the ha-dust settles
      ingestionParallelism,
      inputMappingParallelism,
      maxInputBufferSize,
      packageMetadataView,
      restartDelay,
      startupMode, // not configurable in canton and decided based on participant-HA replica role
      submissionBatchSize,
      maxOutputBatchedBufferSize,
      maxTailerBatchSize,
    ) = config

    val (schemaMigrationAttempts, schemaMigrationAttemptBackoff) = (startupMode match {
      case IndexerStartupMode.ValidateAndStart => None
      case IndexerStartupMode.MigrateAndStart(_) => None
      case IndexerStartupMode.ValidateAndWaitOnly(
            schemaMigrationAttempts,
            schemaMigrationAttemptBackoff,
          ) =>
        Some((schemaMigrationAttempts, schemaMigrationAttemptBackoff))
      case IndexerStartupMode.MigrateOnEmptySchemaAndStart => None
    }).getOrElse(
      (
        IndexerStartupMode.DefaultSchemaMigrationAttempts,
        IndexerStartupMode.DefaultSchemaMigrationAttemptBackoff,
      )
    )

    IndexerConfig(
      restartDelay = NonNegativeFiniteDuration.ofSeconds(restartDelay.toSeconds),
      maxInputBufferSize = NonNegativeInt.tryCreate(maxInputBufferSize),
      inputMappingParallelism = NonNegativeInt.tryCreate(inputMappingParallelism),
      batchingParallelism = NonNegativeInt.tryCreate(batchingParallelism),
      ingestionParallelism = NonNegativeInt.tryCreate(ingestionParallelism),
      submissionBatchSize = submissionBatchSize,
      enableCompression = enableCompression,
      schemaMigrationAttempts = schemaMigrationAttempts,
      schemaMigrationAttemptBackoff =
        NonNegativeFiniteDuration.ofSeconds(schemaMigrationAttemptBackoff.toSeconds),
      packageMetadataView = packageMetadataView,
      maxOutputBatchedBufferSize = maxOutputBatchedBufferSize,
      maxTailerBatchSize = maxTailerBatchSize,
    ).discard
  }

}

/** Optional ledger api time service configuration for demo and testing only */
sealed trait TestingTimeServiceConfig
object TestingTimeServiceConfig {

  /** A variant of [[TestingTimeServiceConfig]] with the ability to read and monotonically advance ledger time */
  case object MonotonicTime extends TestingTimeServiceConfig
}

/** General participant node parameters
  *
  * @param adminWorkflow Configuration options for Canton admin workflows
  * @param partyChangeNotification Determines how eagerly the participant nodes notify the ledger api of party changes.
  *                                By default ensure that parties are added via at least one domain before ACKing party creation to ledger api server indexer.
  *                                This not only avoids flakiness in tests, but reflects that a party is not actually usable in canton until it's
  *                                available through at least one domain.
  * @param maxUnzippedDarSize maximum allowed size of unzipped DAR files (in bytes) the participant can accept for uploading. Defaults to 1GB.
  * @param ledgerApiServerParameters ledger api server parameters
  *
  * The following specialized participant node performance tuning parameters may be grouped once a more final set of configs emerges.
  * @param transferTimeProofFreshnessProportion Proportion of the target domain exclusivity timeout that is used as a freshness bound when
  *                                             requesting a time proof. Setting to 3 means we'll take a 1/3 of the target domain exclusivity timeout
  *                                             and potentially we reuse a recent timeout if one exists within that bound, otherwise a new time proof
  *                                             will be requested.
  *                                             Setting to zero will disable reusing recent time proofs and will instead always fetch a new proof.
  * @param minimumProtocolVersion The minimum protocol version that this participant will speak when connecting to a domain
  * @param unsafeEnableDamlLfDevVersion If set to true (default false), packages referring to the `dev` LF version can be used with Canton.
  * @param initialProtocolVersion The initial protocol version used by the participant (default latest), e.g., used to create the initial topology transactions.
  * @param willCorruptYourSystemDevVersionSupport If set to true, development protocol versions (and database schemas) will be supported. Do NOT use this in production, as it will break your system.
  * @param dontWarnOnDeprecatedPV If true, then this participant will not emit a warning when connecting to a sequencer using a deprecated protocol version (such as 2.0.0).
  * @param warnIfOverloadedFor If all incoming commands have been rejected due to PARTICIPANT_BACKPRESSURE during this interval, the participant will log a warning.
  * @param excludeInfrastructureTransactions If set, infrastructure transactions (i.e. ping, bong and dar distribution) will be excluded from participant metering.
  */
case class ParticipantNodeParameterConfig(
    adminWorkflow: AdminWorkflowConfig = AdminWorkflowConfig(),
    partyChangeNotification: PartyNotificationConfig = PartyNotificationConfig.ViaDomain,
    maxUnzippedDarSize: Int = 1024 * 1024 * 1024,
    stores: ParticipantStoreConfig = ParticipantStoreConfig(),
    transferTimeProofFreshnessProportion: NonNegativeInt = NonNegativeInt.tryCreate(3),
    minimumProtocolVersion: Option[ParticipantProtocolVersion] = Some(
      ParticipantProtocolVersion(
        ProtocolVersion.v3
      )
    ),
    unsafeEnableDamlLfDevVersion: Boolean = false,
    initialProtocolVersion: ParticipantProtocolVersion = ParticipantProtocolVersion(
      ProtocolVersion.latest
    ),
    willCorruptYourSystemDevVersionSupport: Boolean = false,
    dontWarnOnDeprecatedPV: Boolean = false,
    warnIfOverloadedFor: Option[NonNegativeFiniteDuration] = Some(
      NonNegativeFiniteDuration.ofSeconds(20)
    ),
    ledgerApiServerParameters: LedgerApiServerParametersConfig = LedgerApiServerParametersConfig(),
    excludeInfrastructureTransactions: Boolean = true,
)

/** Parameters for the participant node's stores
  *
  * @param maxItemsInSqlClause    maximum number of items to place in sql "in clauses"
  * @param maxPruningBatchSize           maximum number of events to prune from a participant at a time, used to break up batches internally
  * @param acsPruningInterval        How often to prune the ACS journal in the background. A very high interval will let the journal grow larger and
  *                                  eventually slow queries down. A very low interval may cause a high load on the journal table and the DB.
  *                                  The default is 60 seconds.
  *                                  A domain's reconciliation interval also limits the frequency of background pruning. Setting the pruning interval
  *                                  below the reconciliation interval doesn't not increase the frequency further.
  * @param dbBatchAggregationConfig Batching configuration for Db queries
  */
case class ParticipantStoreConfig(
    maxItemsInSqlClause: PositiveNumeric[Int] = PositiveNumeric.tryCreate(100),
    maxPruningBatchSize: Int = 1000,
    acsPruningInterval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(60),
    dbBatchAggregationConfig: BatchAggregatorConfig = BatchAggregatorConfig.Batching(),
)

/** Parameters for the ledger api server
  *
  * @param contractIdSeeding  test-only way to override the contract-id seeding scheme. Must be Strong in production (and Strong is the default).
  *                           Only configurable to reduce the amount of secure random numbers consumed by tests and to avoid flaky timeouts during continuous integration.
  * @param indexer            parameters how the participant populates the index db used to serve the ledger api
  * @param jwtTimestampLeeway leeway parameters for JWTs
  */
case class LedgerApiServerParametersConfig(
    contractIdSeeding: Seeding = Seeding.Strong,
    indexer: IndexerConfig = IndexerConfig(),
    jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
)
