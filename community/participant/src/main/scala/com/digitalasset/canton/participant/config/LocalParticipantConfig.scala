// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.config

import cats.syntax.option.*
import com.daml.http.HttpApiConfig
import com.daml.jwt.JwtTimestampLeeway
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.DeprecatedConfigUtils.DeprecatedFieldsFor
import com.digitalasset.canton.config.LocalNodeConfig.LocalNodeConfigDeprecationImplicits
import com.digitalasset.canton.config.RequireTypes.*
import com.digitalasset.canton.config.*
import com.digitalasset.canton.ledger.api.tls.{SecretsUrl, TlsConfiguration, TlsVersion}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.networking.grpc.CantonServerBuilder
import com.digitalasset.canton.participant.admin.AdminWorkflowConfig
import com.digitalasset.canton.participant.config.LedgerApiServerConfig.DefaultRateLimit
import com.digitalasset.canton.platform.apiserver.SeedService.Seeding
import com.digitalasset.canton.platform.apiserver.configuration.RateLimitingConfig
import com.digitalasset.canton.platform.apiserver.ApiServerConfig as DamlApiServerConfig
import com.digitalasset.canton.platform.config.{
  ActiveContractsServiceStreamsConfig,
  CommandServiceConfig,
  IndexServiceConfig as LedgerIndexServiceConfig,
  TransactionFlatStreamsConfig,
  TransactionTreeStreamsConfig,
  UserManagementServiceConfig,
}
import com.digitalasset.canton.platform.indexer.IndexerConfig
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.version.{ParticipantProtocolVersion, ProtocolVersion}
import com.digitalasset.canton.{DiscardOps, config}
import io.netty.handler.ssl.{ClientAuth, SslContext}
import monocle.macros.syntax.lens.*

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
        ) ++ deprecatedLocalNodeConfig.movedFields

        override def deprecatePath: List[DeprecatedConfigUtils.DeprecatedConfigPath[_]] = List(
          DeprecatedConfigUtils
            .DeprecatedConfigPath[Boolean]("parameters.unique-contract-keys", "2.7.0"),
          DeprecatedConfigUtils
            .DeprecatedConfigPath[Boolean]("init.parameters.unique-contract-keys", "2.7.0"),
        )
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

  /** parameters for configuring the interaction with ledger via the HTTP JSON API.
    * Configuring this key will enable the HTTP JSON API server.
    * NOTE: This feature is experimental and MUST NOT be used in production code.
    */
  def httpLedgerApiExperimental: Option[HttpApiConfig]

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

final case class ParticipantProtocolConfig(
    minimumProtocolVersion: Option[ProtocolVersion],
    override val devVersionSupport: Boolean,
    override val dontWarnOnDeprecatedPV: Boolean,
    override val initialProtocolVersion: ProtocolVersion,
) extends ProtocolConfig

/** Configuration parameters for a single participant
  *
  * Please note that any client connecting to the ledger-api of the respective participant must set his GRPC max inbound
  * message size to 2x the value defined here, as we assume that a Canton transaction of N bytes will not be bigger
  * than 2x N on the ledger-api. Though this is just an assumption.
  * Please also note that the participant will refuse to connect to a domain where its max inbound message size is not
  * sufficient to guarantee the processing of all transactions.
  */
final case class CommunityParticipantConfig(
    override val init: ParticipantInitConfig = ParticipantInitConfig(),
    override val crypto: CommunityCryptoConfig = CommunityCryptoConfig(),
    override val ledgerApi: LedgerApiServerConfig = LedgerApiServerConfig(),
    override val httpLedgerApiExperimental: Option[HttpApiConfig] = None,
    override val adminApi: CommunityAdminServerConfig = CommunityAdminServerConfig(),
    override val storage: CommunityStorageConfig = CommunityStorageConfig.Memory(),
    override val testingTime: Option[TestingTimeServiceConfig] = None,
    override val parameters: ParticipantNodeParameterConfig = ParticipantNodeParameterConfig(),
    override val sequencerClient: SequencerClientConfig = SequencerClientConfig(),
    override val caching: CachingConfigs = CachingConfigs(),
    override val monitoring: NodeMonitoringConfig = NodeMonitoringConfig(),
    override val topologyX: TopologyXConfig = TopologyXConfig(),
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
final case class RemoteParticipantConfig(
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
  * @param bufferedEventsProcessingParallelism parallelism for loading and decoding ledger events for populating Ledger API internal buffers
  * @param activeContractsService    configurations pertaining to the ledger api server's "active contracts service"
  * @param transactionFlatStreams    configurations pertaining to the ledger api server's streams of transaction trees
  * @param transactionTreeStreams    configurations pertaining to the ledger api server's streams of flat transactions
  * @param commandService            configurations pertaining to the ledger api server's "command service"
  * @param managementServiceTimeout  ledger api server management service maximum duration. Duration has to be finite
  *                                  as the ledger api server uses java.time.duration that does not support infinite scala durations.
  * @param postgresDataSource        config for ledger api server when using postgres
  * @param authServices              type of authentication services used by ledger-api server. If empty, we use a wildcard.
  *                                  Otherwise, the first service response that does not say "unauthenticated" will be used.
  * @param keepAliveServer                 keep-alive configuration for ledger api requests
  * @param maxContractStateCacheSize       maximum caffeine cache size of mutable state cache of contracts
  * @param maxContractKeyStateCacheSize    maximum caffeine cache size of mutable state cache of contract keys
  * @param maxInboundMessageSize maximum inbound message size on the ledger api
  * @param databaseConnectionTimeout database connection timeout
  * @param maxTransactionsInMemoryFanOutBufferSize maximum number of transactions to hold in the "in-memory fanout" (if enabled)
  * @param additionalMigrationPaths Optional extra paths for the database migrations
  * @param inMemoryStateUpdaterParallelism The processing parallelism of the Ledger API server in-memory state updater
  * @param inMemoryFanOutThreadPoolSize Size of the thread-pool backing the Ledger API in-memory fan-out.
  *                                     If not set, defaults to ((number of thread)/4 + 1)
  * @param rateLimit limit the ledger api server request rates based on system metrics
  * @param preparePackageMetadataTimeOutWarning Timeout for package metadata preparation after which a warning will be logged
  * @param completionsPageSize database / akka page size for batching of ledger api server index ledger completion queries
  * @param explicitDisclosureUnsafe enable usage of explicitly disclosed contracts in command submission and transaction validation.
  *                                 This feature is deemed unstable and unsafe. Should NOT be enabled in production!
  */
final case class LedgerApiServerConfig(
    address: String = "127.0.0.1",
    internalPort: Option[Port] = None,
    maxEventCacheWeight: Long = 0L,
    maxContractCacheWeight: Long = 0L,
    tls: Option[TlsServerConfig] = None,
    configurationLoadTimeout: NonNegativeFiniteDuration =
      LedgerApiServerConfig.DefaultConfigurationLoadTimeout,
    bufferedEventsProcessingParallelism: Int =
      LedgerApiServerConfig.DefaultEventsProcessingParallelism,
    bufferedStreamsPageSize: Int = LedgerIndexServiceConfig.DefaultBufferedStreamsPageSize,
    activeContractsService: ActiveContractsServiceStreamsConfig =
      ActiveContractsServiceStreamsConfig(),
    transactionTreeStreams: TransactionTreeStreamsConfig = TransactionTreeStreamsConfig(),
    transactionFlatStreams: TransactionFlatStreamsConfig = TransactionFlatStreamsConfig(),
    globalMaxEventIdQueries: Int = LedgerIndexServiceConfig().globalMaxEventIdQueries,
    globalMaxEventPayloadQueries: Int = LedgerIndexServiceConfig().globalMaxEventPayloadQueries,
    commandService: CommandServiceConfig = CommandServiceConfig(),
    userManagementService: UserManagementServiceConfig = UserManagementServiceConfig(),
    managementServiceTimeout: NonNegativeFiniteDuration =
      LedgerApiServerConfig.DefaultManagementServiceTimeout,
    postgresDataSource: PostgresDataSourceConfig = PostgresDataSourceConfig(),
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
    additionalMigrationPaths: Seq[String] = Seq.empty,
    inMemoryStateUpdaterParallelism: Int =
      LedgerApiServerConfig.DefaultInMemoryStateUpdaterParallelism,
    inMemoryFanOutThreadPoolSize: Option[Int] = None,
    rateLimit: Option[RateLimitingConfig] = Some(DefaultRateLimit),
    preparePackageMetadataTimeOutWarning: NonNegativeFiniteDuration =
      LedgerApiServerConfig.DefaultPreparePackageMetadataTimeOutWarning,
    completionsPageSize: Int = LedgerApiServerConfig.DefaultCompletionsPageSize,
    explicitDisclosureUnsafe: Boolean = false,
    adminToken: Option[String] = None,
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

  private val DefaultEventsProcessingParallelism: Int = 8
  private val DefaultConfigurationLoadTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(10L)
  private val DefaultManagementServiceTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofMinutes(2L)
  private val DefaultMaxContractStateCacheSize: Long = 100000L
  private val DefaultMaxContractKeyStateCacheSize: Long = 100000L
  private val DefaultDatabaseConnectionTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(30)
  private val DefaultMaxTransactionsInMemoryFanOutBufferSize: Int = 10000
  private val DefaultApiStreamShutdownTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(5)
  private val DefaultInMemoryStateUpdaterParallelism: Int = 2
  private val DefaultPreparePackageMetadataTimeOutWarning: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration(LedgerIndexServiceConfig.PreparePackageMetadataTimeOutWarning)
  val DefaultRateLimit: RateLimitingConfig =
    RateLimitingConfig.Default.copy(
      maxApiServicesQueueSize = 20000,
      // The two options below are to turn off memory based rate limiting by default
      maxUsedHeapSpacePercentage = 100,
      minFreeHeapSpaceBytes = 0,
    )
  private val DefaultCompletionsPageSize = 1000

  def DefaultInMemoryFanOutThreadPoolSize(implicit loggingContext: ErrorLoggingContext): Int = {
    val numberOfThreads =
      Threading.detectNumberOfThreads(loggingContext.logger)(loggingContext.traceContext)
    numberOfThreads / 4 + 1
  }

  trait LedgerApiServerConfigDeprecationsImplicits {
    implicit def deprecatedLedgerApiServerConfig[X <: LedgerApiServerConfig]
        : DeprecatedFieldsFor[X] = new DeprecatedFieldsFor[LedgerApiServerConfig] {
      override def movedFields: List[DeprecatedConfigUtils.MovedConfigPath] = List(
        DeprecatedConfigUtils.MovedConfigPath(
          "active-contracts-service.acs-global-parallelism",
          "global-max-event-payload-queries",
        ),
        DeprecatedConfigUtils.MovedConfigPath(
          "events-page-size",
          "active-contracts-service.max-payloads-per-payloads-page",
          "transaction-flat-streams.max-payloads-per-payloads-page",
          "transaction-tree-streams.max-payloads-per-payloads-page",
        ),
        DeprecatedConfigUtils.MovedConfigPath(
          "events-processing-parallelism",
          "buffered-events-processing-parallelism",
          "active-contracts-service.contract-processing-parallelism",
        ),
      )
    }
  }

  object DeprecatedImplicits extends LedgerApiServerConfigDeprecationsImplicits

  /** the following case class match will help us detect any additional configuration options added.
    * If the below match fails because there are more config options, add them to our "LedgerApiServerConfig".
    */
  private def _completenessCheck(
      apiServerConfig: DamlApiServerConfig,
      indexServiceConfig: LedgerIndexServiceConfig,
  ): Unit = {

    val DamlApiServerConfig(
      address,
      apiStreamShutdownTimeout,
      _command,
      _configurationLoadTimeout, // time the ledger api submit services wait for canton to send time model configuration
      managementServiceTimeout,
      _maxInboundMessageSize, // configured via participant.maxInboundMessageSize
      port,
      _rateLimitingConfig,
      _seeding,
      _timeProviderType,
      tlsConfiguration,
      _userManagement,
      _identityProviderManagement,
    ) = apiServerConfig

    val LedgerIndexServiceConfig(
      bufferedEventsProcessingParallelism,
      bufferedStreamsPageSize,
      maxContractStateCacheSize,
      maxContractKeyStateCacheSize,
      maxTransactionsInMemoryFanOutBufferSize,
      _apiStreamShutdownTimeout, // configured via LedgerApiServerConfig.apiStreamShutdownTimeout
      inMemoryStateUpdaterParallelism,
      inMemoryFanOutThreadPoolSize,
      preparePackageMetadataTimeOutWarning,
      completionsPageSize,
      activeContractsServiceStreamsConfig,
      transactionFlatStreams,
      transactionTreeStreams,
      globalMaxEventIdQueries,
      globalMaxEventPayloadQueries,
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
      internalPort = Some(port),
      tls = tlsConfig,
      bufferedEventsProcessingParallelism = bufferedEventsProcessingParallelism,
      maxContractStateCacheSize = maxContractStateCacheSize,
      maxContractKeyStateCacheSize = maxContractKeyStateCacheSize,
      maxTransactionsInMemoryFanOutBufferSize = maxTransactionsInMemoryFanOutBufferSize,
      apiStreamShutdownTimeout =
        NonNegativeFiniteDuration.ofMillis(apiStreamShutdownTimeout.toMillis),
      activeContractsService = activeContractsServiceStreamsConfig,
      transactionFlatStreams = transactionFlatStreams,
      transactionTreeStreams = transactionTreeStreams,
      managementServiceTimeout = NonNegativeFiniteDuration(managementServiceTimeout.toJava),
      inMemoryStateUpdaterParallelism = inMemoryStateUpdaterParallelism,
      inMemoryFanOutThreadPoolSize = Some(inMemoryFanOutThreadPoolSize),
      preparePackageMetadataTimeOutWarning =
        NonNegativeFiniteDuration(preparePackageMetadataTimeOutWarning.toJava),
      completionsPageSize = completionsPageSize,
      bufferedStreamsPageSize = bufferedStreamsPageSize,
      globalMaxEventIdQueries = globalMaxEventIdQueries,
      globalMaxEventPayloadQueries = globalMaxEventPayloadQueries,
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
  * @param initialProtocolVersion The initial protocol version used by the participant (default latest), e.g., used to create the initial topology transactions.
  * @param devVersionSupport If set to true, will allow the participant to connect to a domain with dev protocol version and will turn on unsafe Daml LF versions.
  * @param dontWarnOnDeprecatedPV If true, then this participant will not emit a warning when connecting to a sequencer using a deprecated protocol version (such as 2.0.0).
  * @param warnIfOverloadedFor If all incoming commands have been rejected due to PARTICIPANT_BACKPRESSURE during this interval, the participant will log a warning.
  * @param excludeInfrastructureTransactions If set, infrastructure transactions (i.e. ping, bong and dar distribution) will be excluded from participant metering.
  * @param enableEngineStackTraces If true, DAMLe stack traces will be enabled
  * @param enableContractUpgrading If true contracts may be automatically upgraded or downgraded as needed.
  */
final case class ParticipantNodeParameterConfig(
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
    initialProtocolVersion: ParticipantProtocolVersion = ParticipantProtocolVersion(
      ProtocolVersion.latest
    ),
    devVersionSupport: Boolean = false,
    dontWarnOnDeprecatedPV: Boolean = false,
    warnIfOverloadedFor: Option[NonNegativeFiniteDuration] = Some(
      NonNegativeFiniteDuration.ofSeconds(20)
    ),
    ledgerApiServerParameters: LedgerApiServerParametersConfig = LedgerApiServerParametersConfig(),
    excludeInfrastructureTransactions: Boolean = true,
    enableEngineStackTraces: Boolean = false,
    enableContractUpgrading: Boolean = false,
)

/** Parameters for the participant node's stores
  *
  * @param maxItemsInSqlClause    maximum number of items to place in sql "in clauses"
  * @param maxPruningBatchSize    maximum number of events to prune from a participant at a time, used to break up canton participant-internal batches
  * @param ledgerApiPruningBatchSize  Number of events to prune from the ledger api server index-database at a time during automatic background pruning.
  *                                   Canton-internal store pruning happens at the smaller batch size of "maxPruningBatchSize" to minimize memory usage
  *                                   whereas ledger-api-server index-db pruning needs sufficiently large batches to amortize the database overhead of
  *                                   "skipping over" active contracts.
  * @param pruningMetricUpdateInterval  How frequently to update the `max-event-age` pruning progress metric in the background.
  *                                     A setting of None disables background metric updating.
  * @param acsPruningInterval        How often to prune the ACS journal in the background. A very high interval will let the journal grow larger and
  *                                  eventually slow queries down. A very low interval may cause a high load on the journal table and the DB.
  *                                  The default is 60 seconds.
  *                                  A domain's reconciliation interval also limits the frequency of background pruning. Setting the pruning interval
  *                                  below the reconciliation interval doesn't not increase the frequency further.
  * @param dbBatchAggregationConfig Batching configuration for Db queries
  */
final case class ParticipantStoreConfig(
    maxItemsInSqlClause: PositiveNumeric[Int] = PositiveNumeric.tryCreate(100),
    maxPruningBatchSize: PositiveNumeric[Int] = PositiveNumeric.tryCreate(1000),
    ledgerApiPruningBatchSize: PositiveNumeric[Int] = PositiveNumeric.tryCreate(50000),
    pruningMetricUpdateInterval: Option[PositiveDurationSeconds] =
      config.PositiveDurationSeconds.ofHours(1L).some,
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
final case class LedgerApiServerParametersConfig(
    contractIdSeeding: Seeding = Seeding.Strong,
    indexer: IndexerConfig = IndexerConfig(),
    jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
    contractLoader: ContractLoaderConfig = ContractLoaderConfig(),
)

/** Parameters to control batch loading during phase 1 / interpretation
  *
  * @param maxQueueSize how many parallel lookups can be queued before we start to backpressure loading
  * @param maxBatchSize how many contract lookups to batch together
  * @param parallelism  how many parallel contract lookup requests should be sent to the db when prepopulating the cache
  */
final case class ContractLoaderConfig(
    maxQueueSize: PositiveInt = ContractLoaderConfig.defaultMaxQueueSize,
    maxBatchSize: PositiveInt = ContractLoaderConfig.defaultMaxBatchSize,
    parallelism: PositiveInt = ContractLoaderConfig.defaultMaxParallelism,
)

object ContractLoaderConfig {
  val defaultMaxQueueSize = PositiveInt.tryCreate(10000)
  val defaultMaxBatchSize = PositiveInt.tryCreate(50)
  val defaultMaxParallelism = PositiveInt.tryCreate(5)
}
