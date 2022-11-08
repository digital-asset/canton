// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import com.digitalasset.canton.*
import com.digitalasset.canton.admin.api.client.commands.{GrpcAdminCommand, HttpAdminCommand}
import com.digitalasset.canton.config.RequireTypes.{Port, PositiveInt}
import com.digitalasset.canton.config.*
import com.digitalasset.canton.console.CommandErrors.NodeNotStarted
import com.digitalasset.canton.console.commands.*
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.domain.config.RemoteDomainConfig
import com.digitalasset.canton.domain.{Domain, DomainNodeBootstrap}
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.health.admin.data.{DomainStatus, NodeStatus, ParticipantStatus}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.participant.ParticipantNode
import com.digitalasset.canton.participant.config.{
  BaseParticipantConfig,
  LocalParticipantConfig,
  RemoteParticipantConfig,
}
import com.digitalasset.canton.participant.domain.DomainConnectionConfig
import com.digitalasset.canton.protocol.{LfContractId, SerializableContractWithWitnesses}
import com.digitalasset.canton.sequencing.SequencerConnection
import com.digitalasset.canton.topology.{DomainId, Identity, ParticipantId, PartyId}
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.version.ProtocolVersion

import java.io.File
import scala.util.hashing.MurmurHash3

trait InstanceReference
    extends AdminCommandRunner
    with Helpful
    with NamedLogging
    with FeatureFlagFilter
    with PrettyPrinting
    with CertificateAdministration {

  type InstanceId <: Identity

  val name: String
  protected val instanceType: String

  override def pretty: Pretty[InstanceReference] =
    prettyOfString(inst => show"${inst.instanceType.unquoted} ${inst.name.singleQuoted}")

  val consoleEnvironment: ConsoleEnvironment

  override protected[console] def tracedLogger: TracedLogger = logger

  override def hashCode(): Int = {
    val init = this.getClass.hashCode()
    val t1 = MurmurHash3.mix(init, consoleEnvironment.hashCode())
    val t2 = MurmurHash3.mix(t1, name.hashCode)
    t2
  }

  // this is just testing, because the cached values should remain unchanged in operation
  @Help.Summary("Clear locally cached variables", FeatureFlag.Testing)
  @Help.Description(
    "Some commands cache values on the client side. Use this command to explicitly clear the caches of these values."
  )
  def clear_cache(): Unit = {
    topology.clearCache()
  }

  type Status <: NodeStatus.Status

  def id: InstanceId

  def health: HealthAdministration[Status]

  def keys: KeyAdministrationGroup

  def parties: PartiesAdministrationGroup

  def topology: TopologyAdministrationGroup
}

/** Pointer for a potentially running instance by instance type (domain/participant) and its id.
  * These methods define the REPL interface for these instances (e.g. participant1 start)
  */
trait LocalInstanceReference extends InstanceReference with NoTracing {

  val name: String
  val consoleEnvironment: ConsoleEnvironment
  protected val nodes: Nodes[CantonNode, CantonNodeBootstrap[CantonNode]]

  @Help.Summary("Database related operations")
  @Help.Group("Database")
  object db extends Helpful {

    @Help.Summary("Migrates the instance's database if using a database storage")
    def migrate(): Unit = consoleEnvironment.run(migrateDbCommand())

    @Help.Summary(
      "Only use when advised - repairs the database migration of the instance's database"
    )
    @Help.Description(
      """In some rare cases, we change already applied database migration files in a new release and the repair
        |command resets the checksums we use to ensure that in general already applied migration files have not been changed.
        |You should only use `db.repair_migration` when advised and otherwise use it at your own risk - in the worst case running 
        |it may lead to data corruption when an incompatible database migration (one that should be rejected because 
        |the already applied database migration files have changed) is subsequently falsely applied.
        |"""
    )
    def repair_migration(force: Boolean = false): Unit =
      consoleEnvironment.run(repairMigrationCommand(force))

  }
  @Help.Summary("Start the instance")
  def start(): Unit = consoleEnvironment.run(startCommand())

  @Help.Summary("Stop the instance")
  def stop(): Unit = consoleEnvironment.run(stopCommand())

  @Help.Summary("Check if the local instance is running")
  def is_running: Boolean = nodes.isRunning(name)

  @Help.Summary("Check if the local instance is running and is fully initialized")
  def is_initialized: Boolean = nodes.getRunning(name).exists(_.isInitialized)

  @Help.Summary("Config of node instance")
  def config: LocalNodeConfig

  @Help.Summary("Manage public and secret keys")
  @Help.Group("Keys")
  override def keys: LocalKeyAdministrationGroup = _keys

  private val _keys =
    new LocalKeyAdministrationGroup(this, consoleEnvironment, crypto)

  private[console] def migrateDbCommand(): ConsoleCommandResult[Unit] =
    migrateInstanceDb().toResult(_.message, _ => ())

  private[console] def repairMigrationCommand(force: Boolean): ConsoleCommandResult[Unit] =
    repairMigrationOfInstance(force).toResult(_.message, _ => ())

  private[console] def startCommand(): ConsoleCommandResult[Unit] =
    startInstance()
      .toResult({
        case m: PendingDatabaseMigration =>
          s"${m.message} Please run `${m.name}.db.migrate` to apply pending migrations"
        case m => m.message
      })

  private[console] def stopCommand(): ConsoleCommandResult[Unit] =
    try {
      stopInstance().toResult(_.message)
    } finally {
      ErrorUtil.withThrowableLogging(clear_cache())
    }

  protected def migrateInstanceDb(): Either[StartupError, _] = nodes.migrateDatabase(name)
  protected def repairMigrationOfInstance(force: Boolean): Either[StartupError, Unit] = {
    Either
      .cond(force, (), DidntUseForceOnRepairMigration(name))
      .flatMap(_ => nodes.repairDatabaseMigration(name))
  }

  protected def startInstance(): Either[StartupError, Unit] = nodes.start(name).map(_ => ())
  protected def stopInstance(): Either[ShutdownError, Unit] = nodes.stop(name)
  protected[canton] def crypto: Crypto

  protected def runCommandIfRunning[Result](
      runner: => ConsoleCommandResult[Result]
  ): ConsoleCommandResult[Result] =
    if (is_running)
      runner
    else
      NodeNotStarted.ErrorCanton(this)

  override protected[console] def adminCommand[Result](
      grpcCommand: GrpcAdminCommand[_, _, Result],
      httpCommand: HttpAdminCommand[_, _, Result],
  ): ConsoleCommandResult[Result] =
    runCommandIfRunning(
      consoleEnvironment.grpcAdminCommandRunner
        .runCommand(name, grpcCommand, config.clientAdminApi, None)
    )

}

trait RemoteInstanceReference extends InstanceReference {
  @Help.Summary("Manage public and secret keys")
  @Help.Group("Keys")
  override val keys: KeyAdministrationGroup =
    new KeyAdministrationGroup(this, consoleEnvironment)
}

trait GrpcRemoteInstanceReference extends RemoteInstanceReference {
  def config: NodeConfig

  override protected[console] def adminCommand[Result](
      grpcCommand: GrpcAdminCommand[_, _, Result],
      httpCommand: HttpAdminCommand[_, _, Result],
  ): ConsoleCommandResult[Result] =
    consoleEnvironment.grpcAdminCommandRunner.runCommand(
      name,
      grpcCommand,
      config.clientAdminApi,
      None,
    )
}

object DomainReference {
  val InstanceType = "Domain"
}

trait DomainReference
    extends InstanceReference
    with DomainAdministration
    with InstanceReferenceWithSequencerConnection {
  val consoleEnvironment: ConsoleEnvironment
  val name: String

  override type InstanceId = DomainId

  override protected val instanceType: String = DomainReference.InstanceType

  override type Status = DomainStatus

  @Help.Summary("Health and diagnostic related commands")
  @Help.Group("Health")
  override def health =
    new HealthAdministration[DomainStatus](this, consoleEnvironment, DomainStatus.fromProtoV0)

  @Help.Summary(
    "Yields the globally unique id of this domain. " +
      "Throws an exception, if the id has not yet been allocated (e.g., the domain has not yet been started)."
  )
  def id: DomainId = topology.idHelper(name, DomainId(_))

  private lazy val topology_ =
    new TopologyAdministrationGroup(
      this,
      this.health.status.successOption.map(_.topologyQueue),
      consoleEnvironment,
      loggerFactory,
    )
  @Help.Summary("Topology management related commands")
  @Help.Group("Topology")
  @Help.Description("This group contains access to the full set of topology management commands.")
  override def topology: TopologyAdministrationGroup = topology_

  override protected val loggerFactory: NamedLoggerFactory = NamedLoggerFactory("domain", name)

  override def equals(obj: Any): Boolean = {
    obj match {
      case x: DomainReference => x.consoleEnvironment == consoleEnvironment && x.name == name
      case _ => false
    }
  }

  @Help.Summary("Inspect configured parties")
  @Help.Group("Parties")
  override def parties: PartiesAdministrationGroup = partiesGroup

  // above command needs to be def such that `Help` works.
  lazy private val partiesGroup = new PartiesAdministrationGroup(this, consoleEnvironment)

  private lazy val sequencer_ =
    new SequencerAdministrationGroup(this, consoleEnvironment, loggerFactory)
  @Help.Summary("Manage the sequencer")
  @Help.Group("Sequencer")
  override def sequencer: SequencerAdministrationGroup = sequencer_

  private lazy val mediator_ =
    new MediatorAdministrationGroup(this, consoleEnvironment, loggerFactory)
  @Help.Summary("Manage the mediator")
  @Help.Group("Mediator")
  def mediator: MediatorAdministrationGroup = mediator_

  @Help.Summary(
    "Yields a domain connection config with default values except for the domain alias and the sequencer connection. " +
      "May throw an exception if the domain alias or sequencer connection is misconfigured."
  )
  def defaultDomainConnection: DomainConnectionConfig =
    DomainConnectionConfig(DomainAlias.tryCreate(name), sequencerConnection)
}

trait RemoteDomainReference extends DomainReference with GrpcRemoteInstanceReference {
  val consoleEnvironment: ConsoleEnvironment
  val name: String

  @Help.Summary("Returns the remote domain configuration")
  def config: RemoteDomainConfig =
    consoleEnvironment.environment.config.remoteDomainsByString(name)

  override def sequencerConnection: SequencerConnection =
    config.publicApi.toConnection
      .fold(
        err => sys.error(s"Domain $name has invalid sequencer connection config: $err"),
        identity,
      )

}

trait CommunityDomainReference {
  this: DomainReference =>
}

class CommunityRemoteDomainReference(val consoleEnvironment: ConsoleEnvironment, val name: String)
    extends DomainReference
    with CommunityDomainReference
    with RemoteDomainReference

trait InstanceReferenceWithSequencerConnection extends InstanceReference {
  def sequencerConnection: SequencerConnection

  def sequencer: SequencerAdministrationGroup
}

trait LocalDomainReference
    extends DomainReference
    with BaseInspection[Domain]
    with LocalInstanceReference {
  override protected val nodes = consoleEnvironment.environment.domains

  @Help.Summary("Returns the domain configuration")
  def config: consoleEnvironment.environment.config.DomainConfigType =
    consoleEnvironment.environment.config.domainsByString(name)

  override def sequencerConnection: SequencerConnection =
    config.sequencerConnectionConfig.toConnection
      .fold(
        err => sys.error(s"Domain $name has invalid sequencer connection config: $err"),
        identity,
      )

  override protected[console] def runningNode: Option[DomainNodeBootstrap] =
    consoleEnvironment.environment.domains.getRunning(name)
}

class CommunityLocalDomainReference(
    override val consoleEnvironment: ConsoleEnvironment,
    val name: String,
) extends DomainReference
    with CommunityDomainReference
    with LocalDomainReference

/** Bare, Canton agnostic parts of the ledger-api client
  *
  * This implementation allows to access any kind of ledger-api client, which does not need to be Canton based.
  * However, this comes at some cost, as some of the synchronization between nodes during transaction submission
  * is not supported
  *
  * @param hostname the hostname of the ledger api server
  * @param port the port of the ledger api server
  * @param tls the tls config to use on the client
  * @param token the jwt token to use on the client
  */
class ExternalLedgerApiClient(
    hostname: String,
    port: Port,
    tls: Option[TlsClientConfig],
    token: Option[String] = None,
)(implicit val consoleEnvironment: ConsoleEnvironment)
    extends BaseLedgerApiAdministration
    with LedgerApiCommandRunner
    with FeatureFlagFilter
    with NamedLogging {

  override protected val name: String = s"$hostname:${port.unwrap}"

  override val loggerFactory: NamedLoggerFactory =
    consoleEnvironment.environment.loggerFactory.append("client", name)

  override protected def domainOfTransaction(transactionId: String): DomainId =
    throw new NotImplementedError("domain_of is not implemented for external ledger api clients")

  override protected[console] def ledgerApiCommand[Result](
      command: GrpcAdminCommand[_, _, Result]
  ): ConsoleCommandResult[Result] =
    consoleEnvironment.grpcAdminCommandRunner
      .runCommand("sourceLedger", command, ClientConfig(hostname, port, tls), token)

  override protected def optionallyAwait[Tx](
      tx: Tx,
      txId: String,
      optTimeout: Option[NonNegativeDuration],
  ): Tx = tx

}

object ExternalLedgerApiClient {

  def forReference(participant: LocalParticipantReference, token: String)(implicit
      env: ConsoleEnvironment
  ): ExternalLedgerApiClient = {
    val cc = participant.config.ledgerApi.clientConfig
    new ExternalLedgerApiClient(
      cc.address,
      cc.port,
      cc.tls,
      Some(token),
    )
  }
}

object ParticipantReference {
  val InstanceType = "Participant"
}

abstract class ParticipantReference(
    override val consoleEnvironment: ConsoleEnvironment,
    val name: String,
) extends InstanceReference
    with ParticipantAdministration
    with LedgerApiAdministration
    with LedgerApiCommandRunner {

  override type InstanceId = ParticipantId

  override protected val instanceType: String = ParticipantReference.InstanceType

  override protected val loggerFactory: NamedLoggerFactory =
    consoleEnvironment.environment.loggerFactory.append("participant", name)

  override type Status = ParticipantStatus

  @Help.Summary("Health and diagnostic related commands")
  @Help.Group("Health")
  override def health: ParticipantHealthAdministration =
    new ParticipantHealthAdministration(this, consoleEnvironment, loggerFactory)

  @Help.Summary(
    "Yields the globally unique id of this participant. " +
      "Throws an exception, if the id has not yet been allocated (e.g., the participant has not yet been started)."
  )
  override def id: ParticipantId = topology.idHelper(name, ParticipantId(_))

  private lazy val topology_ =
    new TopologyAdministrationGroup(
      this,
      health.status.successOption.map(_.topologyQueue),
      consoleEnvironment,
      loggerFactory,
    )
  @Help.Summary("Topology management related commands")
  @Help.Group("Topology")
  @Help.Description("This group contains access to the full set of topology management commands.")
  def topology: TopologyAdministrationGroup = topology_

  @Help.Summary("Commands used for development and testing", FeatureFlag.Testing)
  @Help.Group("Testing")
  def testing: ParticipantTestingGroup

  @Help.Summary("Commands to pruning the archive of the ledger", FeatureFlag.Preview)
  @Help.Group("Ledger Pruning")
  def pruning: ParticipantPruningAdministrationGroup

  @Help.Summary("Inspect and manage parties")
  @Help.Group("Parties")
  override def parties: ParticipantPartiesAdministrationGroup

  def config: BaseParticipantConfig

}

class RemoteParticipantReference(environment: ConsoleEnvironment, override val name: String)
    extends ParticipantReference(environment, name)
    with GrpcRemoteInstanceReference {

  @Help.Summary("Return remote participant config")
  def config: RemoteParticipantConfig =
    consoleEnvironment.environment.config.remoteParticipantsByString(name)

  override def equals(obj: Any): Boolean = {
    obj match {
      case x: RemoteParticipantReference =>
        x.consoleEnvironment == consoleEnvironment && x.name == name
      case _ => false
    }
  }

  override protected[console] def ledgerApiCommand[Result](
      command: GrpcAdminCommand[_, _, Result]
  ): ConsoleCommandResult[Result] =
    consoleEnvironment.grpcAdminCommandRunner.runCommand(
      name,
      command,
      config.ledgerApi,
      config.token,
    )

  @Help.Summary("Inspect and manage parties")
  @Help.Group("Parties")
  override def parties: ParticipantPartiesAdministrationGroup = partiesGroup
  // above command needs to be def such that `Help` works.
  lazy private val partiesGroup =
    new ParticipantPartiesAdministrationGroup(id, this, consoleEnvironment)

  private lazy val testing_ = new ParticipantTestingGroup(this, consoleEnvironment, loggerFactory)
  @Help.Summary("Commands used for development and testing", FeatureFlag.Testing)
  @Help.Group("Testing")
  override def testing: ParticipantTestingGroup = testing_

  private lazy val pruning_ =
    new ParticipantPruningAdministrationGroup(this, consoleEnvironment, loggerFactory)
  @Help.Summary("Commands to prune the archive of the participant ledger")
  @Help.Group("Participant Pruning")
  def pruning: ParticipantPruningAdministrationGroup = pruning_

}

class LocalParticipantReference(override val consoleEnvironment: ConsoleEnvironment, name: String)
    extends ParticipantReference(consoleEnvironment, name)
    with LocalInstanceReference
    with BaseInspection[ParticipantNode] {

  protected val nodes = consoleEnvironment.environment.participants

  @Help.Summary("Return participant config")
  def config: LocalParticipantConfig =
    consoleEnvironment.environment.config.participantsByString(name)

  private lazy val testing_ =
    new LocalParticipantTestingGroup(this, consoleEnvironment, loggerFactory)
  @Help.Summary("Commands used for development and testing", FeatureFlag.Testing)
  override def testing: LocalParticipantTestingGroup = testing_

  private lazy val pruning_ =
    new LocalParticipantPruningAdministrationGroup(this, consoleEnvironment, loggerFactory)
  @Help.Summary("Commands to truncate the archive of the ledger", FeatureFlag.Preview)
  @Help.Group("Ledger Pruning")
  def pruning: LocalParticipantPruningAdministrationGroup = pruning_

  private lazy val commitments_ =
    new LocalCommitmentsAdministrationGroup(this, consoleEnvironment, loggerFactory)
  @Help.Summary("Commands to inspect and extract bilateral commitments", FeatureFlag.Preview)
  @Help.Group("Commitments")
  def commitments: LocalCommitmentsAdministrationGroup = commitments_

  @Help.Summary("Commands to repair the local participant contract state", FeatureFlag.Repair)
  @Help.Group("Repair")
  object repair extends Helpful {

    @Help.Summary("Add specified contracts to specific domain on local participant.")
    @Help.Description(
      """This is a last resort command to recover from data corruption, e.g. in scenarios in which participant
        |contracts have somehow gotten out of sync and need to be manually created. The participant needs to be
        |disconnected from the specified "domain" at the time of the call, and as of now the domain cannot have had
        |any inflight requests.
        |For each "contractsToAdd", specify "witnesses", local parties, in case no local party is a stakeholder.
        |The "ignoreAlreadyAdded" flag makes it possible to invoke the command multiple times with the same
        |parameters in case an earlier command invocation has failed.
        |As repair commands are powerful tools to recover from unforeseen data corruption, but dangerous under normal
        |operation, use of this command requires (temporarily) enabling the "features.enable-repair-commands"
        |configuration. In addition repair commands can run for an unbounded time depending on the number of
        |contracts passed in. Be sure to not connect the participant to the domain until the call returns."""
    )
    def add(
        domain: DomainAlias,
        contractsToAdd: Seq[SerializableContractWithWitnesses],
        ignoreAlreadyAdded: Boolean = true,
    ): Unit =
      runRepairCommand(tc =>
        access(_.sync.repairService.addContracts(domain, contractsToAdd, ignoreAlreadyAdded)(tc))
      )

    private def runRepairCommand[T](command: TraceContext => Either[String, T]): T =
      check(FeatureFlag.Repair) {
        consoleEnvironment.run {
          ConsoleCommandResult.fromEither {
            // Ensure that admin repair commands have a non-empty trace context.
            TraceContext.withNewTraceContext(command(_))
          }
        }
      }

    @Help.Summary("Purge contracts with specified Contract IDs from local participant.")
    @Help.Description(
      """This is a last resort command to recover from data corruption, e.g. in scenarios in which participant
        |contracts have somehow gotten out of sync and need to be manually purged, or in situations in which 
        |stakeholders are no longer available to agree to their archival. The participant needs to be disconnected from
        |the domain on which the contracts with "contractIds" reside at the time of the call, and as of now the domain
        |cannot have had any inflight requests.
        |The "ignoreAlreadyPurged" flag makes it possible to invoke the command multiple times with the same
        |parameters in case an earlier command invocation has failed.
        |As repair commands are powerful tools to recover from unforeseen data corruption, but dangerous under normal
        |operation, use of this command requires (temporarily) enabling the "features.enable-repair-commands"
        |configuration. In addition repair commands can run for an unbounded time depending on the number of
        |contract ids passed in. Be sure to not connect the participant to the domain until the call returns."""
    )
    def purge(
        domain: DomainAlias,
        contractIds: Seq[LfContractId],
        ignoreAlreadyPurged: Boolean = true,
    ): Unit =
      runRepairCommand(tc =>
        access(_.sync.repairService.purgeContracts(domain, contractIds, ignoreAlreadyPurged)(tc))
      )

    @Help.Summary("Move contracts with specified Contract IDs from one domain to another.")
    @Help.Description(
      """This is a last resort command to recover from data corruption in scenarios in which a domain is
        |irreparably broken and formerly connected participants need to move contracts to another, healthy domain.
        |The participant needs to be disconnected from both the "sourceDomain" and the "targetDomain". Also as of now
        |the target domain cannot have had any inflight requests.
        |Contracts already present in the target domain will be skipped, and this makes it possible to invoke this
        |command in an "idempotent" fashion in case an earlier attempt had resulted in an error.
        |The "skipInactive" flag makes it possible to only move active contracts in the "sourceDomain".
        |As repair commands are powerful tools to recover from unforeseen data corruption, but dangerous under normal
        |operation, use of this command requires (temporarily) enabling the "features.enable-repair-commands"
        |configuration. In addition repair commands can run for an unbounded time depending on the number of
        |contract ids passed in. Be sure to not connect the participant to either domain until the call returns.
        
        Arguments:
        - contractIds - set of contract ids that should be moved to the new domain
        - sourceDomain - alias of the source domain
        - targetDomain - alias of the target domain
        - skipInactive - (default true) whether to skip inactive contracts mentioned in the contractIds list
        - batchSize - (default 100) how many contracts to write at once to the database"""
    )
    def change_domain(
        contractIds: Seq[LfContractId],
        sourceDomain: DomainAlias,
        targetDomain: DomainAlias,
        skipInactive: Boolean = true,
        batchSize: Int = 100,
    ): Unit =
      runRepairCommand(tc =>
        access(
          _.sync.repairService.changeDomainAwait(
            contractIds,
            sourceDomain,
            targetDomain,
            skipInactive,
            PositiveInt.tryCreate(batchSize),
          )(tc)
        )
      )

    @Help.Summary("Migrate domain to a new version.")
    @Help.Description(
      """This method can be used to migrate all the contracts associated with a domain to a new domain connection.
         This method will register the new domain, connect to it and then re-associate all contracts on the source
         domain to the target domain. Please note that this migration needs to be done by all participants 
         at the same time. The domain should only be used once all participants have finished their migration.
         
         The arguments are:
         source: the domain alias of the source domain
         target: the configuration for the target domain
         """
    )
    def migrate_domain(
        source: DomainAlias,
        target: DomainConnectionConfig,
    ): Unit = {
      implicit val ec = consoleEnvironment.environment.executionContext
      runRepairCommand(tc =>
        consoleEnvironment.commandTimeouts.unbounded
          .await(s"running command to migrate from domain $source to domain $target")(
            access(
              _.sync
                .migrateDomain(source, target)(tc)
                .leftMap(_.asGrpcError.getStatus.getDescription)
                .value
                .onShutdown {
                  Left(("Aborted due to shutdown. Please restart me."))
                }
            )
          )
      )
    }

    @Help.Summary("Mark sequenced events as ignored.")
    @Help.Description(
      """This is the last resort to ignore events that the participant is unable to process.
        |Ignoring events may lead to subsequent failures, e.g., if the event creating a contract is ignored and
        |that contract is subsequently used. It may also lead to ledger forks if other participants still process
        |the ignored events.
        |It is possible to mark events as ignored that the participant has not yet received.
        |
        |The command will fail, if marking events between `from` and `to` as ignored would result in a gap in sequencer counters,
        |namely if `from <= to` and `from` is greater than `maxSequencerCounter + 1`, 
        |where `maxSequencerCounter` is the greatest sequencer counter of a sequenced event stored by the underlying participant.
        |
        |The command will also fail, if `force == false` and `from` is smaller than the sequencer counter of the last event
        |that has been marked as clean. 
        |(Ignoring such events would normally have no effect, as they have already been processed.)"""
    )
    def ignore_events(
        domainId: DomainId,
        from: SequencerCounter,
        to: SequencerCounter,
        force: Boolean = false,
    ): Unit =
      runRepairCommand(tc =>
        access { _.sync.repairService.ignoreEvents(domainId, from, to, force)(tc) }
      )

    @Help.Summary("Remove the ignored status from sequenced events.")
    @Help.Description(
      """This command has no effect on ordinary (i.e., not ignored) events and on events that do not exist.
        |
        |The command will fail, if marking events between `from` and `to` as unignored would result in a gap in sequencer counters,
        |namely if there is one empty ignored event with sequencer counter between `from` and `to` and 
        |another empty ignored event with sequencer counter greater than `to`.
        |An empty ignored event is an event that has been marked as ignored and not yet received by the participant.
        |
        |The command will also fail, if `force == false` and `from` is smaller than the sequencer counter of the last event
        |that has been marked as clean. 
        |(Unignoring such events would normally have no effect, as they have already been processed.)"""
    )
    def unignore_events(
        domainId: DomainId,
        from: SequencerCounter,
        to: SequencerCounter,
        force: Boolean = false,
    ): Unit =
      runRepairCommand(tc =>
        access { _.sync.repairService.unignoreEvents(domainId, from, to, force)(tc) }
      )

    @Help.Summary("Download all contracts for the given set of parties to a file.")
    @Help.Description(
      """This command can be used to download the current active contract set of a given set of parties to a text file.
        |This is mainly interesting for recovery and operational purposes.
        |
        |The file will contain base64 encoded strings, one line per contract. The lines are written 
        |sorted according to their domain and contract id. This allows to compare the contracts stored
        |by two participants using standard file comparison tools.
        |The domain-id is printed with the prefix domain-id before the block of contracts starts.
        |
        |This command may take a long time to complete and may require significant resources. 
        |It will first load the contract ids of the active contract set into memory and then subsequently
        |load the contracts in batches and inspect their stakeholders. As this operation needs to traverse
        |the entire datastore, it might take a long time to complete. 
        |
        The arguments are:
        - parties: identifying contracts having at least one stakeholder from the given set
        - target: the target file where to store the data. Use .gz as a suffix to get a compressed file (recommended)
        - protocolVersion: the protocol version to use for the serialization. Defaults to latest.
        - filterDomainId: restrict the export to a given domain 
        - batchSize: batch size used to load contracts. Defaults to 1000. 
        """
    )
    def download(
        parties: Set[PartyId],
        target: String,
        protocolVersion: ProtocolVersion = ProtocolVersion.latest,
        filterDomainId: String = "",
        batchSize: PositiveInt = PositiveInt.tryCreate(1000),
    ): Unit = {
      runRepairCommand { tc =>
        access(
          _.sync.stateInspection
            .storeActiveContractsToFile(
              parties,
              new File(target),
              batchSize,
              _.filterString.startsWith(filterDomainId),
              protocolVersion,
            )(tc)
        )
      }
    }

  }

  @Help.Summary("Inspect and manage parties")
  @Help.Group("Parties")
  override def parties: LocalParticipantPartiesAdministrationGroup = partiesGroup
  // above command needs to be def such that `Help` works.
  lazy private val partiesGroup =
    new LocalParticipantPartiesAdministrationGroup(this, this, consoleEnvironment, loggerFactory)

  @Help.Summary("Manage participant replication")
  @Help.Group("Replication")
  def replication: ParticipantReplicationAdministrationGroup = replicationGroup
  lazy private val replicationGroup =
    new ParticipantReplicationAdministrationGroup(this, consoleEnvironment)

  /** secret, not publicly documented way to get the admin token */
  def adminToken: Option[String] = underlying.map(_.adminToken.secret)

  override def equals(obj: Any): Boolean = {
    obj match {
      case x: LocalParticipantReference =>
        x.consoleEnvironment == consoleEnvironment && x.name == name
      case _ => false
    }
  }

  override def runningNode: Option[CantonNodeBootstrap[ParticipantNode]] =
    consoleEnvironment.environment.participants.getRunning(name)

  override protected[console] def ledgerApiCommand[Result](
      command: GrpcAdminCommand[_, _, Result]
  ): ConsoleCommandResult[Result] =
    runCommandIfRunning(
      consoleEnvironment.grpcAdminCommandRunner
        .runCommand(name, command, config.clientLedgerApi, adminToken)
    )

}
