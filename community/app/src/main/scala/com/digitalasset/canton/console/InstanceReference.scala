// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import com.digitalasset.canton.*
import com.digitalasset.canton.admin.api.client.commands.{GrpcAdminCommand, HttpAdminCommand}
import com.digitalasset.canton.config.RequireTypes.Port
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
import com.digitalasset.canton.sequencing.SequencerConnection
import com.digitalasset.canton.topology.{DomainId, NodeIdentity, ParticipantId}
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.ErrorUtil

import scala.util.hashing.MurmurHash3

trait InstanceReference
    extends AdminCommandRunner
    with Helpful
    with NamedLogging
    with FeatureFlagFilter
    with PrettyPrinting
    with CertificateAdministration {

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

  def id: NodeIdentity

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
    new LocalKeyAdministrationGroup(this, this, consoleEnvironment, crypto)

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
    new KeyAdministrationGroup(this, this, consoleEnvironment)
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

  @Help.Summary("Manage participant replication")
  @Help.Group("Replication")
  def replication: ParticipantReplicationAdministrationGroup = replicationGroup
  lazy private val replicationGroup =
    new ParticipantReplicationAdministrationGroup(this, consoleEnvironment)

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

  private lazy val repair_ =
    new ParticipantRepairAdministration(consoleEnvironment, loggerFactory) {
      override protected def access[T](handler: ParticipantNode => T): T =
        LocalParticipantReference.this.access(handler)
    }
  @Help.Summary("Commands to repair the local participant contract state", FeatureFlag.Repair)
  @Help.Group("Repair")
  def repair: ParticipantRepairAdministration = repair_

  @Help.Summary("Inspect and manage parties")
  @Help.Group("Parties")
  override def parties: LocalParticipantPartiesAdministrationGroup = partiesGroup
  // above command needs to be def such that `Help` works.
  lazy private val partiesGroup =
    new LocalParticipantPartiesAdministrationGroup(this, this, consoleEnvironment, loggerFactory)

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
