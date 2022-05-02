// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import cats.syntax.option._
import cats.syntax.traverse._
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands.Resources.{
  GetResourceLimits,
  SetResourceLimits,
}
import com.digitalasset.canton.admin.api.client.commands.{
  DomainTimeCommands,
  LedgerApiCommands,
  ParticipantAdminCommands,
}
import com.digitalasset.canton.admin.api.client.data.ListConnectedDomainsResult
import com.digitalasset.canton.config.TimeoutDuration
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  BaseInspection,
  CommandFailure,
  ConsoleCommandResult,
  ConsoleEnvironment,
  ConsoleMacros,
  DomainReference,
  FeatureFlag,
  FeatureFlagFilter,
  Help,
  Helpful,
  InstanceReferenceWithSequencerConnection,
  LedgerApiCommandRunner,
  ParticipantReference,
}
import com.digitalasset.canton.crypto.SyncCryptoApiProvider
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.admin.data.ParticipantStatus
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.participant.ParticipantNode
import com.digitalasset.canton.participant.admin.grpc.TransferSearchResult
import com.digitalasset.canton.participant.admin.{ResourceLimits, SyncStateInspection, v0}
import com.digitalasset.canton.participant.domain.DomainConnectionConfig
import com.digitalasset.canton.participant.sync.TimestampedEvent
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  SignedProtocolMessage,
}
import com.digitalasset.canton.protocol.{
  LfCommittedTransaction,
  LfContractId,
  SerializableContract,
  TransferId,
}
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  PossiblyIgnoredProtocolEvent,
  SequencerConnection,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.time.{DomainTimeTrackerConfig, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.{DomainId, ParticipantId, PartyId}
import com.digitalasset.canton.util.ShowUtil._
import com.digitalasset.canton.util._
import com.digitalasset.canton.DomainAlias

import java.time.Instant
import scala.concurrent.TimeoutException
import scala.concurrent.duration.Duration

sealed trait DomainChoice
object DomainChoice {
  object All extends DomainChoice
  case class Only(aliases: Seq[DomainAlias]) extends DomainChoice
}

private[console] object ParticipantCommands {

  object dars {

    def upload(
        runner: AdminCommandRunner,
        path: String,
        vetAllPackages: Boolean,
        synchronizeVetting: Boolean,
        logger: TracedLogger,
    ) =
      runner.adminCommand(
        ParticipantAdminCommands.Package
          .UploadDar(Some(path), vetAllPackages, synchronizeVetting, logger)
      )

  }

  object domains {

    def referenceToConfig(
        domain: InstanceReferenceWithSequencerConnection,
        manualConnect: Boolean = false,
        alias: Option[DomainAlias] = None,
        maxRetryDelay: Option[NonNegativeFiniteDuration] = None,
        priority: Int = 0,
    ): DomainConnectionConfig = {
      val domainAlias = alias.getOrElse(DomainAlias.tryCreate(domain.name))
      DomainConnectionConfig(
        domainAlias,
        domain.sequencerConnection,
        manualConnect = manualConnect,
        None,
        priority,
        None,
        maxRetryDelay,
        DomainTimeTrackerConfig(),
      )
    }

    def toConfig(
        domainAlias: DomainAlias,
        connection: String,
        manualConnect: Boolean = false,
        domainId: Option[DomainId] = None,
        certificatesPath: String = "",
        priority: Int = 0,
        initialRetryDelay: Option[NonNegativeFiniteDuration] = None,
        maxRetryDelay: Option[NonNegativeFiniteDuration] = None,
        timeTrackerConfig: DomainTimeTrackerConfig = DomainTimeTrackerConfig(),
    ): DomainConnectionConfig = {
      // architecture-handbook-entry-begin: OnboardParticipantToConfig
      val certificates = OptionUtil.emptyStringAsNone(certificatesPath).map { path =>
        BinaryFileUtil.readByteStringFromFile(path) match {
          case Left(err) => throw new IllegalArgumentException(s"failed to load ${path}: ${err}")
          case Right(bs) => bs
        }
      }
      DomainConnectionConfig.grpc(
        domainAlias,
        connection,
        manualConnect,
        domainId,
        certificates,
        priority,
        initialRetryDelay,
        maxRetryDelay,
        timeTrackerConfig,
      )
      // architecture-handbook-entry-end: OnboardParticipantToConfig
    }

    def register(runner: AdminCommandRunner, config: DomainConnectionConfig) =
      runner.adminCommand(
        ParticipantAdminCommands.DomainConnectivity.RegisterDomain(config)
      )
    def reconnect(runner: AdminCommandRunner, domainAlias: DomainAlias, retry: Boolean) = {
      runner.adminCommand(
        ParticipantAdminCommands.DomainConnectivity.ConnectDomain(domainAlias, retry)
      )
    }

    def reconnect_all(runner: AdminCommandRunner, ignoreFailures: Boolean) =
      runner.adminCommand(
        ParticipantAdminCommands.DomainConnectivity.ReconnectDomains(ignoreFailures)
      )

    def disconnect(runner: AdminCommandRunner, domainAlias: DomainAlias) =
      runner.adminCommand(ParticipantAdminCommands.DomainConnectivity.DisconnectDomain(domainAlias))

  }
}

class ParticipantTestingGroup(
    participantRef: ParticipantReference,
    val consoleEnvironment: ConsoleEnvironment,
    val loggerFactory: NamedLoggerFactory,
) extends FeatureFlagFilter
    with Helpful {
  import participantRef._

  @Help.Summary(
    "Send a bong to a set of target parties over the ledger. Levels > 0 leads to an exploding ping with exponential number of contracts. " +
      "Throw a RuntimeException in case of failure.",
    FeatureFlag.Testing,
  )
  @Help.Description(
    """Initiates a racy ping to multiple participants,
     measuring the roundtrip time of the fastest responder, with an optional timeout.
     Grace-period is the time the bong will wait for a duplicate spent (which would indicate an error in the system) before exiting.
     If levels > 0, the ping command will lead to a binary explosion and subsequent dilation of
     contracts, where ``level`` determines the number of levels we will explode. As a result, the system will create
     (2^(L+2) - 3) contracts (where L stands for ``level``).
     Normally, only the initiator is a validator. Additional validators can be added using the validators argument.
     The bong command comes handy to run a burst test against the system and quickly leads to an overloading state."""
  )
  def bong(
      targets: Set[ParticipantId],
      validators: Set[ParticipantId] = Set(),
      timeout: TimeoutDuration = consoleEnvironment.commandTimeouts.testingBong,
      levels: Long = 0,
      gracePeriodMillis: Long = 1000,
      workflowId: String = "",
      id: String = "",
  ): Duration = {
    val result =
      maybe_bong(targets, validators, timeout, levels, gracePeriodMillis, workflowId, id)
        .toRight(
          s"Unable to bong $targets with $levels levels within ${LoggerUtil.roundDurationForHumans(timeout.duration)}"
        )
    consoleEnvironment.run(ConsoleCommandResult.fromEither(result))
  }

  @Help.Summary("Like bong, but returns None in case of failure.", FeatureFlag.Testing)
  def maybe_bong(
      targets: Set[ParticipantId],
      validators: Set[ParticipantId] = Set(),
      timeout: TimeoutDuration = consoleEnvironment.commandTimeouts.testingBong,
      levels: Long = 0,
      gracePeriodMillis: Long = 1000,
      workflowId: String = "",
      id: String = "",
  ): Option[Duration] =
    check(FeatureFlag.Testing)(consoleEnvironment.run {
      adminCommand(
        ParticipantAdminCommands.Ping
          .Ping(
            targets.map(_.adminParty.toLf),
            validators.map(_.adminParty.toLf),
            timeout.duration.toMillis,
            levels,
            gracePeriodMillis,
            workflowId,
            id,
          )
      )
    })

  @Help.Summary("Fetch the current time from the given domain", FeatureFlag.Testing)
  def fetch_domain_time(
      domainAlias: DomainAlias,
      timeout: TimeoutDuration,
  ): CantonTimestamp =
    check(FeatureFlag.Testing) {
      val id = participantRef.domains.id_of(domainAlias)
      fetch_domain_time(id, timeout)
    }

  @Help.Summary("Fetch the current time from the given domain", FeatureFlag.Testing)
  def fetch_domain_time(
      domainId: DomainId,
      timeout: TimeoutDuration = consoleEnvironment.commandTimeouts.ledgerCommand,
  ): CantonTimestamp =
    check(FeatureFlag.Testing) {
      consoleEnvironment.run {
        adminCommand(
          DomainTimeCommands.FetchTime(
            domainId.some,
            NonNegativeFiniteDuration.Zero,
            timeout,
          )
        )
      }.timestamp
    }

  @Help.Summary("Fetch the current time from all connected domains", FeatureFlag.Testing)
  def fetch_domain_times(
      timeout: TimeoutDuration = consoleEnvironment.commandTimeouts.ledgerCommand
  ): Unit =
    check(FeatureFlag.Testing) {
      participantRef.domains.list_connected().foreach { item =>
        fetch_domain_time(item.domainId, timeout)
      }
    }

  @Help.Summary("Await for the given time to be reached on the given domain", FeatureFlag.Testing)
  def await_domain_time(
      domainAlias: DomainAlias,
      time: CantonTimestamp,
      timeout: TimeoutDuration,
  ): Unit =
    check(FeatureFlag.Testing) {
      val id = participantRef.domains.id_of(domainAlias)
      await_domain_time(id, time, timeout)
    }

  @Help.Summary("Await for the given time to be reached on the given domain", FeatureFlag.Testing)
  def await_domain_time(
      domainId: DomainId,
      time: CantonTimestamp,
      timeout: TimeoutDuration = consoleEnvironment.commandTimeouts.ledgerCommand,
  ): Unit =
    check(FeatureFlag.Testing) {
      consoleEnvironment.run {
        adminCommand(
          DomainTimeCommands.AwaitTime(
            domainId.some,
            time,
            timeout,
          )
        )
      }
    }
}

class LocalParticipantTestingGroup(
    participantRef: ParticipantReference with BaseInspection[ParticipantNode],
    consoleEnvironment: ConsoleEnvironment,
    loggerFactory: NamedLoggerFactory,
) extends ParticipantTestingGroup(participantRef, consoleEnvironment, loggerFactory)
    with FeatureFlagFilter {

  import participantRef._
  @Help.Summary("Lookup contracts in the Private Contract Store", FeatureFlag.Testing)
  @Help.Description("""Get raw access to the PCS of the given domain sync controller.
  The filter commands will check if the target value ``contains`` the given string.
  The arguments can be started with ``^`` such that ``startsWith`` is used for comparison or ``!`` to use ``equals``.
  The ``activeSet`` argument allows to restrict the search to the active contract set.
  """)
  def pcs_search(
      domainAlias: DomainAlias,
      // filter by id (which is txId::discriminator, so can be used to look for both)
      filterId: String = "",
      filterPackage: String = "",
      filterTemplate: String = "",
      // only include active contracts
      activeSet: Boolean = false,
      limit: Int = 100,
  ): List[(Boolean, SerializableContract)] = {
    def toOpt(str: String) = OptionUtil.emptyStringAsNone(str)

    val pcs = state_inspection
      .findContracts(
        domainAlias,
        toOpt(filterId),
        toOpt(filterPackage),
        toOpt(filterTemplate),
        limit,
      )
    if (activeSet) pcs.filter { case (isActive, _) => isActive }
    else pcs
  }

  @Help.Summary("Lookup of active contracts", FeatureFlag.Testing)
  def acs_search(
      domainAlias: DomainAlias,
      // filter by id (which is txId::discriminator, so can be used to look for both)
      filterId: String = "",
      filterPackage: String = "",
      filterTemplate: String = "",
      limit: Int = 100,
  ): List[SerializableContract] = check(FeatureFlag.Testing) {
    pcs_search(domainAlias, filterId, filterPackage, filterTemplate, activeSet = true, limit).map(
      _._2
    )
  }

  @Help.Summary("Lookup of events", FeatureFlag.Testing)
  @Help.Description(
    """Show the event logs. To select only events from a particular domain, use the domain alias.
       Leave the domain blank to search the combined event log containing the events of all domains.
       Note that if the domain is left blank, the values of `from` and `to` cannot be set.
       This is because the combined event log isn't guaranteed to have increasing timestamps.
    """
  )
  def event_search(
      domain: DomainAlias = DomainAlias.tryCreate(""),
      from: Option[Instant] = None,
      to: Option[Instant] = None,
      limit: Option[Int] = None,
  ): Seq[(String, TimestampedEvent)] = {
    check(FeatureFlag.Testing) {
      if (domain == DomainAlias.tryCreate("") && (from.isDefined || to.isDefined)) {
        logger.error(
          s"You are not allowed to set values for 'from' and 'to' if searching the combined event log " +
            s"(you are searching the combined event log because you left the domain blank)."
        )
        throw new CommandFailure()
      } else {
        stateInspection.findEvents(
          domain,
          from.map(timestampFromInstant),
          to.map(timestampFromInstant),
          limit,
        )
      }
    }
  }

  @Help.Summary("Lookup of accepted transactions", FeatureFlag.Testing)
  @Help.Description("""Show the accepted transactions as they appear in the event logs.
       To select only transactions from a particular domain, use the domain alias.
       Leave the domain blank to search the combined event log containing the events of all domains.
       Note that if the domain is left blank, the values of `from` and `to` cannot be set.
       This is because the combined event log isn't guaranteed to have increasing timestamps.
    """)
  def transaction_search(
      domain: DomainAlias,
      from: Option[Instant] = None,
      to: Option[Instant] = None,
      limit: Option[Int] = None,
  ): Seq[(String, LfCommittedTransaction)] =
    check(FeatureFlag.Testing) {
      if (domain.unwrap == "" && (from.isDefined || to.isDefined)) {
        logger.error(
          s"You are not allowed to set values for 'from' and 'to' if searching the combined event log " +
            s"(you are searching the combined event log because you left the domain blank)."
        )
        throw new CommandFailure()
      } else {
        stateInspection.findAcceptedTransactions(
          domain,
          from.map(timestampFromInstant),
          to.map(timestampFromInstant),
          limit,
        )
      }
    }

  @Help.Summary("Retrieve all sequencer messages", FeatureFlag.Testing)
  @Help.Description("""Optionally allows filtering for sequencer from a certain time span (inclusive on both ends) and
      |limiting the number of displayed messages. The returned messages will be ordered on most domain ledger implementations
      |if a time span is given.
      |
      |Fails if the participant has never connected to the domain.""")
  def sequencer_messages(
      domain: DomainAlias,
      from: Option[Instant] = None,
      to: Option[Instant] = None,
      limit: Option[Int] = None,
  ): Seq[PossiblyIgnoredProtocolEvent] =
    state_inspection.findMessages(domain, from, to, limit)

  @Help.Summary(
    "Return the sync crypto api provider, which provides access to all cryptographic methods",
    FeatureFlag.Testing,
  )
  def crypto_api(): SyncCryptoApiProvider = check(FeatureFlag.Testing) {
    access(node => node.sync.syncCrypto)
  }

  @Help.Summary(
    "The latest timestamp before or at the given one for which no commitment is outstanding",
    FeatureFlag.Testing,
  )
  @Help.Description(
    """The latest timestamp before or at the given one for which no commitment is outstanding.
      |Note that this doesn't imply that pruning is possible at this timestamp, as the system might require some
      |additional data for crash recovery. Thus, this is useful for testing commitments; use the commands in the pruning
      |group for pruning.
      |Additionally, the result needn't fall on a "commitment tick" as specified by the reconciliation interval."""
  )
  def find_clean_commitments_timestamp(
      domain: DomainAlias,
      beforeOrAt: CantonTimestamp = CantonTimestamp.now(),
  ): Option[CantonTimestamp] =
    state_inspection.noOutstandingCommitmentsTs(domain, beforeOrAt)

  @Help.Summary(
    "Obtain access to the state inspection interface. Use at your own risk.",
    FeatureFlag.Testing,
  )
  @Help.Description(
    """The state inspection methods can fatally and permanently corrupt the state of a participant. 
      |The API is subject to change in any way."""
  )
  def state_inspection: SyncStateInspection = check(FeatureFlag.Testing) { stateInspection }

  private def stateInspection: SyncStateInspection = access(node => node.sync.stateInspection)

}

class ParticipantPruningAdministrationGroup(
    runner: LedgerApiCommandRunner with AdminCommandRunner,
    val consoleEnvironment: ConsoleEnvironment,
    val loggerFactory: NamedLoggerFactory,
) extends FeatureFlagFilter
    with Helpful {

  import runner._

  @Help.Summary("Prune the ledger up to the specified offset inclusively.")
  @Help.Description(
    """Prunes the participant ledger up to the specified offset inclusively returning ``Unit`` if the ledger has been
      |successfully pruned.
      |Note that upon successful pruning, subsequent attempts to read transactions via ``ledger_api.transactions.flat`` or
      |``ledger_api.transactions.trees`` or command completions via ``ledger_api.completions.list`` by specifying a begin offset
      |lower than the returned pruning offset will result in a ``NOT_FOUND`` error.
      |In the Enterprise Edition, ``prune`` performs a "full prune" freeing up significantly more space and also
      |performs additional safety checks returning a ``NOT_FOUND`` error if ``pruneUpTo`` is higher than the
      |offset returned by ``find_safe_offset`` on any domain with events preceding the pruning offset."""
  )
  def prune(pruneUpTo: LedgerOffset): Unit =
    consoleEnvironment.run(
      ledgerApiCommand(LedgerApiCommands.ParticipantPruningService.Prune(pruneUpTo))
    )

  @Help.Summary(
    "Prune only internal ledger state up to the specified offset inclusively.",
    FeatureFlag.Preview,
  )
  @Help.Description(
    """Special-purpose variant of the ``prune`` command only available in the Enterprise Edition that prunes only partial,
      |internal participant ledger state freeing up space not needed for serving ``ledger_api.transactions``
      |and ``ledger_api.completions`` requests. In conjunction with ``prune``, ``prune_internally`` enables pruning
      |internal ledger state more aggressively than externally observable data via the ledger api. In most use cases
      |``prune`` should be used instead. Unlike ``prune``, ``prune_internally`` has no visible effect on the Ledger API.
      |The command returns ``Unit`` if the ledger has been successfully pruned or an error if the timestamp
      |performs additional safety checks returning a ``NOT_FOUND`` error if ``pruneUpTo`` is higher than the
      |offset returned by ``find_safe_offset`` on any domain with events preceding the pruning offset."""
  )
  // Consider adding an "Enterprise" annotation if we end up having more enterprise-only commands than this lone enterprise command.
  def prune_internally(pruneUpTo: LedgerOffset): Unit =
    check(FeatureFlag.Preview) {
      consoleEnvironment.run(
        adminCommand(ParticipantAdminCommands.Pruning.PruneInternallyCommand(pruneUpTo))
      )
    }

  @Help.Summary(
    "Identify the participant ledger offset to prune up to based on the specified timestamp."
  )
  @Help.Description(
    """Return the largest participant ledger offset that has been processed before or at the specified timestamp.
      |The time is measured on the participant's local clock at some point while the participant has processed the
      |the event. Returns ``None`` if no such offset exists.
    """
  )
  def get_offset_by_time(upToInclusive: Instant): Option[LedgerOffset] =
    consoleEnvironment.run(
      adminCommand(
        ParticipantAdminCommands.Inspection.LookupOffsetByTime(
          ProtoConverter.InstantConverter.toProtoPrimitive(upToInclusive)
        )
      )
    ) match {
      case "" => None
      case offset => Some(LedgerOffset(LedgerOffset.Value.Absolute(offset)))
    }

  @Help.Summary("Identify the participant ledger offset to prune up to.", FeatureFlag.Preview)
  @Help.Description(
    """Return the participant ledger offset that corresponds to pruning "n" number of transactions
      |from the beginning of the ledger. Errors if the ledger holds less than "n" transactions. Specifying "n" of 1
      |returns the offset of the first transaction (if the ledger is non-empty).
    """
  )
  def locate_offset(n: Long): LedgerOffset =
    check(FeatureFlag.Preview) {
      val rawOffset = consoleEnvironment.run(
        adminCommand(ParticipantAdminCommands.Inspection.LookupOffsetByIndex(n))
      )
      LedgerOffset(LedgerOffset.Value.Absolute(rawOffset))
    }

}

class LocalParticipantPruningAdministrationGroup(
    runner: AdminCommandRunner with LedgerApiCommandRunner with BaseInspection[ParticipantNode],
    consoleEnvironment: ConsoleEnvironment,
    loggerFactory: NamedLoggerFactory,
) extends ParticipantPruningAdministrationGroup(runner, consoleEnvironment, loggerFactory) {

  import runner._

  @Help.Summary(
    "Return the highest participant ledger offset whose record time is before or at the given one (if any) at which pruning is safely possible",
    FeatureFlag.Preview,
  )
  def find_safe_offset(beforeOrAt: Instant = Instant.now()): Option[LedgerOffset] =
    check(FeatureFlag.Preview)(consoleEnvironment.run(access { node =>
      ConsoleCommandResult.fromEither(for {
        ledgerEnd <- ledgerApiCommand(LedgerApiCommands.TransactionService.GetLedgerEnd()).toEither
        offset <- node.sync.stateInspection.safeToPrune(timestampFromInstant(beforeOrAt), ledgerEnd)
      } yield offset)
    }))
}

class LocalCommitmentsAdministrationGroup(
    runner: AdminCommandRunner with BaseInspection[ParticipantNode],
    val consoleEnvironment: ConsoleEnvironment,
    val loggerFactory: NamedLoggerFactory,
) extends FeatureFlagFilter
    with Helpful {

  import runner._

  @Help.Summary(
    "Lookup ACS commitments received from other participants as part of the reconciliation protocol"
  )
  def received(
      domain: DomainAlias,
      start: Instant,
      end: Instant,
      counterParticipant: Option[ParticipantId] = None,
  ): Iterable[SignedProtocolMessage[AcsCommitment]] = {
    access(node =>
      node.sync.stateInspection
        .findReceivedCommitments(
          domain,
          timestampFromInstant(start),
          timestampFromInstant(end),
          counterParticipant,
        )
    )
  }

  @Help.Summary("Lookup ACS commitments locally computed as part of the reconciliation protocol")
  def computed(
      domain: DomainAlias,
      start: Instant,
      end: Instant,
      counterParticipant: Option[ParticipantId] = None,
  ): Iterable[(CommitmentPeriod, ParticipantId, AcsCommitment.CommitmentType)] =
    access { node =>
      node.sync.stateInspection.findComputedCommitments(
        domain,
        timestampFromInstant(start),
        timestampFromInstant(end),
        counterParticipant,
      )
    }

  def outstanding(
      domain: DomainAlias,
      start: Instant,
      end: Instant,
      counterParticipant: Option[ParticipantId] = None,
  ): Iterable[(CommitmentPeriod, ParticipantId)] =
    access { node =>
      node.sync.stateInspection.outstandingCommitments(
        domain,
        timestampFromInstant(start),
        timestampFromInstant(end),
        counterParticipant,
      )
    }

}

class ParticipantReplicationAdministrationGroup(
    runner: AdminCommandRunner with BaseInspection[ParticipantNode],
    consoleEnvironment: ConsoleEnvironment,
) extends Helpful {

  @Help.Summary("Set the participant replica to passive")
  @Help.Description(
    "Trigger a graceful fail-over from this active replica to another passive replica."
  )
  def set_passive(): Unit = {
    consoleEnvironment.run {
      runner.adminCommand(
        ParticipantAdminCommands.Replication.SetPassiveCommand()
      )
    }
  }

}

/** Administration commands supported by a participant.
  */
trait ParticipantAdministration extends FeatureFlagFilter {
  this: AdminCommandRunner
    with LedgerApiCommandRunner
    with LedgerApiAdministration
    with NamedLogging =>

  import ConsoleEnvironment.Implicits._
  implicit protected val consoleEnvironment: ConsoleEnvironment

  private val runner = this

  def topology: TopologyAdministrationGroup
  def id: ParticipantId

  @Help.Summary("Manage DAR packages")
  @Help.Group("DAR Management")
  object dars extends Helpful {
    @Help.Summary(
      "Remove a DAR from the participant",
      FeatureFlag.Preview,
    )
    @Help.Description(
      """Can be used to remove a DAR from the participant, when:
        | - The main package of the DAR is unused
        | - Other packages in the DAR are either unused or found in another DAR
        | - The main package of the DAR can be automatically un-vetted (or is already not vetted)  
        | """
    )
    def remove(darHash: String): Unit = {
      check(FeatureFlag.Preview)(consoleEnvironment.run {
        adminCommand(ParticipantAdminCommands.Package.RemoveDar(darHash))
      })
    }

    @Help.Summary("List installed DAR files")
    def list(limit: Option[Int] = None): Seq[v0.DarDescription] = consoleEnvironment.run {
      adminCommand(ParticipantAdminCommands.Package.ListDars(limit: Option[Int]))
    }

    @Help.Summary("Upload a Dar to Canton")
    @Help.Description("""Daml code is normally shipped as a Dar archive and must explicitly be uploaded to a participant.
        |A Dar is a collection of LF-packages, the native binary representation of Daml smart contracts.
        |In order to use Daml templates on a participant, the Dar must first be uploaded and then 
        |vetted by the participant. Vetting will ensure that other participants can check whether they 
        |can actually send a transaction referring to a particular Daml package and participant.
        |Vetting is done by registering a VettedPackages topology transaction with the topology manager.
        |By default, vetting happens automatically and this command waits for 
        |the vetting transaction to be successfully registered on all connected domains.
        |This is the safe default setting minimizing race conditions. 
        |          
        |If vetAllPackages is true (default), the packages will all be vetted on all domains the participant is registered.
        |If synchronizeVetting is true (default), then the command will block until the participant has observed the vetting transactions to be registered with the domain.
        |
        |Note that synchronize vetting might block on permissioned domains that do not just allow participants to update the topology state.
        |In such cases, synchronizeVetting should be turned off. 
        |Synchronize vetting can be invoked manually using $participant.package.synchronize_vettings()         
        |""")
    def upload(
        path: String,
        vetAllPackages: Boolean = true,
        synchronizeVetting: Boolean = true,
    ): String = {
      val res = consoleEnvironment.run {
        ConsoleCommandResult.fromEither(for {
          hash <- ParticipantCommands.dars
            .upload(runner, path, vetAllPackages, synchronizeVetting, logger)
            .toEither
        } yield hash)
      }
      if (synchronizeVetting && vetAllPackages) {
        packages.synchronize_vetting()
      }
      res
    }

    @Help.Summary("Downloads the DAR file with the given hash to the given directory")
    def download(darHash: String, directory: String): Unit = {
      val _ = consoleEnvironment.run {
        adminCommand(
          ParticipantAdminCommands.Package.GetDar(Some(darHash), Some(directory), logger)
        )
      }
    }

    @Help.Summary("Share DARs with other participants", FeatureFlag.Preview)
    @Help.Group("Sharing")
    object sharing extends Helpful {

      @Help.Summary("Incoming DAR sharing offers", FeatureFlag.Preview)
      @Help.Group("Offers")
      object offers extends Helpful {
        @Help.Summary("List received DAR sharing offers")
        def list(): Seq[v0.ListShareOffersResponse.Item] =
          check(FeatureFlag.Preview)(consoleEnvironment.run {
            adminCommand(ParticipantAdminCommands.Package.ListShareOffers)
          })

        @Help.Summary("Accept the offer to share a DAR", FeatureFlag.Preview)
        def accept(shareId: String): Unit =
          check(FeatureFlag.Preview)(consoleEnvironment.run {
            adminCommand(ParticipantAdminCommands.Package.AcceptShareOffer(shareId))
          })

        @Help.Summary("Reject the offer to share a DAR", FeatureFlag.Preview)
        def reject(shareId: String, reason: String): Unit =
          check(FeatureFlag.Preview)(consoleEnvironment.run {
            adminCommand(ParticipantAdminCommands.Package.RejectShareOffer(shareId, reason))
          })

      }

      @Help.Summary("Outgoing DAR sharing requests", FeatureFlag.Preview)
      @Help.Group("Requests")
      object requests extends Helpful {

        @Help.Summary("Share a DAR with other participants", FeatureFlag.Preview)
        def propose(darHash: String, participantId: ParticipantId): Unit =
          check(FeatureFlag.Preview)(consoleEnvironment.run {
            adminCommand(
              ParticipantAdminCommands.Package
                .ShareDar(darHash, participantId.adminParty.uid.toProtoPrimitive)
            )
          })

        @Help.Summary("List pending requests to share a DAR with others", FeatureFlag.Preview)
        def list(): Seq[v0.ListShareRequestsResponse.Item] =
          check(FeatureFlag.Preview)(consoleEnvironment.run {
            adminCommand(ParticipantAdminCommands.Package.ListShareRequests)
          })

      }

      @Help.Summary("Whitelist DAR sharing counter-parties", FeatureFlag.Preview)
      @Help.Group("Whitelist")
      object whitelist extends Helpful {
        @Help.Summary(
          "List parties that are currently whitelisted to share DARs with me",
          FeatureFlag.Preview,
        )
        def list(): Unit = check(FeatureFlag.Preview) {
          val _ = consoleEnvironment.run {
            adminCommand(ParticipantAdminCommands.Package.WhitelistList)
          }
        }

        @Help.Summary("Add party to my DAR sharing whitelist", FeatureFlag.Preview)
        def add(partyId: PartyId): Unit =
          check(FeatureFlag.Preview)(consoleEnvironment.run {
            adminCommand(
              ParticipantAdminCommands.Package.WhitelistAdd(partyId.uid.toProtoPrimitive)
            )
          })

        @Help.Summary("Remove party from my DAR sharing whitelist", FeatureFlag.Preview)
        def remove(partyId: PartyId): Unit =
          check(FeatureFlag.Preview)(consoleEnvironment.run {
            adminCommand(
              ParticipantAdminCommands.Package.WhitelistRemove(partyId.uid.toProtoPrimitive)
            )
          })

      }

    }

  }

  @Help.Summary("Manage raw Daml-LF packages")
  @Help.Group("Package Management")
  object packages extends Helpful {

    @Help.Summary("List packages stored on the participant")
    @Help.Description("If a limit is given, only up to `limit` packages are returned. ")
    def list(limit: Option[Int] = None): Seq[v0.PackageDescription] = consoleEnvironment.run {
      adminCommand(ParticipantAdminCommands.Package.List(limit))
    }

    @Help.Summary("List package contents")
    def list_contents(packageId: String): Seq[v0.ModuleDescription] = consoleEnvironment.run {
      adminCommand(ParticipantAdminCommands.Package.ListContents(packageId))
    }

    @Help.Summary("Find packages that contain a module with the given name")
    def find(moduleName: String): Seq[v0.PackageDescription] = consoleEnvironment.run {
      val packageC = adminCommand(ParticipantAdminCommands.Package.List(None)).toEither
      val matchingC = packageC
        .flatMap { packages =>
          packages.traverse(x =>
            adminCommand(ParticipantAdminCommands.Package.ListContents(x.packageId)).toEither.map(
              r => (x, r)
            )
          )
        }
      ConsoleCommandResult.fromEither(matchingC.map(_.filter { case (_, content) =>
        content.map(_.name).contains(moduleName)
      }.map(_._1)))
    }

    @Help.Summary(
      "Remove the package from Canton's package store.",
      FeatureFlag.Preview,
    )
    @Help.Description(
      """The standard operation of this command checks that a package is unused and unvetted, and if so 
        |removes the package. The force flag can be used to disable the checks, but do not use the force flag unless
        |you're certain you know what you're doing. """
    )
    def remove(packageId: String, force: Boolean = false): Unit = {
      check(FeatureFlag.Preview)(consoleEnvironment.run {
        adminCommand(ParticipantAdminCommands.Package.RemovePackage(packageId, force))
      })
    }

    @Help.Summary(
      "Ensure that all vetting transactions issued by this participant have been observed by all configured participants"
    )
    @Help.Description("""Sometimes, when scripting tests and demos, a dar or package is uploaded and we need to ensure 
        |that commands are only submitted once the package vetting has been observed by some other connected participant
        |known to the console. This command can be used in such cases.""")
    def synchronize_vetting(
        timeout: TimeoutDuration = consoleEnvironment.commandTimeouts.bounded
    ): Unit = {
      val connected = domains.list_connected().map(_.domainId).toSet
      def vetted: Set[PackageId] = topology.vetted_packages
        .list(filterStore = "Authorized", filterParticipant = id.filterString)
        .flatMap(_.item.packageIds)
        .toSet

      // ensure that the ledger api server has seen all packages
      try {
        AdminCommandRunner.retryUntilTrue(timeout) {
          val canton = packages.list().map(_.packageId).toSet
          val lApi = consoleEnvironment
            .run {
              ledgerApiCommand(LedgerApiCommands.PackageService.ListKnownPackages(None))
            }
            .map(_.packageId)
            .toSet
          (canton -- lApi).isEmpty
        }
      } catch {
        case _: TimeoutException =>
          logger.error(
            show"Participant $id ledger Api server has still a different set of packages than the sync server"
          )
      }

      def waitForPackages(
          topology: TopologyAdministrationGroup,
          observer: String,
          domainId: DomainId,
      ): Unit = {
        try {
          AdminCommandRunner
            .retryUntilTrue(timeout) {
              // ensure that vetted packages on the domain match the ones in the authorized store
              val onDomain = topology.vetted_packages
                .list(filterStore = domainId.filterString, filterParticipant = id.filterString)
                .flatMap(_.item.packageIds)
                .toSet
              val ret = vetted == onDomain
              if (!ret) {
                logger.debug(
                  show"Still waiting for package vetting updates to be observed by $observer on $domainId: vetted - onDomain is ${vetted -- onDomain} while onDomain -- vetted is ${onDomain -- vetted}"
                )
              }
              ret
            }
            .discard
        } catch {
          case _: TimeoutException =>
            logger.error(
              show"$observer has not observed all vetting txs of $id on domain $domainId within the given timeout."
            )
        }
      }

      // for every domain this participant is connected to
      consoleEnvironment.domains.all
        .filter(d => d.health.running() && d.health.initialized() && connected.contains(d.id))
        .foreach { domain =>
          waitForPackages(domain.topology, s"Domain ${domain.name}", domain.id)
        }

      // for every participant
      consoleEnvironment.participants.all
        .filter(p => p.health.running() && p.health.initialized())
        .foreach { participant =>
          // for every domain this participant is connected to as well
          participant.domains.list_connected().foreach {
            case item if connected.contains(item.domainId) =>
              waitForPackages(
                participant.topology,
                s"Participant ${participant.name}",
                item.domainId,
              )
            case _ =>
          }
        }
    }

  }

  @Help.Summary("Manage domain connections")
  @Help.Group("Domains")
  object domains extends Helpful {

    @Help.Summary("Returns the id of the given domain alias")
    def id_of(domainAlias: DomainAlias): DomainId = {
      consoleEnvironment.run {
        adminCommand(ParticipantAdminCommands.DomainConnectivity.GetDomainId(domainAlias))
      }
    }

    @Help.Summary(
      "Test whether a participant is connected to and permissioned on a domain where we have a healthy subscription."
    )
    def active(domain: DomainAlias): Boolean =
      list_connected().find(_.domainAlias == domain) match {
        case Some(item) if item.healthy =>
          topology.participant_domain_states.active(item.domainId, id)
        case _ => false
      }

    @Help.Summary(
      "Test whether a participant is connected to and permissioned on a domain reference"
    )
    def active(reference: DomainAdministration): Boolean = {
      val domainId = reference.id
      list_connected().find(_.domainId == domainId) match {
        case None => false
        case Some(item) =>
          active(item.domainAlias) && reference.participants.active(id)
      }
    }
    @Help.Summary(
      "Test whether a participant is connected to a domain reference"
    )
    def is_connected(reference: DomainAdministration): Boolean =
      list_connected().exists(_.domainId == reference.id)

    private def confirm_agreement(domainAlias: DomainAlias): Unit = {

      val response = get_agreement(domainAlias)

      val autoApprove =
        sys.env.getOrElse("CANTON_AUTO_APPROVE_AGREEMENTS", "no").toLowerCase == "yes"
      response.foreach {
        case (agreement, accepted) if !accepted =>
          if (autoApprove) {
            accept_agreement(domainAlias.unwrap, agreement.id)
          } else {
            println(s"Service Agreement for `$domainAlias`:")
            println(agreement.text)
            println("Do you accept the license? yes/no")
            print("> ")
            val answer = Option(scala.io.StdIn.readLine())
            if (answer.exists(_.toLowerCase == "yes"))
              accept_agreement(domainAlias.unwrap, agreement.id)
          }
        case _ => () // Don't do anything if the license has already been accepted
      }
    }

    @Help.Summary(
      "Macro to connect a participant to a locally configured domain given by reference"
    )
    @Help.Description("""
        The arguments are:
          domain - A local domain or sequencer reference
          manualConnect - Whether this connection should be handled manually and also excluded from automatic re-connect.
          alias - The name you will be using to refer to this domain. Can not be changed anymore.
          certificatesPath - Path to TLS certificate files to use as a trust anchor.
          priority - The priority of the domain. The higher the more likely a domain will be used.
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
        """)
    def connect_local(
        domain: InstanceReferenceWithSequencerConnection,
        manualConnect: Boolean = false,
        alias: Option[DomainAlias] = None,
        maxRetryDelayMillis: Option[Long] = None,
        priority: Int = 0,
        synchronize: Option[TimeoutDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): Unit = {
      val config = ParticipantCommands.domains.referenceToConfig(
        domain,
        manualConnect,
        alias,
        maxRetryDelayMillis.map(NonNegativeFiniteDuration.ofMillis),
        priority,
      )
      connectFromConfig(config, synchronize)
    }

    @Help.Summary("Macro to connect a participant to a domain given by connection")
    @Help.Description("""This variant of connect expects a domain connection config.
        |Otherwise the behaviour is equivalent to the connect command with explicit
        |arguments. If the domain is already configured, the domain connection
        |will be attempted. If however the domain is offline, the command will fail.
        |Generally, this macro should only be used to setup a new domain. However, for 
        |convenience, we support idempotent invocations where subsequent calls just ensure
        |that the participant reconnects to the domain.
        |""")
    def connect(
        config: DomainConnectionConfig
    ): Unit = {
      connectFromConfig(config, None)
    }

    private def connectFromConfig(
        config: DomainConnectionConfig,
        synchronize: Option[TimeoutDuration],
    ): Unit = {
      val current = this.config(config.domain)
      // if the config did not change, we'll just treat this as idempotent, otherwise, we'll use register to fail
      if (current.isEmpty) {
        // architecture-handbook-entry-begin: OnboardParticipantConnect
        // register the domain configuration
        register(config.copy(manualConnect = true))
        if (!config.manualConnect) {
          // fetch and confirm domain agreement
          config.sequencerConnection match {
            case _: GrpcSequencerConnection =>
              confirm_agreement(config.domain.unwrap)
            case _ => ()
          }
          reconnect(config.domain.unwrap, retry = false)
          // now update the domain settings to auto-connect
          modify(config.domain.unwrap, _.copy(manualConnect = false))
        }
        // architecture-handbook-entry-end: OnboardParticipantConnect
      } else if (!config.manualConnect) {
        val _ = reconnect(config.domain, retry = false)
        modify(config.domain.unwrap, _.copy(manualConnect = false))
      }
      synchronize.foreach { timeout =>
        ConsoleMacros.utils.synchronize_topology(Some(timeout))(consoleEnvironment)
      }
    }

    @Help.Summary("Macro to connect a participant to a domain given by connection")
    @Help.Description("""The connect macro performs a series of commands in order to connect this participant to a domain.
        |First, `register` will be invoked with the given arguments, but first registered  
        |with manualConnect = true. If you already set manualConnect = true, then nothing else
        |will happen and you will have to do the remaining steps yourselves.
        |Otherwise, if the domain requires an agreement, it is fetched and presented to the user for evaluation.
        |If the user is fine with it, the agreement is confirmed. If you want to auto-confirm,
        |then set the environment variable CANTON_AUTO_APPROVE_AGREEMENTS=yes.
        |Finally, the command will invoke `reconnect` to startup the connection. 
        |If the reconnect succeeded, the registered configuration will be updated  
        |with manualStart = true. If anything fails, the domain will remain registered with `manualConnect = true` and
        |you will have to perform these steps manually.
        The arguments are:
          domainAlias - The name you will be using to refer to this domain. Can not be changed anymore.
          connection - The connection string to connect to this domain. I.e. https://url:port
          manualConnect - Whether this connection should be handled manually and also excluded from automatic re-connect.
          domainId - Optionally the domainId you expect to see on this domain.
          certificatesPath - Path to TLS certificate files to use as a trust anchor.
          priority - The priority of the domain. The higher the more likely a domain will be used.
          timeTrackerConfig - The configuration for the domain time tracker.
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
        """)
    def connect(
        domainAlias: DomainAlias,
        connection: String,
        manualConnect: Boolean = false,
        domainId: Option[DomainId] = None,
        certificatesPath: String = "",
        priority: Int = 0,
        timeTrackerConfig: DomainTimeTrackerConfig = DomainTimeTrackerConfig(),
        synchronize: Option[TimeoutDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): DomainConnectionConfig = {
      val config = ParticipantCommands.domains.toConfig(
        domainAlias,
        connection,
        manualConnect,
        domainId,
        certificatesPath,
        priority,
        timeTrackerConfig = timeTrackerConfig,
      )
      connectFromConfig(config, synchronize)
      config
    }

    @Help.Summary(
      "Deprecated macro to connect a participant to a domain that supports connecting via many endpoints"
    )
    @Help.Description("""Use the command connect_ha with the updated arguments list""")
    @Deprecated(since = "2.2.0")
    def connect_ha(
        domainAlias: DomainAlias,
        firstConnection: SequencerConnection,
        additionalConnections: SequencerConnection*
    ): DomainConnectionConfig = {
      val sequencerConnection = SequencerConnection
        .merge(firstConnection +: additionalConnections)
        .getOrElse(sys.error("Invalid sequencer connection"))
      val config = DomainConnectionConfig(domainAlias, sequencerConnection)
      connect(config)
      config
    }

    @Help.Summary(
      "Macro to connect a participant to a domain that supports connecting via many endpoints"
    )
    @Help.Description("""Domains can provide many endpoints to connect to for availability and performance benefits.
        This version of connect allows specifying multiple endpoints for a single domain connection:
           connect_ha("mydomain", Seq(sequencer1, sequencer2))
           or:
           connect_ha("mydomain", Seq("https://host1.mydomain.net", "https://host2.mydomain.net", "https://host3.mydomain.net"))
        
        To create a more advanced connection config use domains.toConfig with a single host,
        |then use config.addConnection to add additional connections before connecting:
           config = myparticipaint.domains.toConfig("mydomain", "https://host1.mydomain.net", ...otherArguments)
           config = config.addConnection("https://host2.mydomain.net", "https://host3.mydomain.net")
           myparticipant.domains.connect(config)
           
        The arguments are:
          domainAlias - The name you will be using to refer to this domain. Can not be changed anymore.
          connections - The sequencer connection definitions (can be an URL) to connect to this domain. I.e. https://url:port
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.           
        """)
    def connect_ha(
        domainAlias: DomainAlias,
        connections: Seq[SequencerConnection],
        synchronize: Option[TimeoutDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): DomainConnectionConfig = {
      val config = DomainConnectionConfig(
        domainAlias,
        SequencerConnection.merge(connections).getOrElse(sys.error("Invalid sequencer connection")),
      )
      connectFromConfig(config, synchronize)
      config
    }

    @Help.Summary("Reconnect this participant to the given domain")
    @Help.Description("""Idempotent attempts to re-establish a connection to a certain domain.
        |If retry is set to false, the command will throw an exception if unsuccessful.
        |If retry is set to true, the command will terminate after the first attempt with the result,
        |but the server will keep on retrying to connect to the domain.
        |
        The arguments are:
          domainAlias - The name you will be using to refer to this domain. Can not be changed anymore.
          retry - Whether the reconnect should keep on retrying until it succeeded or abort noisly if the connection attempt fails.
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
        """)
    def reconnect(
        domainAlias: DomainAlias,
        retry: Boolean = true,
        synchronize: Option[TimeoutDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): Boolean = {
      val ret = consoleEnvironment.run {
        adminCommand(ParticipantAdminCommands.DomainConnectivity.ConnectDomain(domainAlias, retry))
      }
      if (ret) {
        synchronize.foreach { timeout =>
          ConsoleMacros.utils.synchronize_topology(Some(timeout))(consoleEnvironment)
        }
      }
      ret
    }

    @Help.Summary("Reconnect this participant to the given local domain")
    @Help.Description("""Idempotent attempts to re-establish a connection to the given local domain. 
        |Same behaviour as generic reconnect.

        The arguments are:
          ref - The domain reference to connect to
          retry - Whether the reconnect should keep on retrying until it succeeded or abort noisly if the connection attempt fails.
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
        """)
    def reconnect_local(
        ref: DomainReference,
        retry: Boolean = true,
        synchronize: Option[TimeoutDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): Boolean = reconnect(ref.name, retry, synchronize)

    @Help.Summary("Reconnect this participant to all domains which are not marked as manual start")
    @Help.Description("""
      The arguments are:
          ignoreFailures - If set to true (default), we'll attempt to connect to all, ignoring any failure
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
    """)
    def reconnect_all(
        ignoreFailures: Boolean = true,
        synchronize: Option[TimeoutDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): Unit = {
      consoleEnvironment.run {
        adminCommand(
          ParticipantAdminCommands.DomainConnectivity.ReconnectDomains(ignoreFailures)
        )
      }
      synchronize.foreach { timeout =>
        ConsoleMacros.utils.synchronize_topology(Some(timeout))(consoleEnvironment)
      }
    }

    @Help.Summary("Disconnect this participant from the given domain")
    def disconnect(domainAlias: DomainAlias): Unit = consoleEnvironment.run {
      adminCommand(ParticipantAdminCommands.DomainConnectivity.DisconnectDomain(domainAlias))
    }

    @Help.Summary("Disconnect this participant from the given local domain")
    def disconnect_local(domain: DomainReference): Unit = consoleEnvironment.run {
      adminCommand(ParticipantAdminCommands.DomainConnectivity.DisconnectDomain(domain.name))
    }

    @Help.Summary("List the connected domains of this participant")
    def list_connected(): Seq[ListConnectedDomainsResult] = consoleEnvironment.run {
      adminCommand(ParticipantAdminCommands.DomainConnectivity.ListConnectedDomains())
    }

    @Help.Summary("List the configured domains of this participant")
    def list_registered(): Seq[(DomainConnectionConfig, Boolean)] = consoleEnvironment.run {
      adminCommand(ParticipantAdminCommands.DomainConnectivity.ListConfiguredDomains)
    }

    @Help.Summary("Returns true if a domain is registered using the given alias")
    def is_registered(domain: DomainAlias): Boolean =
      config(domain).nonEmpty

    @Help.Summary("Returns the current configuration of a given domain")
    def config(domain: DomainAlias): Option[DomainConnectionConfig] =
      list_registered().map(_._1).find(_.domain == domain)

    @Help.Summary("Register new domain connection")
    @Help.Description("""When connecting to a domain, we need to register the domain connection and eventually
        |accept the terms of service of the domain before we can connect. The registration process is therefore
        |a subset of the operation. Therefore, register is equivalent to connect if the domain does not require
        |a service agreement. However, you would usually call register only in advanced scripts.""")
    def register(config: DomainConnectionConfig): Unit = {
      consoleEnvironment.run {
        ParticipantCommands.domains.register(runner, config)
      }
    }

    @Help.Summary("Modify existing domain connection")
    def modify(
        domain: DomainAlias,
        modifier: DomainConnectionConfig => DomainConnectionConfig,
    ): Unit = {
      consoleEnvironment.run {
        val ret = for {
          configured <- adminCommand(
            ParticipantAdminCommands.DomainConnectivity.ListConfiguredDomains
          ).toEither
          cfg <- configured
            .map(_._1)
            .find(_.domain == domain)
            .toRight(s"No such domain ${domain} configured")
          newConfig = modifier(cfg)
          _ <-
            if (newConfig.domain == cfg.domain) Right(())
            else Left("We don't support modifying the domain alias of a DomainConnectionConfig.")
          _ <- adminCommand(
            ParticipantAdminCommands.DomainConnectivity.ModifyDomainConnection(modifier(cfg))
          ).toEither
        } yield ()
        ConsoleCommandResult.fromEither(ret)
      }
    }

    @Help.Summary(
      "Get the service agreement of the given domain alias and if it has been accepted already."
    )
    def get_agreement(domainAlias: DomainAlias): Option[(v0.Agreement, Boolean)] =
      consoleEnvironment.run {
        adminCommand(ParticipantAdminCommands.DomainConnectivity.GetAgreement(domainAlias))
      }
    @Help.Summary("Accept the service agreement of the given domain alias")
    def accept_agreement(domainAlias: DomainAlias, agreementId: String): Unit =
      consoleEnvironment.run {
        adminCommand(
          ParticipantAdminCommands.DomainConnectivity.AcceptAgreement(domainAlias, agreementId)
        )
      }

  }

  @Help.Summary("Composability related functionality", FeatureFlag.Preview)
  @Help.Group("Transfer")
  object transfer extends Helpful {
    @Help.Summary(
      "Transfer-out a contract from the origin domain with destination target domain",
      FeatureFlag.Preview,
    )
    @Help.Description(
      """Transfers the given contract out of the origin domain with destination target domain.
       The command returns the ID of the transfer when the transfer-out has completed successfully.
       The contract is in transit until the transfer-in has completed on the target domain.
       The submitting party must be a stakeholder of the contract and the participant must have submission rights
       for the submitting party on the origin domain. It must also be connected to the target domain."""
    )
    def out(
        submittingParty: PartyId,
        contractId: LfContractId,
        originDomain: DomainAlias,
        targetDomain: DomainAlias,
    ): TransferId =
      check(FeatureFlag.Preview)(consoleEnvironment.run {
        adminCommand(
          ParticipantAdminCommands.Transfer
            .TransferOut(submittingParty, contractId, originDomain, targetDomain)
        )
      })

    @Help.Summary("Transfer-in a contract in transit to the target domain", FeatureFlag.Preview)
    @Help.Description("""Manually transfers a contract in transit into the target domain.
      The command returns when the transfer-in has completed successfully.
      If the transferExclusivityTimeout in the target domain's parameters is set to a positive value,
      all participants of all stakeholders connected to both origin and target domain will attempt to transfer-in
      the contract automatically after the exclusivity timeout has elapsed.""")
    def in(submittingParty: PartyId, transferId: TransferId, targetDomain: DomainAlias): Unit =
      check(FeatureFlag.Preview)(consoleEnvironment.run {
        adminCommand(
          ParticipantAdminCommands.Transfer
            .TransferIn(submittingParty, transferId.toProtoV0, targetDomain)
        )
      })

    @Help.Summary("Search the currently in-flight transfers", FeatureFlag.Preview)
    @Help.Description(
      "Returns all in-flight transfers with the given target domain that match the filters, but no more than the limit specifies."
    )
    def search(
        targetDomain: DomainAlias,
        filterOriginDomain: Option[DomainAlias],
        filterTimestamp: Option[Instant],
        filterSubmittingParty: Option[PartyId],
        limit: Int = 100,
    ): Seq[TransferSearchResult] =
      check(FeatureFlag.Preview)(consoleEnvironment.run {
        adminCommand(
          ParticipantAdminCommands.Transfer
            .TransferSearch(
              targetDomain,
              filterOriginDomain,
              filterTimestamp,
              filterSubmittingParty,
              limit,
            )
        )
      })

    @Help.Summary(
      "Transfer the contract from the origin domain to the target domain",
      FeatureFlag.Preview,
    )
    @Help.Description(
      "Macro that first calls transfer_out and then transfer_in. No error handling is done."
    )
    def execute(
        submittingParty: PartyId,
        contractId: LfContractId,
        originDomain: DomainAlias,
        targetDomain: DomainAlias,
    ): Unit = {
      val transferId = out(submittingParty, contractId, originDomain, targetDomain)
      in(submittingParty, transferId, targetDomain)
    }

    @Help.Summary("Lookup the active domain for the provided contracts", FeatureFlag.Preview)
    def lookup_contract_domain(contractIds: LfContractId*): Map[LfContractId, String] =
      check(FeatureFlag.Preview) {
        consoleEnvironment.run {
          adminCommand(ParticipantAdminCommands.Inspection.LookupContractDomain(contractIds.toSet))
        }
      }
  }

  @Help.Summary("Functionality for managing resources")
  @Help.Group("Resource Management")
  object resources extends Helpful {

    @Help.Summary("Set resource limits for the participant.")
    @Help.Description(
      """While a resource limit is attained or exceeded, the participant will reject any additional submission with GRPC status ABORTED.
        |Most importantly, a submission will be rejected **before** it consumes a significant amount of resources.
        |
        |There are two kinds of limits: `max_dirty_requests` and `max_rate`.
        |The number of dirty requests of a participant P covers (1) requests initiated by P as well as 
        |(2) requests initiated by participants other than P that need to be validated by P.
        |Compared to the maximum rate, the maximum number of dirty requests reflects the load on the participant more accurately.
        |However, the maximum number of dirty requests alone does not protect the system from "bursts":
        |If an application submits a huge number of commands at once, the maximum number of dirty requests will likely be exceeded.
        |
        |The maximum rate is a hard limit on the rate of commands submitted to this participant through the ledger API.
        |As the rate of commands is checked and updated immediately after receiving a new command submission,
        |an application cannot exceed the maximum rate, even when it sends a "burst" of commands.
        |
        |To determine a suitable value for `max_dirty_requests`, you should test the system under high load.
        |If you choose a higher value, throughput may increase, as more commands are validated in parallel.
        |If you observe a high latency (time between submission and observing a command completion) 
        |or even command timeouts, you should choose a lower value.
        |Once a suitable value for `max_dirty_requests` has been found, you should include "bursts" into the tests
        |to also find a suitable value for `max_rate`.
        |
        |Resource limits can only be changed, if the server runs Canton enterprise.
        |In the community edition, the server uses fixed limits that cannot be changed."""
    )
    def set_resource_limits(limits: ResourceLimits): Unit =
      consoleEnvironment.run { adminCommand(SetResourceLimits(limits)) }

    @Help.Summary("Get the resource limits of the participant.")
    def resource_limits(): ResourceLimits = consoleEnvironment.run {
      adminCommand(GetResourceLimits())
    }
  }
}

class ParticipantHealthAdministration(
    runner: AdminCommandRunner,
    val consoleEnvironment: ConsoleEnvironment,
    val loggerFactory: NamedLoggerFactory,
) extends HealthAdministration[ParticipantStatus](
      runner,
      consoleEnvironment,
      ParticipantStatus.fromProtoV0,
    )
    with FeatureFlagFilter {

  import runner._

  @Help.Summary(
    "Sends a ping to the target participant over the ledger. " +
      "Yields the duration in case of success and throws a RuntimeException in case of failure."
  )
  def ping(
      participantId: ParticipantId,
      timeout: TimeoutDuration = consoleEnvironment.commandTimeouts.ping,
      workflowId: String = "",
      id: String = "",
  ): Duration = {
    // duplicating the code from `maybe_ping` here so `maybe_ping` so ping doesn't depend on it and
    // thus isn't affected by `maybe_ping` being marked as 'Testing'
    val adminApiRes: Option[Duration] = consoleEnvironment.run {
      adminCommand(
        ParticipantAdminCommands.Ping
          .Ping(
            Set[String](participantId.adminParty.toLf),
            Set(),
            timeout.asFiniteApproximation.toMillis,
            0,
            0,
            workflowId,
            id,
          )
      )
    }
    val result = adminApiRes.toRight(
      s"Unable to ping $participantId within ${LoggerUtil.roundDurationForHumans(timeout.duration)}"
    )
    consoleEnvironment.run(ConsoleCommandResult.fromEither(result))
  }

  @Help.Summary(
    "Sends a ping to the target participant over the ledger. Yields Some(duration) in case of success and None in case of failure.",
    FeatureFlag.Testing,
  )
  def maybe_ping(
      participantId: ParticipantId,
      timeout: TimeoutDuration = consoleEnvironment.commandTimeouts.ping,
      workflowId: String = "",
      id: String = "",
  ): Option[Duration] = check(FeatureFlag.Testing) {
    consoleEnvironment.run {
      adminCommand(
        ParticipantAdminCommands.Ping
          .Ping(
            Set[String](participantId.adminParty.toLf),
            Set(),
            timeout.asFiniteApproximation.toMillis,
            0,
            0,
            workflowId,
            id,
          )
      )
    }
  }
}
