// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import cats.instances.either.*
import cats.instances.option.*
import cats.syntax.traverse.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{
  ConsoleCommandResult,
  ConsoleEnvironment,
  FeatureFlag,
  FeatureFlagFilter,
  Help,
  Helpful,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.ParticipantNode
import com.digitalasset.canton.participant.domain.DomainConnectionConfig
import com.digitalasset.canton.protocol.{LfContractId, SerializableContractWithWitnesses}
import com.digitalasset.canton.topology.{DomainId, PartyId}
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DomainAlias, SequencerCounter}

import java.io.File
import java.time.Instant

abstract class ParticipantRepairAdministration(
    val consoleEnvironment: ConsoleEnvironment,
    val loggerFactory: NamedLoggerFactory,
) extends FeatureFlagFilter
    with NoTracing
    with Helpful {

  protected def access[T](handler: ParticipantNode => T): T

  @Help.Summary("Add specified contracts to specific domain on local participant.")
  @Help.Description(
    """This is a last resort command to recover from data corruption, e.g. in scenarios in which participant
        |contracts have somehow gotten out of sync and need to be manually created. The participant needs to be
        |disconnected from the specified "domain" at the time of the call, and as of now the domain cannot have had
        |any inflight requests.
        |For each "contractsToAdd", specify "witnesses", local parties, in case no local party is a stakeholder.
        |The "ignoreAlreadyAdded" flag makes it possible to invoke the command multiple times with the same
        |parameters in case an earlier command invocation has failed.
        |
        |As repair commands are powerful tools to recover from unforeseen data corruption, but dangerous under normal
        |operation, use of this command requires (temporarily) enabling the "features.enable-repair-commands"
        |configuration. In addition repair commands can run for an unbounded time depending on the number of
        |contracts passed in. Be sure to not connect the participant to the domain until the call returns.
        |
        The arguments are:
        - domain: the alias of the domain to which to add the contract
        - contractsToAdd: list of contracts to add with witness information
        - ignoreAlreadyAdded: (default true) if set to true, it will ignore contracts that already exist on the target domain.
        - ignoreStakeholderCheck: (default false) if set to true, add will work for contracts that don't have a local party (useful for party migration).
        """
  )
  def add(
      domain: DomainAlias,
      contractsToAdd: Seq[SerializableContractWithWitnesses],
      ignoreAlreadyAdded: Boolean = true,
      ignoreStakeholderCheck: Boolean = false,
  ): Unit =
    runRepairCommand(tc =>
      access(
        _.sync.repairService
          .addContracts(domain, contractsToAdd, ignoreAlreadyAdded, ignoreStakeholderCheck)(tc)
      )
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
        |The command will return a map of domainId -> number of active contracts stored
        |
        The arguments are:
        - parties: identifying contracts having at least one stakeholder from the given set
        - target: the target file where to store the data. Use .gz as a suffix to get a compressed file (recommended)
        - protocolVersion: optional the protocol version to use for the serialization. Defaults to the one of the domains.
        - filterDomainId: restrict the export to a given domain
        - timestamp: optionally a timestamp for which we should take the state (useful to reconcile states of a domain)
        - batchSize: batch size used to load contracts. Defaults to 1000. 
        """
  )
  def download(
      parties: Set[PartyId],
      target: String,
      filterDomainId: String = "",
      timestamp: Option[Instant] = None,
      batchSize: PositiveInt = PositiveInt.tryCreate(1000),
      protocolVersion: Option[ProtocolVersion] = None,
  ): Map[DomainId, Long] = {
    runRepairCommand { tc =>
      access(participant =>
        for {
          timestampConverted <- timestamp.traverse(CantonTimestamp.fromInstant)
          res <- participant.sync.stateInspection
            .storeActiveContractsToFile(
              parties,
              new File(target),
              batchSize,
              _.filterString.startsWith(filterDomainId),
              timestampConverted,
              protocolVersion,
            )(tc)
        } yield res
      )
    }
  }

}
