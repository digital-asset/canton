// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import cats.syntax.either._
import cats.syntax.traverse._
import com.digitalasset.canton.admin.api.client.commands.{
  ParticipantAdminCommands,
  TopologyAdminCommands,
}
import com.digitalasset.canton.admin.api.client.data.{ListConnectedDomainsResult, ListPartiesResult}
import com.digitalasset.canton.config.RequireTypes.String255
import com.digitalasset.canton.config.TimeoutDuration
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  BaseInspection,
  CommandExecutionFailedException,
  ConsoleCommandResult,
  ConsoleEnvironment,
  ConsoleMacros,
  FeatureFlag,
  FeatureFlagFilter,
  Help,
  Helpful,
  LocalParticipantReference,
  ParticipantReference,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.ParticipantNode
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.transaction.{
  ParticipantPermission,
  RequestSide,
  TopologyChangeOp,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{DomainId, LedgerParticipantId}
import com.google.protobuf.ByteString

import java.time.Instant

class PartiesAdministrationGroup(runner: AdminCommandRunner, consoleEnvironment: ConsoleEnvironment)
    extends Helpful {

  import runner._

  @Help.Summary(
    "List active parties, their active participants, and the participants' permissions on domains."
  )
  @Help.Description(
    """This command allows you to deeply inspect the topology state used for synchronisation.
                      |The response is built from the timestamped topology transactions of each domain.
                      |The filterDomain parameter is used to filter the results by domain id; 
                      |the result only contains entries whose domain id starts with `filterDomain`."""
  )
  def list(
      filterParty: String = "",
      filterParticipant: String = "",
      filterDomain: String = "",
      asOf: Option[Instant] = None,
      limit: Int = 100,
  ): Seq[ListPartiesResult] =
    consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommands.Aggregation.ListParties(
          filterDomain = filterDomain,
          filterParty = filterParty,
          filterParticipant = filterParticipant,
          asOf = asOf,
          limit = limit,
        )
      )
    }

}

class ParticipantPartiesAdministrationGroup(
    participantId: => ParticipantId,
    runner: AdminCommandRunner with ParticipantAdministration,
    consoleEnvironment: ConsoleEnvironment,
) extends PartiesAdministrationGroup(runner, consoleEnvironment) {

  @Help.Summary("List parties managed by this participant")
  @Help.Description("""The filterDomain parameter is used to filter the results by domain id; 
                      |the result only contains entries whose domain id starts with `filterDomain`.
                      |Inactive participants hosting the party are not shown in the result.""")
  def hosted(
      filterParty: String = "",
      filterDomain: String = "",
      asOf: Option[Instant] = None,
  ): Seq[ListPartiesResult] = {
    list(filterParty, filterParticipant = participantId.filterString, filterDomain, asOf)
  }

  @Help.Summary("Enable/add party to participant")
  @Help.Description("""This function registers a new party with the current participant within the participants
      |namespace. The function fails if the participant does not have appropriate signing keys
      |to issue the corresponding PartyToParticipant topology transaction. 
      |Optionally, a local display name can be added. This display name will be exposed on the
      |ledger API party management endpoint.
      |Specifying a set of domains via the `WaitForDomain` parameter ensures that the domains have
      |enabled/added a party by the time the call returns, but other participants connected to the same domains may not
      |yet be aware of the party.
      |Additionally, a sequence of additional participants can be added to be synchronized to
      |ensure that the party is known to these participants as well before the function terminates.
      |""")
  def enable(
      name: String,
      displayName: Option[String] = None,
      waitForDomain: DomainChoice = DomainChoice.Only(Seq()),
      synchronizeParticipants: Seq[ParticipantReference] = Seq(),
  ): PartyId = {
    def registered(lst: => Seq[ListPartiesResult]): Set[DomainId] = {
      lst
        .flatMap(_.participants.flatMap(_.domains))
        .map(_.domain)
        .toSet
    }
    def primaryRegistered(partyId: PartyId) =
      registered(
        list(filterParty = partyId.filterString, filterParticipant = participantId.filterString)
      )

    def primaryConnected: Either[String, Seq[ListConnectedDomainsResult]] =
      runner
        .adminCommand(ParticipantAdminCommands.DomainConnectivity.ListConnectedDomains())
        .toEither

    def findDomainIds(
        name: String,
        connected: Either[String, Seq[ListConnectedDomainsResult]],
    ): Either[String, Set[DomainId]] = {
      for {
        domainIds <- waitForDomain match {
          case DomainChoice.All =>
            connected.map(_.map(_.domainId))
          case DomainChoice.Only(Seq()) =>
            Right(Seq())
          case DomainChoice.Only(aliases) =>
            connected.flatMap { res =>
              val connectedM = res.map(x => (x.domainAlias, x.domainId)).toMap
              aliases.traverse(alias => connectedM.get(alias).toRight(s"Unknown: $alias for $name"))
            }
        }
      } yield domainIds.toSet
    }
    def waitForParty(
        partyId: PartyId,
        domainIds: Set[DomainId],
        registered: => Set[DomainId],
    ): Either[String, Unit] = {
      if (domainIds.nonEmpty) {
        AdminCommandRunner
          .retryUntilTrue(consoleEnvironment.commandTimeouts.ledgerCommand) {
            domainIds subsetOf registered
          }
          .toEither
          .leftMap(_ => s"Party ${partyId} did not appear on domain ${domainIds.diff(registered)}")
      } else Right(())
    }
    consoleEnvironment.run {
      ConsoleCommandResult.fromEither {
        for {
          // validating party and display name here to prevent, e.g., a party being registered despite it having an invalid display name
          // assert that name is valid ParticipantId
          id <- Identifier.create(name)
          partyId = PartyId(participantId.uid.copy(id = id))
          _ <- Either
            .catchOnly[IllegalArgumentException](LedgerParticipantId.assertFromString(name))
            .leftMap(_.getMessage)
          validDisplayName <- displayName.map(String255.create(_, Some("display name"))).sequence
          // find the domain ids
          domainIds <- findDomainIds(this.participantId.uid.id.unwrap, primaryConnected)
          // find the domain ids the additional participants are connected to
          additionalSync <- synchronizeParticipants.traverse { p =>
            findDomainIds(
              p.name,
              Either
                .catchOnly[CommandExecutionFailedException](p.domains.list_connected())
                .leftMap(_.getMessage),
            )
              .map(domains => (p, domains intersect domainIds))
          }
          _ <- runPartyCommand(partyId, TopologyChangeOp.Add).toEither
          _ <- validDisplayName match {
            case None => Right(())
            case Some(name) =>
              runner
                .adminCommand(
                  ParticipantAdminCommands.PartyNameManagement
                    .SetPartyDisplayName(partyId, name.unwrap)
                )
                .toEither
          }
          _ <- waitForParty(partyId, domainIds, primaryRegistered(partyId))
          _ <- additionalSync.traverse { case (p, domains) =>
            waitForParty(
              partyId,
              domains,
              registered(
                p.parties.list(
                  filterParty = partyId.filterString,
                  filterParticipant = participantId.filterString,
                )
              ),
            )
          }
        } yield partyId
      }
    }
  }

  private def runPartyCommand(
      partyId: PartyId,
      op: TopologyChangeOp,
  ): ConsoleCommandResult[ByteString] = {
    runner
      .adminCommand(
        TopologyAdminCommands.Write.AuthorizePartyToParticipant(
          op,
          None,
          RequestSide.Both,
          partyId,
          participantId,
          ParticipantPermission.Submission,
          replaceExisting = false,
        )
      )
  }

  @Help.Summary("Disable party on participant")
  def disable(name: Identifier): Unit = {
    val partyId = PartyId(participantId.uid.copy(id = name))
    val _ = consoleEnvironment.run {
      runPartyCommand(partyId, TopologyChangeOp.Remove)
    }
  }

  @Help.Summary("Set party display name")
  @Help.Description(
    "Locally set the party display name (shown on the ledger-api) to the given value"
  )
  def set_display_name(party: PartyId, displayName: String): Unit = consoleEnvironment.run {
    // takes displayName as String argument which is validated at GrpcPartyNameManagementService
    runner.adminCommand(
      ParticipantAdminCommands.PartyNameManagement.SetPartyDisplayName(party, displayName)
    )
  }

}

class LocalParticipantPartiesAdministrationGroup(
    reference: LocalParticipantReference,
    runner: AdminCommandRunner with BaseInspection[ParticipantNode] with ParticipantAdministration,
    val consoleEnvironment: ConsoleEnvironment,
    val loggerFactory: NamedLoggerFactory,
) extends ParticipantPartiesAdministrationGroup(reference.id, runner, consoleEnvironment)
    with FeatureFlagFilter {

  import runner._

  @Help.Summary("Waits for any topology changes to be observed", FeatureFlag.Preview)
  @Help.Description(
    "Will throw an exception if the given topology has not been observed within the given timeout."
  )
  def await_topology_observed[T <: ParticipantReference](
      partyAssignment: Set[(PartyId, T)],
      timeout: TimeoutDuration = consoleEnvironment.commandTimeouts.bounded,
  )(implicit env: ConsoleEnvironment): Unit =
    check(FeatureFlag.Preview) {
      access(node =>
        TopologySynchronisation.awaitTopologyObserved(reference, partyAssignment, timeout)
      )
    }

}

object TopologySynchronisation {

  def awaitTopologyObserved[T <: ParticipantReference](
      reference: ParticipantReference,
      partyAssignment: Set[(PartyId, T)],
      timeout: TimeoutDuration,
  )(implicit env: ConsoleEnvironment): Unit =
    TraceContext.withNewTraceContext { _ =>
      ConsoleMacros.utils.retry_until_true(timeout) {
        val partiesWithId = partyAssignment.map { case (party, participantRef) =>
          (party, participantRef.id)
        }
        env.domains.all.forall { domain =>
          val domainId = domain.id
          !reference.domains.active(domain) || {
            val timestamp = reference.testing.fetch_domain_time(domainId)
            partiesWithId.subsetOf(
              reference.parties
                .list(asOf = Some(timestamp.toInstant))
                .flatMap(res => res.participants.map(par => (res.party, par.participant)))
                .toSet
            )
          }
        }
      }
    }
}
