// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.admin.api.client.commands.DomainAdminCommands.GetDomainParameters
import com.digitalasset.canton.admin.api.client.commands.{
  DomainAdminCommands,
  TopologyAdminCommands,
}
import com.digitalasset.canton.admin.api.client.data.ListParticipantDomainStateResult
import com.digitalasset.canton.config.{ConsoleCommandTimeout, TimeoutDuration}
import com.digitalasset.canton.console.{AdminCommandRunner, ConsoleEnvironment, Help, Helpful}
import com.digitalasset.canton.domain.service.ServiceAgreementAcceptance
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.topology.admin.grpc.BaseQuery
import com.digitalasset.canton.topology.store.{TimeQuery, TopologyStoreId}
import com.digitalasset.canton.protocol.{DynamicDomainParameters, StaticDomainParameters}

trait DomainAdministration {
  this: AdminCommandRunner =>
  protected val consoleEnvironment: ConsoleEnvironment

  def id: DomainId
  def topology: TopologyAdministrationGroup
  protected def timeouts: ConsoleCommandTimeout = consoleEnvironment.commandTimeouts

  @Help.Summary("Manage participant permissions")
  @Help.Group("Participants")
  object participants extends Helpful {

    @Help.Summary("List participant states")
    @Help.Description(
      """This command will list the currently valid state as stored in the authorized store.
        | For a deep inspection of the identity management history, use the `topology.participant_domain_states.list` command."""
    )
    def list(): Seq[ListParticipantDomainStateResult] = {
      consoleEnvironment
        .run {
          adminCommand(
            TopologyAdminCommands.Read.ListParticipantDomainState(
              BaseQuery(
                filterStore = TopologyStoreId.AuthorizedStore.filterName,
                useStateStore = false,
                ops = None,
                timeQuery = TimeQuery.HeadState,
                filterSigningKey = "",
              ),
              filterDomain = "",
              filterParticipant = "",
            )
          )
        }
        .filter(_.item.side != RequestSide.To)
    }

    @Help.Summary("Change state and trust level of participant")
    @Help.Description("""Set the state of the participant within the domain.
    Valid permissions are 'Submission', 'Confirmation', 'Observation' and 'Disabled'.
    Valid trust levels are 'Vip' and 'Ordinary'.
    Synchronize timeout can be used to ensure that the state has been propagated into the node
    """)
    def set_state(
        participant: ParticipantId,
        permission: ParticipantPermission,
        trustLevel: TrustLevel = TrustLevel.Ordinary,
        synchronize: Option[TimeoutDuration] = Some(timeouts.bounded),
    ): Unit = {
      val _ = consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Write.AuthorizeParticipantDomainState(
            TopologyChangeOp.Add,
            None,
            RequestSide.From,
            id,
            participant,
            permission,
            trustLevel,
            replaceExisting = true,
          )
        )
      }
      synchronize.foreach(topology.synchronisation.await_idle)
    }

    @Help.Summary("Test whether a participant is permissioned on this domain")
    def active(participantId: ParticipantId): Boolean =
      topology.participant_domain_states.active(id, participantId)
  }

  @Help.Summary("Domain service commands")
  @Help.Group("Service")
  object service extends Helpful {

    @Help.Summary("List the accepted service agreements")
    def list_accepted_agreements(): Seq[ServiceAgreementAcceptance] =
      consoleEnvironment.run(adminCommand(DomainAdminCommands.ListAcceptedServiceAgreements))

    @Help.Summary("Get the Static Domain Parameters configured for the domain")
    def get_static_domain_parameters: StaticDomainParameters =
      consoleEnvironment.run(adminCommand(GetDomainParameters))

    @Help.Summary("Get the Dynamic Domain Parameters configured for the domain")
    def get_dynamic_domain_parameters: DynamicDomainParameters = topology.domain_parameters_changes
      .list("Authorized")
      .sortBy(_.context.validFrom)(implicitly[Ordering[java.time.Instant]].reverse)
      .headOption
      .map(_.item)
      .getOrElse(
        throw new IllegalStateException("No dynamic domain parameters found in the domain")
      )

    @Help.Summary("Set the Dynamic Domain Parameters configured for the domain")
    def set_dynamic_domain_parameters(dynamicDomainParameters: DynamicDomainParameters) =
      topology.domain_parameters_changes.authorize(id, dynamicDomainParameters).discard

    @Help.Summary("Update the Dynamic Domain Parameters for the domain")
    def update_dynamic_parameters(
        modifier: DynamicDomainParameters => DynamicDomainParameters
    ): Unit = {
      val currentDomainParameters = get_dynamic_domain_parameters
      val newDomainParameters = modifier(currentDomainParameters)
      set_dynamic_domain_parameters(newDomainParameters)
    }
  }

}
