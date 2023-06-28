// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.admin.api.client.commands.SequencerAdminCommands
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  ConsoleEnvironment,
  FeatureFlagFilter,
  Help,
  Helpful,
  InstanceReferenceX,
}
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerTrafficStatus
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.topology.*

class TrafficControlSequencerAdministrationGroup(
    instance: InstanceReferenceX,
    topology: TopologyAdministrationGroupX,
    runner: AdminCommandRunner,
    override val consoleEnvironment: ConsoleEnvironment,
    override val loggerFactory: NamedLoggerFactory,
) extends TrafficControlAdministrationGroup(
      instance,
      topology,
      runner,
      consoleEnvironment,
      loggerFactory,
    )
    with Helpful
    with FeatureFlagFilter {

  @Help.Summary("Return the traffic state of the given members")
  @Help.Description(
    """Use this command to get the traffic state of a list of members"""
  )
  def traffic_state_of_members(
      members: Seq[Member]
  ): SequencerTrafficStatus = {
    consoleEnvironment.run(
      runner.adminCommand(
        SequencerAdminCommands.GetTrafficControlState(members)
      )
    )
  }
}
