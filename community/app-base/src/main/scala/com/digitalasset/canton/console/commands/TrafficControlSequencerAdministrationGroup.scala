// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.admin.api.client.commands.SequencerAdminCommands
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  ConsoleEnvironment,
  FeatureFlagFilter,
  Help,
  Helpful,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.sequencer.admin.v30.TrafficSummary
import com.digitalasset.canton.synchronizer.sequencer.BlockSequencerConfig.ThroughputCapByMessageTypeConfig
import com.digitalasset.canton.synchronizer.sequencer.traffic.{
  SequencerTrafficStatus,
  TimestampSelector,
}
import com.digitalasset.canton.topology.*
import com.google.protobuf.ByteString

class TrafficControlSequencerAdministrationGroup(
    runner: AdminCommandRunner,
    override val consoleEnvironment: ConsoleEnvironment,
    override val loggerFactory: NamedLoggerFactory,
) extends Helpful
    with FeatureFlagFilter {

  @Help.Summary("Return the traffic state of the given members")
  @Help.Description(
    """Use this command to get the traffic state of a list of members at the latest safe
      |timestamp.
      |Set includeOnboardingMembers to true to also return members that traffic was purchased
      |for before they joined the synchronizer.
      """
  )
  def traffic_state_of_members(
      members: Seq[Member],
      includeOnboardingMembers: Boolean = false,
  ): SequencerTrafficStatus =
    consoleEnvironment.run(
      runner.adminCommand(
        SequencerAdminCommands.GetTrafficControlState(
          members,
          TimestampSelector.LatestSafe,
          includeOnboardingMembers,
        )
      )
    )

  @Help.Summary("Return the traffic state of the given members at the latest approximate time")
  @Help.Description(
    """Use this command to get the traffic state of a list of members using the latest possible
      |time the sequencer can estimate the state.
      |
      |CAREFUL: The returned state is only an approximation in the future and might not be the
      |actual correct state by the time this timestamp is reached by the synchronizer.
      |
      |Set includeOnboardingMembers to true to also return members that traffic was purchased
      |for before they joined the synchronizer.
      """
  )
  def traffic_state_of_members_approximate(
      members: Seq[Member],
      includeOnboardingMembers: Boolean = false,
  ): SequencerTrafficStatus =
    consoleEnvironment.run(
      runner.adminCommand(
        SequencerAdminCommands.GetTrafficControlState(
          members,
          TimestampSelector.LatestApproximate,
          includeOnboardingMembers,
        )
      )
    )

  @Help.Summary("Return the last traffic state update of the given members, per member")
  @Help.Description(
    """Use this command to get the last traffic state update of each member. It will be last
      |updated when a member consumed traffic.
      |Set includeOnboardingMembers to true to also return members that traffic was purchased
      |for before they joined the synchronizer.
      |Such members have not consumed traffic yet, so their state does not show the purchase.
      """
  )
  def last_traffic_state_update_of_members(
      members: Seq[Member],
      includeOnboardingMembers: Boolean = false,
  ): SequencerTrafficStatus =
    consoleEnvironment.run(
      runner.adminCommand(
        SequencerAdminCommands.GetTrafficControlState(
          members,
          TimestampSelector.LastUpdatePerMember,
          includeOnboardingMembers,
        )
      )
    )

  @Help.Summary("Return the traffic state of the given members at a specific timestamp")
  @Help.Description(
    """Use this command to get the traffic state of specified members at a given timestamp.
      |Set includeOnboardingMembers to true to also return members that traffic was purchased
      |for before they joined the synchronizer.
      """
  )
  // TODO(#25930): Make this command return what its name suggests, i.e., the traffic state exactly at the given timestamp.
  def traffic_state_of_members_at_timestamp(
      members: Seq[Member],
      timestamp: CantonTimestamp,
      includeOnboardingMembers: Boolean = false,
  ): SequencerTrafficStatus =
    consoleEnvironment.run(
      runner.adminCommand(
        SequencerAdminCommands.GetTrafficControlState(
          members,
          TimestampSelector.ExactTimestamp(timestamp),
          includeOnboardingMembers,
        )
      )
    )

  @Help.Summary("Return the traffic state of the all members")
  @Help.Description(
    """Use this command to get the traffic state of all members.
      |Set latestApproximate to true to get an approximation of the traffic state (including
      |base traffic) at the latest possible timestamp the sequencer can calculate it. This an
      |approximation only because the sequencer may use its wall clock which could be beyond
      |the synchronizer time.
      |Set includeOnboardingMembers to true to also return members that traffic was purchased
      |for before they joined the synchronizer.
      """
  )
  def traffic_state_of_all_members(
      latestApproximate: Boolean = false,
      includeOnboardingMembers: Boolean = false,
  ): SequencerTrafficStatus =
    consoleEnvironment.run(
      runner.adminCommand(
        SequencerAdminCommands.GetTrafficControlState(
          Seq.empty,
          if (latestApproximate) TimestampSelector.LatestApproximate
          else TimestampSelector.LatestSafe,
          includeOnboardingMembers,
        )
      )
    )

  @Help.Summary("Set the traffic purchased entry of a member")
  @Help.Description(
    """Use this command to set the new traffic purchased entry of a member.
      |
      |Parameters:
      |- member: Member for which the traffic purchased entry is to be set.
      |- serial: Serial number of the request, must be strictly greater than the latest update
      |  made for that member.
      |- newBalance: New traffic purchased entry to be set.
      |
      |Returns: The max sequencing time used for the update. After and only after that time,
      |if the new balance still does not appear in the traffic state, the update can be
      |considered failed and should be retried.
      """
  )
  def set_traffic_balance(
      member: Member,
      serial: PositiveInt,
      newBalance: NonNegativeLong,
  ): Unit =
    consoleEnvironment.run(
      runner.adminCommand(
        SequencerAdminCommands.SetTrafficPurchased(member, serial, newBalance)
      )
    )

  @Help.Summary("Obtain traffic summaries for sequenced events")
  @Help.Description(
    """Use this command to retrieve the traffic summaries of
      |sequenced events from their timestamps.
      |
      |Parameters:
      |- timestamps: list of timestamps for which to retrieve traffic summaries
      |
      |Returns: traffic summaries of sequenced events available
      |on the sequencer matching the input timestamps.
      |The response may contain less events than input timestamps.
      """
  )
  def traffic_summaries(timestamps: Seq[CantonTimestamp]): Seq[TrafficSummary] =
    consoleEnvironment.run(
      runner.adminCommand(
        SequencerAdminCommands.GetTrafficSummaries(timestamps)
      )
    )

  @Help.Summary(
    "Get the traffic control state to be transferred to a new sequencer during a logical synchronizer upgrade"
  )
  @Help.Description(
    """Use this command on the old synchronizer to get the input for `set_lsu_state`
      |to be run on the new synchronizer.
      |
      |Parameters:
      |- timestamp: The time used to fetch the traffic state.
      | MUST be empty for regular LSUs. In that case, an LSU must be announced
      | and sequencer must have reached the upgrade time for this call to succeed.
      |
      | SHOULD be defined in a disaster recovery scenario when requesting the topology
      | from a synchronizer without an announced LSU.
      """
  )
  def get_lsu_state(timestamp: Option[CantonTimestamp] = None): ByteString =
    consoleEnvironment.run(
      runner.adminCommand(SequencerAdminCommands.GetLsuTrafficControlState(timestamp))
    )

  @Help.Summary(
    "Set the traffic control state on a fresh sequencer during a logical synchronizer upgrade"
  )
  @Help.Description(
    """Use this command on the new synchronizer with an output of
      | `get_lsu_traffic_control_state` from the old synchronizer.
      | A logical synchronizer upgrade must be ongoing and the command must be called only once
      | on the successor synchronizer sequencer.
      | Parameters:
      | - memberTraffic: ByteString produced by `get_lsu_state`.
      """
  )
  def set_lsu_state(
      memberTraffic: ByteString
  ): Unit =
    consoleEnvironment.run(
      runner.adminCommand(SequencerAdminCommands.SetLsuTrafficControlState(memberTraffic))
    )

  @Help.Summary("Set the request throughput caps")
  @Help.Description(
    """Throughput caps set general limits on the number of requests being accepted
       | to protect the general availability of the system.
       | Settings are identified by name and specify the message type and observation period.
       Note that this option has only an effect if the caps are enabled. Upon restart,
       | the caps will be reset to their original configuration settings.
      """
  )
  def set_throughput_cap(
      name: String,
      config: ThroughputCapByMessageTypeConfig,
  ): Unit = consoleEnvironment.run(
    runner.adminCommand(
      SequencerAdminCommands.SetThroughputCap(name, Some(config))
    )
  )

  @Help.Summary("Delete a request throughput cap")
  def unset_throughput_cap(name: String): Unit =
    consoleEnvironment.run(
      runner.adminCommand(
        SequencerAdminCommands.SetThroughputCap(name, None)
      )
    )

  @Help.Summary("Get the current configured request throughput cap")
  def get_throughput_cap(
      name: String
  ): Option[ThroughputCapByMessageTypeConfig] = consoleEnvironment.run(
    runner.adminCommand(
      SequencerAdminCommands.GetThroughputCap(name)
    )
  )

  @Help.Summary("List all named throughput cap settings")
  def list_throughput_caps(): Map[String, ThroughputCapByMessageTypeConfig] =
    consoleEnvironment.run(runner.adminCommand(SequencerAdminCommands.ListThroughputCaps()))

  @Help.Summary("Update the current configured request throughput cap")
  def update_throughput_cap(
      name: String,
      update: ThroughputCapByMessageTypeConfig => ThroughputCapByMessageTypeConfig,
  ): Unit =
    consoleEnvironment.runE {
      for {
        currentO <- runner
          .adminCommand(
            SequencerAdminCommands.GetThroughputCap(name)
          )
          .toEither
        current <- currentO.toRight(s"Cap '$name' is not configured")
        _ <- runner
          .adminCommand(SequencerAdminCommands.SetThroughputCap(name, Some(update(current))))
          .toEither
      } yield ()
    }
}
