// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.admin.api.client.commands.DomainTimeCommands
import com.digitalasset.canton.admin.api.client.commands.EnterpriseMediatorAdministrationCommands.{
  Initialize,
  Prune,
}
import com.digitalasset.canton.config.TimeoutDuration
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  ConsoleEnvironment,
  FeatureFlag,
  FeatureFlagFilter,
  Help,
  Helpful,
}
import com.digitalasset.canton.crypto.PublicKey
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.SequencerConnection
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{DomainId, MediatorId}
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.topology.transaction.TopologyChangeOp

import scala.concurrent.duration.FiniteDuration

class MediatorTestingGroup(
    runner: AdminCommandRunner,
    val consoleEnvironment: ConsoleEnvironment,
    val loggerFactory: NamedLoggerFactory,
) extends FeatureFlagFilter
    with Helpful {

  @Help.Summary("Fetch the current time from the domain", FeatureFlag.Testing)
  def fetch_domain_time(
      timeout: TimeoutDuration = consoleEnvironment.commandTimeouts.ledgerCommand
  ): CantonTimestamp =
    check(FeatureFlag.Testing) {
      consoleEnvironment.run {
        runner.adminCommand(
          DomainTimeCommands.FetchTime(None, NonNegativeFiniteDuration.Zero, timeout)
        )
      }.timestamp
    }

  @Help.Summary("Await for the given time to be reached on the domain", FeatureFlag.Testing)
  def await_domain_time(time: CantonTimestamp, timeout: TimeoutDuration): Unit =
    check(FeatureFlag.Testing) {
      consoleEnvironment.run {
        runner.adminCommand(
          DomainTimeCommands.AwaitTime(None, time, timeout)
        )
      }
    }
}

@Help.Summary("Manage the mediator component")
@Help.Group("Mediator")
class MediatorAdministrationGroup(
    runner: AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
    loggerFactory: NamedLoggerFactory,
) extends Helpful {
  @Help.Summary("Initialize a mediator")
  def initialize(
      domainId: DomainId,
      mediatorId: MediatorId,
      domainParameters: StaticDomainParameters,
      sequencerConnection: SequencerConnection,
      topologySnapshot: Option[StoredTopologyTransactions[TopologyChangeOp.Positive]],
      cryptoType: String = "",
  ): PublicKey = consoleEnvironment.run {
    runner.adminCommand(
      Initialize(
        domainId,
        mediatorId,
        Option(cryptoType).filterNot(_.isEmpty),
        topologySnapshot,
        domainParameters,
        sequencerConnection,
      )
    )
  }

  @Help.Summary(
    "Prune the mediator of unnecessary data while keeping data for the default retention period"
  )
  @Help.Description(
    """Removes unnecessary data from the Mediator that is earlier than the default retention period.
          |The default retention period is set in the configuration of the canton node running this
          |command under `parameters.retention-period-defaults.mediator`."""
  )
  def prune(): Unit = {
    val defaultRetention =
      consoleEnvironment.environment.config.parameters.retentionPeriodDefaults.mediator
    prune_with_retention_period(defaultRetention.toScala)
  }

  @Help.Summary(
    "Prune the mediator of unnecessary data while keeping data for the provided retention period"
  )
  def prune_with_retention_period(retentionPeriod: FiniteDuration): Unit = {
    import scala.jdk.DurationConverters._
    val pruneUpTo = consoleEnvironment.environment.clock.now.minus(retentionPeriod.toJava)
    prune_at(pruneUpTo)
  }

  @Help.Summary("Prune the mediator of unnecessary data up to and including the given timestamp")
  def prune_at(timestamp: CantonTimestamp): Unit = consoleEnvironment.run {
    runner.adminCommand(Prune(timestamp))
  }

  private lazy val testing_ = new MediatorTestingGroup(runner, consoleEnvironment, loggerFactory)
  @Help.Summary("Testing functionality for the mediator")
  @Help.Group("Testing")
  def testing: MediatorTestingGroup = testing_
}
