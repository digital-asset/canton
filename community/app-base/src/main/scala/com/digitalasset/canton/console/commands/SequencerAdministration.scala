// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import cats.syntax.either.*
import com.digitalasset.canton.admin.api.client.commands.SequencerAdminCommands.{
  InitializeFromGenesisState,
  InitializeFromGenesisStateV2,
  InitializeFromLsuPredecessor,
  InitializeFromOnboardingStateV2,
}
import com.digitalasset.canton.admin.api.client.commands.{GrpcAdminCommand, SequencerAdminCommands}
import com.digitalasset.canton.admin.api.client.data.{
  NodeStatus,
  SequencerStatus,
  StaticSynchronizerParameters,
}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.ConsoleCommandTimeout
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  ConsoleEnvironment,
  FeatureFlagFilter,
  Help,
  SequencerReference,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.grpc.{ByteStringStreamObserver, OutputFileStreamObserver}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.sequencer.admin.v30.OnboardingStateV2Response
import com.digitalasset.canton.synchronizer.sequencer.SequencerSnapshot
import com.digitalasset.canton.synchronizer.sequencer.admin.grpc.InitializeSequencerResponse
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.{SequencerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ResourceUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import java.io.{BufferedInputStream, FileInputStream}
import scala.concurrent.ExecutionContext

class SequencerAdministration(node: SequencerReference) extends ConsoleCommandGroup.Impl(node) {
  private def timeouts: ConsoleCommandTimeout = consoleEnvironment.commandTimeouts
  private implicit val ec: ExecutionContext = consoleEnvironment.environment.executionContext
  @Help.Summary(
    "Download sequencer snapshot at given point in time to bootstrap another sequencer"
  )
  @Help.Description("""This method is only used for testing.
      |It is recommended to use onboarding_state_for_sequencerV2 for onboarding a new sequencer.""")
  def snapshot(timestamp: CantonTimestamp, protocolVersion: ProtocolVersion): SequencerSnapshot =
    consoleEnvironment.run {
      runner.adminCommand(SequencerAdminCommands.Snapshot(timestamp, protocolVersion))
    }

  @Help.Summary(
    "Download the onboarding state for a given sequencer"
  )
  @Help.Description(
    """A byte string cannot exceed 2 GB. For larger onboarding states, use
      |export_onboarding_state_for_sequencer, which streams the onboarding state to a file."""
  )
  def onboarding_state_for_sequencerV2(
      sequencerId: SequencerId,
      timeout: config.NonNegativeDuration = timeouts.unbounded,
  ): ByteString =
    consoleEnvironment.run {
      val responseObserver =
        new ByteStringStreamObserver[OnboardingStateV2Response](_.onboardingStateForSequencer)

      def call =
        runner.adminCommand(
          SequencerAdminCommands.OnboardingStateV2(
            observer = responseObserver,
            sequencerOrTimestamp = Left(sequencerId),
          )
        )
      processResult(call, responseObserver.resultBytes, timeout, "Downloading onboarding state")
    }

  @Help.Summary(
    "Stream the onboarding state for a given sequencer to a file"
  )
  def export_onboarding_state_for_sequencer(
      outputFile: String,
      sequencerId: SequencerId,
      timeout: config.NonNegativeDuration = timeouts.unbounded,
  ): Unit =
    consoleEnvironment.run {
      val file = better.files.File(outputFile)
      val fileStreamObserver = new OutputFileStreamObserver[OnboardingStateV2Response](
        file,
        _.onboardingStateForSequencer,
      )

      def call =
        runner.adminCommand(
          SequencerAdminCommands.OnboardingStateV2(
            observer = fileStreamObserver,
            sequencerOrTimestamp = Left(sequencerId),
          )
        )
      processResult(
        call,
        fileStreamObserver.result,
        timeout,
        "Downloading onboarding state to a file",
        cleanupOnError = () => file.delete(),
      )
    }

  @Help.Summary(
    "Download the onboarding state at a given point in time to bootstrap another sequencer"
  )
  @Help.Description(
    """A byte string cannot exceed 2 GB. For larger onboarding states, use
      |export_onboarding_state_at_timestamp, which streams the onboarding state to a file."""
  )
  def onboarding_state_at_timestampV2(
      timestamp: CantonTimestamp,
      timeout: config.NonNegativeDuration = timeouts.unbounded,
  ): ByteString =
    consoleEnvironment.run {
      val responseObserver =
        new ByteStringStreamObserver[OnboardingStateV2Response](_.onboardingStateForSequencer)

      def call =
        runner.adminCommand(
          SequencerAdminCommands.OnboardingStateV2(
            observer = responseObserver,
            sequencerOrTimestamp = Right(timestamp),
          )
        )

      processResult(call, responseObserver.resultBytes, timeout, "Downloading onboarding state")
    }

  @Help.Summary(
    "Stream the onboarding state at a given point in time to a file to bootstrap another sequencer"
  )
  def export_onboarding_state_at_timestamp(
      outputFile: String,
      timestamp: CantonTimestamp,
      timeout: config.NonNegativeDuration = timeouts.unbounded,
  ): Unit =
    consoleEnvironment.run {
      val file = better.files.File(outputFile)
      val fileStreamObserver = new OutputFileStreamObserver[OnboardingStateV2Response](
        file,
        _.onboardingStateForSequencer,
      )

      def call =
        runner.adminCommand(
          SequencerAdminCommands.OnboardingStateV2(
            observer = fileStreamObserver,
            sequencerOrTimestamp = Right(timestamp),
          )
        )

      processResult(
        call,
        fileStreamObserver.result,
        timeout,
        "Downloading onboarding state to a file",
        cleanupOnError = () => file.delete(),
      )
    }

  @Help.Summary(
    "Initialize a sequencer from the beginning of the event stream"
  )
  @Help.Description(
    """This should only be called for sequencer nodes being initialized at the same time as the
      |corresponding synchronizer node.
      |
      |This is called as part of the synchronizer.setup.bootstrap command, so you are unlikely
      |to need to call this directly.
      """"
  )
  @deprecated(
    "Use assign_from_genesis_stateV2 instead.",
    since = "3.5",
  )
  def assign_from_genesis_state(
      genesisState: ByteString,
      synchronizerParameters: StaticSynchronizerParameters,
      waitForReady: Boolean = true,
  ): InitializeSequencerResponse = {
    if (waitForReady) node.health.wait_for_ready_for_initialization()

    val nodeStatus = node.health.status

    TraceContext.withNewTraceContext("assign_from_genesis_state") { implicit traceContext =>
      consoleEnvironment.run {
        runner.adminCommand(
          InitializeFromGenesisState(
            genesisState.newInput(),
            synchronizerParameters.toInternal.valueOr(consoleEnvironment.raiseError),
            serverVersion = nodeStatus.releaseVersion,
          )
        )
      }
    }
  }

  @Help.Summary(
    "Initialize a sequencer from the beginning of the event stream"
  )
  @Help.Description(
    """This should only be called for sequencer nodes being initialized at the same time as the
      |corresponding synchronizer node.
      |
      |This is called as part of the synchronizer.setup.bootstrap command, so you are unlikely
      |to need to call this directly.
      |
      |A byte string cannot exceed 2 GB. For larger genesis states, use
      |import_genesis_state, which streams the genesis state from a file.
      """"
  )
  def assign_from_genesis_stateV2(
      genesisState: ByteString,
      synchronizerParameters: StaticSynchronizerParameters,
      waitForReady: Boolean = true,
  ): InitializeSequencerResponse = {
    if (waitForReady) node.health.wait_for_ready_for_initialization()

    val nodeStatus = node.health.status

    TraceContext.withNewTraceContext("assign_from_genesis_stateV2") { implicit traceContext =>
      consoleEnvironment.run {
        runner.adminCommand(
          InitializeFromGenesisStateV2(
            genesisState.newInput(),
            synchronizerParameters.toInternal.valueOr(consoleEnvironment.raiseError),
            serverVersion = nodeStatus.releaseVersion,
          )
        )
      }
    }
  }

  @Help.Summary(
    "Initialize a sequencer from the beginning of the event stream, streaming the genesis state from a file"
  )
  @Help.Description(
    """This should only be called for sequencer nodes being initialized at the same time as the
      |corresponding synchronizer node.
      |
      |Parameters:
      |- inputFile: Retrieved using sequencer.topology.transactions.export_genesis_state
      |- synchronizerParameters: Synchronizer parameters of the node
      |"""
  )
  def import_genesis_state(
      inputFile: String,
      synchronizerParameters: StaticSynchronizerParameters,
      waitForReady: Boolean = true,
  ): InitializeSequencerResponse = {
    if (waitForReady) node.health.wait_for_ready_for_initialization()

    val nodeStatus = node.health.status
    val internalSynchronizerParameters =
      synchronizerParameters.toInternal.valueOr(consoleEnvironment.raiseError)

    ResourceUtil.withResource(new BufferedInputStream(new FileInputStream(inputFile))) {
      inputStream =>
        TraceContext.withNewTraceContext("import_genesis_state") { implicit traceContext =>
          consoleEnvironment.run {
            runner.adminCommand(
              InitializeFromGenesisStateV2(
                inputStream,
                internalSynchronizerParameters,
                serverVersion = nodeStatus.releaseVersion,
              )
            )
          }
        }
    }
  }

  @Help.Summary(
    "Initialize a sequencer for the logical upgrade from the state of its predecessor, streaming the state from a file"
  )
  @Help.Description("""
      |Parameters:
      |- predecessorState: Retrieved using sequencer.topology.transactions.sequencer_lsu_state
      |- synchronizerParameters: Synchronizer parameters of the node
      |- ignorePsidCheck:
      |    If true, it will not be checked that that current physical synchronizer id matches
      |    the successor in the topology state. Should be used *only* in disaster
      |    recovery scenarios (roll forward).
      |""")
  def initialize_from_lsu_predecessor(
      inputFile: String,
      synchronizerId: SynchronizerId,
      synchronizerParameters: StaticSynchronizerParameters,
      ignorePsidCheck: Boolean = false,
      waitForReady: Boolean = true,
  ): Unit = {
    if (waitForReady) node.health.wait_for_ready_for_initialization()

    val nodeStatus = node.health.status

    TraceContext.withNewTraceContext("initialize_from_lsu_predecessor") { implicit traceContext =>
      consoleEnvironment.run {
        runner.adminCommand(
          InitializeFromLsuPredecessor(
            new BufferedInputStream(
              new FileInputStream(inputFile)
            ),
            synchronizerParameters.toInternal.valueOr(consoleEnvironment.raiseError),
            ignorePsidCheck = ignorePsidCheck,
            synchronizerId = synchronizerId,
            serverVersion = nodeStatus.releaseVersion,
          )
        )
      }
    }
  }

  @Help.Summary(
    "Dynamically initialize a sequencer from a point later than the beginning of the event stream"
  )
  @Help.Description(
    """A byte string cannot exceed 2 GB. For larger onboarding states, use
      |import_onboarding_state, which streams the onboarding state from a file."""
  )
  def assign_from_onboarding_stateV2(
      onboardingState: ByteString,
      waitForReady: Boolean = true,
  ): InitializeSequencerResponse = {
    if (waitForReady && !node.health.initialized())
      node.health.wait_for_ready_for_initialization()

    TraceContext.withNewTraceContext("assign_from_onboarding_stateV2") { implicit traceContext =>
      consoleEnvironment.run {
        runner.adminCommand(
          InitializeFromOnboardingStateV2(onboardingState.newInput())
        )
      }
    }
  }

  @Help.Summary(
    "Dynamically initialize a sequencer from a point later than the beginning of the event stream, from a file"
  )
  @Help.Description(
    """Parameters:
      |- inputFile: Retrieved using sequencer.setup.export_onboarding_state_for_sequencer or
      |  sequencer.setup.export_onboarding_state_at_timestamp
      |"""
  )
  def import_onboarding_state(
      inputFile: String,
      waitForReady: Boolean = true,
  ): InitializeSequencerResponse = {
    if (waitForReady && !node.health.initialized())
      node.health.wait_for_ready_for_initialization()

    ResourceUtil.withResource(new BufferedInputStream(new FileInputStream(inputFile))) {
      inputStream =>
        TraceContext.withNewTraceContext("import_onboarding_state") { implicit traceContext =>
          consoleEnvironment.run {
            runner.adminCommand(InitializeFromOnboardingStateV2(inputStream))
          }
        }
    }
  }

  @Help.Summary("Test sequencing around LSU upgrade")
  @Help.Description(
    """Allows to test sequencing on the successor synchronizer by sequencing a message.
      |
      |Parameters:
      |- recipientMediatorGroup: Recipient group that will receive the message.
      |"""
  )
  def test_lsu_sequencing(recipientMediatorGroup: MediatorGroupIndex): Unit =
    consoleEnvironment.run(
      runner.adminCommand(SequencerAdminCommands.PerformLsuSequencingTest(recipientMediatorGroup))
    )
}

class SequencerHealthAdministration(
    val runner: AdminCommandRunner,
    val consoleEnvironment: ConsoleEnvironment,
    override val loggerFactory: NamedLoggerFactory,
) extends HealthAdministration[SequencerStatus](
      runner,
      consoleEnvironment,
    )
    with FeatureFlagFilter {
  override protected def nodeStatusCommand: GrpcAdminCommand[?, ?, NodeStatus[SequencerStatus]] =
    SequencerAdminCommands.Health.SequencerStatusCommand()
}
