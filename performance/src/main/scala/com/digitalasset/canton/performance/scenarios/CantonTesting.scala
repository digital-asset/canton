// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.scenarios

import com.digitalasset.canton.admin.api.client.data.{
  NodeStatus,
  ParticipantStatus,
  StaticSynchronizerParameters,
}
import com.digitalasset.canton.concurrent.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.*
import com.digitalasset.canton.console.ConsoleEnvironment.Implicits.*
import com.digitalasset.canton.console.ConsoleMacros.utils
import com.digitalasset.canton.console.NodeReferences.ParticipantNodeReferences
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.client.configuration.CommandClientConfiguration
import com.digitalasset.canton.logging.*
import com.digitalasset.canton.participant.admin.ResourceLimits
import com.digitalasset.canton.performance.*
import com.digitalasset.canton.performance.PartyRole.*
import com.digitalasset.canton.performance.RateSettings.SubmissionRateSettings
import com.digitalasset.canton.performance.elements.DriverStatus.MasterStatus
import com.digitalasset.canton.performance.elements.dvp.TraderDriver
import com.digitalasset.canton.performance.model.java as M
import com.digitalasset.canton.sequencing.client.RecordingConfig
import com.digitalasset.canton.synchronizer.mediator.MediatorNodeBootstrap
import com.digitalasset.canton.topology.{MediatorId, SynchronizerId}
import com.digitalasset.canton.version.ProtocolVersion

import java.nio.file.Path
import java.time.Duration as JDuration
import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.duration.*

/** Our regression test scripts */
object CantonTesting {

  /** Startup script invoked by primary synchronizer process */
  def runSynchronizers(sequencerName: String = "sequencer", mediatorName: String = "mediator")(
      implicit consoleEnvironment: ConsoleEnvironment
  ): Unit = {

    import consoleEnvironment.*
    val sequencer =
      sequencers.local.find(_.name == sequencerName).getOrElse(sys.error("No sequencer found"))
    val mediator =
      mediators.local.find(_.name == mediatorName).getOrElse(sys.error("No mediator found"))

    sys.env.get("RECORDING_PREFIX") match {
      case Some(prefix) =>
        val path = Path.of(sys.env("RECORDINGS_DIR")).resolve(prefix)
        MediatorNodeBootstrap.recordSequencerInteractions.set { case MediatorId(_) =>
          RecordingConfig(path)
        }
      case None => // no recording
    }

    Seq[LocalInstanceReference](sequencer, mediator).foreach(_.start())

    if (!sequencer.health.initialized()) {
      ConsoleMacros.bootstrap
        .synchronizer(
          "synchronizer",
          Seq(sequencer),
          Seq(mediator),
          Seq(sequencer),
          PositiveInt.one,
          staticSynchronizerParameters =
            StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest),
          mediatorThreshold = PositiveInt.one,
        )
        .discard
    }

    mediator.health.wait_for_initialized()
    sequencer.health.wait_for_initialized()

    utils.retry_until_true(timeout = 1.minute) {
      mediator.health.status.successOption.exists(_.active)
    }

    // Increase synchronizer timeouts to avoid rejections under high load.
    sequencer.topology.synchronizer_parameters.propose_update(
      sequencer.synchronizer_id,
      _.update(
        confirmationResponseTimeout = 60.seconds,
        mediatorReactionTimeout = 60.seconds,
        maxRequestSize = 2000000000,
      ),
    )

  }

  def runSynchronizersSec(
      sequencerName: String = "sequencer",
      mediatorName: String = "mediator",
      mediatorSecName: String = "mediatorSec",
  )(implicit consoleEnvironment: ConsoleEnvironment): Unit = {

    import consoleEnvironment.*

    val mediatorSec =
      mediators.local.find(_.name == mediatorSecName).getOrElse(sys.error("No mediator found"))
    val mediator =
      mediators.remote.find(_.name == mediatorName).getOrElse(sys.error("No remote mediator found"))
    val sequencer = sequencers.remote
      .find(_.name == sequencerName)
      .getOrElse(sys.error("No remote sequencer found"))

    // Wait until synchronizer is running
    sequencer.health.wait_for_initialized()
    mediator.health.wait_for_initialized()

    // Wait until the other mediator is active
    utils.retry_until_true(timeout = 1.minute) {
      mediator.health.status.successOption.exists(_.active)
    }
    mediatorSec.start()

    // Make sure that mediatorSec is passive
    utils.retry_until_true(timeout = 1.minute) {
      mediatorSec.health.status.successOption.exists(s => !s.active)
    }

  }

  def runParticipants(
      sequencerHost: String = sys.env("SEQUENCER_PUBLIC_API_HOST"),
      sequencerPort: String = sys.env("SEQUENCER_PUBLIC_API_PORT"),
  )(implicit consoleEnvironment: ConsoleEnvironment): Unit = {
    import consoleEnvironment.*
    val cantonConfig = consoleEnvironment.environment.config

    require(
      !cantonConfig.parameters.enableAdditionalConsistencyChecks,
      "It does not make sense to run performance tests with additional consistency checks enabled. " +
        "Please configure canton.parameters.enable-additional-consistency-checks = false.",
    )
    participants.local.start()

    sys.env.get("RECORDING_PREFIX") match {
      case Some(prefix) =>
        val path = Path.of(sys.env("RECORDINGS_DIR")).resolve(prefix)
        participants.local.foreach {
          _.underlying
            .getOrElse(sys.error("Node not initialized?"))
            .recordSequencerInteractions
            .set(Some(RecordingConfig(path)))
        }
      case None => // no recording
    }

    // Wait until participants are active
    utils.retry_until_true(timeout = 1.minute) {
      participants.local
        .groupBy(_.config.storage.config.getString("properties.databaseName"))
        .values
        .map(_.find(_.health.active))
        .forall(_.isDefined)
    }

    // Wait until synchronizer is running
    nodes.remote.foreach { node =>
      node.health.wait_for_initialized()
    }

    utils.retry_until_true(timeout = 10.minutes) {
      health.status().unreachableSequencers.isEmpty
    }

    // Connect to synchronizers, in particular to "synchronizer":
    participants.local.active.synchronizers.reconnect_all()

    val connection =
      s"http://$sequencerHost:$sequencerPort"
    participants.local.active.filterNot(_.synchronizers.active("synchronizer")).foreach { p =>
      p.synchronizers.connect("synchronizer", connection).discard
    }

    // Set some resource limits to measure the impact of the feature on performance.
    // Choose the limit high enough such that the resource limit will not be hit.
    participants.local.active
      .foreach(
        _.resources.set_resource_limits(
          ResourceLimits(
            maxInflightValidationRequests = Some(10000),
            maxSubmissionRate = Some(10000),
          )
        )
      )

  }

  def runParticipantsSec(
      awaitParticipants: ParticipantNodeReferences => Seq[ParticipantReference] = _.remote
  )(implicit consoleEnvironment: ConsoleEnvironment): Unit = {
    import consoleEnvironment.*
    // Wait until participant is active
    utils.retry_until_true(timeout = 10.minutes) {
      awaitParticipants(participants).forall { p =>
        p.health.status match {
          case s: NodeStatus.Success[ParticipantStatus] => s.status.active
          case _ => false
        }
      }
    }
    participants.local.start()
  }

  def runPerformanceRunners(
      maxTestDuration: FiniteDuration =
        FiniteDuration(sys.env("TEST_DURATION").toLong, sys.env("TEST_DURATION_UNIT")),
      pollInterval: FiniteDuration = 10.seconds,
      startupTimeout: FiniteDuration = 2.minutes,
      numberOfIssuersPerParticipant: Int = 1,
      numberOfTradersPerParticipant: Int = 5,
      numAssetsPerIssuer: Int = sys.env("NUM_ASSETS_PER_ISSUER").toInt,
      batchSize: Int = sys.env("BATCH_SIZE").toInt,
      masterName: String = "master",
      payloadSize: Long = sys.env("PAYLOAD_SIZE").toLong,
      selectParticipants: ParticipantNodeReferences => Seq[ParticipantReference] = _.all,
      repositoryRoot: String = sys.env("REPOSITORY_ROOT"),
  )(implicit consoleEnvironment: ConsoleEnvironment): Unit = {

    import consoleEnvironment.*

    val rateSettings = RateSettings(
      submissionRateSettings = SubmissionRateSettings.TargetLatency(
        startRate = 3,
        targetLatencyMs = 10000,
      ),
      batchSize = batchSize,
      factorOfMaxSubmissionsPerIteration = 0.5,
      commandExpiryCheckSeconds = 120,
      commandClientConfiguration = CommandClientConfiguration(
        maxCommandsInFlight = 10000,
        maxParallelSubmissions = 10000,
        defaultDeduplicationTime = JDuration.ofSeconds(60),
      ),
    )

    val totalCycles = 10000000
    val masterRole: Master = Master(
      name = masterName,
      runConfig = MasterDynamicConfig(
        totalCycles = totalCycles,
        reportFrequency = 1000,
        runType = new M.orchestration.runtype.DvpRun(
          numAssetsPerIssuer,
          0,
          payloadSize,
          TraderDriver.toPartyGrowth(0),
        ),
      ),
      quorumIssuers = numberOfIssuersPerParticipant,
      quorumParticipants = numberOfTradersPerParticipant,
    )

    // ------------------------------------------------------------------
    // Declarations
    // ------------------------------------------------------------------

    val loggerFactory: NamedLoggerFactory = consoleEnvironment.environment.loggerFactory

    def commonRolesOfParticipant(participantReference: ParticipantReference): Set[PartyRole] = {
      val issuers = (1 to numberOfIssuersPerParticipant).map(i =>
        DvpIssuer(s"${participantReference.name}-issuer$i", settings = RateSettings.defaults)
      )
      val traders = (1 to numberOfTradersPerParticipant).map(i =>
        DvpTrader(s"${participantReference.name}-trader$i", rateSettings)
      )

      (issuers ++ traders).toSet
    }

    def createPerformanceRunner(
        p: ParticipantReference,
        baseSynchronizerId: SynchronizerId,
        isMaster: Boolean,
    ): PerformanceRunner = {
      val localRoles =
        if (isMaster) commonRolesOfParticipant(p) + masterRole else commonRolesOfParticipant(p)
      val connectivity = Connectivity(
        name = p.name,
        host = p.config.clientLedgerApi.address,
        port = p.config.clientLedgerApi.port,
      )
      val config = PerformanceRunnerConfig(masterName, localRoles, connectivity, baseSynchronizerId)
      new PerformanceRunner(config, consoleEnvironment.environment.metricsRegistry, loggerFactory)(
        environment.executionContext
      )
    }

    def await(startMessage: String, timeoutMessage: => String)(condition: => Boolean): Unit = {
      print(startMessage + "\t..")
      utils.retry_until_true(timeout = startupTimeout)(
        {
          print(".")
          condition
        },
        "\n" + timeoutMessage,
      )
      println(" done")
    }

    // ------------------------------------------------------------------
    // Execution
    // ------------------------------------------------------------------
    await(
      "Wait until all nodes have started",
      s"Unable to reach Canton. Has it been started? Status:\n${health.status()}",
    ) {
      val status = health.status()
      status.unreachableSequencers.isEmpty && status.unreachableParticipants.isEmpty
    }

    await(
      "Wait until all participants are connected to a synchronizer",
      "Some of the Canton participants are not connected to a synchronizer. Giving up.",
    ) {
      selectParticipants(participants).active.forall(_.synchronizers.list_connected().nonEmpty)
    }

    selectParticipants(participants).active.dars
      .upload(
        s"$repositoryRoot/community/performance-driver/target/scala-2.13/resource_managed/main/PerformanceTest.dar"
      )
      .discard
    selectParticipants(participants).active.foreach(_.packages.synchronize_vetting())

    val runners = selectParticipants(participants).active.toList match {
      case masterParticipant :: otherParticipants =>
        val synchronizerId = masterParticipant.synchronizers
          .list_connected()
          .map(_.physicalSynchronizerId)
          .minByOption(_.toProtoPrimitive)
          .getOrElse(
            throw new IllegalStateException(
              s"List of connected synchronizers for ${masterParticipant.id} cannot be empty"
            )
          )

        val masterRunner =
          createPerformanceRunner(masterParticipant, synchronizerId.logical, isMaster = true)
        val otherRunners =
          otherParticipants.map(
            createPerformanceRunner(_, synchronizerId.logical, isMaster = false)
          )
        masterRunner :: otherRunners

      case Nil => Nil
    }

    val runnersStatusF: Seq[Future[Either[String, Unit]]] = runners.map(_.startup())

    await(
      "Wait until performance runners are in mode 'THROUGHPUT' or 'DONE'",
      "Initialization of the performance runners timed out.",
    ) {
      val modes = runners.flatMap(_.status()).collect { case s: MasterStatus => s.mode }
      modes.contains("THROUGHPUT") || modes.contains("DONE")
    }

    println(
      s"Testing performance for $maxTestDuration or $totalCycles cycles (whichever elapses first)..."
    )

    val measurements = selectParticipants(participants).map { p =>
      val parties = p.parties.list().map(_.party)
      p.ledger_api.updates.start_measuring(parties.toSet, "canton.transactions-emitted")
    }

    @tailrec
    def awaitCompletion(deadline: Deadline, pollInterval: Duration): Unit =
      if (runnersStatusF.forall(_.isCompleted)) {
        println(s"Maximum number of cycles ($totalCycles) reached. Terminating...")
      } else if (deadline.hasTimeLeft()) {
        Threading.sleep(pollInterval.toMillis)
        awaitCompletion(deadline, pollInterval)
      } else {
        println(s"Maximum test duration reached ($maxTestDuration). Terminating...")
      }

    awaitCompletion(maxTestDuration.fromNow, pollInterval)

    measurements.foreach(_.close())

    runners.foreach(_.close())

    // Give the participants time to get idle before stopping the test.
    selectParticipants(participants).active.foreach { p =>
      p.health.ping(p, timeout = 120.seconds).discard
    }

  }

}
