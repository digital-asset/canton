// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.scenarios

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.PositiveDurationSeconds
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.ConsoleMacros.{pruning, utils}
import com.digitalasset.canton.console.{ConsoleEnvironment, ParticipantReference}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.performance.*
import com.digitalasset.canton.performance.PartyRole.{Master, MasterDynamicConfig}
import com.digitalasset.canton.performance.RateSettings.SubmissionRateSettings
import com.digitalasset.canton.performance.console.{RunTypeConfig as C, *}
import com.digitalasset.canton.performance.model.java as M
import com.digitalasset.canton.performance.model.java.orchestration.partygrowth.{
  LocalGrowth,
  NoPartyGrowth,
  RemoteGrowth,
}
import com.digitalasset.canton.performance.model.java.orchestration.runtype.DvpRun
import com.digitalasset.canton.performance.util.{
  ParticipantSimulator,
  ParticipantSimulatorController,
}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*

object LongRunning {

  /** Start long runnign test
    *
    * @param partyGrowthFactor
    *   if > 0 then the test will continuously allocate parties until it hits maxParties per
    *   allocator. a larger value means that the party will be used several times before replaced.
    *   this means that growth is slower with a larger factor
    * @param maxPendingParties
    *   how many party allocation requests can be pending simultaneously during local growth per
    *   allocator
    * @param maxPartiesInPool
    *   how many parties to keep in the allocator pool
    * @param maxParties
    *   how many parties per allocator to allocate at best
    * @param lightweightParticipants
    *   how many lightweight participants to spin up and connect to the first sequencer. if set to
    *   true, any party allocation will refer to the lightweight participants (for a constant party
    *   test, set maxParties = maxPartiesInPool and partyGrowthFactor > 1)
    */
  def startup(
      masterName: String = "1Master",
      localMaster: Boolean = !sys.env.contains("REMOTE_MASTER"),
      totalCycles: Int = sys.env.get("NUM_CYCLES").map(_.toInt).getOrElse(Int.MaxValue),
      exitWhenDone: Boolean = sys.env.contains("EXIT_WHEN_DONE"),
      singleSynchronizer: Boolean = sys.env.contains("SINGLE_SYNCHRONIZER"),
      reportFrequency: Int = 1000,
      batchSize: Int = 3, // number of propose / accept actions sent in one batch
      issuersPerNode: Int = 5,
      tradersPerNode: Int = 5,
      numAssetsPerIssuer: Int = 1000,
      acsGrowthFactor: Int = 1,
      partyGrowthFactor: Int = 0,
      maxPendingParties: Int = 5,
      maxPartiesInPool: Int = 1000,
      maxParties: Int = Int.MaxValue,
      enableLightweightParticipants: Boolean = false,
      dvpPayloadSize: Int = 0,
      sequencerTrustThreshold: PositiveInt = PositiveInt.one,
      sequencerLivenessMargin: NonNegativeInt = NonNegativeInt.zero,
      enablePruning: Boolean = true,
      pruningWindowStartCron: String = "0 0 7,19 * * ? *",
      pruningWindowMaxDuration: FiniteDuration = 2.hours,
      pruningRetention: FiniteDuration = 7.days,
      otherSynchronizersRatio: Double = 0.5,
      disableTrafficControl: Boolean = sys.env.contains("DISABLE_TRAFFIC_CONTROL"),
  )(implicit
      consoleEnvironment: ConsoleEnvironment
  ): (MissionControl, Option[ParticipantSimulatorController]) = {

    import ConsoleEnvironment.Implicits.*
    import consoleEnvironment.*

    val loggerFactory = consoleEnvironment.environment.loggerFactory
    val myLogger = loggerFactory.getLogger(LongRunning.getClass)
    val onClose = new AtomicReference[Option[java.lang.AutoCloseable]](None)

    // Initial debug information
    myLogger.info(s"Available mediators: ${mediators.all.mkString(", ")}")
    myLogger.info(s"Available sequencers: ${sequencers.all.mkString(", ")}")
    myLogger.info(s"Available participants: ${participants.all.mkString(", ")}")

    val synchronizersConf =
      if (singleSynchronizer) SynchronizersConfiguration.singleSynchronizer
      else {
        val synchronizerName =
          // participant only mode
          if (sequencers.all.isEmpty)
            participants.all
              .find(_.synchronizers.list_connected().nonEmpty)
              .flatMap(_.synchronizers.list_connected().headOption)
          else None
        synchronizerName match {
          case Some(synchronizerAlias) =>
            new SynchronizersConfiguration(
              NonEmpty.mk(
                List,
                new SynchronizerConfiguration(
                  synchronizerAlias.synchronizerAlias.unwrap,
                  Seq(),
                  Seq(),
                ),
              )
            )
          case None =>
            SynchronizersConfiguration.twoSynchronizers.valueOr(err =>
              throw new RuntimeException(s"Unable to create domains configuration: $err")
            )
        }
      }

    // register auto-close first such that we can orderly shut down performance runner as first thing
    utils.auto_close(() => onClose.get().foreach(_.close()))
    nodes.local.start()

    val partyPrefix = "bystander-"
    val partyGrowth = if (partyGrowthFactor > 0 && enableLightweightParticipants) {
      new RemoteGrowth(partyGrowthFactor, maxParties, maxPartiesInPool, partyPrefix)
    } else if (partyGrowthFactor > 0) {
      new LocalGrowth(partyGrowthFactor, maxParties, maxPartiesInPool, maxPendingParties)
    } else {
      new NoPartyGrowth(com.daml.ledger.javaapi.data.Unit.getInstance())
    }

    // parametrise test run
    val (runTypeConfig, runTypeParams) = {
      val protoConf = new M.orchestration.runtype.DvpRun(
        numAssetsPerIssuer,
        acsGrowthFactor,
        dvpPayloadSize,
        partyGrowth,
      )
      (C.DvpRun(masterName), protoConf)
    }
    val allParticipants =
      participants.all.active.groupBy(_.id).flatMap(_._2.headOption.toList).toList

    val masterSetup = getMasterSetup(
      masterName,
      localMaster,
      totalCycles,
      reportFrequency,
      issuersPerNode,
      tradersPerNode,
      runTypeConfig,
      runTypeParams,
      allParticipants,
    )

    // find performance dar
    val dar = os
      .walk(os.Path(".", base = os.pwd))
      .find(x => x.ext == "dar" && x.baseName.startsWith("PerformanceTest"))
      .map(_.toString)

    // ---------------------------------------------------------
    // first step: start our nodes and establish connectivity
    // ---------------------------------------------------------
    // bootstrap distributed synchronizers if necessary
    val bootstrapped = synchronizersConf.bootstrap()

    // Enable traffic control on the newly bootstrapped synchronizers
    if (!disableTrafficControl) {
      bootstrapped.foreach { case (synchronizerConfig, synchronizerId) =>
        synchronizerConfig.enableTrafficControlWithHighBaseRate(synchronizerId)
      }
    }

    // start and reconnect participants
    participants.local.start()
    allParticipants.zipWithIndex.foreach { case (participant, index) =>
      if (participant.synchronizers.list_registered().isEmpty)
        synchronizersConf.connect(
          participant,
          index,
          sequencerTrustThreshold,
          sequencerLivenessMargin,
        )
      else participant.synchronizers.reconnect_all()

      for {
        synchronizer <- participant.synchronizers.list_connected()
        filename <- dar.toList
      } {
        myLogger.info(
          s"uploading $filename to ${participant.name} for synchronizer ${synchronizer.synchronizerId}"
        )
        participant.dars.upload(filename, synchronizerId = synchronizer.synchronizerId).discard
      }
    }

    val baseSynchronizerAlias = synchronizersConf.synchronizers.head1
    val participant1 = participants.all.headOption.getOrElse(
      throw new IllegalStateException("Unable to find a participant")
    )

    val baseSynchronizer =
      participant1.synchronizers.physical_id_of(baseSynchronizerAlias.synchronizerName)
    val otherSynchronizers =
      participant1.synchronizers
        .list_connected()
        .filterNot(_.physicalSynchronizerId == baseSynchronizer)
        .map(_.physicalSynchronizerId)

    // ---------------------------------------------------------
    // second step: startup our runners
    // ---------------------------------------------------------
    val participantInfo = (participants.local.active.map(x =>
      (x.name, x.config.ledgerApi.port, x.config.ledgerApi.tls.map(_.clientConfig))
    ) ++ participants.remote.active.map(x => (x.name, x.config.ledgerApi.port, None))).map {
      case (name, port, tls) =>
        Connectivity(name = name, port = port, tls = tls)
    }

    val participantSimController = if (enableLightweightParticipants && singleSynchronizer) {
      (for {
        localNode <- participants.local.headOption
        sequencersB <- NonEmpty.from(sequencers.all)
        sequencers = NonEmpty.mk(Seq, sequencersB.head1)
      } yield {
        val simulator =
          new ParticipantSimulator(
            localNode,
            sequencers,
            sequencers.head1.synchronizer_parameters.static.get().toInternal,
            respondToAcsCommitments = true,
            loggerFactory,
            environmentTimeouts,
          )
        simulator.uploadRootCert().discard

        // copy the packages to vet from participant1
        val packagesToVet = participant1.topology.vetted_packages
          .list(sequencers.head1.synchronizer_id, filterParticipant = localNode.filterString)
          .headOption
          .getOrElse(sys.error(s"No package vetting for $localNode"))
          .item
          .packages

        new ParticipantSimulatorController(
          simulator = simulator,
          prefix = partyPrefix,
          packagesToVet = packagesToVet,
          loggerFactory = loggerFactory,
        )

      })
    } else None

    myLogger.info(
      s"Starting runners now, using synchronizer $baseSynchronizer as base, and other synchronizers $otherSynchronizers with participants at ${participantInfo
          .map(x => s"${x.host}:${x.port.unwrap}")
          .mkString(",")}"
    )

    val mc = new MissionControl(
      loggerFactory,
      consoleEnvironment.environment.metricsRegistry,
      consoleEnvironment.environment.clock,
      masterSetup,
      participantInfo,
      events = List(),
      issuersPerNode = issuersPerNode,
      tradersPerNode = tradersPerNode,
      settings = RateSettings(SubmissionRateSettings.TargetLatency(), batchSize = batchSize),
      baseSynchronizerId = baseSynchronizer.logical,
      otherSynchronizers = otherSynchronizers.map(_.logical),
      otherSynchronizersRatio = otherSynchronizersRatio,
    )(consoleEnvironment.environment.executionContext)

    onClose.set(Some(() => mc.close()))

    myLogger.info("Started runners")

    // configure automatic pruning on all active nodes that support pruning
    if (enablePruning) {
      pruning.set_schedule(
        pruningWindowStartCron,
        PositiveDurationSeconds(pruningWindowMaxDuration),
        PositiveDurationSeconds(pruningRetention),
      )
    }

    if (exitWhenDone) {
      myLogger.info("registering shutdown hook")
      mc.isDoneF.onComplete { x =>
        myLogger.info(s"Runner finished with $x. Exiting process.")
        sys.exit(0)
      }(environment.executionContext)
    }
    (mc, participantSimController)
  }

  private def getMasterSetup(
      masterName: String,
      localMaster: Boolean,
      totalCycles: Int,
      reportFrequency: Int,
      issuersPerNode: Int,
      tradersPerNode: Int,
      runTypeConfig: C.DvpRun,
      runTypeParams: DvpRun,
      allParticipants: List[ParticipantReference],
  ): Either[Master, C.DvpRun] =
    if (localMaster)
      Left(
        Master(
          masterName,
          quorumParticipants = (tradersPerNode * allParticipants.length),
          quorumIssuers = (issuersPerNode * allParticipants.length),
          runConfig = MasterDynamicConfig(
            totalCycles = totalCycles,
            reportFrequency = Math.min(reportFrequency, totalCycles / 2 + 1),
            runType = runTypeParams,
          ),
        )
      )
    else
      Right(runTypeConfig)

  def growTopologyState(
      participant: ParticipantReference,
      numVettings: Int = 10,
      numOtk: Int = 10,
  ): Unit = {

    val syncId = participant.synchronizers
      .list_connected()
      .headOption
      .getOrElse(sys.error("Not connected to any synchroniser"))
      .synchronizerId
    val storeId = TopologyStoreId.Synchronizer(syncId)
    val cur = participant.topology.owner_to_key_mappings
      .list(
        Some(storeId),
        filterKeyOwnerUid = participant.id.filterString,
      )
      .headOption
      .getOrElse(sys.error("No OTK?"))

    (1 to numOtk).foreach { idx =>
      participant.topology.owner_to_key_mappings
        .propose(
          member = participant.id,
          keys = cur.item.keys,
          serial = Some(cur.context.serial + PositiveInt.tryCreate(idx)),
          store = storeId,
        )
        .discard
    }

    val curVetting = participant.topology.vetted_packages
      .list(
        Some(storeId),
        filterParticipant = participant.id.filterString,
      )
      .headOption
      .getOrElse(sys.error("No Vettings?"))

    (1 to numVettings).foreach { idx =>
      participant.topology.vetted_packages
        .propose(
          participant = participant.id,
          packages = curVetting.item.packages,
          serial = Some(cur.context.serial + PositiveInt.tryCreate(idx)),
          store = storeId,
        )
        .discard
    }

  }

}
