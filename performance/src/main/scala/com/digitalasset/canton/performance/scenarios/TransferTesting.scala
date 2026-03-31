// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.scenarios

import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnections,
  StaticSynchronizerParameters,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.ConsoleMacros.{bootstrap, utils}
import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  InstanceReference,
  LocalParticipantReference,
  RemoteParticipantReference,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.performance.PartyRole.{DvpIssuer, Transfer}
import com.digitalasset.canton.performance.RateSettings.SubmissionRateSettings
import com.digitalasset.canton.performance.model.java as M
import com.digitalasset.canton.performance.scenarios.LongRunning.WithPreparedLogging
import com.digitalasset.canton.performance.{
  Connectivity,
  PerformanceRunner,
  PerformanceRunnerConfig,
  RateSettings,
}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.util.FutureInstances.parallelFuture
import com.digitalasset.canton.util.{FutureUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{SynchronizerAlias, config}

import scala.concurrent.{Await, ExecutionContext, Future}

object TransferTesting {

  def startupMaster(
      masterName: String = "1Master",
      totalCycles: Int = sys.env.get("NUM_CYCLES").map(_.toInt).getOrElse(Int.MaxValue),
      numAssetsPerActor: Int = sys.env.get("NUM_ASSETS_PER_ISSUER").map(_.toInt).getOrElse(50000),
      transferBatchSize: Int = sys.env.get("TRANSFER_BATCH_SIZE").map(_.toInt).getOrElse(150),
      reportFrequency: Int = sys.env.get("REPORT_FREQUENCY").map(_.toInt).getOrElse(250000),
      waitUntilReady: config.NonNegativeDuration = config.NonNegativeDuration.ofMinutes(
        sys.env.get("WAIT_UNTIL_READY_MINUTES").map(_.toLong).getOrElse(15L)
      ),
      protocolVersion: ProtocolVersion = ProtocolVersion.v35,
      startupParallelism: Int = sys.env.get("STARTUP_PARALLELISM").map(_.toInt).getOrElse(4),
  )(implicit
      consoleEnvironment: ConsoleEnvironment
  ) = {

    import consoleEnvironment.*
    val prepared = (new WithPreparedLogging("master", consoleEnvironment))
    import prepared.*
    implicit val ec: ExecutionContext = consoleEnvironment.environment.executionContext

    // ---------------------------------------------------
    // wait until all nodes are loaded
    // ---------------------------------------------------
    def waitForAllNodes(nodeType: String, nodes: Seq[InstanceReference]) =
      utils.retry_until_true(
        timeout = waitUntilReady
      )(
        {
          val (active, inactive) = nodes.partition(_.health.active)
          val str =
            s"Checking if nodes of type $nodeType are ready. ready=${active.length}, not-ready=${inactive.length}"
          myLogger.info(str)
          println(str)
          inactive.isEmpty
        },
        s"Nodes did not become active ${val (active, inactive) = nodes.partition(_.health.active)
          s"\n  active=${active.map(_.name)}\n  inactive=${inactive.map(_.name)}" }",
      )

    com.digitalasset.canton.util.ErrorUtil.requireState(
      sequencers.all.sizeIs == mediators.all.size,
      "Require same amount of mediators as synchronizers",
    )

    val opsparticipant = participants.local.headOption
      .getOrElse(
        throw new IllegalStateException("cannot find local participant which i want to use for ops")
      )

    waitForAllNodes("sequencers", sequencers.all)
    waitForAllNodes("mediators", mediators.all)
    waitForAllNodes("participants", participants.all)

    // ---------------------------------------------------
    // setup synchronizers
    // ---------------------------------------------------
    val connections = Await.result(
      MonadUtil.parTraverseWithLimit(PositiveInt.tryCreate(startupParallelism))(
        sequencers.all.active
          .sortBy(_.name)
          .zip(mediators.all.active.sortBy(_.name))
          .zipWithIndex
      ) { case ((sequencer, mediator), idx) =>
        val alias = SynchronizerAlias.tryCreate(s"sync$idx")
        myLogger.info(s"Bootstrapping $alias with ${sequencer.name} and ${mediator.name}")
        println(s"Bootstrapping $alias with ${sequencer.name} and ${mediator.name}")
        Future {
          bootstrap
            .synchronizer(
              synchronizerName = alias.unwrap,
              sequencers = Seq(sequencer),
              mediators = Seq(mediator),
              synchronizerOwners = Seq(sequencer),
              synchronizerThreshold = PositiveInt.one,
              staticSynchronizerParameters =
                StaticSynchronizerParameters.defaultsWithoutKMS(protocolVersion),
            )
            .discard
          (alias, sequencer.sequencerConnection)
        }
      },
      waitUntilReady.unwrap,
    )

    // ---------------------------------------------------
    // setup participants
    // ---------------------------------------------------
    Await
      .result(
        MonadUtil.parTraverseWithLimit(PositiveInt.tryCreate(startupParallelism))(
          participants.all.sortBy(_.name)
        ) { participant =>
          Future {
            connections.sortBy(_._1.unwrap).foreach { case (alias, connection) =>
              myLogger.info(s"Connecting ${participant.name} to $alias")
              println(s"Connecting ${participant.name} to $alias")
              participant.synchronizers.connect_by_config(
                SynchronizerConnectionConfig(
                  alias,
                  SequencerConnections.single(connection),
                ),
                synchronize = None,
              )
            }
            LongRunning.uploadDarToNodes(
              Seq(participant),
              myLogger,
            )
          }
        },
        waitUntilReady.unwrap,
      )
      .discard

    // ---------------------------------------------------
    // start master process
    // ---------------------------------------------------
    val (baseSynchronizer, otherSynchronizers) =
      opsparticipant.synchronizers
        .list_connected()
        .map(_.synchronizerId)
        .sortBy(_.uid.toProtoPrimitive)
        .toList match {
        case (one :: rest) => (one, rest)
        case _ => sys.error("ops participant is not connected anywhere?")
      }
    val opsConfig = com.digitalasset.canton.performance.PerformanceRunnerConfig(
      master = masterName,
      localRoles = Set(
        com.digitalasset.canton.performance.PartyRole.Master(
          masterName,
          runConfig = com.digitalasset.canton.performance.PartyRole.MasterDynamicConfig(
            totalCycles = totalCycles,
            reportFrequency = reportFrequency,
            runType =
              new com.digitalasset.canton.performance.model.java.orchestration.runtype.TransferRun(
                numAssetsPerActor, // num assets per actor
                transferBatchSize, // transfer batch size
                0, // payload
              ),
          ),
        )
      ),
      ledger = Connectivity(opsparticipant.name, opsparticipant.config.ledgerApi.clientConfig),
      baseSynchronizerId = baseSynchronizer,
      otherSynchronizers = otherSynchronizers,
    )

    val runnerP1 = new PerformanceRunner(
      opsConfig,
      environment.metricsRegistry,
      loggerFactory.append("runner", "master"),
    )(environment.executionContext)
    environment.addUserCloseable(runnerP1)
    FutureUtil.doNotAwait(runnerP1.startup(), "perf-runner failed")
    runnerP1
  }

  def startupParticipants(
      masterName: String = "1Master",
      partyDiscriminator: String = "",
      issuersPerNode: Int = 1,
      transferersPerNode: Int = 5,
      onlyLocalIssuer: Boolean = true,
      rateSettings: RateSettings = RateSettings(
        SubmissionRateSettings.TargetLatencyNew(
          targetLatencyMs = sys.env.get("TARGET_LATENCY_MS").map(_.toInt).getOrElse(9000)
        ),
        batchSize = 1,
      ),
      waitUntilReady: config.NonNegativeDuration = config.NonNegativeDuration.ofMinutes(
        sys.env.get("WAIT_UNTIL_READY_MINUTES").map(_.toLong).getOrElse(15L)
      ),
  )(implicit consoleEnvironment: ConsoleEnvironment) = {

    import consoleEnvironment.*
    val prepared = (new WithPreparedLogging("transfertest", consoleEnvironment))
    import prepared.*

    // wait until we see the synchronizer connections appearing
    // we use the dar vetting state to decide whether we are ready to go
    utils.retry_until_true(timeout = waitUntilReady) {
      val (good, notgood) = participants.all.partition(_.health.initialized())
      myLogger.info(
        s"Waiting for participant nodes to be initialised: ready=${good.length}, not-yet=${notgood.length}"
      )
      println(
        s"Waiting for participant nodes to be initialised: ready=${good.length}, not-yet=${notgood.length}"
      )
      notgood.isEmpty
    }

    // wait until we have all synchronizer connections ready
    utils.retry_until_true(timeout = waitUntilReady) {
      val (good, notgood) = participants.all.partition { p =>
        val tmp = p.synchronizers.list_registered().map(c => c._3)
        tmp.nonEmpty && tmp.forall(identity)
      }
      myLogger.info(
        s"Waiting for nodes to be connected: ready=${good.length}, not-yet=${notgood.length}"
      )
      println(s"Waiting for nodes to be connected: ready=${good.length}, not-yet=${notgood.length}")
      notgood.isEmpty
    }

    // wait until we see the vetting state to include the key dar
    utils.retry_until_true(timeout = waitUntilReady) {

      val (good, notgood) = participants.all.partition { c =>
        val state = c.synchronizers.list_connected().map(_.synchronizerId).map { synchronizerId =>
          (
            synchronizerId,
            c.topology.vetted_packages
              .list(
                Some(TopologyStoreId.Synchronizer(synchronizerId)),
                filterParticipant = c.id.filterString,
              )
              .exists(_.item.packages.exists(_.packageId == M.orchestration.TestRun.PACKAGE_ID)),
          )
        }
        val res = state.filterNot(_._2)
        if (res.nonEmpty) {
          myLogger.info(s"Package not yet vetted for ${c.name} at ${res.map(_._1)}")
        }
        state.nonEmpty && state.forall(_._2)
      }
      myLogger.info(
        s"Waiting for all packages to be ready and vetted: ready=${good.length}, not-yet=${notgood.length}"
      )
      println(
        s"Waiting for all packages to be ready and vetted: ready=${good.length}, not-yet=${notgood.length}"
      )
      notgood.isEmpty
    }

    participants.all.sortBy(_.name).map { participant =>
      val (baseSynchronizer, otherSynchronizers) =
        participant.synchronizers
          .list_connected()
          .map(_.synchronizerId)
          .sortBy(_.uid.toProtoPrimitive)
          .toList match {
          case (one :: rest) => (one, rest)
          case _ => sys.error("ops participant is not connected anywhere?")
        }
      val connectivity = participant match {
        case ref: LocalParticipantReference =>
          Connectivity(ref.name, ref.config.ledgerApi.clientConfig)
        case ref: RemoteParticipantReference => Connectivity(ref.name, ref.config.ledgerApi)
        case _ => sys.error("unknown type")
      }

      val issuers = (0 until issuersPerNode).map(idx =>
        DvpIssuer(s"${partyDiscriminator}issuer-${participant.name}-$idx", rateSettings)
      )

      val useIssuers = if (onlyLocalIssuer) issuers.map(_.name).toSet else Set.empty[String]

      val transfers = (0 until transferersPerNode).map(idx =>
        Transfer(
          s"${partyDiscriminator}transfer-${participant.name}-$idx",
          rateSettings,
          issuers = useIssuers,
        )
      )
      val prconfig = PerformanceRunnerConfig(
        master = masterName,
        localRoles = (issuers ++ transfers).toSet,
        ledger = connectivity,
        baseSynchronizerId = baseSynchronizer,
        otherSynchronizers = otherSynchronizers,
        maxRetries = PositiveInt
          .tryCreate((waitUntilReady.unwrap.toMillis / 500L).toInt), // retry interval is 500ms
      )
      myLogger.info(s"Starting up perf runner for ${participant.name}")
      println(s"Starting up perf runner for ${participant.name}")
      val runner = new PerformanceRunner(
        prconfig,
        environment.metricsRegistry,
        loggerFactory.append("runner", participant.name),
      )(environment.executionContext)

      environment.addUserCloseable(runner)
      FutureUtil.doNotAwait(runner.startup(), "perf-runner failed")
      runner
    }
  }

}
