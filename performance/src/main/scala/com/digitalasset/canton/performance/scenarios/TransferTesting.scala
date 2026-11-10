// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.scenarios

import com.digitalasset.canton.admin.api.client.data.{
  NodeStatus,
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
  ParticipantReference,
  RemoteParticipantReference,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.performance.PartyRole.{DvpIssuer, Transfer}
import com.digitalasset.canton.performance.RateSettings.SubmissionRateSettings
import com.digitalasset.canton.performance.model.java as M
import com.digitalasset.canton.performance.scenarios.LongRunning.WithPreparedLogging
import com.digitalasset.canton.performance.{
  Connectivity,
  PartyRole,
  PerformanceRunner,
  PerformanceRunnerConfig,
  RateSettings,
}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.util.FutureInstances.parallelFuture
import com.digitalasset.canton.util.{FutureUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{SynchronizerAlias, config}

import java.time.Instant
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object TransferTesting {

  def startupMaster(
      masterName: String = "1Master",
      totalCycles: Int = sys.env.get("NUM_CYCLES").map(_.toInt).getOrElse(Int.MaxValue),
      numAssetsPerActor: Int = sys.env.get("NUM_ASSETS_PER_ISSUER").map(_.toInt).getOrElse(100),
      transferBatchSize: Int = sys.env.get("TRANSFER_BATCH_SIZE").map(_.toInt).getOrElse(150),
      reportFrequency: Int = sys.env.get("REPORT_FREQUENCY").map(_.toInt).getOrElse(10000),
      waitUntilReady: config.NonNegativeDuration = config.NonNegativeDuration.ofMinutes(
        sys.env.get("WAIT_UNTIL_READY_MINUTES").map(_.toLong).getOrElse(15L)
      ),
      protocolVersion: ProtocolVersion = sys.env
        .get("CANTON_PROTOCOL_VERSION")
        .map(ProtocolVersion.tryCreate)
        .getOrElse(ProtocolVersion.v37),
      startupParallelism: Int = sys.env.get("STARTUP_PARALLELISM").map(_.toInt).getOrElse(4),
      runType: String = sys.env.getOrElse("RUN_TYPE", "transfer"), // or triparty
      // only used in triparty setup
      issuersPerNode: Int = sys.env.get("ISSUERS_PER_NODE").map(_.toInt).getOrElse(1),
      tradersPerNode: Int = sys.env.get("TRADERS_PER_NODE").map(_.toInt).getOrElse(5),
      numAccountsPerIssuer: Int = sys.env.get("NUM_ACCOUNTS_PER_ISSUER").map(_.toInt).getOrElse(5),
      capNumCounterparties: Int = sys.env.get("CAP_NUM_COUNTERPARTIES").map(_.toInt).getOrElse(15),
      assetIssuers: Int = 2,
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
    def waitForAllNodes(
        nodeType: String,
        nodes: Seq[InstanceReference],
        mustBeInitialized: Boolean = true,
    ) =
      utils.retry_until_true(
        timeout = waitUntilReady
      )(
        Try {
          val (active, inactive) = nodes.map(_.health.status).partition {
            case NodeStatus.Failure(_) => false
            case NodeStatus.NotInitialized(active, _, _) => active && !mustBeInitialized
            case NodeStatus.Success(status) => status.active
          }
          val str =
            s"Checking if nodes of type $nodeType are ready. ready=${active.length}, not-ready=${inactive.length}"
          myLogger.info(str)
          println(str)
          inactive.isEmpty
        } match {
          case Failure(exception) =>
            myLogger.error("Error while checking node status. Retrying", exception)
            false
          case Success(value) => value
        },
        s"Nodes did not become active ${val (active, inactive) = nodes.partition(_.health.active)
          s"\n  active=${active.map(_.name)}\n  inactive=${inactive.map(_.name)}" }",
      )

    com.digitalasset.canton.util.ErrorUtil.requireState(
      sequencers.all.sizeIs == mediators.all.size,
      "Require same amount of mediators as synchronizers",
    )

    val opsparticipant =
      participants.local
        .find(_.name.startsWith("ops"))
        .orElse(participants.local.headOption)
        .getOrElse(
          throw new IllegalStateException(
            "cannot find local participant which i want to use for ops"
          )
        )

    waitForAllNodes("sequencers", sequencers.all, mustBeInitialized = false)
    waitForAllNodes("mediators", mediators.all, mustBeInitialized = false)

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
              staticSynchronizerParameters = StaticSynchronizerParameters.defaults(protocolVersion),
            )
            .discard
          (alias, sequencer.sequencerConnection)
        }
      },
      waitUntilReady.unwrap,
    )

    def connectParticipant(participant: ParticipantReference): Unit = {
      myLogger.info(s"Waiting for ${participant.name} to become ready")
      println(s"${Instant.now} Waiting for ${participant.name} to become ready")
      Try {
        waitForAllNodes(participant.name, Seq(participant))
        connections.sortBy(_._1.unwrap).foreach { case (alias, connection) =>
          myLogger.info(s"Connecting ${participant.name} to $alias")
          println(s"${Instant.now} Connecting ${participant.name} to $alias")
          participant.synchronizers.connect_by_config(
            SynchronizerConnectionConfig(
              alias,
              SequencerConnections.single(connection),
            ),
            synchronize = None,
          )
        }
        // wait for all synchronizer connections to be established
        utils.retry_until_true(
          timeout = waitUntilReady
        )(participant.synchronizers.list_registered().forall(_._3))
        // once that is done, we can upload the dar to the participant
        LongRunning.uploadDarToNodes(
          Seq(participant),
          myLogger,
        )
      } match {
        case Failure(exception) =>
          myLogger.error(s"Failed to connect participant ${participant.name}", exception)
          throw exception
        case Success(_) => ()
      }
    }
    // ---------------------------------------------------
    // setup ops participant
    // ---------------------------------------------------
    connectParticipant(opsparticipant)
    LongRunning.enableMultisync(opsparticipant)

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
    val localRoles: Set[PartyRole] =
      if (runType != "triparty")
        Set(
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
        )
      else {
        val numActiveNodes =
          participants.all.size - (if (opsparticipant.name.startsWith("ops")) 1 else 0)
        // we need to ensure we have all roles covered
        require(
          (issuersPerNode * numActiveNodes) % 3 == 0,
          "need to align agent, taker and subregister",
        )
        val numAgents = numActiveNodes * (issuersPerNode / 3)
        Set(
          com.digitalasset.canton.performance.PartyRole.Master(
            masterName,
            runConfig = com.digitalasset.canton.performance.PartyRole.MasterDynamicConfig(
              totalCycles = totalCycles,
              reportFrequency = reportFrequency,
              runType =
                new com.digitalasset.canton.performance.model.java.orchestration.runtype.TriParty(
                  numAgents, // numAgents
                  numAssetsPerActor, // num assets per actor
                  numAccountsPerIssuer, // numAccountsPerIssuer
                  // every custodian will have a max num of randomly picked counterparties. we do have to limit this as otherwise, the
                  // number of combinations will grow quadratically which means we'll wait hours for the test to startup
                  capNumCounterparties,
                ),
            ),
            quorumProviders = (3 * numAgents + 1 + assetIssuers), // +1 for cash issuer
            quorumUsers = tradersPerNode * numActiveNodes,
          ),
          com.digitalasset.canton.performance.PartyRole.TriPartyProvider(
            "cash-issuer",
            RateSettings(SubmissionRateSettings.TargetLatencyNew()),
          ),
        ) ++ (0 until assetIssuers).map { idx =>
          com.digitalasset.canton.performance.PartyRole.TriPartyProvider(
            "issuer-" + idx,
            RateSettings(SubmissionRateSettings.TargetLatencyNew()),
          )
        }
      }

    val opsConfig = com.digitalasset.canton.performance.PerformanceRunnerConfig(
      master = masterName,
      localRoles = localRoles,
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

    // ---------------------------------------------------
    // setup remaining participants once they are ready
    // ---------------------------------------------------
    Await
      .result(
        MonadUtil.parTraverseWithLimit(PositiveInt.tryCreate(startupParallelism))(
          participants.all.filter(p => p.name != opsparticipant.name).sortBy(_.name)
        ) { participant =>
          Future {
            connectParticipant(participant)
          }
        },
        waitUntilReady.unwrap,
      )
      .discard

    runnerP1
  }

  private def rateSettingsFromEnv(): SubmissionRateSettings =
    (sys.env.get("RATE_TYPE"), sys.env.get("FIXED_RATE"), sys.env.get("TARGET_LATENCY_MS")) match {
      case (Some("fixed"), Some(rate), _) =>
        SubmissionRateSettings.FixedRate(rate.toDouble)
      case (Some("target-latency"), _, Some(latency)) =>
        SubmissionRateSettings.TargetLatencyNew(targetLatencyMs = latency.toInt, startPending = 1.5)
      case _ =>
        SubmissionRateSettings.TargetLatencyNew(targetLatencyMs = 9000)
    }

  def startupParticipants(
      masterName: String = "1Master",
      partyDiscriminator: String = "",
      issuersPerNode: Int = sys.env.get("ISSUERS_PER_NODE").map(_.toInt).getOrElse(1),
      tradersPerNode: Int = sys.env.get("TRADERS_PER_NODE").map(_.toInt).getOrElse(5),
      onlyLocalIssuer: Boolean = true,
      rateSettings: RateSettings = RateSettings(
        rateSettingsFromEnv(),
        batchSize = 1,
      ),
      waitUntilReady: config.NonNegativeDuration = config.NonNegativeDuration.ofMinutes(
        sys.env.get("WAIT_UNTIL_READY_MINUTES").map(_.toLong).getOrElse(15L)
      ),
      ignoreParticipants: Set[String] = Set.empty,
      runType: String = sys.env.getOrElse("RUN_TYPE", "transfer"), // or triparty
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
      val start: (Boolean, Option[Int], Int, Int) = (true, None, 0, 0)
      val (allReady, _, good, notgood) = participants.all.foldLeft(start) {
        case ((allReady, numConnections, good, notgood), participant) =>
          val conns = participant.synchronizers.list_registered().map(_._3)
          val ready = conns.forall(identity) && conns.nonEmpty && numConnections.forall(
            conns.sizeCompare(_) == 0
          )
          (
            ready && allReady,
            Some(conns.size),
            if (ready) good + 1 else good,
            if (!ready) notgood + 1 else notgood,
          )
      }
      myLogger.info(
        s"Waiting for nodes to be connected: ready=$good, not-yet=$notgood"
      )
      println(s"Waiting for nodes to be connected: ready=$good, not-yet=$notgood")
      allReady
    }

    // enable multi-sync
    participants.all.foreach(LongRunning.enableMultisync)

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

    participants.all.sortBy(_.name).filterNot(p => ignoreParticipants.contains(p.name)).map {
      participant =>
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

        val localRoles: Set[PartyRole] = if (runType != "triparty") {
          val issuers = (0 until issuersPerNode).map(idx =>
            DvpIssuer(s"${partyDiscriminator}issuer-${participant.name}-$idx", rateSettings)
          )

          val useIssuers = if (onlyLocalIssuer) issuers.map(_.name).toSet else Set.empty[String]

          val transfers = (0 until tradersPerNode).map(idx =>
            Transfer(
              s"${partyDiscriminator}transfer-${participant.name}-$idx",
              rateSettings,
              issuers = useIssuers,
            )
          )
          (issuers ++ transfers).toSet
        } else {
          require(issuersPerNode % 3 == 0, "need to align agent, taker and subregister")
          val issuers = Seq("agent", "taker", "subregister").flatMap { role =>
            (0 until issuersPerNode / 3).map(idx =>
              PartyRole.TriPartyProvider(
                s"$role$partyDiscriminator-${participant.name}-$idx",
                rateSettings,
              )
            )
          }.toSet
          val users = (0 until tradersPerNode)
            .map(idx =>
              PartyRole.TriPartyUser(
                s"user$partyDiscriminator-${participant.name}-$idx",
                rateSettings,
              )
            )
            .toSet
          issuers ++ users
        }
        val prconfig = PerformanceRunnerConfig(
          master = masterName,
          localRoles = localRoles,
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
