// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.elements

import cats.data.EitherT
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.event.{ArchivedEvent, CreatedEvent}
import com.daml.ledger.api.v2.value.Identifier
import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.Party
import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.performance.PartyRole.MasterDynamicConfig
import com.digitalasset.canton.performance.acs.{ContractObserver, ContractStore}
import com.digitalasset.canton.performance.elements.DriverStatus.MasterStatus
import com.digitalasset.canton.performance.model.java as M
import com.digitalasset.canton.performance.model.java.orchestration.runtype.DvpRun
import com.digitalasset.canton.performance.model.java.orchestration.{
  ParticipantFlag,
  ParticipationRequest,
  ProbeType,
  Role,
  TestParticipant,
  TestRun,
}
import com.digitalasset.canton.performance.{Connectivity, PartyRole}
import com.digitalasset.canton.topology.SynchronizerId
import org.apache.pekko.actor.ActorSystem

import java.time.Instant
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters.*

/** Trait to amend the current run config when it hits a certain condition */
abstract class AmendMasterConfig(val description: String) {
  def check(stats: MasterStatus, master: M.orchestration.TestRun): Option[MasterDynamicConfig]
}

object AmendMasterConfig {
  abstract class AmendMasterConfigDvp(description: String) extends AmendMasterConfig(description) {
    def checkDvp(
        stats: MasterStatus,
        master: M.orchestration.TestRun,
        dvp: M.orchestration.runtype.DvpRun,
    ): Option[MasterDynamicConfig]

    override def check(stats: MasterStatus, master: TestRun): Option[MasterDynamicConfig] =
      master.runType match {
        case x: DvpRun => checkDvp(stats, master, x)
        case _ => None
      }
  }

  class UpdatePayloadSize(target: Long) extends AmendMasterConfigDvp("update payload size") {
    override def checkDvp(
        stats: MasterStatus,
        master: TestRun,
        dvp: DvpRun,
    ): Option[MasterDynamicConfig] =
      Some(
        MasterDynamicConfig(
          totalCycles = master.totalCycles.toInt,
          reportFrequency = master.reportFrequency.toInt,
          runType = new DvpRun(
            dvp.numAssetsPerIssuer,
            dvp.withAcsGrowth,
            target,
            dvp.partyGrowth,
          ),
        )
      )
  }
}

class MasterDriver(
    connectivity: Connectivity,
    masterPartyLf: LfPartyId,
    config: PartyRole.Master,
    prefix: MetricName,
    metricsFactory: LabeledMetricsFactory,
    loggerFactory: NamedLoggerFactory,
    control: DriverControl,
    baseSynchronizerId: SynchronizerId,
)(implicit
    ec: ExecutionContextExecutor,
    actorSystem: ActorSystem,
    executionSequencerFactory: ExecutionSequencerFactory,
) extends BaseDriver(
      connectivity,
      masterPartyLf,
      masterPartyLf,
      config.commandClientConfiguration,
      loggerFactory,
      control,
    ) {

  def name: String = config.name

  def addConfigAdjustment(amender: AmendMasterConfig): Unit =
    amendedConfig.updateAndGet(x => x :+ amender).discard

  private val actors =
    metricsFactory.counter(
      MetricInfo(
        prefix :+ "actors",
        "How many actors are currently part of the test and in what state",
        MetricQualification.Debug,
      )
    )(
      MetricsContext.Empty
    )

  protected val requests: ContractStore[
    ParticipationRequest.Contract,
    ParticipationRequest.ContractId,
    ParticipationRequest,
    Party,
  ] = new ContractStore[
    M.orchestration.ParticipationRequest.Contract,
    M.orchestration.ParticipationRequest.ContractId,
    M.orchestration.ParticipationRequest,
    Party,
  ](
    "participant requests",
    M.orchestration.ParticipationRequest.COMPANION,
    index = x => new Party(x.data.party),
    filter = _ => true,
    loggerFactory,
  ) {

    private def contextFromContract(contract: ParticipationRequest.Contract) = {
      val role = roleAsString(contract.data.role)
      MetricsContext(("flag", "onboarding"), ("role", role))
    }

    override protected def contractCreated(
        create: ParticipationRequest.Contract,
        index: Party,
        synchronizerId: String,
    ): Unit =
      actors.inc()(contextFromContract(create))

    override protected def contractArchived(
        archive: ParticipationRequest.Contract,
        index: Party,
    ): Unit =
      actors.dec()(contextFromContract(archive))

  }

  private val amendedConfig = new AtomicReference[Seq[AmendMasterConfig]](config.amendments)

  private val statsUpdateNeeded = new AtomicBoolean(false)
  private val currentStats = mutable.ArrayDeque[(Instant, Long, Long)]()
  private val testProbes =
    new ContractStore[
      M.orchestration.TestProbe.Contract,
      M.orchestration.TestProbe.ContractId,
      M.orchestration.TestProbe,
      (Party, M.orchestration.ProbeType),
    ](
      "test probes",
      M.orchestration.TestProbe.COMPANION,
      index = x => (new Party(x.data.party), x.data.typ),
      filter = x => x.data.master == masterParty.getValue,
      loggerFactory,
    )

  protected def roleAsString(role: Role): String =
    if (role == Role.PROVIDER) "provider"
    else if (role == Role.USER) "user"
    else "unknown"

  private val testParticipants =
    new ContractStore[
      M.orchestration.TestParticipant.Contract,
      M.orchestration.TestParticipant.ContractId,
      M.orchestration.TestParticipant,
      Party,
    ](
      "test participant",
      M.orchestration.TestParticipant.COMPANION,
      index = x => new Party(x.data.party),
      filter = x => x.data.master == masterParty.getValue,
      loggerFactory,
    ) {

      private def flagAsString(flag: ParticipantFlag): String =
        if (flag == ParticipantFlag.INITIALISING) "initialising"
        else if (flag == ParticipantFlag.READY) "ready"
        else if (flag == ParticipantFlag.FINISHED) "finished"
        else "unknown"

      private def contextFromContract(contract: TestParticipant.Contract) = {
        val flag = flagAsString(contract.data.flag)
        val role = roleAsString(contract.data.role)
        MetricsContext(("flag", flag), ("role", role))
      }

      override protected def contractArchived(
          archive: TestParticipant.Contract,
          index: Party,
      ): Unit =
        actors.dec()(contextFromContract(archive))

      override protected def contractCreated(
          create: TestParticipant.Contract,
          index: Party,
          synchronizerId: String,
      ): Unit =
        actors.inc()(contextFromContract(create))
    }

  private def templateMatches(expected: javaapi.data.Identifier)(actual: Identifier): Boolean =
    Identifier.fromJavaProto(expected.toProto) == actual

  listeners.appendAll(
    Seq(
      requests,
      testParticipants,
      testProbes,
      new ContractObserver() {
        override def processCreate(create: CreatedEvent, synchronizerId: String): Boolean = {
          if (
            create.templateId.fold(false)(
              templateMatches(M.orchestration.TestProbe.TEMPLATE_ID_WITH_PACKAGE_ID)(_)
            ) ||
            create.templateId
              .fold(false)(templateMatches(M.orchestration.TestRun.TEMPLATE_ID_WITH_PACKAGE_ID)(_))
          ) {
            statsUpdateNeeded.set(true)
          }
          false
        }

        override def processReassign(
            create: CreatedEvent,
            synchronizerId: String,
            reassignmentCounter: Long,
        ): Boolean = false

        override def processArchive(archive: ArchivedEvent): Boolean = false

        override def reset(): Unit = {}
      },
    )
  )

  protected def subscribeToTemplates: Seq[javaapi.data.Identifier] = Seq(
    M.triparty.bootstrap.Bootstrap.TEMPLATE_ID,
    M.triparty.bootstrap.PreBootstrap.TEMPLATE_ID,
    M.triparty.bootstrap.BootstrapUser.TEMPLATE_ID,
  )

  override def start(): Future[Either[String, Unit]] =
    (for {
      _ <- EitherT(super.start())
      _ <- masterContract.allAvailable.headOption match {
        case None =>
          logger.debug("Adding new test run contract")
          mapCommand(setupTestRun(), "setup test run")
        case Some(_) =>
          logger.debug("Resuming with existing test run contract")
          EitherT.rightT[Future, String](())
      }
    } yield ()).value

  private def setupTestRun(): Future[Boolean] =
    submitCommand(
      baseDescription = "setup-test-run",
      commandJava = new M.orchestration.TestRun(
        M.orchestration.Mode.PREPAREPROVIDER,
        List().asJava,
        List().asJava,
        party.getValue,
        config.runConfig.totalCycles.toLong,
        config.runConfig.reportFrequency.toLong,
        config.runConfig.runType,
      ).create.commands.asScala.toSeq,
      reference = "Test Run Orchestration",
      synchronizerId = Some(baseSynchronizerId),
    ).flatMap { success =>
      if (!success && !isClosing) {
        logger.warn("Test run orchestration initialisation failed. Retrying")
        Threading.sleep(2000)
        setupTestRun()
      } else {
        Future.successful(success)
      }
    }

  private def updateTestRunConfigIfNecessary(): Unit =
    (for {
      newConfigChecker <- amendedConfig.get().headOption
      master <- masterContract.one(()).map(_._2)
      stats <- currentStatus.get().collect { case x: MasterStatus => x }
      updated <- newConfigChecker.check(stats, master.data)
    } yield {
      if (updated != MasterDynamicConfig.fromContract(master.data)) {
        logger.info(s"Adjusting test run config to $updated")
        val amendMaster = master.id
          .exerciseToggleRunType(
            updated.runType,
            updated.totalCycles.toLong,
            updated.reportFrequency.toLong,
          )
          .commands
          .asScala
          .toSeq
        val submissionF = submitCommand(
          "amend-master",
          amendMaster,
          s"Amending master run-type",
          synchronizerId = Some(baseSynchronizerId),
        )
        setPending(masterContract, master.id, submissionF)
      } else {
        logger.info("Dropping amender as we are on it")
        // dropping adjuster now that we are set and have already adjusted the master contract
        amendedConfig.updateAndGet(
          _.drop(1)
        )
      }
    }).discard

  override def flush(): Boolean = {
    if (!prepareBootstrap()) ()
    else {
      // process participation requests
      masterContract.one(()).foreach { case (_, master) =>
        flushParticipantRequests(master)
      }
    }
    updateTestRunConfigIfNecessary()
    if (statsUpdateNeeded.getAndSet(false)) {
      updateStatus()
    }
    // adapt state based on progress
    adaptTestRunState()
    false
  }

  protected def prepareBootstrap(): Boolean = true

  protected def flushParticipantRequests(master: M.orchestration.TestRun.Contract): Unit = if (
    requests.allAvailable.nonEmpty
  ) {
    val allRequests = requests.allAvailable.take(config.batchSize).toList
    val acceptRegistration =
      master.id.exerciseBulkAddParticipant(allRequests.map(_.id).asJava).commands.asScala.toSeq
    logger.info(
      s"Accepting participation requests of\n  ${allRequests.map(_.data.party).mkString("\n  ")}"
    )
    val submissionF = submitCommand(
      "accept-participation",
      acceptRegistration,
      s"registering participants",
      synchronizerId = Some(baseSynchronizerId),
    )
    setPending(masterContract, master.id, submissionF)
    allRequests.foreach(request => setPending(requests, request.id, submissionF))
  }

  private def cleanupTestProbes(cleanup: List[M.orchestration.TestProbe.Contract]): Unit =
    cleanup.foreach { probe =>
      val command = probe.id
        .exerciseAcknowledge()
        .commands
        .asScala
        .toSeq
      val submissionF = submitCommand(
        "acknowledge-test-probe",
        command,
        s"Acknowledging test probe from ${probe.data.party} of type ${probe.data.typ} at ${probe.data.count}",
      )
      setPending(testProbes, probe.id, submissionF)
    }

  private def calculateTotals(): Option[(Long, Long)] =
    masterContract.one(()).map { case (_, master) =>
      val stats: Seq[(String, ProbeType, Long)] =
        master.data.users.asScala.toSeq.flatMap { user =>
          Seq(M.orchestration.ProbeType.ACCEPTANCE, M.orchestration.ProbeType.PROPOSAL).map { typ =>
            // reverse sort by epoch milli (Ordering.by(_.data.timestamp).reverse fails due to diverging implicit expansion
            testProbes.find((new Party(user), typ)).map(_._2).sortBy(-_.data.count) match {
              case last :: rest =>
                cleanupTestProbes(rest)
                val lastCount = last.data.count
                (user, typ, lastCount)
              case _ => (user, typ, 0L)
            }
          }
        }
      val (accTot, proTot) = stats.foldLeft((0L, 0L)) {
        case ((acc, pro), (_, M.orchestration.ProbeType.ACCEPTANCE, count)) =>
          (acc + count, pro)
        case ((acc, pro), (_, M.orchestration.ProbeType.PROPOSAL, count)) =>
          (acc, pro + count)
      }

      (accTot, proTot)
    }

  @tailrec
  private def chopHistory(now: Instant): Unit = if (currentStats.sizeIs > 1) {
    currentStats.headOption match {
      case Some((lastUpdate, _, _))
          if java.time.Duration.between(lastUpdate, now).toMillis > config.averageTpsOverMs =>
        currentStats.removeHead().discard
        chopHistory(now)
      case _ => ()
    }
  }

  private def updateStatus(): Unit =
    calculateTotals().foreach { case (accTot, proTot) =>
      val now = Instant.now
      chopHistory(now)
      val (accPs, proPs) = currentStats.headOption match {
        case Some((lastUpdate, lastAcc, lastPro)) =>
          val duration = java.time.Duration.between(lastUpdate, now).toMillis.toDouble / 1000.0
          val accPs = if (duration > 0) (accTot - lastAcc) / duration else 0d
          val proPs = if (duration > 0) (proTot - lastPro) / duration else 0d
          (accPs, proPs)
        case None => (0.0, 0.0)
      }
      currentStats.append((now, accTot, proTot))
      masterContract.one(()).map { case (_, master) =>
        val states = testParticipants.all.foldLeft(Map.empty[(String, String), Int]) {
          case (acc, (_, _, testParticipant)) =>
            val key = (testParticipant.data.role.toString, testParticipant.data.flag.toString)
            acc.updatedWith(key) {
              case Some(x) => Some(x + 1)
              case None => Some(1)
            }
        }
        val newStatus = MasterStatus(
          timestamp = Instant.now,
          mode = master.data.mode.toString,
          proposalsPerSecond = proPs,
          approvalsPerSecond = accPs,
          totalProposals = proTot,
          totalApprovals = accTot,
          participants = states,
        )
        logger.info(newStatus.toString)
        currentStatus.set(Some(newStatus))
      }
    }

  private def adaptTestRunState(): Unit =
    // only run if master contract is not pending
    masterContract.one(()).foreach { _ =>
      // if everyone is done, disable
      if (runCompleted()) {
        adjustState(M.orchestration.Mode.DONE)
      }
      // if trader quorum is reached and enough are ready, start
      else if (tradersAreReady()) {
        adjustState(M.orchestration.Mode.THROUGHPUT)
      }
      // if issuer quorum is reached, prepare
      else if (issuersAreReady()) {
        adjustState(M.orchestration.Mode.PREPAREUSER)
      }
    }

  private def runCompleted(): Boolean = {
    val (notDone, doneIssuer, doneTrader) = testParticipants.allAvailable.foldLeft((0, 0, 0)) {
      case ((notDone, doneIssuer, doneTrader), tp) =>
        if (tp.data.flag == M.orchestration.ParticipantFlag.FINISHED)
          if (tp.data.role == Role.USER)
            (notDone, doneIssuer, doneTrader + 1)
          else
            (notDone, doneIssuer + 1, doneTrader)
        else
          (notDone + 1, doneIssuer, doneTrader)
    }
    notDone == 0 && doneIssuer >= config.quorumProviders && doneTrader >= config.quorumUsers
  }

  private def tradersAreReady(): Boolean = {
    val count = testParticipants.allAvailable
      .count(x =>
        x.data.role == Role.USER && x.data.flag != M.orchestration.ParticipantFlag.INITIALISING
      )
    count >= config.quorumUsers
  }

  private def issuersAreReady(): Boolean = {
    val count = testParticipants.allAvailable
      .count(x =>
        x.data.role == Role.PROVIDER && x.data.flag != M.orchestration.ParticipantFlag.INITIALISING
      )
    count >= config.quorumProviders
  }

  private def adjustState(mode: M.orchestration.Mode): Unit =
    masterContract.one(()).foreach { case (_, master) =>
      if (master.data.mode != mode && master.data.mode != M.orchestration.Mode.DONE) {
        logger.info(s"Change mode from ${master.data.mode} to $mode")
        val command = master.id
          .exerciseToggleMode(mode)
          .commands
          .asScala
          .toSeq
        val submissionF =
          submitCommand(
            "toggle-mode",
            command,
            s"Changing mode ${master.data.mode} => $mode",
            synchronizerId = Some(baseSynchronizerId),
          )
        setPending(masterContract, master.id, submissionF)
      }
      if (master.data.mode == M.orchestration.Mode.DONE) {
        finished()
      }
    }

}
