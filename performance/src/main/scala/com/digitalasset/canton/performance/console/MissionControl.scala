// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.console

import com.daml.ledger.javaapi.data.Party
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.MetricsFactoryProvider
import com.digitalasset.canton.performance.PartyRole.*
import com.digitalasset.canton.performance.elements.{AmendMasterConfig, MasterDriver}
import com.digitalasset.canton.performance.model.java.orchestration.runtype
import com.digitalasset.canton.performance.{
  Connectivity,
  PartyRole,
  PerformanceRunner,
  PerformanceRunnerConfig,
  RateSettings,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.HasFlushFuture

import java.time.{Duration as JDuration, Instant}
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.control.NonFatal

trait RunEvent {
  def name: String

  /** whether the load should be turned off prior to running the event */
  def turnOffLoad: Boolean

  /** execute the event */
  def run(): Unit

  /** how long to wait before we invoked this event */
  def interval: FiniteDuration = 180.seconds

  /** whether to skip the event in particular situations (avoids turning off load unnecessarily) */
  def skip(): Boolean = false
}

final case class CounterPartyStats(
    party: String,
    counterParty: String,
    role: String,
    pending: Int,
    completed: Int,
) {
  override def toString: String =
    s"(party=$party, counterParty=$counterParty, role=$role, pending=$pending, completed=$completed"
}
object CounterPartyStats {

  def apply(party: String, role: String, record: (Party, Int, Int)): CounterPartyStats =
    CounterPartyStats(party, record._1.getValue, role, record._2, record._3)

}

sealed trait RunTypeConfig {
  def masterName: String
}

object RunTypeConfig {
  final case class DvpRun(masterName: String) extends RunTypeConfig

  def fromMasterRole(tp: Master): RunTypeConfig = (tp.runConfig.runType: @unchecked) match {
    case _: runtype.DvpRun => DvpRun(tp.name)
  }
}

class MissionControl(
    val loggerFactory: NamedLoggerFactory,
    metricsRegistry: MetricsFactoryProvider,
    clock: Clock,
    masterDefinition: Either[Master, RunTypeConfig],
    participants: Seq[Connectivity],
    events: List[RunEvent] = List(),
    issuersPerNode: Int = 1,
    tradersPerNode: Int = 1,
    settings: RateSettings = RateSettings.defaults,
    baseSynchronizerId: SynchronizerId,
    otherSynchronizers: Seq[SynchronizerId],
    otherSynchronizersRatio: Double,
)(implicit ec: ExecutionContextExecutor)
    extends AutoCloseable
    with NoTracing
    with NamedLogging
    with HasFlushFuture {

  private val user = System.getProperty("user.name").replace(" ", "_")
  private val runEvents = new AtomicBoolean(false)

  private def generateRoleSet(config: RunTypeConfig, ii: Int): Set[PartyRole] = config match {
    case RunTypeConfig.DvpRun(_) =>
      ((0 until issuersPerNode).map(jj => DvpIssuer(s"Issuer$ii-$jj-$user", settings = settings)) ++
        (0 until tradersPerNode).map(jj =>
          DvpTrader(s"Trader$ii-$jj-$user", settings = settings)
        )).toSet
  }

  private val (masterConfig, configs): (RunTypeConfig, Seq[(Set[PartyRole], Connectivity)]) =
    masterDefinition match {
      case Left(masterDef) =>
        val config = RunTypeConfig.fromMasterRole(masterDef)
        (
          config,
          participants.sortBy(_.name).zipWithIndex.map { case (participant, idx) =>
            val roles = generateRoleSet(config, idx)
            if (idx == 0)
              (roles + masterDef, participant)
            else
              (roles, participant)
          },
        )
      case Right(master) =>
        (
          master,
          participants.sortBy(_.name).zipWithIndex.map { case (port, idx) =>
            (generateRoleSet(master, idx), port)
          },
        )
    }

  def isDoneF: Future[Unit] = doFlush()

  val runners: Seq[PerformanceRunner] = configs.map { case (roles, participant) =>
    val runner = new PerformanceRunner(
      PerformanceRunnerConfig(
        master = masterConfig.masterName,
        localRoles = roles,
        ledger = participant,
        baseSynchronizerId = baseSynchronizerId,
        otherSynchronizers = otherSynchronizers,
        otherSynchronizersRatio = otherSynchronizersRatio,
      ),
      metricsRegistry,
      loggerFactory.appendUnnamedKey("participant", participant.name),
    )
    addToFlushAndLogError("runner startup")(runner.startup())
    runner
  }

  override def close(): Unit = {
    disable()
    runners.foreach(_.close())
  }

  def stats(): Unit =
    println(runners.map(_.status().map(_.toString).mkString("\n")).mkString("\n"))

  def updatePayloadSize(target: Long): Unit =
    this.master().foreach(_.addConfigAdjustment(new AmendMasterConfig.UpdatePayloadSize(target)))

  def master(): Option[MasterDriver] = runners.find(_.master().nonEmpty).flatMap(_.master())

  def counterparties(): Seq[CounterPartyStats] = {
    import com.digitalasset.canton.performance.elements.DriverStatus.TraderStatus
    runners
      .flatMap(_.status().collect { case traderStatus: TraderStatus =>
        traderStatus.counterParties.map { rr =>
          CounterPartyStats(traderStatus.name, "trader", rr)
        } ++ traderStatus.issuers.map { rr =>
          CounterPartyStats(traderStatus.name, "issuer", rr)
        }
      })
      .flatten
  }

  private def runEvent(index: Int): Unit =
    if (runEvents.get()) {
      val event = events(index)
      if (event.skip()) {
        logger.debug(s"""Skipping event "${event.name}"""")
      } else {
        if (event.turnOffLoad) {
          runners.foreach(_.setActive(false))
        }
        logger.info(s"""Starting event "${event.name}"""")
        val before = Instant.now
        try {
          event.run()
        } catch {
          case NonFatal(t) =>
            logger.error(s"""Event "${event.name}" failed with an exception!""", t)
        }
        val after = Instant.now
        logger.info(
          s"""Finished event "${event.name}" after ${after.toEpochMilli - before.toEpochMilli} ms"""
        )
        if (event.turnOffLoad) {
          runners.foreach(_.setActive(true))
        }
      }
      scheduleEvent((index + 1) % events.length)
    }

  private def scheduleEvent(index: Int): Unit = {
    val event = events(index)
    logger.info(s"""Scheduling event "${event.name}" in ${event.interval}""")
    val _ = clock.scheduleAfter(_ => runEvent(index), JDuration.ofMillis(event.interval.toMillis))
  }

  private def startSchedulingEvents(): Unit =
    if (!runEvents.getAndSet(true))
      events.headOption.foreach(_ => scheduleEvent(0))

  def enable(): Unit =
    runners.foreach(_.setActive(true))

  def disable(): Unit =
    runners.foreach(_.setActive(false))

  def updateSettings(update: RateSettings => RateSettings): Unit =
    runners.foreach(_.updateRateSettings(update))

  def stopSchedulingEvents(): Unit = {
    val _ = runEvents.getAndSet(false)
  }

  startSchedulingEvents()
}
