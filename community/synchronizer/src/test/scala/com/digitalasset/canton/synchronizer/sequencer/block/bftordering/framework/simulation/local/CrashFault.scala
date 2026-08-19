// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.local

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.TraceContextGenerator
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.local.CrashFault.GlobalCrashStatus.{
  TickResult,
  WaitUntil,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.local.CrashFault.{
  CrashNodeFaultStatus,
  GlobalCrashStatus,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.{
  Agenda,
  CrashFaultSettings,
  CrashNode,
  RestartNode,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Random

class CrashFault(
    settings: CrashFaultSettings,
    nodes: Set[BftNodeId],
    agenda: Agenda,
    traceContextGenerator: TraceContextGenerator,
    random: Random,
) {
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var havePermanentlyCrashedNodes = false

  private val crashNodeStatus: mutable.Map[BftNodeId, CrashNodeFaultStatus] =
    mutable.Map.from(
      nodes.map(node => node -> CrashNodeFaultStatus.Uninitialized)
    )

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var globalRestartFault: GlobalCrashStatus = if (settings.globalRestartEnabled) {
    GlobalCrashStatus.Uninitialized
  } else {
    GlobalCrashStatus.NoGlobalCrashes
  }

  def tick(at: CantonTimestamp): Unit = {
    if (!havePermanentlyCrashedNodes) {
      settings.permanentlyCrashNodes.foreach { numberOfNodesToPermanentlyCrash =>
        val nodesToCrashPermanently = random.shuffle(nodes).take(numberOfNodesToPermanentlyCrash)
        nodesToCrashPermanently.foreach { node =>
          crashNodeStatus(node) = CrashNodeFaultStatus.Permanent
          agenda.addOne(
            CrashNode(node, permanent = true, traceContextGenerator.newTraceContext),
            duration = 1.microsecond,
          )
        }
      }
      havePermanentlyCrashedNodes = true
    }
    crashNodeStatus.mapValuesInPlace { case (node, status) =>
      if (status.shouldUpdate(at)) {
        val gracePeriod =
          at.add(settings.crashRestartGracePeriod.generateRandomDuration(random))
        if (settings.crashRestartChance.flipCoin(random)) {
          crashNode(node, None)
        } else {
          CrashNodeFaultStatus.Initialized(gracePeriod)
        }
      } else {
        status
      }
    }
    val globalTickResult = globalRestartFault.tick(settings, at, random)
    globalRestartFault = globalTickResult.newState
    if (globalTickResult.shouldPerformGlobalRestart) {
      crashNodeStatus.mapValuesInPlace { case (node, status) =>
        status match {
          case CrashNodeFaultStatus.Uninitialized => status
          case CrashNodeFaultStatus.Initialized(_) =>
            crashNode(node, Some(1.microsecond))
          case CrashNodeFaultStatus.Crashed(_) => status
          case CrashNodeFaultStatus.Permanent => status
        }
      }
    }
  }

  private def crashNode(
      node: BftNodeId,
      durationUntilRestart: Option[FiniteDuration],
  ): CrashNodeFaultStatus = {
    val traceContext = traceContextGenerator.newTraceContext
    agenda.addOne(
      CrashNode(node, permanent = false, traceContext),
      duration = 1.microsecond,
    )
    agenda.addOne(
      RestartNode(node, traceContext),
      duration = durationUntilRestart.getOrElse(
        settings.crashTimeDistribution.generateRandomDuration(random)
      ),
    )
    CrashNodeFaultStatus.Crashed(traceContext)
  }

  def heal(): Unit =
    crashNodeStatus.foreach { case (node, status) =>
      status match {
        case CrashNodeFaultStatus.Uninitialized => ()
        case CrashNodeFaultStatus.Initialized(_) => ()
        case CrashNodeFaultStatus.Crashed(traceContext) =>
          agenda.addOne(RestartNode(node, traceContext), 1.microsecond)
        case CrashNodeFaultStatus.Permanent => ()
      }
    }

  def restartingNode(node: BftNodeId, at: CantonTimestamp): Unit =
    crashNodeStatus.updateWith(node) {
      case Some(value) =>
        value match {
          case CrashNodeFaultStatus.Uninitialized => Some(value)
          case CrashNodeFaultStatus.Initialized(_) =>
            Some(value)
          case CrashNodeFaultStatus.Crashed(_) =>
            Some(
              CrashNodeFaultStatus.Initialized(
                at.add(settings.crashRestartGracePeriod.generateRandomDuration(random))
              )
            )
          case CrashNodeFaultStatus.Permanent =>
            throw new Exception(s"Should not restart node $node that is permanently crashed")
        }
      case None => throw new Exception(s"Should not restart node $node which is unknown")
    }
}

object CrashFault {

  sealed trait GlobalCrashStatus {
    def tick(settings: CrashFaultSettings, at: CantonTimestamp, random: Random): TickResult

    protected def rollAndWait(
        settings: CrashFaultSettings,
        at: CantonTimestamp,
        random: Random,
    ): TickResult = TickResult(
      settings.globalRestartChance.flipCoin(random),
      WaitUntil(at.add(settings.globalRestartTimeDistribution.generateRandomDuration(random))),
    )
  }

  object GlobalCrashStatus {
    final case class TickResult(shouldPerformGlobalRestart: Boolean, newState: GlobalCrashStatus)

    case object NoGlobalCrashes extends GlobalCrashStatus {
      override def tick(
          settings: CrashFaultSettings,
          at: CantonTimestamp,
          random: Random,
      ): TickResult = TickResult(shouldPerformGlobalRestart = false, this)
    }

    case object Uninitialized extends GlobalCrashStatus {
      override def tick(
          settings: CrashFaultSettings,
          at: CantonTimestamp,
          random: Random,
      ): TickResult =
        rollAndWait(settings, at, random)
    }

    final case class WaitUntil(timeToTryGlobalRestart: CantonTimestamp) extends GlobalCrashStatus {
      override def tick(
          settings: CrashFaultSettings,
          at: CantonTimestamp,
          random: Random,
      ): TickResult =
        if (at.isAfter(timeToTryGlobalRestart)) {
          rollAndWait(settings, at, random)
        } else {
          TickResult(shouldPerformGlobalRestart = false, this)
        }
    }
  }

  sealed trait CrashNodeFaultStatus {
    def shouldUpdate(at: CantonTimestamp): Boolean
  }

  object CrashNodeFaultStatus {
    object Uninitialized extends CrashNodeFaultStatus {
      override def shouldUpdate(at: CantonTimestamp): Boolean = true
    }

    final case class Initialized(dontUpdateUntil: CantonTimestamp) extends CrashNodeFaultStatus {
      override def shouldUpdate(at: CantonTimestamp): Boolean = dontUpdateUntil.isBefore(at)
    }

    final case class Crashed(traceContext: TraceContext) extends CrashNodeFaultStatus {
      override def shouldUpdate(at: CantonTimestamp): Boolean = false
    }

    object Permanent extends CrashNodeFaultStatus {
      override def shouldUpdate(at: CantonTimestamp): Boolean = false
    }
  }
}
