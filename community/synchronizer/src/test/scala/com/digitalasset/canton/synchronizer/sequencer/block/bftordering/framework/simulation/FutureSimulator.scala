// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleName
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  FutureId,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.FutureSimulator.{
  Continuation,
  RunningFuture,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future.{
  FutureAllocator,
  FutureSimulatorAllocator,
  SimulationFuture,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.mutable
import scala.util.{Random, Try}

class FutureSimulator(
    agenda: Agenda,
    settings: FutureSettings,
    state: FutureSimulator.FutureSimulatorState,
) {

  private val random = new Random(settings.randomSeed)

  def snapshotState: FutureSimulator.FutureSimulatorState =
    state.copy()

  private def wakeUp(
      nodeId: BftNodeId,
      futureToWakeUp: RunningFuture[?],
      futureThatCompleted: FutureId,
  ): Unit = {
    val isWaitingSetEmpty = futureToWakeUp.waitingSet.remove(futureThatCompleted)
    if (isWaitingSetEmpty) {
      agenda.addOne(
        RunFuture(nodeId, futureToWakeUp),
        futureToWakeUp.howFastToRun.generateDuration(settings, random),
      )
    }
  }

  def runFuture[T](nodeId: BftNodeId, runningFuture: RunningFuture[T]): Unit = {
    val futureId = runningFuture.futureId
    runningFuture.future.resolveValue()

    runningFuture.parent.foreach { futureToWakeUp =>
      wakeUp(nodeId, futureToWakeUp, futureId)
    }

    runningFuture.continuation.foreach { continuation =>
      scheduleContinuation(continuation)
    }
  }

  def scheduleFuture[FutureT, MessageT](
      nodeId: BftNodeId,
      to: ModuleName,
      future: SimulationFuture[FutureT],
      futureResultToMessage: Try[FutureT] => Option[MessageT],
      traceContext: TraceContext,
  ): Unit = {
    val allocator = new FutureSimulatorAllocator(state, settings, agenda, random, nodeId)
    val runningFuture = future.schedule(allocator)
    runningFuture.addContinuation(
      Continuation(nodeId, to, runningFuture, futureResultToMessage, traceContext)
    )
  }

  def scheduleContinuation[FutureT, MessageT](
      value: Continuation[FutureT, MessageT]
  ): Unit =
    agenda.addOne(
      RunFutureContinuation(
        value.node,
        value.to,
        value.runningFuture.future.resolveValue(),
        value.fun,
        value.traceContext,
      ),
      settings.trivialFutureTimeDistribution.generateRandomDuration(random),
    )
}

object FutureSimulator {

  final case class RunningFuture[T](
      futureId: FutureId,
      future: SimulationFuture[T],
      waitingSet: WaitingSet,
      howFastToRun: FutureAllocator.HowFastToRun,
      var parent: Option[RunningFuture[?]],
      var continuation: Option[Continuation[T, ?]],
  ) extends PrettyPrinting {
    def addParent(newParent: RunningFuture[?]): Unit = {
      assert(parent.isEmpty)
      parent = Some(newParent)
    }

    def addContinuation(newContinuation: Continuation[T, ?]): Unit = {
      assert(continuation.isEmpty)
      continuation = Some(newContinuation)
    }

    override protected def pretty: Pretty[RunningFuture.this.type] =
      prettyOfClass(
        param("id", _.futureId),
        param("future", _.future.debugName.doubleQuoted),
        param("childrenFinished?", _.waitingSet.isFinished),
        param("parent", _.parent.map(_.futureId)),
        param("haveContinuation", _.continuation.isDefined),
      )
  }

  final case class WaitingSet(initialSet: Set[FutureId]) {
    private val waitingFor = mutable.Set.from(initialSet)

    def remove(futureId: FutureId): Boolean = {
      waitingFor.remove(futureId)
      isFinished
    }

    def isFinished: Boolean = waitingFor.isEmpty
  }

  class FutureSimulatorState private (private var id: FutureId) {
    def copy(): FutureSimulatorState = new FutureSimulatorState(id)

    def allocateFutureId(): FutureId = {
      val newId = id
      id = FutureId(newId + 1)
      newId
    }
  }

  object FutureSimulatorState {
    def create(id: FutureId = FutureId.First): FutureSimulatorState = new FutureSimulatorState(id)
  }

  final case class Continuation[X, T](
      node: BftNodeId,
      to: ModuleName,
      runningFuture: RunningFuture[X],
      fun: Try[X] => Option[T],
      traceContext: TraceContext,
  )
}
