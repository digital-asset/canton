// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.FutureSimulator.{
  FutureSimulatorState,
  RunningFuture,
  WaitingSet,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future.FutureAllocator.WrappedRunningFuture
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.{
  Agenda,
  FutureSettings,
  RunFuture,
}

import scala.util.Random

class FutureSimulatorAllocator(
    state: FutureSimulatorState,
    settings: FutureSettings,
    agenda: Agenda,
    random: Random,
    nodeId: BftNodeId,
) extends FutureAllocator {

  override def newFuture[T](
      future: SimulationFuture[T],
      howFastToRun: FutureAllocator.HowFastToRun,
  )(
      getDependencies: () => Set[WrappedRunningFuture]
  ): RunningFuture[T] = {
    val myId = state.allocateFutureId()
    val waitingFor = getDependencies()
    val runningFuture = RunningFuture(
      myId,
      future,
      WaitingSet(waitingFor.map(_.runningFuture.futureId)),
      howFastToRun,
      None,
      None,
    )
    if (waitingFor.isEmpty) {
      agenda.addOne(
        RunFuture(nodeId, runningFuture),
        howFastToRun.generateDuration(settings, random),
      )
    }
    waitingFor.foreach { dependency =>
      dependency.runningFuture.addParent(runningFuture)
    }
    runningFuture
  }
}
