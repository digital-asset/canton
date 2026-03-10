// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.FutureSettings
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.FutureSimulator.RunningFuture
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future.FutureAllocator.WrappedRunningFuture

import scala.concurrent.duration.FiniteDuration
import scala.util.Random

trait FutureAllocator {

  /** Make a new Future that is scheduled to run
    * @param future
    *   the future to run
    * @param howFastToRun
    *   config for how long time to schedule when dependencies are done
    * @param getDependencies
    *   the dependencies this future have (i.e. futures that need to complete before this can run)
    * @return
    *   The [[RunningFuture]] that represents this future running
    */
  def newFuture[T](
      future: SimulationFuture[T],
      howFastToRun: FutureAllocator.HowFastToRun,
  )(
      getDependencies: () => Set[WrappedRunningFuture]
  ): RunningFuture[T]
}

object FutureAllocator {
  final case class WrappedRunningFuture(runningFuture: RunningFuture[?])

  sealed trait HowFastToRun extends Product with Serializable {
    def generateDuration(settings: FutureSettings, random: Random): FiniteDuration
  }
  object HowFastToRun {
    case object Trivial extends HowFastToRun {
      override def generateDuration(settings: FutureSettings, random: Random): FiniteDuration =
        settings.trivialFutureTimeDistribution.generateRandomDuration(random)
    }
    case object Normal extends HowFastToRun {
      override def generateDuration(settings: FutureSettings, random: Random): FiniteDuration =
        settings.futureTimeDistribution.generateRandomDuration(random)
    }
  }
}
