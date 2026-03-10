// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future

import cats.Traverse
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.PureFun
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.FutureSimulator.RunningFuture
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future.FutureAllocator.{
  HowFastToRun,
  WrappedRunningFuture,
}

import scala.util.Try

sealed trait SimulationFuture[T] {
  def resolveValue(): Try[T]

  def schedule(allocator: FutureAllocator): RunningFuture[T]
  def debugName: String
}

object SimulationFuture {
  final class Pure[T](name: => String, getValue: () => Try[T]) extends SimulationFuture[T] {
    private lazy val resolvedValue: Try[T] = getValue()

    override def resolveValue(): Try[T] = resolvedValue

    override def schedule(
        allocator: FutureAllocator
    ): RunningFuture[T] = allocator.newFuture(this, HowFastToRun.Normal) { () =>
      Set.empty
    }

    override def debugName: String = name
  }

  final case class Zip[X, Y](fut1: SimulationFuture[X], fut2: SimulationFuture[Y])
      extends SimulationFuture[(X, Y)] {
    override def resolveValue(): Try[(X, Y)] =
      fut1.resolveValue().flatMap(x => fut2.resolveValue().map(y => (x, y)))

    override def schedule(
        allocator: FutureAllocator
    ): RunningFuture[(X, Y)] = allocator.newFuture(this, HowFastToRun.Trivial) { () =>
      val runningFuture1 = fut1.schedule(allocator)
      val runningFuture2 = fut2.schedule(allocator)
      Set(WrappedRunningFuture(runningFuture1), WrappedRunningFuture(runningFuture2))
    }

    override def debugName: String = s"zip(${fut1.debugName}, ${fut2.debugName})"
  }

  final case class Zip3[X, Y, Z](
      fut1: SimulationFuture[X],
      fut2: SimulationFuture[Y],
      fut3: SimulationFuture[Z],
  ) extends SimulationFuture[(X, Y, Z)] {
    override def resolveValue(): Try[(X, Y, Z)] =
      for {
        f1 <- fut1.resolveValue()
        f2 <- fut2.resolveValue()
        f3 <- fut3.resolveValue()
      } yield (f1, f2, f3)

    override def schedule(
        allocator: FutureAllocator
    ): RunningFuture[(X, Y, Z)] = allocator.newFuture(this, HowFastToRun.Trivial) { () =>
      val runningFuture1 = fut1.schedule(allocator)
      val runningFuture2 = fut2.schedule(allocator)
      val runningFuture3 = fut3.schedule(allocator)
      Set(
        WrappedRunningFuture(runningFuture1),
        WrappedRunningFuture(runningFuture2),
        WrappedRunningFuture(runningFuture3),
      )
    }

    override def debugName: String =
      s"zip3(${fut1.debugName}, ${fut2.debugName}, ${fut3.debugName})"
  }

  final case class Sequence[A, F[_]](in: F[SimulationFuture[A]])(implicit ev: Traverse[F])
      extends SimulationFuture[F[A]] {
    override def resolveValue(): Try[F[A]] = ev.sequence(ev.map(in)(_.resolveValue()))

    override def schedule(
        allocator: FutureAllocator
    ): RunningFuture[F[A]] = allocator.newFuture(this, HowFastToRun.Trivial) { () =>
      ev.toList(in).map(future => WrappedRunningFuture(future.schedule(allocator))).toSet
    }

    override def debugName: String = s"sequence(${ev.toList(in).map(_.debugName)})"
  }

  final case class Map[X, Y](future: SimulationFuture[X], fun: X => Y) extends SimulationFuture[Y] {
    private lazy val resolvedValue: Try[Y] = future.resolveValue().map(fun)
    override def resolveValue(): Try[Y] = resolvedValue

    override def schedule(
        allocator: FutureAllocator
    ): RunningFuture[Y] = allocator.newFuture(this, HowFastToRun.Trivial) { () =>
      val runningFuture = future.schedule(allocator)
      Set(WrappedRunningFuture(runningFuture))
    }

    override def debugName: String = s"map(${future.debugName})"
  }

  final case class FlatMap[R1, R2](
      fut1: SimulationFuture[R1],
      fut2: PureFun[R1, SimulationFuture[R2]],
  ) extends SimulationFuture[R2] {
    override def resolveValue(): Try[R2] =
      fut1.resolveValue().map(fut2).flatMap(_.resolveValue())

    // TODO(#23754): support finer-grained simulation of `FlatMap` futures
    override def schedule(
        allocator: FutureAllocator
    ): RunningFuture[R2] = allocator.newFuture(this, HowFastToRun.Trivial) { () =>
      val runningFuture = fut1.schedule(allocator)
      Set(WrappedRunningFuture(runningFuture))
    }

    override def debugName: String = s"flatMap(${fut1.debugName},...)"
  }

  def apply[T](name: => String)(resolveValue: () => Try[T]): SimulationFuture[T] =
    new Pure(name, resolveValue)
}
