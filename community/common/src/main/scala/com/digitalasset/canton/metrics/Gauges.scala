// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import cats.data.{EitherT, OptionT}
import com.codahale.metrics.{Gauge, Timer}
import com.daml.metrics.Timed
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.util.CheckedT
import com.digitalasset.canton.util.Thereafter.syntax.*

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, blocking}

class TimedLoadGauge(interval: FiniteDuration, timer: Timer) extends LoadGauge(interval) {

  override def event[T](fut: => Future[T])(implicit ec: ExecutionContext): Future[T] =
    Timed.future(timer, super.event(fut))

  override def syncEvent[T](body: => T): T =
    Timed.value(timer, super.syncEvent(body))

  def checkedTEvent[A, N, R](checked: => CheckedT[Future, A, N, R])(implicit
      ec: ExecutionContext
  ): CheckedT[Future, A, N, R] =
    CheckedT(event(checked.value))

  def eitherTEvent[A, B](eitherT: => EitherT[Future, A, B])(implicit
      ec: ExecutionContext
  ): EitherT[Future, A, B] =
    EitherT(event(eitherT.value))

  def eitherTEventUnlessShutdown[A, B](eitherT: => EitherT[FutureUnlessShutdown, A, B])(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, A, B] =
    EitherT(FutureUnlessShutdown(event(eitherT.value.unwrap)))

  def optionTEvent[A](optionT: => OptionT[Future, A])(implicit
      ec: ExecutionContext
  ): OptionT[Future, A] =
    OptionT(event(optionT.value))
}

class LoadGauge(interval: FiniteDuration, now: => Long = System.nanoTime) extends Gauge[Double] {

  private val intervalNanos = interval.toNanos
  private val measure = mutable.ListBuffer[(Boolean, Long)]()
  private val eventCount = new AtomicInteger(1)

  record(false)

  private def record(loaded: Boolean): Unit = blocking(synchronized {
    val count = if (loaded) {
      eventCount.getAndIncrement()
    } else {
      eventCount.decrementAndGet()
    }
    if (count == 0) {
      measure.append((loaded, now))
    }
    cleanup(now)
  })

  def event[T](fut: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    record(true)
    fut.thereafter { _ =>
      record(false)
    }
  }

  def syncEvent[T](body: => T): T = {
    record(true)
    try {
      body
    } finally {
      record(false)
    }
  }

  @tailrec
  private def cleanup(tm: Long): Unit = {
    // keep on cleaning up
    if (measure.lengthCompare(1) > 0 && measure(1)._2 <= tm - intervalNanos) {
      measure.remove(0).discard
      cleanup(tm)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.IterableOps"))
  override def getValue: Double = {
    val endTs = now
    val startTs = endTs - intervalNanos

    def computeLoad(lastLoaded: Boolean, lastTs: Long, ts: Long): Long =
      if (lastLoaded) ts - lastTs else 0

    blocking(synchronized {
      cleanup(endTs)
      // We know that t0 <= t1 <= ... <= tn <= endTs and
      // startTs < t1, if t1 exists.

      val (loaded0, t0) = measure.head

      var lastLoaded = loaded0
      var lastTs = Math.max(t0, startTs)
      var load = 0L

      for ((loaded, ts) <- measure.tail) {
        load += computeLoad(lastLoaded, lastTs, ts)

        lastLoaded = loaded
        lastTs = ts
      }

      load += computeLoad(lastLoaded, lastTs, endTs)

      load.toDouble / intervalNanos
    })
  }
}

class RefGauge[T](empty: T) extends Gauge[T] {
  private val ref = new AtomicReference[Option[() => T]]()
  def setReference(inspect: Option[() => T]): Unit = {
    ref.set(inspect)
  }
  override def getValue: T = ref.get().map(_()).getOrElse(empty)
}

class IntGauge(initial: Integer) extends Gauge[Integer] {
  private val ref = new AtomicInteger(initial)

  def incrementAndGet(): Integer = ref.incrementAndGet()
  def increment(): Unit = incrementAndGet().discard
  def decrementAndGet(): Integer = ref.decrementAndGet()
  def decrement(): Unit = decrementAndGet().discard
  def setValue(value: Int): Unit = ref.set(value)

  override def getValue: Integer = ref.get()
}
