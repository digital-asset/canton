// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.api.MetricHandle.Timer.TimerHandle
import com.daml.metrics.api.MetricHandle.{Counter, Timer}
import com.daml.metrics.api.MetricsContext
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.{BoundedSourceQueue, Materializer, QueueOfferResult}

object InstrumentedGraph {

  final class InstrumentedBoundedSourceQueue[T](
      delegate: BoundedSourceQueue[(TimerHandle, T)],
      bufferSize: Int,
      capacityCounter: Counter,
      lengthCounter: Counter,
      delayTimer: Timer,
  ) extends BoundedSourceQueue[T] {

    override def isCompleted: Boolean = delegate.isCompleted

    override def complete(): Unit = {
      delegate.complete()
      capacityCounter.dec(bufferSize.toLong)(MetricsContext.Empty)
    }

    override def size(): Int = bufferSize

    override def fail(ex: Throwable): Unit = delegate.fail(ex)

    override def offer(elem: T): QueueOfferResult = {
      val result = delegate.offer(
        delayTimer.startAsync() -> elem
      )
      result match {
        case QueueOfferResult.Enqueued =>
          lengthCounter.inc()

        case _ => // do nothing
      }
      result
    }
  }

  /** Returns a `Source` that can be fed via the materialized queue.
    *
    * The queue length counter can at most be eventually consistent due to the counter increment and
    * decrement operation being scheduled separately and possibly not in the same order as the
    * actual enqueuing and dequeueing of items.
    *
    * For this reason, you may also read values on the saturation counter which are negative or
    * exceed `bufferSize`.
    *
    * Note that the fact that the count is decremented in a second operator means that its buffering
    * will likely skew the measurements to be greater than the actual value, rather than the other
    * way around.
    *
    * We track the queue capacity as a counter as we may want to aggregate the metrics for multiple
    * individual queues of the same kind and we want to be able to decrease the capacity when the
    * queue gets completed.
    */
  def queue[T](
      bufferSize: Int,
      capacityCounter: Counter,
      lengthCounter: Counter,
      delayTimer: Timer,
  )(implicit
      materializer: Materializer
  ): Source[T, BoundedSourceQueue[T]] = {
    val (boundedQueue, source) =
      Source.queue[(TimerHandle, T)](bufferSize).preMaterialize()

    val instrumentedQueue =
      new InstrumentedBoundedSourceQueue[T](
        boundedQueue,
        bufferSize,
        capacityCounter,
        lengthCounter,
        delayTimer,
      )
    capacityCounter.inc(bufferSize.toLong)(MetricsContext.Empty)

    source.mapMaterializedValue(_ => instrumentedQueue).map { case (timer, item) =>
      timer.stop()
      lengthCounter.dec()
      item
    }
  }
}
