// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.util.PriorityBlockingQueueUtil

import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.locks.ReentrantLock
import scala.annotation.tailrec
import scala.concurrent.{Future, Promise}
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.logging.NamedLogging
import scala.jdk.CollectionConverters._

/** Utility to implement a time awaiter
  *
  * Note, you need to invoke expireOnShutdown onClosed
  */
trait TimeAwaiter {

  this: FlagCloseable with NamedLogging =>

  private abstract class Awaiting[T] {
    val promise: Promise[T] = Promise[T]()
    def shutdown(): Boolean
    def success(): Unit
  }
  private class General extends Awaiting[Unit] {
    override def shutdown(): Boolean = false
    override def success(): Unit = promise.success(())
  }
  private class ShutdownAware extends Awaiting[UnlessShutdown[Unit]] {
    override def shutdown(): Boolean = {
      promise.trySuccess(UnlessShutdown.AbortedDueToShutdown)
      true
    }
    override def success(): Unit = promise.trySuccess(UnlessShutdown.unit).discard
  }

  protected def expireTimeAwaiter(): Unit =
    awaitTimestampFutures.iterator().asScala.foreach(_._2.shutdown())

  protected def currentKnownTime: CantonTimestamp

  protected def awaitKnownTimestamp(timestamp: CantonTimestamp): Option[Future[Unit]] = {
    awaitKnownTimestampGen(timestamp, new General()).map(_.promise.future)
  }

  protected def awaitKnownTimestampUS(
      timestamp: CantonTimestamp
  ): Option[FutureUnlessShutdown[Unit]] = if (isClosing)
    Some(FutureUnlessShutdown.abortedDueToShutdown)
  else {
    awaitKnownTimestampGen(timestamp, new ShutdownAware()).map(x =>
      FutureUnlessShutdown(x.promise.future)
    )
  }

  private def awaitKnownTimestampGen[T](
      timestamp: CantonTimestamp,
      create: => Awaiting[T],
  ): Option[Awaiting[T]] = {
    val current = currentKnownTime
    if (current >= timestamp) None
    else {
      val awaiter = create
      awaitTimestampFutures.offer(timestamp -> awaiter)
      // If the timestamp has been advanced while we're inserting into the priority queue,
      // make sure that we're completing the future.
      val newCurrent = currentKnownTime
      if (newCurrent >= timestamp) notifyAwaitedFutures(newCurrent)
      Some(awaiter)
    }
  }

  private val awaitTimestampFutures: PriorityBlockingQueue[(CantonTimestamp, Awaiting[_])] =
    new PriorityBlockingQueue[(CantonTimestamp, Awaiting[_])](
      PriorityBlockingQueueUtil.DefaultInitialCapacity,
      Ordering.by[(CantonTimestamp, Awaiting[_]), CantonTimestamp](_._1),
    )
  private val awaitTimestampFuturesLock: ReentrantLock = new ReentrantLock()

  protected def notifyAwaitedFutures(upToInclusive: CantonTimestamp): Unit = {
    @tailrec def go(): Unit = Option(awaitTimestampFutures.peek()) match {
      case Some((timestamp, _)) if timestamp <= upToInclusive =>
        // run success on the current head as there might have been a racy insert into the priority queue between the peek and the take
        // this is fine, as any new promise will have a timestamp <= as the one we just checked
        val (_, currentHead) = awaitTimestampFutures.take()
        currentHead.success()
        go()
      case _ =>
    }
    try {
      awaitTimestampFuturesLock.lock()
      go()
    } finally {
      awaitTimestampFuturesLock.unlock()
    }
  }

}
