// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.util.PriorityBlockingQueueUtil

import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.locks.ReentrantLock
import scala.annotation.tailrec
import scala.concurrent.{Future, Promise}

trait TimeAwaiter {

  protected def currentKnownTime: CantonTimestamp

  protected def awaitKnownTimestamp(timestamp: CantonTimestamp): Option[Future[Unit]] = {
    val current = currentKnownTime
    if (current >= timestamp) None
    else {
      val promise = Promise[Unit]()
      awaitTimestampFutures.offer(timestamp -> promise)
      // If the timestamp has been advanced while we're inserting into the priority queue,
      // make sure that we're completing the future.
      val newCurrent = currentKnownTime
      if (newCurrent >= timestamp) notifyAwaitedFutures(newCurrent)
      Some(promise.future)
    }
  }

  private val awaitTimestampFutures: PriorityBlockingQueue[(CantonTimestamp, Promise[Unit])] =
    new PriorityBlockingQueue[(CantonTimestamp, Promise[Unit])](
      PriorityBlockingQueueUtil.DefaultInitialCapacity,
      Ordering.by[(CantonTimestamp, Promise[Unit]), CantonTimestamp](_._1),
    )
  private val awaitTimestampFuturesLock: ReentrantLock = new ReentrantLock()

  protected def notifyAwaitedFutures(upToInclusive: CantonTimestamp): Unit = {
    @tailrec def go(): Unit = Option(awaitTimestampFutures.peek()) match {
      case Some((timestamp, _)) if timestamp <= upToInclusive =>
        // run success on the current head as there might have been a racy insert into the priority queue between the peek and the take
        // this is fine, as any new promise will have a timestamp <= as the one we just checked
        val (_, currentHead) = awaitTimestampFutures.take()
        currentHead.success(())
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
