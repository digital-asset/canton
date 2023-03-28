// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.utils

import com.digitalasset.canton.DiscardOps

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

trait ConcurrencyLimiter {
  def execute[T](task: => Future[T]): Future[T]
}

class QueueBasedConcurrencyLimiter(
    parallelism: Int,
    executionContext: ExecutionContext,
) extends ConcurrencyLimiter {
  assert(parallelism > 0)

  type Task = () => Unit
  private val waiting = mutable.Queue[Task]()
  private var running: Int = 0

  override def execute[T](task: => Future[T]): Future[T] = synchronized {
    val promise = Promise[T]()

    val waitingTask = () => {
      task.andThen { case result =>
        synchronized {
          running = running - 1
          promise.tryComplete(result).discard
          startTasks()
        }
      }(executionContext).discard
    }

    waiting.enqueue(waitingTask)
    startTasks()

    promise.future
  }

  private def startTasks(): Unit = synchronized {
    while (running < parallelism && waiting.nonEmpty) {
      val head = waiting.dequeue()
      running = running + 1
      head()
    }
  }
}
