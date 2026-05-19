// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.test

import io.grpc.Context.CancellableContext
import io.grpc.stub.StreamObserver

import scala.concurrent.duration.FiniteDuration

final class TimeBoundObserver[A](
    duration: FiniteDuration,
    delegate: StreamObserver[A],
    cancellableContext: CancellableContext,
) extends StreamObserver[A] {

  private var done = false

  val timer = new java.util.Timer("testing-utils");
  timer.schedule(
    new java.util.TimerTask() {
      def run(): Unit =
        synchronized {
          if (!done) {
            done = true
            delegate.onCompleted()
            val _ = cancellableContext.cancel(null)
            ()
          }
        }
    },
    duration.toMillis,
  )

  override def onNext(value: A): Unit = synchronized {
    if (!done) {
      delegate.onNext(value)
    }
  }

  override def onError(t: Throwable): Unit = synchronized {
    if (!done) {
      delegate.onError(t)
      done = true
      val _ = cancellableContext.cancel(null)
    }
  }

  override def onCompleted(): Unit = synchronized {
    if (!done) {
      delegate.onCompleted()
      done = true
      val _ = cancellableContext.cancel(null)
    }
  }
}
