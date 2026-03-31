// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.test

import com.daml.timer.Delayed
import io.grpc.Context.CancellableContext
import io.grpc.stub.StreamObserver

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

final class TimeBoundObserver[A](
    duration: FiniteDuration,
    delegate: StreamObserver[A],
    cancellableContext: CancellableContext,
)(implicit executionContext: ExecutionContext)
    extends StreamObserver[A] {

  private var done = false

  Delayed.by(duration)({
    synchronized {
      if (!done) {
        done = true
        delegate.onCompleted()
        cancellableContext.cancel(null)
      }
    }
  })

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
