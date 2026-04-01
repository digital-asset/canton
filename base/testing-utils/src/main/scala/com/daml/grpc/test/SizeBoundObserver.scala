// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.test

import io.grpc.Context.CancellableContext
import io.grpc.stub.StreamObserver

private[test] final class SizeBoundObserver[A](
    sizeCap: Int,
    delegate: StreamObserver[A],
    cancellableContext: CancellableContext,
) extends StreamObserver[A] {

  if (sizeCap < 0) {
    throw new IllegalArgumentException(
      s"sizeCap = $sizeCap. The size of the stream cannot be less than 0."
    )
  } else if (sizeCap == 0) {
    delegate.onCompleted()
    val _ = cancellableContext.cancel(null)
  }

  private var counter = 0

  override def onNext(value: A): Unit = synchronized {
    delegate.onNext(value)
    counter += 1
    if (counter == sizeCap) {
      delegate.onCompleted()
      val _ = cancellableContext.cancel(null)
    }
  }

  override def onError(t: Throwable): Unit = synchronized {
    delegate.onError(t)
    val _ = cancellableContext.cancel(null)
  }

  override def onCompleted(): Unit = synchronized {
    delegate.onCompleted()
    val _ = cancellableContext.cancel(null)
  }
}
