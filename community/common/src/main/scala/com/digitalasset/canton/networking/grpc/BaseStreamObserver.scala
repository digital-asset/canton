// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.digitalasset.canton.config
import com.digitalasset.canton.discard.Implicits.DiscardOps
import io.grpc.stub.StreamObserver

import scala.concurrent.{Future, Promise}

trait BaseStreamObserver[T, R] extends StreamObserver[T] {
  // A promise to deliver the final Either result to the caller
  protected val promise = Promise[R]()
  def result(): Future[R] = promise.future

  // Logic to process the final state into the result R
  protected def finalizeResult(): R

  override def onError(t: Throwable): Unit =
    promise.tryFailure(t).discard

  override def onCompleted(): Unit =
    promise.trySuccess(finalizeResult()).discard

  def onTimeout(timeout: config.NonNegativeDuration): Either[String, R]
}
