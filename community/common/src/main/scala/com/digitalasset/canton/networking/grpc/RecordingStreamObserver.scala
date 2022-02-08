// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import java.util.concurrent.atomic.AtomicInteger

import io.grpc.stub.StreamObserver

import scala.jdk.CollectionConverters._
import scala.concurrent.{Future, Promise}

/** Stream observer that records all incoming events.
  */
class RecordingStreamObserver[RespT](completeAfter: Int = Int.MaxValue)
    extends StreamObserver[RespT] {

  val responseQueue: java.util.concurrent.BlockingQueue[RespT] =
    new java.util.concurrent.LinkedBlockingDeque[RespT](completeAfter)

  def responses: Seq[RespT] = responseQueue.asScala.toSeq

  private val completionP: Promise[Unit] = Promise[Unit]()
  val completion: Future[Unit] = completionP.future

  val responseCount: AtomicInteger = new AtomicInteger()

  override def onNext(value: RespT): Unit = {
    responseQueue.offer(value)
    if (responseCount.incrementAndGet() >= completeAfter) {
      val _ = completionP.trySuccess(())
    }
  }

  override def onError(t: Throwable): Unit = {
    val _ = completionP.tryFailure(t)
  }

  override def onCompleted(): Unit = {
    val _ = completionP.trySuccess(())
  }
}
