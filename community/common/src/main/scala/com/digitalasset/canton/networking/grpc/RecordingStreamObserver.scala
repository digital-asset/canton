// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.discard.Implicits.DiscardOps

import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters.*

/** Stream observer that records all incoming events.
  */
class RecordingStreamObserver[RespT](
    completeAfter: PositiveInt = PositiveInt.MaxValue,
    filter: RespT => Boolean = (_: RespT) => true,
) extends BaseStreamObserver[RespT, Seq[RespT]] {

  private val responseQueue: java.util.concurrent.BlockingQueue[RespT] =
    new java.util.concurrent.LinkedBlockingDeque[RespT](completeAfter.value)

  def responses: Seq[RespT] = responseQueue.asScala.toSeq

  val responseCount: AtomicInteger = new AtomicInteger()

  override def onNext(value: RespT): Unit = if (filter(value)) {
    responseQueue.offer(value)
    if (responseCount.incrementAndGet() >= completeAfter.value) {
      promise.trySuccess(responses).discard
    }
  }

  override protected def finalizeResult(): Seq[RespT] = responses

  override def onTimeout(
      timeout: config.NonNegativeDuration
  ): Either[String, Seq[RespT]] =
    Right(responses)
}
