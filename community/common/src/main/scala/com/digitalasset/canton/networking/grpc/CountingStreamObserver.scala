// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.digitalasset.canton.config

/** Stream observer that reports the count of the observed events.
  *
  * Note: even though [[RecordingStreamObserver]] also counts the events, it is memory heavy
  */
class CountingStreamObserver[T] extends BaseStreamObserver[T, Int] {
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var responseCount: Int = 0

  override def onNext(value: T): Unit = responseCount += 1

  override protected def finalizeResult(): Int = responseCount

  override def onTimeout(timeout: config.NonNegativeDuration): Either[String, Int] =
    Left(
      s"CountingStreamObserver couldn't count the result due to timeout (more than $timeout elapsed!)"
    )
}
