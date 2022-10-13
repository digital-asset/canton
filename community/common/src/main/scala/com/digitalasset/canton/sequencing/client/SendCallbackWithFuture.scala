// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.digitalasset.canton.DiscardOps

import scala.concurrent.{Future, Promise}

class SendCallbackWithFuture extends SendCallback {
  private val promise = Promise[SendResult]()
  override def apply(result: SendResult): Unit = {
    promise.trySuccess(result).discard
    ()
  }
  val result: Future[SendResult] = promise.future
}
