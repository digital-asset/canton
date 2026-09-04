// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.util.Timer
import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Promise}

object WithTimeout {

  private[this] val timer = new Timer("timeout-timer", true)

  /** Prefer the timeouts already built into `ParticipantTestContext` for calls that have them. This
    * is for waits that need a deadline of their own, in particular the ones asserting that nothing
    * arrives.
    */
  def apply[A](operation: String, t: Duration)(f: => Future[A]): Future[A] = {
    val p = Promise[A]()
    timer.schedule(new TimeoutTask(p, TimeoutException(operation, t)), t.toMillis)
    p.completeWith(f).future
  }

}
