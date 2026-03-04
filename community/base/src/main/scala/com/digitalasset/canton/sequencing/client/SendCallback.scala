// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, RunOnClosing, UnlessShutdown}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Promise

/** Utilities for a SendCallback passed to the send method of the [[SequencerClient]] */
object SendCallback {

  /** Do nothing when send result is observed */
  val empty: SendCallback = _ => ()

  /** Callback that just logs the eventual result with provided logger and traceContext available at
    * the callsite.
    * @param sendDescription
    *   Description of the send appropriate for a log message. Will have the outcome appended to it.
    */
  def log(sendDescription: String, logger: TracedLogger)(implicit
      traceContext: TraceContext
  ): SendCallback =
    SendResult.log(sendDescription, logger)(_)

  /** Provides an easy mechanism for waiting for the send result. Should likely not be used within
    * event handlers as this could prevent reading further events that may complete this callback,
    * and cause a deadlock.
    */
  class CallbackFuture extends SendCallback {
    import com.digitalasset.canton.discard.Implicits.DiscardOps

    private val promise = Promise[UnlessShutdown[SendResult]]()

    val future: FutureUnlessShutdown[SendResult] =
      FutureUnlessShutdown(promise.future)

    override def apply(result: UnlessShutdown[SendResult]): Unit =
      promise.trySuccess(result).discard

    /** A utility to be used in conjunction with
      * [[com.digitalasset.canton.lifecycle.HasRunOnClosing]] methods to integrate the callback
      * future with a component's shutdown. Will complete the future with
      * [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] when called.
      */
    def runOnClosing: RunOnClosing =
      new RunOnClosing {
        override def name: String = "close-send-callback-future"
        override def done: Boolean = promise.future.isCompleted
        override def run()(implicit traceContext: TraceContext): Unit =
          promise.trySuccess(AbortedDueToShutdown).discard
      }
  }

  def future: CallbackFuture = new CallbackFuture
}
