// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.concurrent

import com.digitalasset.canton.util.ThrowableUtil
import com.typesafe.scalalogging.Logger

import scala.concurrent.{BatchingExecutorCanton, ExecutionContextExecutor}

final case class DirectExecutionContext private[concurrent] (logger: Logger)
    extends ExecutionContextExecutor
    with BatchingExecutorCanton {

  private val reporter: Throwable => Unit =
    ThrowableUtil.createReporter(getClass.getSimpleName, logger, exitOnFatal = true)

  override def submitForExecution(runnable: Runnable): Unit = runnable.run()

  override def execute(runnable: Runnable): Unit = submitSyncBatched(runnable)

  override def reportFailure(cause: Throwable): Unit =
    // Do not rethrow cause.
    // If this method throws an exception, the exception would ultimately be reported by a different EC,
    // but this leads to a messy repetition of error messages in the log file.
    reporter(cause)
}

object DirectExecutionContext {

  /** A light-weight execution context that runs tasks on the thread calling `execute`. Only use
    * this for running tasks that will terminate very quickly.
    */
  val directExecutionContext: DirectExecutionContext = DirectExecutionContext(Logger(getClass))
}
