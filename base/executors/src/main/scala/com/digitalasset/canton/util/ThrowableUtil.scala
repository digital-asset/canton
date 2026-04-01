// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.lifecycle.ClosingException
import com.typesafe.scalalogging.Logger

import java.io.{PrintWriter, StringWriter}

object ThrowableUtil {

  /** @param exitOnFatal
    *   terminate the JVM on fatal errors. Enable this in production to prevent data corruption by
    *   termination of specific threads.
    */
  def createReporter(name: String, logger: Logger, exitOnFatal: Boolean)(
      throwable: Throwable
  ): Unit = {
    if (exitOnFatal) doExitOnFatal(name, logger)(throwable)
    throwable match {
      case ex: io.grpc.StatusRuntimeException
          if ex.getStatus.getCode == io.grpc.Status.Code.CANCELLED =>
        logger.info(s"Grpc channel cancelled in $name.", ex)
      case ClosingException(_) =>
        logger.info(s"Unclean shutdown due to cancellation in $name.", throwable)
      case _: Throwable =>
        logger.error(s"A fatal error has occurred in $name. Terminating thread.", throwable)
    }
  }

  private def doExitOnFatal(name: String, logger: Logger)(throwable: Throwable): Unit =
    throwable match {
      case _: LinkageError | _: VirtualMachineError =>
        // Output the error reason both to stderr and the logger,
        // because System.exit tends to terminate the JVM before everything has been output.
        Console.err.println(
          s"A fatal error has occurred in $name. Terminating immediately.\n${messageWithStacktrace(throwable)}"
        )
        Console.err.flush()
        logger.error(s"A fatal error has occurred in $name. Terminating immediately.", throwable)
        System.exit(-1)
      case _: Throwable => // no fatal error, nothing to do
    }

  /** Yields a string representation of a throwable (including stack trace and causes).
    */
  def messageWithStacktrace(t: Throwable): String = {
    val result = new StringWriter()
    t.printStackTrace(new PrintWriter(result))
    result.toString
  }
}
