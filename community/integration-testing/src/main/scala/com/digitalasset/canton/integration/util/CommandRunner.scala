// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.digitalasset.canton.console.BufferedProcessLogger
import org.scalatest.matchers.should.Matchers.*

import java.io.File
import scala.sys.process.{Process, ProcessLogger}

class CommandRunner(command: String) {

  private val processLogger = new BufferedProcessLogger()

  private def checkReturn(res: Int, log: BufferedProcessLogger): Unit =
    if (res != 0)
      fail(s"""
              |$command failed:
              |${log.output("  ")}
        """.stripMargin.trim)

  def run(argument: String, cwd: Option[File], extraEnv: Seq[(String, String)] = Seq()): Unit = {
    val res = Process(s"$command $argument", cwd, extraEnv = extraEnv*) ! processLogger

    checkReturn(res, processLogger)
  }

  def run(argument: String, cwd: Option[File], processLogger: ProcessLogger): Unit = {
    val res = Process(s"$command $argument", cwd) ! processLogger
    if (res != 0) {
      fail(s"$command failed to run $argument")
    }
  }

}
