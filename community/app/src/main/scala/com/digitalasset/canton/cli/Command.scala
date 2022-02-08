// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.cli

import java.io.File

sealed trait Command {}

object Command {

  /** Run the process as a server (rather than an interactive repl)
    */
  object Daemon extends Command

  /** Run a console script then close
    *
    * @param scriptPath the path to the script
    */
  case class RunScript(scriptPath: File) extends Command

  case class Generate(target: Generate.Target) extends Command

  object Generate {
    sealed trait Target

    object RemoteConfig extends Target
  }
}
