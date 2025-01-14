// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import sbt.MessageOnlyException
import sbt.{Def, *}
import sbt.Keys.*

import scala.sys.process.*

object BufPlugin extends AutoPlugin {

  object autoImport {
    val bufFormat = taskKey[Unit]("Re-formats the Protobuf files in place")
    val bufFormatCheck = taskKey[Unit]("Checks whether the Protobuf files are already formatted")
    val bufLintCheck = taskKey[Unit]("Checks whether the Protobuf files pass linting")
    val bufWrapperValueCheck =
      taskKey[Unit]("Checks that google.protobuf.*Value wrappers are not used")
  }
  import autoImport.*

  // Use the existing configuration relative to Protobuf files instead of duplicating
  // This means this plugin cannot be used independently but ensures that we don't have
  // to somehow manually synchronize the configuration of two different sbt plugins
  override def trigger: PluginTrigger = allRequirements
  override def requires: Plugins = sbtprotoc.ProtocPlugin
  import sbtprotoc.ProtocPlugin.autoImport.*

  private final case class Command(override val toString: String) extends AnyVal
  private object Command {
    val FormatOverwrite: Command = Command("buf format --diff --write")
    val FormatCheck: Command = Command("buf format --diff --exit-code")
    val LintCheck: Command = Command("buf lint")
    val WrapperValueCheck: Command = Command("bash ./scripts/ci/check-protobuf-wrappers.sh")
  }

  private def run(command: Command): Def.Initialize[Task[Unit]] =
    Def.taskDyn {
      val logger = streams.value.log
      val protoSources = (ThisScope / PB.protoSources).value
      Def.task {
        for (source <- protoSources if source.exists) {
          val fullCommand = s"$command $source"
          val status = fullCommand ! logger
          if (status != 0) {
            throw new MessageOnlyException(s"'$fullCommand' returned non-zero status code $status")
          }
        }
      }
    }

  private val unscopedProjectSettings = Seq(
    bufFormat := {
      run(Command.FormatOverwrite).value
    },
    bufFormatCheck := {
      run(Command.FormatCheck).value
    },
    bufLintCheck := {
      run(Command.LintCheck).value
    },
    bufWrapperValueCheck := {
      run(Command.WrapperValueCheck).value
    },
  )

  override def projectSettings: Seq[Def.Setting[_]] =
    inConfig(Compile)(unscopedProjectSettings) ++ inConfig(Test)(unscopedProjectSettings)

}
