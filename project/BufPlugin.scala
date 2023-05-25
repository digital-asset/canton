// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import sbt.{Def, *}
import sbt.Keys.*

import scala.sys.process.*

object BufPlugin extends AutoPlugin {

  object autoImport {
    val bufFormat = taskKey[Unit]("Re-formats the Protobuf files in place")
    val bufFormatCheck = taskKey[Unit]("Checks whether the Protobuf files are already formatted")
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
    val Overwrite: Command = Command("buf format --diff --write")
    val Check: Command = Command("buf format --diff --exit-code")
  }

  private def run(command: Command): Def.Initialize[Task[Unit]] =
    Def.taskDyn {
      val logger = streams.value.log
      val compileProtoSources = (Compile / PB.protoSources).value
      val testProtoSources = (Test / PB.protoSources).value
      Def.task {
        for (source <- compileProtoSources ++ testProtoSources if source.exists) {
          val fullCommand = s"$command $source"
          val status = fullCommand ! logger
          if (status != 0) {
            throw new RuntimeException(s"'$fullCommand' returned non-zero status code $status")
          }
        }
      }
    }

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    bufFormat := {
      run(Command.Overwrite).value
    },
    bufFormatCheck := {
      run(Command.Check).value
    },
  )
}
