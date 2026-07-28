// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import sbt.*

import scala.sys.process.*

object ProtocRetryPlugin extends AutoPlugin {
  override def trigger: PluginTrigger = allRequirements
  override def requires: Plugins = sbtprotoc.ProtocPlugin
  import sbtprotoc.ProtocPlugin.autoImport.*

  override def projectSettings: Seq[Def.Setting[_]] =
    inConfig(Compile)(unscopedProjectSettings) ++ inConfig(Test)(unscopedProjectSettings)

  private val unscopedProjectSettings = Seq(
    PB.runProtoc := {
      val log = Keys.streams.value.log
      val exec = PB.protocExecutable.value.getAbsolutePath.toString
      new protocbridge.ProtocRunner[Int] {
        override def run(args: Seq[String], extraEnv: Seq[(String, String)]): Int = {
          def runAndRetry(maxRetries: Int): Int = {
            val exitCode = ProtocRunner(exec).run(args, extraEnv)
            if (exitCode != 0) {
              // TODO(#33465) Remove this debug logs once the flaky protoc CI failure is resolved.
              log.info(s"protoc failed with exit code $exitCode")
              log.info(s"Full protoc args: ${args.mkString("[", ", ", "]")}")
              val file = args.collectFirst {
                case arg if arg.contains("protocbridge") => arg.split("=").last
              }
              file match {
                case Some(file) =>
                  val pLogger = ProcessLogger(
                    out => log.info(s"[process] $out"),
                    err => log.error(s"[process] $err"),
                  )
                  log.info(s"Running 'ls -la $file'...")
                  Process(Seq("ls", "-la", file)).!(pLogger)
                  log.info(s"Running 'cat $file'...")
                  Process(Seq("cat", file)).!(pLogger)
                  log.info("Running 'ls -la /bin/sh'...")
                  Process(Seq("ls", "-la", "/bin/sh")).!(pLogger)
                case None =>
                  log.error(s"Cannot find protocbridge from args")
              }
            }
            if (exitCode != 0 && maxRetries > 0) {
              log.info(s"protoc failed with exit code $exitCode. Retrying after 2 seconds...")
              Thread.sleep(2000)
              runAndRetry(maxRetries - 1)
            } else exitCode
          }
          // Retry protoc execution to work around race condition
          runAndRetry(maxRetries = 1)
        }
      }
    }
  )

}
