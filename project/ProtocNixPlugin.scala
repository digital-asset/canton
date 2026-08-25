// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import sbt.*
import sbt.Keys.*

/** Points `protocGenerate` at a stable launcher for every native protoc plugin.
  *
  * When `NIX_CC` is set (it always is, since sbt runs inside the nix shell) sbt-protoc wraps each
  * native plugin binary in a fresh `/tmp/nix<random>` script per task run and passes that path as
  * `--plugin=`. The path is part of the task's cache key, so codegen and the doc rendering rerun on
  * every build even when no proto changed, and the scripts pile up in `/tmp` for the life of the
  * sbt server. Writing the launcher ourselves under a digest-named path keeps the key stable, and
  * the `.sh` suffix stops sbt-protoc from wrapping it again.
  */
object ProtocNixPlugin extends AutoPlugin {
  override def trigger: PluginTrigger = allRequirements
  override def requires: Plugins = sbtprotoc.ProtocPlugin

  import sbtprotoc.ProtocPlugin.ProtobufConfig
  import sbtprotoc.ProtocPlugin.autoImport.PB

  // Per user, so a shared /tmp does not hand one user a directory the next cannot write to.
  private val launcherDir =
    IO.temporaryDirectory / s"canton-protoc-plugins-${sys.props("user.name")}"

  /** The artifact of a plugin binary that needs a launcher, if this entry is one. */
  private def nativePluginArtifact(entry: Attributed[File]): Option[Artifact] =
    entry
      .get(artifact.key)
      .filter(a => a.`type` == PB.ProtocPlugin && !entry.data.getName.endsWith(".sh"))

  private val launcherLock = new Object

  private def launcherFor(name: String, binary: File, linker: String): File = {
    val script =
      s"""#!/bin/sh
         |exec $linker ${binary.getAbsolutePath} "$$@"
         |""".stripMargin
    // Digest in the name, so a new linker or binary is a new path and thus a new cache key.
    val launcher = launcherDir / s"$name-${Hash.toHex(Hash(script)).take(12)}.sh"
    // Two projects can share a plugin, so they race on one path; IO.write truncates.
    launcherLock.synchronized {
      // Recreate if a tmp sweeper removed it. The path stays the same, so the cache still holds.
      if (!launcher.isFile) {
        IO.createDirectory(launcherDir)
        IO.write(launcher, script)
      }
      launcher.setExecutable(true)
      binary.setExecutable(true)
    }
    launcher
  }

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    // Scoped exactly as sbt-protoc reads it, so PB.unpackDependencies keeps seeing the real files.
    ProtobufConfig / PB.generate / managedClasspath := {
      val classpath = (ProtobufConfig / managedClasspath).value
      protocbridge.ProtocRunner.maybeNixDynamicLinker() match {
        case None => classpath
        case Some(linker) =>
          classpath.map { entry =>
            nativePluginArtifact(entry).fold(entry) { pluginArtifact =>
              // Keep the metadata: sbt-protoc names the --plugin= flag after the artifact.
              Attributed(launcherFor(pluginArtifact.name, entry.data, linker))(entry.metadata)
            }
          }
      }
    }
  )
}
