// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import sbt.*
import sbt.Keys.*

/** [TEST ONLY experiment] Let test shards reuse the compile job's classes without running the
  * incremental compiler.
  *
  * A test shard restores the compile job's `target` directories for the same commit and then runs
  * `testOnly`, which depends on `Compile / compile` and `Test / compile` of every project. Even
  * with nothing to recompile, that regenerates sources (protobuf, Daml Java codegen) and has zinc
  * check every project, which takes tens of seconds per shard.
  *
  * When `CANTON_REUSE_COMPILED_CLASSES=true`, `compile` returns the analysis that the compile job
  * stored (`previousCompile`) instead. Test discovery and the class directories are then exactly
  * the compile job's. CI sets the variable only after an exact restore of the compile cache for the
  * current commit, so the stored classes match the checkout. A project with sources but no stored
  * analysis fails the run rather than silently running no tests.
  */
object CantonReuseCompiled extends AutoPlugin {
  override def trigger = allRequirements
  override def requires = plugins.JvmPlugin

  private val enabled: Boolean = sys.env.get("CANTON_REUSE_COMPILED_CLASSES").contains("true")

  override def projectSettings: Seq[Setting[_]] =
    if (!enabled) Nil
    else
      Seq(Compile, Test).flatMap { config =>
        inConfig(config)(
          Seq(
            compile := {
              val previous = previousCompile.value.analysis()
              // Looked up unconditionally: sbt evaluates task dependencies regardless of `if`,
              // and CI rejects the linter warning for a lookup inside one. Listing the source
              // directories is cheap and generates nothing.
              val sources = unmanagedSources.value
              if (previous.isPresent) previous.get()
              else if (sources.isEmpty) sbt.internal.inc.Analysis.Empty
              else
                sys.error(
                  s"CANTON_REUSE_COMPILED_CLASSES is set but ${thisProject.value.id} / " +
                    s"${configuration.value.name} has no stored compile analysis; " +
                    "the compile cache for this commit was not restored"
                )
            }
          )
        )
      }
}
