// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import sbt.*
import sbt.Keys.*

/** [TEST ONLY experiment] Generate bytecode on several threads (`-Ybackend-parallelism`).
  *
  * Scala 2.13 runs the backend (bytecode generation and class file writing) single-threaded by
  * default. For the largest projects (`community-base`, the `community-app` tests) it is a
  * noticeable share of compile time. Scala 3 has no such option, so it is added for 2.13 only.
  *
  * Threads: `-Dcanton.backend.parallelism=N`, default min(8, available processors), at most 16; 1 disables it.
  */
object CantonBackendParallelism extends AutoPlugin {
  override def trigger = allRequirements
  override def requires = plugins.JvmPlugin

  private val threads: Int =
    sys.props
      .get("canton.backend.parallelism")
      .flatMap(s => scala.util.Try(s.trim.toInt).toOption)
      .getOrElse(math.min(8, java.lang.Runtime.getRuntime.availableProcessors()))

  override def projectSettings: Seq[Setting[_]] = Seq(
    scalacOptions ++= {
      if (scalaBinaryVersion.value == "2.13" && threads > 1)
        // One token: some settings in this build deduplicate scalacOptions, which would drop a
        // separate "8" argument when another option (such as `--release 8`) also uses it.
        Seq(s"-Ybackend-parallelism:${math.min(threads, 16)}")
      else Nil
    }
  )
}
