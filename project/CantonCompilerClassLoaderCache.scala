// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import sbt.*
import sbt.Keys.*

/** [TEST ONLY experiment] Reuse the class loaders of compiler plugins and macros across compiler
  * runs.
  *
  * Every Scala 2.13 compile in this build loads the compiler plugins (wartremover,
  * kind-projector, semanticdb) and the macro implementations it calls into fresh class loaders.
  * With ~190 projects, two configurations each, and build pipelining running more of them at once,
  * that is a lot of repeated class loading, and it exhausted a 2 GB Metaspace in a local cold
  * build. `last-modified` caches a class loader per set of jars and reuses it while the jars are
  * unchanged, which they are within a build.
  *
  * Disable with `-Dcanton.compiler.classloader.cache=false`.
  */
object CantonCompilerClassLoaderCache extends AutoPlugin {
  override def trigger = allRequirements
  override def requires = plugins.JvmPlugin

  private val enabled: Boolean =
    sys.props.get("canton.compiler.classloader.cache").forall(_ != "false")

  override def projectSettings: Seq[Setting[_]] = Seq(
    scalacOptions ++= {
      if (enabled && scalaBinaryVersion.value == "2.13")
        Seq("-Ycache-plugin-class-loader:last-modified", "-Ycache-macro-class-loader:last-modified")
      else Nil
    }
  )
}
