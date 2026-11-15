// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import sbt.*
import sbt.Keys.*

/** [TEST ONLY experiment] sbt build pipelining.
  *
  * With pipelining, a downstream project starts compiling against the upstream project's early
  * output (pickles written after the typer phase) instead of waiting for the upstream bytecode.
  * That shortens the serial chain `base -> common -> participant/synchronizer -> app-base -> app
  * -> app tests` on a cold compile.
  *
  * Exceptions:
  *   - Scala 3 projects (transcode, Scala 3.3) do not support sbt pipelining.
  *   - Projects that compile Java and Scala separately (`compileOrder := JavaThenScala`, such as
  *     `bindings-java`): their Scala sources then no longer see the Java classes.
  *   - Projects whose classes must be loaded while compiling a downstream project cannot hand out
  *     early output: macro definitions (`nameof`, `magnolify-addon`, `scalatest-addon`) and the
  *     custom wartremover rules, which run as a compiler plugin (`wartremover-extension`).
  *   - Projects without hand-written Scala sources (pure Java, or generated code only) do not
  *     export early output either: scalac writes no pickles for them.
  *
  * Test inherits Compile's scalacOptions, including Compile's `-Ypickle-write` target, which is
  * removed for Test below.
  *
  * Disable with `-Dcanton.pipelining=false`.
  */
object CantonPipelining extends AutoPlugin {
  override def trigger = allRequirements
  override def requires = plugins.JvmPlugin

  // Off when CantonReuseCompiled replaces `compile` (test shards): nothing is compiled there, but
  // with pipelining on, test runs still evaluated `compileEarly`, which runs the source
  // generators (protobuf, Daml) and zinc's up-to-date check for every project.
  private val enabled: Boolean =
    sys.props.get("canton.pipelining").forall(_ != "false") &&
      !sys.env.get("CANTON_REUSE_COMPILED_CLASSES").contains("true")

  private val noEarlyOutput: Set[String] =
    Set("nameof", "magnolify-addon", "scalatest-addon", "wartremover-extension")

  // Early output holds the pickles scalac writes. A project without hand-written Scala sources
  // (pure Java such as `bindings-java`, or only generated code) gives downstream projects no
  // usable early output, so they must wait for its class files.
  private def hasHandWrittenScala(config: Configuration) = Def.setting {
    (config / unmanagedSourceDirectories).value.exists(dir => (dir ** "*.scala").get.nonEmpty)
  }

  override def projectSettings: Seq[Setting[_]] = Seq(
    // Projects without any hand-written Scala gain nothing from pipelining (javac cannot read
    // pickles), and in CI the pure-Java `daml-lf-snapshot-proto` was compiled again by the
    // `printTests` that followed a full `Test/compile` when it was pipelined. Test sources
    // count: the community-app test modules have Scala only in Test.
    usePipelining := enabled && scalaBinaryVersion.value == "2.13" &&
      compileOrder.value == CompileOrder.Mixed &&
      (hasHandWrittenScala(Compile).value || hasHandWrittenScala(Test).value),
    exportPipelining := usePipelining.value && !noEarlyOutput.contains(thisProject.value.id) &&
      hasHandWrittenScala(Compile).value,
    // sbt adds `-Ypickle-write <early output>` to each configuration's scalacOptions, and Test
    // inherits Compile's options, so the Test compile sees both its own and Compile's
    // `-Ypickle-write`. scalac uses the last one, so the Test compile overwrote Compile's early
    // output with test pickles and downstream projects then missed main classes. Drop Compile's.
    Test / scalacOptions := {
      val options = (Test / scalacOptions).value
      if (!(Compile / usePipelining).value) options
      else {
        val mainEarly = fileConverter.value.toPath((Compile / earlyOutput).value).toString
        val at = options.indexOf(mainEarly)
        if (at > 0 && options(at - 1) == "-Ypickle-write") options.patch(at - 1, Nil, 2)
        else options.filterNot(_ == mainEarly)
      }
    },
  )
}
