// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// In package sbt to reach sbt's private[sbt] test plumbing (Tests.processOptions, ForkTests).
package sbt

import sbt.Def.Initialize
import sbt.Keys.*
import sbt.std.TaskExtra.*
import sbt.testing.{Framework, Runner}
import sbt.internal.util.ManagedLogger
import sjsonnew.BasicJsonProtocol.*

import scala.io.Source
import scala.util.Try

/** [TEST ONLY experiment] Runs the in-process test suites of a `testOnly` run longest first.
  *
  * sbt registers the suite tasks of a test run through an identity-hash set (`Execute.addNew`), so
  * suites start in an effectively random order whatever order `testOnly` receives them in. A long
  * suite that happens to start late then sets the end of the whole shard.
  *
  * When the environment variable `CANTON_TEST_ORDER_FILE` names a file of `<fully qualified class
  * name><TAB><seconds>` lines, `Test / testOnly` is replaced by a variant that deals the suites of
  * each in-process test group, heaviest first, into `CANTON_TEST_ORDER_LANES` (default 4) serial
  * lanes that run in parallel (greedy longest-processing-time assignment). Every suite task keeps
  * the configured tags, so global limits such as `Tags.limit(Tags.Test, n)` still apply. Suites
  * missing from the file get the median weight. Forked groups are unchanged. Without the variable
  * the settings are empty and sbt's default `testOnly` is used.
  *
  * The task wiring is adapted from sbt 1.12.9 (`Defaults.inputTests0`,
  * `Defaults.allTestGroupsTask`, `Tests.testTask`), Apache License 2.0.
  */
object CantonOrderedTests {

  private val orderFile: Option[java.io.File] =
    sys.env.get("CANTON_TEST_ORDER_FILE").map(new java.io.File(_)).filter(_.isFile)

  private lazy val weights: Map[String, Double] =
    orderFile
      .map { file =>
        val source = Source.fromFile(file, "UTF-8")
        try
          source
            .getLines()
            .flatMap { line =>
              line.split('\t') match {
                case Array(name, seconds) => Try(name.trim -> seconds.trim.toDouble).toOption
                case _ => None
              }
            }
            .toMap
        finally source.close()
      }
      .getOrElse(Map.empty)

  private lazy val defaultWeight: Double = {
    val sorted = weights.values.toVector.sorted
    if (sorted.isEmpty) 0.0 else sorted(sorted.size / 2)
  }

  private val lanes: Int =
    sys.env
      .get("CANTON_TEST_ORDER_LANES")
      .flatMap(s => Try(s.trim.toInt).toOption)
      .filter(_ > 0)
      .getOrElse(4)

  /** Add to every project. Empty unless `CANTON_TEST_ORDER_FILE` names an existing file. */
  def settings: Seq[Setting[_]] =
    if (orderFile.isEmpty) Nil
    else
      inConfig(Test)(
        Seq(
          testOnly := {
            try orderedInputTests(testOnly).evaluated
            finally close(testLoader.value)
          }
        )
      )

  private def close(loader: ClassLoader): Unit = loader match {
    case closeable: AutoCloseable => closeable.close()
    case _ =>
  }

  private def orderedInputTests(key: InputKey[_]): Initialize[InputTask[Unit]] =
    orderedInputTests0.mapReferenced(Def.mapScope(_.copy(task = Select(key.key))))

  private lazy val orderedInputTests0: Initialize[InputTask[Unit]] = {
    val parser = Defaults.loadForParser(definedTestNames)((s, i) =>
      Defaults.testOnlyParser(s, i getOrElse Nil)
    )
    Def.inputTaskDyn {
      val (selected, frameworkOptions) = parser.parsed
      val s = streams.value
      val filter = testFilter.value
      val config = testExecution.value
      val display = Project.showContextKey(state.value)
      val modifiedOpts =
        Tests.Filters(filter(selected)) +: Tests.Argument(frameworkOptions: _*) +: config.options
      val newConfig = config.copy(options = modifiedOpts)
      val output = allTestGroupsTask(
        s,
        loadedTestFrameworks.value,
        testLoader.value,
        testGrouping.value,
        newConfig,
        fullClasspath.value,
        testForkedParallel.value,
      )
      val taskName = display.show(resolvedScoped.value)
      val trl = testResultLogger.value
      output.map(out => trl.run(s.log, out, taskName))
    }
  }

  private def allTestGroupsTask(
      s: TaskStreams,
      frameworks: Map[TestFramework, Framework],
      loader: ClassLoader,
      groups: Seq[Tests.Group],
      config: Tests.Execution,
      cp: Classpath,
      forkedParallelExecution: Boolean,
  ): Initialize[Task[Tests.Output]] = {
    val processedOptions: Map[Tests.Group, Tests.ProcessedOptions] =
      groups.map(group => group -> Tests.processOptions(config, group.tests.toVector, s.log)).toMap
    val testDefinitions = processedOptions.values.flatMap(_.tests)
    val filteredFrameworks = frameworks.filter { case (_, framework) =>
      TestFramework.getFingerprints(framework).exists { fingerprint =>
        testDefinitions.exists(test => TestFramework.matches(fingerprint, test.fingerprint))
      }
    }
    val runners = Defaults.createTestRunners(filteredFrameworks, loader, config)
    val groupTasks = groups.map { group =>
      group.runPolicy match {
        case Tests.SubProcess(opts) =>
          val forkedConfig = config.copy(parallel = config.parallel && forkedParallelExecution)
          ForkTests(
            runners,
            processedOptions(group),
            forkedConfig,
            cp.map(_.data),
            opts,
            s.log,
            (Tags.ForkedTestGroup, 1) +: group.tags: _*
          )
        case Tests.InProcess =>
          orderedTestTask(
            loader,
            frameworks,
            runners,
            processedOptions(group),
            config.copy(tags = config.tags ++ group.tags),
            s.log,
          )
      }
    }
    val output = Tests.foldTasks(groupTasks, config.parallel)
    val result = output.map { out =>
      val summaries = runners.map { case (tf, r) => Tests.Summary(frameworks(tf).name, r.done()) }
      out.copy(summaries = summaries)
    }
    Def.value(result)
  }

  private def orderedTestTask(
      loader: ClassLoader,
      frameworks: Map[TestFramework, Framework],
      runners: Map[TestFramework, Runner],
      o: Tests.ProcessedOptions,
      config: Tests.Execution,
      log: ManagedLogger,
  ): Task[Tests.Output] = {
    def fj(actions: Iterable[() => Unit]): Task[Unit] = nop.dependsOn(actions.toSeq.fork(_()): _*)
    def partApp(actions: Iterable[ClassLoader => Unit]) = actions.toSeq.map(a => () => a(loader))

    val (frameworkSetup, runnables, frameworkCleanup) =
      TestFramework.testTasks(frameworks, runners, loader, o.tests, log, o.testListeners)
    val setupTasks = fj(partApp(o.setup) :+ frameworkSetup)
    val mainTasks: Task[List[(String, SuiteResult)]] =
      if (config.parallel)
        setupTasks.flatMap(_ => laneTasks(loader, runnables, config.tags)).map(_.toList)
      else Tests.makeSerial(loader, runnables, setupTasks)
    mainTasks
      .tagw(config.tags: _*)
      .map(Tests.processResults)
      .flatMap { results =>
        val cleanupTasks = fj(partApp(o.cleanup) :+ frameworkCleanup(results.overall))
        cleanupTasks.map(_ => results)
      }
  }

  /** Deals the suites, heaviest first, into `lanes` serial chains that run in parallel. */
  private def laneTasks(
      loader: ClassLoader,
      runnables: Seq[(String, TestFunction)],
      tags: Seq[(Tags.Tag, Int)],
  ): Task[Map[String, SuiteResult]] = {
    def weight(name: String): Double = weights.getOrElse(name, defaultWeight)
    val sorted = runnables.zipWithIndex
      .sortBy { case ((name, _), index) => (-weight(name), index) }
      .map(_._1)
    val laneCount = math.max(1, math.min(lanes, sorted.size))
    val buckets = Vector.fill(laneCount)(Vector.newBuilder[(String, TestFunction)])
    val load = Array.fill(laneCount)(0.0)
    sorted.foreach { case runnable @ (name, _) =>
      val lane = load.indices.minBy(i => load(i))
      buckets(lane) += runnable
      load(lane) += weight(name)
    }
    val chains = buckets.map(_.result()).filter(_.nonEmpty).map { lane =>
      lane.foldLeft(task(Map.empty[String, SuiteResult])) { case (acc, (name, fun)) =>
        acc.flatMap(done => Tests.toTask(loader, name, fun, tags).map(merge(done, _)))
      }
    }
    chains.join.map(_.foldLeft(Map.empty[String, SuiteResult])(merge))
  }

  private def merge(
      a: Map[String, SuiteResult],
      b: Map[String, SuiteResult],
  ): Map[String, SuiteResult] =
    b.foldLeft(a) { case (acc, (name, result)) =>
      acc.updated(name, acc.get(name).fold(result)(_ + result))
    }
}
