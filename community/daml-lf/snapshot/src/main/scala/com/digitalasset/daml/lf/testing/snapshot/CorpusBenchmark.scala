// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml.lf.transaction.Node
import org.openjdk.jmh.annotations.*

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

/** Replays corpus units on a warmed-up engine; one operation replays every selected unit once.
  *
  * Parameters: `corpus` is the corpus directory; `select` is a comma-separated list of prefixes of
  * `<suite>/<test>` (e.g. `cn-apps/` or `cn-apps/some-suite/pkg:Mod:script`), empty for all units;
  * `version` is the version of this benchmark, recorded in the output.
  */
@State(Scope.Benchmark)
class CorpusBenchmark {

  @Param(Array())
  var corpus: String = _

  @Param(Array(""))
  var select: String = _

  // shows in the JMH output which version of the benchmark produced it; not meant to be set
  @Param(Array(CorpusBenchmark.Version))
  var version: String = CorpusBenchmark.Version

  private var replay: CorpusReplay = _
  private var engine: Engine = _
  private var units: Vector[ReplayUnit] = _
  private var transactionNodes: Long = _
  private var fetchNodes: Long = _

  @Setup(Level.Trial)
  def init(): Unit = {
    require(
      version == CorpusBenchmark.Version,
      s"this is version ${CorpusBenchmark.Version}, not $version",
    )
    val c = Corpus.read(Paths.get(corpus))
    val prefixes = select.split(",").map(_.trim).filter(_.nonEmpty).toSeq
    replay = new CorpusReplay(c, NamedLoggerFactory.root)
    engine = replay.newPreloadedEngine()
    units = c.units
      .filter(u => prefixes.isEmpty || prefixes.exists(s"${u.suite}/${u.test}".startsWith))
      .map(u => replay.prepare(u).fold(err => sys.error(s"${describe(u)}: $err"), identity))
      .toVector
    require(units.nonEmpty, s"no units selected by '$select'")
    // a benchmark of transactions that don't replay correctly would be meaningless
    units.foreach(u =>
      replay.validate(engine, u).left.foreach(err => sys.error(s"${describe(u.unit)}: $err"))
    )
    // validation checked that each replay gives the recorded transaction, so its nodes are what
    // the replay produces; counting them there keeps metric plugins, which slow speedy down, out
    transactionNodes = units.map(_.transaction.nodes.size.toLong).sum
    fetchNodes = units.map(_.transaction.nodes.values.count(_.isInstanceOf[Node.Fetch]).toLong).sum
    println(s"%%% benchmarking ${units.size} units")
  }

  @Fork(value = 2)
  @Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
  @Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def bench(counters: CorpusBenchmark.EventCounter): Unit = {
    units.foreach(u =>
      replay.replay(engine, u).left.foreach(err => sys.error(s"${describe(u.unit)}: $err"))
    )
    counters.transactionNodeCount = transactionNodes
    counters.fetchNodeCount = fetchNodes
  }

  private def describe(u: CorpusUnit): String = s"${u.suite} ${u.test} #${u.index}"
}

object CorpusBenchmark {

  /** Bump when a change makes results incomparable with earlier ones, e.g. what an operation does.
    */
  final val Version = "2"

  @State(Scope.Thread)
  @AuxCounters(AuxCounters.Type.EVENTS)
  class EventCounter {
    var transactionNodeCount: Long = 0

    var fetchNodeCount: Long = 0
  }
}
