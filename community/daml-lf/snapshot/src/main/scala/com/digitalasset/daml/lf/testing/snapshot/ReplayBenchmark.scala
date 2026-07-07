// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.speedy.metrics.{FetchNodeCount, StepCount, TxNodeCount}
import com.digitalasset.daml.lf.value.ContractIdVersion
import org.openjdk.jmh.annotations.*

import java.nio.file.{FileSystems, Files, Paths}
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

@State(Scope.Benchmark)
class ReplayBenchmark {

  @Param(Array("true", "false"))
  var gasOn: Boolean = _

  @Param(Array())
  // choiceName of the exercise to benchmark
  // format:
  // - "ModuleName:TemplateName:ChoiceName"
  // - or "PackageId:ModuleName:TemplateName:ChoiceName"
  var choiceName: String = _

  @Param(Array("0"))
  var choiceIndex: Int = _

  @Param(Array(""))
  // directory containing Dar files
  var darDir: String = _

  @Param(Array())
  // path of the ledger entries - i.e. path to file of saved transaction trees
  var entriesFile: String = _

  @Param(Array("V1"))
  // the contract ID version to use for locally created contracts
  var contractIdVersion: String = _

  @Param(Array(""))
  var commit: String = sys.env.getOrElse("CIRCLE_SHA1", "unknown")

  private var benchmark: TransactionSnapshot = _

  @Fork(value = 2, warmups = 3)
  @Warmup(iterations = 7, time = 500, timeUnit = TimeUnit.MILLISECONDS)
  @Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def bench(counters: ReplayBenchmark.EventCounter): Unit = {
    counters.reset()
    val result = benchmark.replay()
    assert(result.isRight)
    val Right(metrics) = result
    counters.stepCount += metrics.totalCount[StepCount].get
    counters.transactionNodeCount += metrics.totalCount[TxNodeCount].get
    counters.fetchNodeCount += metrics.totalCount[FetchNodeCount].get
    metrics.reset()
  }

  @Setup(Level.Trial)
  def init(): Unit = {
    // Indexing from the end of the array allows the package ID to be optional in the choice name
    val reversedChoiceNameParts = choiceName.split(":").reverse
    val name = reversedChoiceNameParts(0)
    val tmplNameStr = reversedChoiceNameParts(1)
    val modNameStr = reversedChoiceNameParts(2)
    val choice = (
      Ref.QualifiedName(
        Ref.DottedName.assertFromString(modNameStr),
        Ref.DottedName.assertFromString(tmplNameStr),
      ),
      Ref.Name.assertFromString(name),
    )
    val contractIdVersionParsed = contractIdVersion match {
      case "V1" => ContractIdVersion.V1
      case "V2" => ContractIdVersion.V2
    }
    benchmark = TransactionSnapshot.loadBenchmark(
      dumpFile = Paths.get(entriesFile),
      choice = choice,
      index = choiceIndex,
      profileDir = None,
      contractIdVersion = contractIdVersionParsed,
      gasBudget = Some(Long.MaxValue).filter(_ => gasOn),
    )
    if (darDir.nonEmpty) {
      val darFileMatcher =
        FileSystems
          .getDefault()
          .getPathMatcher(s"glob:$darDir/*.dar")
      val darFilesToLoad =
        Files.list(Paths.get(darDir)).filter(darFileMatcher.matches).toList.asScala
      val loadedPackages = darFilesToLoad.foldLeft(Map.empty[Ref.PackageId, Ast.Package]) {
        case (pkgs, darFile) =>
          pkgs ++ TransactionSnapshot.loadDar(darFile)
      }
      benchmark = benchmark.copy(pkgs = loadedPackages)
    }

    assert(benchmark.validate().isRight)
  }

}

object ReplayBenchmark {
  @State(Scope.Thread)
  @AuxCounters(AuxCounters.Type.EVENTS)
  class EventCounter {
    var stepCount: Long = 0

    var transactionNodeCount: Long = 0

    var fetchNodeCount: Long = 0

    def reset(): Unit = {
      stepCount = 0
      transactionNodeCount = 0
      fetchNodeCount = 0
    }
  }
}
