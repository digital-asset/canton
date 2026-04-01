// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased.tool

import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.testing.modelbased.generators.ConcreteGenerators
import com.digitalasset.canton.testing.modelbased.solver.SymbolicSolver
import com.digitalasset.canton.testing.modelbased.solver.SymbolicSolver.KeyMode
import com.digitalasset.canton.testing.modelbased.syntax.Pretty
import com.digitalasset.daml.lf.language.LanguageVersion

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.*

/** Command-line tool for sampling and displaying random scenarios produced by the model-based
  * testing generator.
  *
  * Repeatedly generates concrete scenarios via `ConcreteGenerators`, then prints each scenario to
  * stdout.
  *
  * All generator parameters (language version, number of parties, key mode, etc.) are configurable
  * via command-line flags. Run with `--help` to see the full list.
  *
  * Intended as a development aid to quickly observe the effect of generator changes and get a sense
  * of performance.
  */
object SampleScenarios {

  final case class Config(
      languageVersion: LanguageVersion = LanguageVersion.assertFromString("2.dev"),
      readOnlyRollbacks: Boolean = true,
      generateQueryByKey: Boolean = true,
      keyMode: KeyMode = SymbolicSolver.KeyMode.NonUniqueContractKeys,
      numParties: Int = 3,
      numPackages: Int = 1,
      numParticipants: Int = 3,
      numCommands: Option[Int] = None,
      size: Int = 50,
      distinctKeyToContractRatio: Double = 1.0,
      numSamples: Option[Int] = None,
      pretty: Boolean = true,
      quiet: Boolean = false,
  )

  private val parser = new scopt.OptionParser[Config]("sample-scenarios") {
    discard {
      head("sample-scenarios", "Samples and pretty-prints random scenarios from the generator.")
    }

    discard {
      opt[String]("language-version")
        .valueName("<version>")
        .text("Daml-LF language version (default: 2.dev)")
        .action((v, c) =>
          c.copy(languageVersion =
            LanguageVersion
              .fromString(v)
              .fold(
                err => throw new IllegalArgumentException(err),
                identity,
              )
          )
        )
    }

    discard {
      opt[Boolean]("read-only-rollbacks")
        .text("Enable read-only rollbacks (default: true)")
        .action((v, c) => c.copy(readOnlyRollbacks = v))
    }

    discard {
      opt[Boolean]("generate-query-by-key")
        .text("Generate query-by-key nodes (default: true)")
        .action((v, c) => c.copy(generateQueryByKey = v))
    }

    discard {
      opt[String]("key-mode")
        .valueName("<uck|nuck>")
        .text(
          "Key mode: 'uck' (unique contract keys) or 'nuck' (non-unique contract keys, default)"
        )
        .validate(v =>
          if (v == "uck" || v == "nuck") success
          else failure(s"key-mode must be 'uck' or 'nuck', got '$v'")
        )
        .action((v, c) =>
          c.copy(keyMode =
            if (v == "uck") SymbolicSolver.KeyMode.UniqueContractKeys
            else SymbolicSolver.KeyMode.NonUniqueContractKeys
          )
        )
    }

    discard {
      opt[Int]("num-parties")
        .text("Number of parties (default: 3)")
        .action((v, c) => c.copy(numParties = v))
    }

    discard {
      opt[Int]("num-packages")
        .text("Number of packages (default: 1)")
        .action((v, c) => c.copy(numPackages = v))
    }

    discard {
      opt[Int]("num-participants")
        .text("Number of participants (default: 3)")
        .action((v, c) => c.copy(numParticipants = v))
    }

    discard {
      opt[Int]("num-commands")
        .text("Number of commands (default: unbounded)")
        .action((v, c) => c.copy(numCommands = Some(v)))
    }

    discard {
      opt[Int]("size")
        .text("Size parameter controlling scenario complexity (default: 50)")
        .action((v, c) => c.copy(size = v))
    }

    discard {
      opt[Double]("distinct-key-to-contract-ratio")
        .text("Ratio of distinct keys to keyed contracts, 0.0 to 1.0 (default: 1.0)")
        .action((v, c) => c.copy(distinctKeyToContractRatio = v))
    }

    discard {
      opt[Int]("num-samples")
        .text("Number of samples to generate (default: infinite)")
        .action((v, c) => c.copy(numSamples = Some(v)))
    }

    discard {
      opt[Boolean]("pretty")
        .text("Use pretty-printer for output (default: true)")
        .action((v, c) => c.copy(pretty = v))
    }

    discard {
      opt[Unit]("quiet")
        .text("Only display samples, no timing info or summary")
        .action((_, c) => c.copy(quiet = true))
    }

    discard {
      help("help").text("Prints this usage text")
    }
  }

  private def formatDuration(nanos: Long): String = {
    val d = Duration.fromNanos(nanos)
    if (d < 1.second) s"${d.toMillis}ms"
    else if (d < 1.minute) f"${d.toMillis / 1000.0}%.2fs"
    else {
      val mins = d.toMinutes
      val secs = (d.toSeconds % 60).toInt
      s"${mins}m${secs}s"
    }
  }

  def main(args: Array[String]): Unit =
    parser.parse(args, Config()) match {
      case Some(config) => run(config)
      case None => sys.exit(1)
    }

  private def run(config: Config): Unit = {
    val generators = new ConcreteGenerators(
      languageVersion = config.languageVersion,
      readOnlyRollbacks = config.readOnlyRollbacks,
      generateQueryByKey = config.generateQueryByKey,
      keyMode = config.keyMode,
    )

    val generator = generators.validScenarioGenerator(
      numParties = config.numParties,
      numPackages = config.numPackages,
      numParticipants = config.numParticipants,
      numCommands = config.numCommands,
    )

    val durations = ArrayBuffer.empty[Long]

    val shutdownHook = new Thread(() => if (!config.quiet) printSummary(durations))
    Runtime.getRuntime.addShutdownHook(shutdownHook)

    try {
      val limit = config.numSamples.getOrElse(Int.MaxValue)
      (1 to limit).foreach { _ =>
        val start = System.nanoTime()
        val scenario = generator.generate(config.size, config.distinctKeyToContractRatio)
        val elapsed = System.nanoTime() - start

        discard(durations += elapsed)

        val rendered =
          if (config.pretty) Pretty.prettyScenario(scenario)
          else scenario.toString

        if (!config.quiet) {
          System.err.println(s"--- Sample ${durations.size} (${formatDuration(elapsed)}) ---\n")
        }
        println(rendered)
      }
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
    }
  }

  private def percentile(sorted: Array[Long], p: Double): Long = {
    val idx = math.ceil(p / 100.0 * sorted.length).toInt - 1
    sorted(math.max(0, idx))
  }

  private def printSummary(durations: collection.Seq[Long]): Unit = {
    val sorted = durations.toArray.sorted
    val count = sorted.length
    if (count > 0) {
      val total = sorted.sum
      val mean = total / count
      val median = percentile(sorted, 50)
      val p90 = percentile(sorted, 90)
      val p99 = percentile(sorted, 99)
      val variance = sorted.map(d => (d - mean).toDouble * (d - mean).toDouble).sum / count
      val stddev = math.sqrt(variance).toLong

      System.err.println(s"Generated $count sample(s) in ${formatDuration(total)}")
      System.err.println(f"  mean:   ${formatDuration(mean)}%s")
      System.err.println(f"  stddev: ${formatDuration(stddev)}%s")
      System.err.println(f"  median: ${formatDuration(median)}%s")
      System.err.println(f"  p90:    ${formatDuration(p90)}%s")
      System.err.println(f"  p99:    ${formatDuration(p99)}%s")
    }
  }
}
