// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.{Files, Path}

class AbReportTest extends AnyWordSpec with Matchers {

  // the shape of JMH's JSON output, reduced to what AbReport reads: two iterations, one fork
  private def jmhJson(
      score: Double,
      nodes: Double = 1010,
      version: Option[String] = Some("1"),
      select: String = "",
      error: String = "0.1",
      corpus: String = "/c",
  ) =
    s"""[{
       |  "benchmark": "com.digitalasset.daml.lf.testing.snapshot.CorpusBenchmark.bench",
       |  "params": {"corpus": "$corpus", "select": "$select"${version.fold("")(v =>
        s""", "version": "$v""""
      )}},
       |  "primaryMetric": {"score": $score, "scoreError": $error, "scoreUnit": "ms/op",
       |    "rawData": [[${score - 0.5}, ${score + 0.5}]]},
       |  "secondaryMetrics": {
       |    "transactionNodeCount": {"score": $nodes, "scoreError": "NaN", "scoreUnit": "#",
       |      "rawData": [[${nodes / 2}, ${nodes / 2}]]},
       |    "fetchNodeCount": {"score": 20.0, "scoreError": "NaN", "scoreUnit": "#",
       |      "rawData": [[10.0, 10.0]]}
       |  }
       |}]""".stripMargin

  private def run(
      score: Double,
      nodes: Double = 1010,
      version: Option[String] = Some("1"),
      select: String = "",
      error: String = "0.1",
      corpus: String = "/c",
  ) = {
    val file = Files.createTempFile("AbReportTest", ".json")
    Files.writeString(file, jmhJson(score, nodes, version, select, error, corpus))
    AbReport.readRun(file)
  }

  // rounds of one corpus each
  private def rounds(scores: Double*) = scores.map(s => Seq(run(s)))

  "readRun" should {
    "read the score, the error, the iterations, the parameters and the counts" in {
      val r = run(8.5)
      r.score shouldBe 8.5
      r.error shouldBe 0.1
      r.iterations shouldBe Seq(8.0, 9.0)
      r.countIterations shouldBe Map(
        "transactionNodeCount" -> Seq(505.0, 505.0),
        "fetchNodeCount" -> Seq(10.0, 10.0),
      )
      r.unit shouldBe "ms/op"
      r.params shouldBe Map("corpus" -> "/c", "select" -> "", "version" -> "1")
      r.counts shouldBe Map("transactionNodeCount" -> 1010.0, "fetchNodeCount" -> 20.0)
    }
  }

  "compare" should {
    "pair the rounds and give a confidence interval around the mean ratio" in {
      val c = AbReport.compare(Seq(9.0, 9.9, 9.45), Seq(10.0, 11.0, 10.5))
      c.ratios.foreach(_ shouldBe 0.9 +- 1e-9)
      c.mean shouldBe 0.9 +- 1e-9
      c.low shouldBe 0.9 +- 1e-9
      c.high shouldBe 0.9 +- 1e-9
    }

    "widen the interval with the spread of the ratios" in {
      val c = AbReport.compare(Seq(9.0, 10.0, 11.0), Seq(10.0, 10.0, 10.0))
      c.mean shouldBe 1.0 +- 1e-9
      // sd 0.1, n 3: 4.303 * 0.1 / sqrt(3)
      c.high - c.mean shouldBe 0.2484 +- 1e-4
      c.low should be < 1.0
    }

    "give no interval for one round" in {
      AbReport.compare(Seq(9.0), Seq(10.0)).low.isNaN shouldBe true
    }
  }

  "render" should {
    "say which side is faster" in {
      val report = AbReport.render(
        Seq("Ours: abc"),
        rounds(9.0, 9.1, 8.9),
        rounds(10.0, 10.1, 9.9),
      )
      report should include("**Ours is faster:** -10.0% time")
      report should include("Node counts are equal on both sides.")
      report should include("- Ours: abc")
    }

    "report no difference when the interval contains it" in {
      val report = AbReport.render(
        Seq.empty,
        rounds(9.0, 11.0, 10.0),
        rounds(10.0, 10.0, 10.0),
      )
      report should include("**No difference detected:**")
    }

    "flag different node counts as an error and different versions as a warning" in {
      val ours = Seq(Seq(run(10.0, nodes = 1010)), Seq(run(10.0, nodes = 1010)))
      val theirs = Seq(
        Seq(run(10.0, nodes = 1000, version = None)),
        Seq(run(10.0, nodes = 1000, version = None)),
      )
      AbReport.invariantViolations(ours, theirs) shouldBe
        Seq("transactionNodeCount differs: ours 1010.0, theirs 1000.0")
      val report = AbReport.render(Seq.empty, ours, theirs)
      report should include("- **Error:** transactionNodeCount differs")
      report should include("- Warning: the benchmark versions differ (ours 1, theirs unversioned)")
    }

    "say which units were selected, after the corpus" in {
      val meta = Seq("Ours: abc", "Corpus: /c, 2 suites, 9 units", "Rounds: 1")
      AbReport.render(meta, Seq(Seq(run(9.0, select = "cn-apps/"))), rounds(10.0)) should include(
        "- Corpus: /c, 2 suites, 9 units\n- Selected units: cn-apps/\n- Rounds: 1"
      )
      AbReport.render(meta, rounds(9.0), rounds(10.0)) should include("- Selected units: all")
    }

    "dump every run before the analysis" in {
      val report = AbReport.render(
        Seq.empty,
        Seq(Seq(run(9.0, error = "\"NaN\"")), Seq(run(9.5))),
        rounds(10.0, 10.5),
      )
      report should include("Parameters of every run: corpus=/c, select=, version=1")
      report should include(
        "| Side | Round | Score (ms/op) | Error | Iterations (ms/op) | fetchNodeCount | transactionNodeCount |"
      )
      report should include("| ours | 1 | 9 | NaN | 8.5 9.5 | 10 10 | 505 505 |")
      report should include("| theirs | 2 | 10.5 | 0.1 | 10 11 | 10 10 | 505 505 |")
      report.indexOf("## Raw results") should be < report.indexOf("## Analysis")
    }

    "list the runs' parameters per run when they differ" in {
      val report =
        AbReport.render(Seq.empty, rounds(9.0), Seq(Seq(run(10.0, version = None))))
      report should not include "Parameters of every run"
      report should include(
        "| ours | 1 | 9 | 0.1 | 8.5 9.5 | 10 10 | 505 505 | corpus=/c, select=, version=1 |"
      )
    }

    "list a result as ignored only when its whole interval is within the margin" in {
      val ours = rounds(9.9, 10.0, 10.1)
      val theirs = rounds(10.0, 10.0, 10.0)
      AbReport.render(Seq.empty, ours, theirs) should include(
        "No margin of error is set (`--margin`), so no result is ignored."
      )
      val within = AbReport.render(Seq.empty, ours, theirs, marginPercent = 5)
      within should include(
        "Ignored as within the margin of error of ±5.0%, because their whole 95% CI lies inside it:"
      )
      within should include("- all units: +0.0% time (95% CI")
      // the interval is about +-2.5%, so it pokes out of a 2% margin
      AbReport.render(Seq.empty, ours, theirs, marginPercent = 2) should include(
        "No result lies within the margin of error of ±2.0%, so none is ignored."
      )
    }
  }

  "render with several corpora" should {
    // per round: corpus /a then /b
    def twoCorpora(a: Seq[Double], b: Seq[Double], bNodes: Double = 1010) =
      a.zip(b).map { case (x, y) =>
        Seq(run(x, corpus = "/a"), run(y, nodes = bNodes, corpus = "/b"))
      }

    "compare the sum over the corpora and list each corpus on its own" in {
      // ours: /a 10% faster, /b unchanged; the sum 14 vs 15 is 6.7% faster
      val report = AbReport.render(
        Seq.empty,
        twoCorpora(Seq(9.0, 9.0, 9.0), Seq(5.0, 5.1, 4.9)),
        twoCorpora(Seq(10.0, 10.0, 10.0), Seq(5.0, 5.1, 4.9)),
      )
      report should include("**Ours is faster:** -6.7% time")
      report should include("| Round | Ours (ms/op) | Theirs (ms/op) | Ours vs theirs | /a | /b |")
      report should include("| 1 | 14.000 | 15.000 | -6.7% | -10.0% | +0.0% |")
      report should include("| /a | -10.0% | -10.0% to -10.0% | faster |")
      report should include("| /b | +0.0% | +0.0% to +0.0% | no difference |")
    }

    "check the node counts per corpus" in {
      val ours = twoCorpora(Seq(9.0, 9.0), Seq(5.0, 5.0))
      val theirs = twoCorpora(Seq(10.0, 10.0), Seq(5.0, 5.0), bNodes = 1000)
      AbReport.invariantViolations(ours, theirs) shouldBe
        Seq("/b: transactionNodeCount differs: ours 1010.0, theirs 1000.0")
    }

    "put the selection after the last corpus" in {
      val meta =
        Seq("Corpus: /a, 1 suites", "Corpus source: x", "Corpus: /b, 1 suites", "Rounds: 2")
      AbReport.render(meta, rounds(9.0), rounds(10.0)) should include(
        "- Corpus: /b, 1 suites\n- Selected units: all\n- Rounds: 2"
      )
    }
  }

  // not main, which prints the report and so fails CI's check of the sbt output
  "writeReport" should {
    "write the report into the work directory" in {
      val dir = Files.createTempDirectory("AbReportTest")
      Files.createDirectories(dir.resolve("runs"))
      Seq("ours" -> Seq(9.0, 9.2), "theirs" -> Seq(10.0, 10.1)).foreach { case (side, scores) =>
        scores.zipWithIndex.foreach { case (s, i) =>
          Files.writeString(dir.resolve("runs").resolve(s"$side-${i + 1}-1.json"), jmhJson(s))
          Files.writeString(
            dir.resolve("runs").resolve(s"$side-${i + 1}-2.json"),
            jmhJson(1.0, corpus = "/d"),
          )
        }
      }
      Files.writeString(dir.resolve("meta.txt"), "Ours: abc\nTheirs: def\n")
      // each run also leaves its log there
      Files.writeString(dir.resolve("runs").resolve("ours-1-1.log"), "JMH output")
      val (written, ok) = AbReport.writeReport(dir, marginPercent = 0)
      ok shouldBe true
      val report = Files.readString(dir.resolve("report.md"): Path)
      report shouldBe written
      report should include("| 2 | 10.200 | 11.100 |")
      report should include("- Theirs: def")
    }
  }
}
