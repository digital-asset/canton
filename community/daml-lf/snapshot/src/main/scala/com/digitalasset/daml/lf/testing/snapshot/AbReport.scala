// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import spray.json.*

import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters.*

/** Compares the CorpusBenchmark results of the two sides of an A/B run (scripts/ab.sh).
  *
  * The work directory holds `runs/<side>-<round>-<corpus>.json`, the JMH results of each side per
  * round and corpus, and `meta.txt`, lines describing the run. A side's time in a round is the sum
  * over the corpora, so a corpus counts by how long it takes to replay. Each round ran both sides
  * one after the other, so the comparison pairs them by round: drift during the run affects both
  * sides of a round alike.
  */
object AbReport {

  final case class Run(
      score: Double,
      // JMH's 99.9% confidence half-width of the score; NaN for a single iteration
      error: Double,
      unit: String,
      iterations: Seq[Double],
      params: Map[String, String],
      // secondary metrics: the sum over the iterations, and the value of each iteration
      counts: Map[String, Double],
      countIterations: Map[String, Seq[Double]],
  )

  /** A side's runs in one round: one per corpus, in the order they ran. */
  type Round = Seq[Run]

  /** Mean of ours / theirs over the rounds, with a 95% confidence interval. */
  final case class Comparison(ratios: Seq[Double], mean: Double, low: Double, high: Double)

  // node counts depend only on the transactions, so both sides must report the same
  val invariantCounts: Seq[String] = Seq("transactionNodeCount", "fetchNodeCount")

  def readRun(file: Path): Run = {
    val results = JsonParser(Files.readString(file)).convertTo[Seq[JsObject]](
      DefaultJsonProtocol.immSeqFormat(DefaultJsonProtocol.RootJsObjectFormat)
    )
    val result = results.filter(field(_, "benchmark") match {
      case JsString(name) => name.endsWith("CorpusBenchmark.bench")
      case _ => false
    }) match {
      case Seq(r) => r
      case rs => sys.error(s"$file: expected one CorpusBenchmark.bench result, got ${rs.size}")
    }
    def number(obj: JsValue, name: String): Double = field(obj, name) match {
      case JsNumber(n) => n.toDouble
      case other => sys.error(s"$file: $name is $other, not a number")
    }
    // JMH writes "NaN" for an undefined error
    def numberOrNaN(obj: JsValue, name: String): Double = field(obj, name) match {
      case JsString("NaN") => Double.NaN
      case _ => number(obj, name)
    }
    // one array of iterations per fork
    def rawData(metric: JsValue): Seq[Double] = field(metric, "rawData") match {
      case JsArray(forks) =>
        forks.flatMap {
          case JsArray(values) =>
            values.map {
              case JsNumber(n) => n.toDouble
              case other => sys.error(s"$file: rawData holds $other, not a number")
            }
          case other => sys.error(s"$file: rawData holds $other, not an array")
        }
      case other => sys.error(s"$file: rawData is $other, not an array")
    }
    val primary = field(result, "primaryMetric")
    val secondary = result.fields.get("secondaryMetrics").collect { case JsObject(ms) => ms }
    Run(
      score = number(primary, "score"),
      error = numberOrNaN(primary, "scoreError"),
      unit = field(primary, "scoreUnit").convertTo[String](DefaultJsonProtocol.StringJsonFormat),
      iterations = rawData(primary),
      params = result.fields
        .get("params")
        .collect { case JsObject(ps) =>
          ps.collect { case (k, JsString(v)) => k -> v }
        }
        .getOrElse(Map.empty),
      counts = secondary
        .map(_.map { case (name, metric) => name -> number(metric, "score") })
        .getOrElse(Map.empty),
      countIterations = secondary
        .map(_.map { case (name, metric) => name -> rawData(metric) })
        .getOrElse(Map.empty),
    )
  }

  private def field(value: JsValue, name: String): JsValue = value match {
    case JsObject(fields) => fields.getOrElse(name, sys.error(s"no field $name"))
    case other => sys.error(s"expected an object with $name, got $other")
  }

  def compare(ours: Seq[Double], theirs: Seq[Double]): Comparison = {
    require(
      ours.size == theirs.size && ours.nonEmpty,
      "need the same number of rounds on both sides",
    )
    val ratios = ours.zip(theirs).map { case (o, t) => o / t }
    val n = ratios.size
    val mean = ratios.sum / n
    if (n < 2) Comparison(ratios, mean, Double.NaN, Double.NaN)
    else {
      val sd = math.sqrt(ratios.map(r => (r - mean) * (r - mean)).sum / (n - 1))
      val half = tQuantile975(n - 1) * sd / math.sqrt(n.toDouble)
      Comparison(ratios, mean, mean - half, mean + half)
    }
  }

  // two-sided 95% quantiles of Student's t distribution
  private val tTable = Vector(12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228,
    2.201, 2.179, 2.160, 2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069,
    2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042)

  private[snapshot] def tQuantile975(degreesOfFreedom: Int): Double =
    tTable.lift(degreesOfFreedom - 1).getOrElse(1.96)

  private def corpus(r: Run): String = r.params.getOrElse("corpus", "")

  private def corpora(rounds: Seq[Round]): Seq[String] =
    rounds.headOption.getOrElse(Seq.empty).map(corpus)

  /** Differences that mean the two sides didn't do the same work. */
  def invariantViolations(ours: Seq[Round], theirs: Seq[Round]): Seq[String] = {
    val names = corpora(ours)
    names.zipWithIndex.flatMap { case (name, k) =>
      val prefix = if (names.size > 1) s"$name: " else ""
      invariantCounts.flatMap { count =>
        val o = ours.flatMap(_.lift(k)).flatMap(_.counts.get(count)).distinct
        val t = theirs.flatMap(_.lift(k)).flatMap(_.counts.get(count)).distinct
        Option.when(o.nonEmpty && t.nonEmpty && (o ++ t).distinct.size > 1)(
          s"$prefix$count differs: ours ${o.mkString(", ")}, theirs ${t.mkString(", ")}"
        )
      }
    }
  }

  def warnings(ours: Seq[Round], theirs: Seq[Round]): Seq[String] = {
    def versions(rounds: Seq[Round]) =
      rounds.flatten.map(_.params.getOrElse("version", "unversioned")).distinct
    Option
      .when(versions(ours) != versions(theirs))(
        s"the benchmark versions differ (ours ${versions(ours).mkString(", ")}, theirs " +
          s"${versions(theirs).mkString(", ")}), so the operations may not be comparable"
      )
      .toList
  }

  private def percent(ratio: Double): String = f"${(ratio - 1) * 100}%+.1f%%"

  // as JMH wrote it, without rounding
  private def number(x: Double): String =
    if (x.isNaN) "NaN" else if (x.isWhole) f"$x%.0f" else x.toString

  /** Whether the whole confidence interval lies within +-marginPercent. */
  def withinMargin(c: Comparison, marginPercent: Double): Boolean =
    marginPercent > 0 && !c.low.isNaN &&
      c.low >= 1 - marginPercent / 100 && c.high <= 1 + marginPercent / 100

  /** Every number the runs recorded, before any analysis. */
  def rawResults(ours: Seq[Round], theirs: Seq[Round]): Seq[String] = {
    val runs = ours.zipAll(theirs, Seq.empty, Seq.empty).zipWithIndex.flatMap { case ((o, t), i) =>
      Seq("ours" -> o, "theirs" -> t).flatMap { case (side, rs) => rs.map(r => (side, i + 1, r)) }
    }
    val unit = runs.headOption.map(_._3.unit).getOrElse("")
    val countNames = runs.flatMap(_._3.countIterations.keys).distinct.sorted
    val allParams = runs.map(_._3.params).distinct
    def params(ps: Map[String, String]) =
      ps.toSeq.sorted.map { case (k, v) => s"$k=$v" }.mkString(", ")
    val paramsColumn = allParams.size > 1
    val header = Seq("Side", "Round", s"Score ($unit)", "Error", s"Iterations ($unit)") ++
      countNames ++ Option.when(paramsColumn)("Params")
    val rows = runs.map { case (side, round, r) =>
      Seq(
        side,
        round.toString,
        number(r.score),
        number(r.error),
        r.iterations.map(number).mkString(" "),
      ) ++
        countNames.map(n => r.countIterations.getOrElse(n, Seq.empty).map(number).mkString(" ")) ++
        Option.when(paramsColumn)(params(r.params))
    }
    Seq(
      "Every JMH run, from `runs/<side>-<round>-<corpus>.json` (the warmup iterations are only in the " +
        "`.log` next to it). Error is JMH's 99.9% confidence half-width of the score; the counts " +
        "are per iteration.",
      "",
    ) ++
      allParams.headOption
        .filter(_ => !paramsColumn)
        .map(ps => s"Parameters of every run: ${params(ps)}")
        .toList
        .flatMap(Seq(_, "")) ++
      (header +: Seq.fill(header.size)("---") +: rows).map(_.mkString("| ", " | ", " |"))
  }

  def render(
      meta: Seq[String],
      ours: Seq[Round],
      theirs: Seq[Round],
      marginPercent: Double = 0,
  ): String = {
    val c = compare(ours.map(_.map(_.score).sum), theirs.map(_.map(_.score).sum))
    val names = corpora(ours)
    // each corpus on its own, from the same runs; only worth listing when there are several
    val perCorpus =
      if (names.size < 2) Seq.empty
      else
        names.zipWithIndex.map { case (name, k) =>
          name -> compare(ours.map(_(k).score), theirs.map(_(k).score))
        }
    val unit = ours.headOption.flatMap(_.headOption).map(_.unit).getOrElse("")
    def interval(c: Comparison) =
      s"${percent(c.mean)} time (95% CI ${percent(c.low)} to ${percent(c.high)})"
    val verdict =
      if (c.low.isNaN)
        s"Ours vs theirs: ${percent(c.mean)} time. One round gives no confidence interval; use more rounds."
      else if (c.high < 1) s"**Ours is faster:** ${interval(c)}."
      else if (c.low > 1) s"**Ours is slower:** ${interval(c)}."
      else s"**No difference detected:** ${interval(c)}."
    def shortVerdict(c: Comparison) =
      if (c.low.isNaN) "one round"
      else if (c.high < 1) "faster"
      else if (c.low > 1) "slower"
      else "no difference"
    val select =
      ours.headOption.flatMap(_.headOption).flatMap(_.params.get("select")).filter(_.nonEmpty)
    val selection = s"Selected units: ${select.getOrElse("all")}"
    val total = select match {
      case Some(select) => s"units selected by `$select`"
      case None => "all units"
    }
    val ignored = ((total -> c) +: perCorpus.map { case (name, pc) => s"corpus `$name`" -> pc })
      .filter { case (_, pc) => withinMargin(pc, marginPercent) }
    val margin =
      if (marginPercent <= 0)
        Seq("No margin of error is set (`--margin`), so no result is ignored.")
      else if (ignored.nonEmpty)
        f"Ignored as within the margin of error of ±$marginPercent%.1f%%, because their whole 95%% CI lies inside it:" +:
          ignored.map { case (name, pc) => s"- $name: ${interval(pc)}" }
      else
        Seq(
          f"No result lies within the margin of error of ±$marginPercent%.1f%%, so none is ignored."
        )
    val rounds = ours.zip(theirs).zip(c.ratios).zipWithIndex.map { case (((o, t), r), i) =>
      val byCorpus = perCorpus.map { case (_, pc) => s" ${percent(pc.ratios(i))} |" }.mkString
      f"| ${i + 1} | ${o.map(_.score).sum}%.3f | ${t.map(_.score).sum}%.3f | ${percent(r)} |$byCorpus"
    }
    val roundsHeader =
      s"| Round | Ours ($unit) | Theirs ($unit) | Ours vs theirs |" +
        perCorpus.map { case (name, _) => s" $name |" }.mkString
    val corpusTable =
      if (perCorpus.isEmpty) Seq.empty
      else
        Seq(
          "",
          "Each corpus on its own, from the same runs:",
          "",
          "| Corpus | Ours vs theirs | 95% CI | Verdict |",
          "|---|---|---|---|",
        ) ++ perCorpus.map { case (name, pc) =>
          s"| $name | ${percent(pc.mean)} | ${percent(pc.low)} to ${percent(pc.high)} | ${shortVerdict(pc)} |"
        }
    // the selection applies to every corpus, so it goes after the last of them
    val lastCorpus = meta.lastIndexWhere(_.startsWith("Corpus"))
    val runLines = meta.patch(if (lastCorpus < 0) meta.size else lastCorpus + 1, Seq(selection), 0)
    val problems = invariantViolations(ours, theirs).map("- **Error:** " + _) ++
      warnings(ours, theirs).map("- Warning: " + _)
    (Seq("# A/B: ours vs theirs", "", "## Run", "") ++
      runLines.map("- " + _) ++
      Seq("", "## Raw results", "") ++
      rawResults(ours, theirs) ++
      Seq("", "## Analysis", "", verdict, "") ++
      Seq(roundsHeader, "|---" * (4 + perCorpus.size) + "|") ++
      rounds ++
      corpusTable ++
      Seq("") ++
      margin ++
      Seq("") ++
      (if (problems.isEmpty) Seq("Node counts are equal on both sides.") else problems))
      .mkString("\n") + "\n"
  }

  /** Writes WORK_DIR/report.md from the runs in it. Returns the report and whether the invariants
    * hold.
    */
  def writeReport(dir: Path, marginPercent: Double): (String, Boolean) = {
    val RunFile = """(ours|theirs)-(\d+)-(\d+)\.json""".r
    val files = Files.list(dir.resolve("runs")).iterator().asScala.toSeq.flatMap { f =>
      f.getFileName.toString match {
        case RunFile(side, round, corpus) => Some((side, round.toInt, corpus.toInt, f))
        case _ => None
      }
    }
    def runs(side: String): Seq[Round] = {
      val rounds = files
        .filter(_._1 == side)
        .groupBy(_._2)
        .toSeq
        .sortBy(_._1)
        .map { case (_, fs) => fs.sortBy(_._3).map(f => readRun(f._4)) }
      require(
        rounds.map(_.size).distinct.size <= 1,
        s"$side: rounds ran different numbers of corpora",
      )
      rounds
    }
    val (ours, theirs) = (runs("ours"), runs("theirs"))
    val meta = Files.readAllLines(dir.resolve("meta.txt")).asScala.toSeq.filter(_.nonEmpty)
    val report = render(meta, ours, theirs, marginPercent)
    Files.writeString(dir.resolve("report.md"), report)
    (report, invariantViolations(ours, theirs).isEmpty)
  }

  /** Usage: AbReport WORK_DIR [MARGIN_PERCENT]. Writes WORK_DIR/report.md, prints it, and fails if
    * an invariant is violated.
    */
  def main(args: Array[String]): Unit = {
    val (report, ok) = writeReport(Paths.get(args(0)), args.lift(1).fold(0.0)(_.toDouble))
    println(report)
    if (!ok) sys.exit(1)
  }
}
