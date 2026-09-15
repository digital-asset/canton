package com.digitalasset.canton.platform.component

import java.sql.Timestamp
import java.util.concurrent.ConcurrentHashMap
import java.nio.file.Paths
import java.io.PrintWriter
import scala.jdk.CollectionConverters.*

class Report(
    private val timedData: ConcurrentHashMap[Timestamp, Map[String, Float]] =
      new ConcurrentHashMap()
) {

  def data: Map[String, Vector[(Timestamp, Float)]] =
    timedData.asScala.foldLeft(Map.empty[String, Vector[(Timestamp, Float)]]) {
      case (acc, (ts, data)) =>
        data
          .map { case (key, value) =>
            key -> (ts, value)
          }
          .foldLeft(acc) {
            case (innerAcc, (key, (ts, value))) => {
              val values = innerAcc.getOrElse(key, Vector.empty[(Timestamp, Float)])
              innerAcc.updated(key, values.appended((ts, value)))
            }
          }
    }

  def add(key: String, value: Float): Unit =
    add(key, new Timestamp(System.currentTimeMillis()), value)

  def add(key: String, timestamp: Timestamp, value: Float): Unit =
    timedData.merge(timestamp, Map(key -> value), (oldValue, _) => oldValue.updated(key, value))

  def get(key: String): Option[Vector[(Timestamp, Float)]] =
    data.get(key)

  def sum(key: String): Option[Float] =
    get(key).map(_.map(_._2).sum)

  def average(key: String): Option[Float] =
    get(key).flatMap(values =>
      if (values.nonEmpty) Some(values.map(_._2).sum / values.size) else None
    )

  def percentile(ratio: Int)(key: String): Option[Float] =
    get(key).map { values =>
      val sorted = values.map(_._2).sorted
      val index = Math.ceil((ratio / 100.0) * sorted.size).toInt - 1
      sorted(index)
    }

  def prettyPrint(key: String): String =
    get(key) match {
      case Some(values) =>
        val sumValue = sum(key).getOrElse(0.0f)
        val avgValue = if (values.nonEmpty) sumValue / values.size else 0.0f
        val p50 = percentile(50)(key).getOrElse(0.0f)
        val p90 = percentile(90)(key).getOrElse(0.0f)
        val p99 = percentile(99)(key).getOrElse(0.0f)
        s"Key: $key, Count: ${values.size}, Sum: $sumValue, Average: $avgValue, P50: $p50, P90: $p90, P99: $p99"
      case None => s"Key: $key not found."
    }

  def show(): String =
    data.keys.map(prettyPrint).mkString("\n")

  def toSnakeCase(string: String): String =
    string.replaceAll("(\\w*)\\W*(\\w+)", "$1_$2").toLowerCase

  def saveAsCsv(filenameSuffix: String = "lapi_perf_test", rootPath: String = "/tmp"): Unit = {
    val target =
      Paths.get(rootPath, s"report_$filenameSuffix.csv").toAbsolutePath().toFile()
    val writer = new PrintWriter(target)
    try {
      writer.println("timestamp,metric,value")
      for {
        (ts, row) <- timedData.asScala
        (key, value) <- row
      } yield {
        val snakeCaseKey = toSnakeCase(key)
        if (!value.isNaN) {
          writer.println(s"${ts.toInstant().getEpochSecond()},$snakeCaseKey,$value")
        }
      }
    } finally {
      writer.close()
    }
  }
}

object Report {
  def apply(): Report = new Report(
    new ConcurrentHashMap[Timestamp, Map[String, Float]]()
  )
}
