// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.metrics.InstrumentType
import io.opentelemetry.sdk.metrics.`export`.MetricExporter
import io.opentelemetry.sdk.metrics.data.{AggregationTemporality, MetricData}

import java.io.{BufferedWriter, File, FileWriter}
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, TimeUnit}
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

class CsvReporter(
    config: MetricsReporterConfig.Csv,
    val loggerFactory: NamedLoggerFactory,
) extends MetricExporter
    with NamedLogging
    with NoTracing {

  private val singleThreadExecutor = Executors.newSingleThreadExecutor()
  private val running = new AtomicBoolean(true)
  private val files = new TrieMap[String, (FileWriter, BufferedWriter)]

  def getAggregationTemporality(instrumentType: InstrumentType): AggregationTemporality =
    AggregationTemporality.CUMULATIVE

  override def flush(): CompletableResultCode =
    runSequentialInBackground {
      files.foreach { case (_, (_, bufferedWriter)) =>
        bufferedWriter.flush()
      }
    }

  override def shutdown(): CompletableResultCode =
    if (running.compareAndSet(true, false)) {
      logger.info("Stopping csv reporter")
      Try {
        singleThreadExecutor.shutdown()
        if (!singleThreadExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
          val stillRunning = singleThreadExecutor.shutdownNow()
          logger.warn(
            s"Failed to close csv metrics, still have ${stillRunning.size()} pending tasks"
          )
        }
        files.foreach { case (_, (file, bufferedWriter)) =>
          bufferedWriter.close()
          file.close()
        }
      } match {
        case Success(_) => CompletableResultCode.ofSuccess()
        case Failure(ex) =>
          val runnables = singleThreadExecutor.shutdownNow()
          logger.warn(
            s"Failed to close csv metrics, still have ${runnables.size()} pending tasks",
            ex,
          )
          CompletableResultCode.ofSuccess()

      }
    } else CompletableResultCode.ofSuccess()

  def `export`(metrics: util.Collection[MetricData]): CompletableResultCode =
    if (running.get()) {
      val ts = CantonTimestamp.now()
      runSequentialInBackground(
        MetricValue
          .allFromMetricData(metrics.asScala)
          .foreach { case (value, metadata) =>
            writeRow(ts, value, metadata)
          }
      )
    } else CompletableResultCode.ofSuccess()

  private def runSequentialInBackground(res: => Unit): CompletableResultCode = if (running.get()) {
    val result = new CompletableResultCode()
    Try {
      singleThreadExecutor.execute { () =>
        Try(res) match {
          case Success(_) => result.succeed().discard
          case Failure(ex) =>
            logger.warn("Failed to write metrics to csv file. Turning myself off", ex)
            running.set(false)
            result.failExceptionally(ex).discard
        }
      }
    } match {
      case Success(_) =>
      case Failure(ex) =>
        logger.warn("Failed to submit task to write metrics to csv file. Turning myself off", ex)
        running.set(false)
        result.succeed().discard // graceful shutdown
    }
    result
  } else CompletableResultCode.ofSuccess()

  private def writeRow(
      ts: CantonTimestamp,
      value: MetricValue,
      data: MetricData,
  ): Unit = if (running.get()) {
    def stripNamespace(s: String): String = {
      val parts = s.split("::")
      if (parts.length > 1) parts.head else s
    }

    val knownKeys = config.contextKeys.filter(value.attributes.contains)
    val unknownKeys = value.attributes.keys.filterNot(config.contextKeys.contains).toSeq.sorted
    val prefix = knownKeys
      .flatMap { key =>
        // remove namespaces from file names ...
        value.attributes.get(key).toList.map(stripNamespace)
      }
      .mkString(".")

    val name =
      ((if (prefix.isEmpty) Seq.empty else Seq(prefix)) ++ Seq(data.getName, "csv")).mkString(".")

    val (_, bufferedWriter) = files.getOrElseUpdate(
      name, {
        val file = new File(config.directory, name)
        logger.info(
          s"Creating new csv file $file for metric using keys=$knownKeys from attributes=${value.attributes.keys}"
        )
        file.getParentFile.mkdirs()
        val writer = new FileWriter(file, true)
        val bufferedWriter = new BufferedWriter(writer)
        if (file.length() == 0) {
          bufferedWriter.append(value.toCsvHeader(data))
          bufferedWriter.newLine()
        }
        (writer, bufferedWriter)
      },
    )
    bufferedWriter.append(value.toCsvRow(ts, data, unknownKeys))
    bufferedWriter.newLine()
  }
}
