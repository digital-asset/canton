// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import better.files.File
import cats.data.NonEmptyList
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.admin.api.client.data.{CantonStatus, NodeStatus}
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeInt,
  NonNegativeLong,
  Port,
  PositiveInt,
}
import com.digitalasset.canton.config.{NonNegativeDuration, Password}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{DelayUtil, MonadUtil}
import io.circe.{Encoder, Json, KeyEncoder, jawn}
import io.opentelemetry.exporter.internal.otlp.metrics.ResourceMetricsMarshaler
import io.opentelemetry.sdk.metrics.data.MetricData

import java.io.ByteArrayOutputStream
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object CantonHealthAdministrationEncoders {

  /** Wraps the standardized log writer from OpenTelemetry, that outputs the metrics as JSON Source:
    * https://github.com/open-telemetry/opentelemetry-java/blob/main/exporters/logging-otlp/src/main/java/io/opentelemetry/exporter/logging/otlp/OtlpJsonLoggingMetricExporter.java
    * The encoder is not the most efficient as we first use the OpenTelemetry JSON serializer to
    * write as a String, and then use the Circe Jawn decoder to transform the string into a
    * circe.Json object. This is fine as the encoder is used only for on demand health dumps.
    */
  implicit val openTelemetryMetricDataEncoder: Encoder[Seq[MetricData]] =
    Encoder.encodeSeq[Json].contramap[Seq[MetricData]] { metrics =>
      val resourceMetrics = ResourceMetricsMarshaler.create(metrics.asJava)
      resourceMetrics.toSeq.map { resource =>
        val byteArrayOutputStream = new ByteArrayOutputStream()
        resource.writeJsonTo(byteArrayOutputStream)
        jawn
          .decode[Json](byteArrayOutputStream.toString)
          .fold(
            error => Json.fromString(s"Failed to decode metrics: $error"),
            identity,
          )
      }
    }

  implicit val uniqueIdEncoder: Encoder[UniqueIdentifier] =
    Encoder.encodeString.contramap(_.toString)
  implicit val traceElemEncoder: Encoder[StackTraceElement] =
    Encoder.encodeString.contramap(_.toString)
  implicit val threadKeyEncoder: KeyEncoder[Thread] = (thread: Thread) => thread.getName

  implicit val physicalSynchronizerIdEncoder: Encoder[PhysicalSynchronizerId] =
    Encoder.encodeString.contramap(_.toString)

  implicit val synchronizerIdKeyEncoder: KeyEncoder[SynchronizerId] = (ref: SynchronizerId) =>
    ref.toString
  implicit val physicalSynchronizerIdKeyEncoder: KeyEncoder[PhysicalSynchronizerId] =
    (ref: PhysicalSynchronizerId) => ref.toString

  implicit val encodePort: Encoder[Port] = Encoder.encodeInt.contramap[Port](_.unwrap)

  // We do not want to serialize the password to JSON, e.g., as part of a config dump.
  implicit val encoder: Encoder[Password] = Encoder.encodeString.contramap(_ => "****")

  implicit val nonNegativeIntEncoder: Encoder[NonNegativeInt] =
    Encoder.encodeInt.contramap(_.unwrap)

  implicit val nonNegativeLongEncoder: Encoder[NonNegativeLong] =
    Encoder.encodeLong.contramap(_.unwrap)

  implicit val positiveIntEncoder: Encoder[PositiveInt] = Encoder.encodeInt.contramap(_.unwrap)
}

object CantonHealthAdministration {
  def defaultHealthDumpName: File = {
    // Replace ':' in the timestamp as they are forbidden on windows
    val name = s"canton-dump-${Instant.now().toString.replace(':', '-')}.zip"
    File(name)
  }

  private def statusMap[A <: InstanceReference](
      nodes: NodeReferences[A, ?, ?]
  ): Map[String, () => NodeStatus[A#Status]] =
    nodes.all.map(node => node.name -> (() => node.health.status)).toMap
}

class CantonHealthAdministration(protected val consoleEnv: ConsoleEnvironment)
    extends Helpful
    with NamedLogging
    with NoTracing {
  import CantonHealthAdministration.*

  implicit private val ec: ExecutionContext = consoleEnv.environment.executionContext
  override val loggerFactory: NamedLoggerFactory = consoleEnv.environment.loggerFactory

  @Help.Summary("Aggregate status info of all participants, sequencers, and mediators")
  def status(): CantonStatus =
    CantonStatus.getStatus(
      statusMap(consoleEnv.sequencers),
      statusMap(consoleEnv.mediators),
      statusMap(consoleEnv.participants),
    )

  @Help.Summary("Collect Canton system information to help diagnose issues")
  @Help.Description(
    """Generates a comprehensive health report for the local Canton process and any connected
      |remote nodes.
      |
      |Parameters:
      |- outputFile: Specifies the file path to save the report. If not set, a default path is
      |  used.
      |- timeout: Sets a custom timeout for gathering data, useful for large reports from slow
      |  remote nodes.
      |- chunkSize: Adjusts the data stream chunk size from remote nodes. Use this to prevent
      |  gRPC errors related to 'max inbound message size'
      |"""
  )
  def dump(
      outputFile: String = CantonHealthAdministration.defaultHealthDumpName.canonicalPath,
      timeout: NonNegativeDuration = consoleEnv.commandTimeouts.ledgerCommand,
      chunkSize: Option[Int] = None,
  ): String = {
    val remoteDumps = consoleEnv.nodes.remote.toList.parTraverse { n =>
      Future {
        n.health.dump(
          File.newTemporaryFile(s"remote-${n.name}-").name,
          timeout,
          chunkSize,
        )
      }
    }

    // Try to get a local dump by going through the local nodes and returning the first one that succeeds
    def getLocalDump(nodes: NonEmptyList[InstanceReference]): Future[String] =
      Future {
        nodes.head.health.dump(
          File.newTemporaryFile(s"local-").canonicalPath,
          timeout,
          chunkSize,
        )
      }.recoverWith { case NonFatal(e) =>
        NonEmptyList.fromList(nodes.tail) match {
          case Some(tail) =>
            logger.info(
              s"Could not get health dump from ${nodes.head.name}, trying the next local node",
              e,
            )
            getLocalDump(tail)
          case None =>
            logger.debug(
              s"Could not get health dumps from any of local nodes: $nodes",
              e,
            )
            Future.failed(e)
        }
      }

    val localDump = NonEmptyList
      // The sorting is not necessary but makes testing easier
      .fromList(consoleEnv.nodes.local.toList.sortBy(_.name))
      .traverse(getLocalDump)
      .map(_.toList)

    val isTimedOut = new AtomicBoolean(false)

    def awaitFileMaterialization(path: String, retries: Int = 20): Future[File] = {
      val file = File(path)
      if (file.exists) {
        Future.successful(file)
      } else if (retries <= 0) {
        Future.failed(
          new java.nio.file.NoSuchFileException(s"File $path has not been materialized in time.")
        )
      } else {
        DelayUtil.delay(100.millis).flatMap(_ => awaitFileMaterialization(path, retries - 1))
      }
    }

    consoleEnv.run {
      val dumpFutures = List(remoteDumps, localDump).flatSequence

      val zippedHealthDump = (for {
        allDumps <- dumpFutures
        readyFiles <- MonadUtil.sequentialTraverse(allDumps)(awaitFileMaterialization(_))
      } yield {
        val timeOutAwareIterator = readyFiles.iterator.map { file =>
          if (isTimedOut.get()) {
            // Need to throw to cancel on ongoing zipIn
            throw new TimeoutException(
              s"Aborted zipping: Timeout occurred mid-zip for ${file.pathAsString}"
            )
          }
          file
        }
        File(outputFile).zipIn(timeOutAwareIterator).pathAsString
      }).thereafter { _ =>
        dumpFutures.value.foreach {
          case Success(paths) =>
            paths.foreach(path => File(path).delete(swallowIOExceptions = true))
          case Failure(_) => // Nothing to clean up
        }
      }

      Try(Await.result(zippedHealthDump, timeout.duration)) match {
        case Success(result) => CommandSuccessful(result)
        case Failure(e: TimeoutException) =>
          // Captures the Await.result TimeoutException
          isTimedOut.set(true)
          CommandErrors.ConsoleTimeout.Error(timeout.asJavaApproximation)
        case Failure(exception) =>
          // Captures NoSuchFileException
          CommandErrors.CommandInternalError.ErrorWithException(exception)
      }
    }
  }
}
