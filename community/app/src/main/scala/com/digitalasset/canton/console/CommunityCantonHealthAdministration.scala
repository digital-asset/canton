// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import better.files.File
import com.codahale.metrics
import com.digitalasset.canton.admin.api.client.data.{CantonStatus, CommunityCantonStatus}
import com.digitalasset.canton.health.admin.data.NodeStatus
import com.digitalasset.canton.metrics.MetricsSnapshot
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.version.ReleaseVersion
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax._
import io.circe.{Encoder, KeyEncoder}

import java.time.Instant
import scala.annotation.nowarn
import scala.concurrent.duration.TimeUnit

object CantonHealthAdministrationEncoders {
  implicit val timeUnitEncoder: Encoder[TimeUnit] = Encoder.encodeString.contramap(_.toString)

  implicit val snapshotEncoder: Encoder[metrics.Snapshot] =
    Encoder.forProduct4("mean", "std-dev", "p95", "median") { snapshot =>
      def toMs(nanos: Double): Double = nanos / 1e6
      (
        toMs(snapshot.getMean),
        toMs(snapshot.getStdDev),
        toMs(snapshot.get95thPercentile()),
        toMs(snapshot.getMedian),
      )
    }

  implicit val counterEncoder: Encoder[metrics.Counter] = Encoder.forProduct1("count") { counter =>
    counter.getCount
  }
  implicit val gaugeEncoder: Encoder[metrics.Gauge[_]] = Encoder.forProduct1("gauge") { gauge =>
    gauge.getValue.toString
  }
  implicit val histoEncoder: Encoder[metrics.Histogram] =
    Encoder.forProduct1("hist")(_.getSnapshot)

  implicit val meterEncoder: Encoder[metrics.Meter] =
    Encoder.forProduct3("count", "one-min-rate", "five-min-rate") { meter =>
      (meter.getCount, meter.getFiveMinuteRate, meter.getOneMinuteRate)
    }

  implicit val timerEncoder: Encoder[metrics.Timer] =
    Encoder.forProduct4("count", "one-min-rate", "five-min-rate", "hist") { timer =>
      (timer.getCount, timer.getFiveMinuteRate, timer.getOneMinuteRate, timer.getSnapshot)
    }

  implicit val traceElemEncoder: Encoder[StackTraceElement] =
    Encoder.encodeString.contramap(_.toString)
  implicit val threadKeyEncoder: KeyEncoder[Thread] = (thread: Thread) => thread.getName

  implicit val participantRefKeyEncoder: KeyEncoder[ParticipantReference] =
    (ref: InstanceReference) => ref.name
  implicit val domainRefKeyEncoder: KeyEncoder[DomainReference] = (ref: InstanceReference) =>
    ref.name
  implicit val domainIdEncoder: KeyEncoder[DomainId] = (ref: DomainId) => ref.toString
}

trait CantonHealthAdministration[Status <: CantonStatus] extends Helpful {

  protected implicit val statusEncoder: Encoder[Status]
  protected val consoleEnv: ConsoleEnvironment

  def status(): Status

  @Help.Summary("Generate and write a dump of Canton's state for a bug report")
  @nowarn("cat=unused")
  @nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
  def dump(): String = {
    import io.circe.generic.auto._
    import CantonHealthAdministrationEncoders._

    case class EnvironmentInfo(os: String, javaVersion: String)

    case class CantonDump(
        releaseVersion: String,
        environment: EnvironmentInfo,
        config: String,
        status: Status,
        metrics: MetricsSnapshot,
        traces: Map[Thread, Array[StackTraceElement]],
    )

    val javaVersion = System.getProperty("java.version")
    val cantonVersion = ReleaseVersion.current.fullVersion
    val env = EnvironmentInfo(sys.props("os.name"), javaVersion)

    val metricsSnapshot = MetricsSnapshot(consoleEnv.environment.metricsFactory.registry)
    val config = consoleEnv.environment.config.dumpString

    val traces = {
      import scala.jdk.CollectionConverters._
      Thread.getAllStackTraces.asScala.toMap
    }

    val dump = CantonDump(cantonVersion, env, config, status(), metricsSnapshot, traces)

    // Replace ':' in the timestamp as they are forbidden on windows
    val ts = Instant.now().toString.replace(':', '-')
    val filename = s"canton-dump-$ts.zip"
    val logFile = File(sys.env.getOrElse("LOG_FILE_NAME", "log/canton.log"))
    val logLastErrorsFile = File(
      sys.env.getOrElse("LOG_LAST_ERRORS_FILE_NAME", "log/canton_errors.log")
    )

    File.usingTemporaryFile("canton-dump-", ".json") { tmpFile =>
      tmpFile.append(dump.asJson.spaces2)
      val files = Iterator(logFile, logLastErrorsFile, tmpFile).filter(_.nonEmpty)
      File(filename).zipIn(files)
    }

    filename
  }

  protected def splitSuccessfulAndFailedStatus[Ref <: InstanceReference](
      refs: Seq[Ref]
  ): (Map[Ref, Ref#Status], Map[Ref, NodeStatus.Failure]) = {
    val map: Map[Ref, NodeStatus[Ref#Status]] =
      refs.map(s => s -> s.health.status).toMap
    val status: Map[Ref, Ref#Status] =
      map.collect { case (n, NodeStatus.Success(status)) =>
        n -> status
      }
    val unreachable: Map[Ref, NodeStatus.Failure] =
      map.collect {
        case (s, entry: NodeStatus.Failure) => s -> entry
        case (s, _: NodeStatus.NotInitialized) =>
          s -> NodeStatus.Failure(s"$s has not been initialized")
      }
    (status, unreachable)
  }
}

@nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
class CommunityCantonHealthAdministration(override val consoleEnv: ConsoleEnvironment)
    extends CantonHealthAdministration[CommunityCantonStatus] {

  override protected implicit val statusEncoder: Encoder[CommunityCantonStatus] = {
    import io.circe.generic.auto._
    import CantonHealthAdministrationEncoders._
    deriveEncoder[CommunityCantonStatus]
  }

  @Help.Summary("Aggregate status info of all participants and domains")
  def status(): CommunityCantonStatus = {
    val (domainStatus, unreachableDomains) = splitSuccessfulAndFailedStatus(consoleEnv.domains.all)
    val (participantStatus, unreachableParticipants) = splitSuccessfulAndFailedStatus(
      consoleEnv.participants.all
    )
    CommunityCantonStatus(
      domainStatus,
      unreachableDomains,
      participantStatus,
      unreachableParticipants,
    )
  }
}
