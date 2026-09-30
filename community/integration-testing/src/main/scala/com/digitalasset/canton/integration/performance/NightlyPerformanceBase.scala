// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.performance

import better.files.File
import com.daml.metrics.MetricsFilterConfig
import com.daml.metrics.api.MetricQualification
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.config
import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  LocalParticipantReference,
  NodeReferences,
  ParticipantReference,
  RemoteParticipantReference,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.environment.CantonEnvironment
import com.digitalasset.canton.integration.plugins.{DockerPostgresDumpRestore, UsePostgres}
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentDefinition}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.{MetricsFactoryProvider, MetricsReporterConfig}
import monocle.macros.syntax.lens.*
import org.scalatest.Assertions.fail

import scala.concurrent.duration.Duration

trait NightlyPerformanceBase {
  protected lazy val isNightlyPerformanceBenchmarkRun: Boolean =
    sys.env.contains("NIGHTLY_PERF_RUN")

  protected lazy val nightlyReplayTestsDir: File =
    if (isNightlyPerformanceBenchmarkRun) {
      sys.env
        .get("REPLAY_TESTS_DIR")
        .orElse(sys.props.get("replay-tests.dir"))
        .map(File(_))
        .getOrElse(
          throw new IllegalArgumentException(
            "REPLAY_TESTS_DIR environment variable or 'replay-tests.dir' system property " +
              "must be set during nightly performance runs!"
          )
        )
    } else {
      File("replay")
    }

  protected def nightlyPostgresDumpRestore(
      postgresPlugin: UsePostgres,
      loggerFactory: NamedLoggerFactory,
  ) =
    DockerPostgresDumpRestore(
      postgresPlugin,
      sys.env.get("CURRENT_JOB_NAME").fold("canton-postgres")(prefix => s"$prefix-postgres"),
      loggerFactory,
    )

  protected def testMetricsFactoryIfNightly(env: CantonEnvironment): MetricsFactoryProvider =
    if (isNightlyPerformanceBenchmarkRun) env.metricsRegistry
    else _ => NoOpMetricsFactory

  protected def startMeasuringPartyUpdatesIfNightly(
      participants: NodeReferences[
        ParticipantReference,
        RemoteParticipantReference,
        LocalParticipantReference,
      ],
      metricName: String,
      preAllocatePartyMap: Map[LocalParticipantReference, Set[String]] = Map(),
  )(implicit env: ConsoleEnvironment): Seq[AutoCloseable] =
    if (isNightlyPerformanceBenchmarkRun) {
      // Pre-allocate the parties ( eg. that the performance runners would create at their startup)
      // to be able to use them in measuring
      preAllocatePartyMap.foreach { case (participant, partiesToPreAllocate) =>
        partiesToPreAllocate.foreach(participant.parties.enable(_).discard)
      }

      // start the measurements on all parties
      participants.all.map { p =>
        val parties = p.parties.list().map(_.party)
        p.ledger_api.updates.start_measuring(
          parties.toSet,
          metricName,
        )
      }
    } else Seq.empty

  protected def performanceBenchmarkEnrichmentIfNightly(
      envDef: EnvironmentDefinition
  ): EnvironmentDefinition =
    if (isNightlyPerformanceBenchmarkRun) {
      val csvReportingInterval = sys.env
        .get("CSV_REPORTING_INTERVAL")
        .map(Duration.create)
        .getOrElse(fail("We need CSV_REPORTING_INTERVAL environment variable to be set!"))

      val reporter = sys.env
        .get("METRICS_DIR")
        .map(dir =>
          MetricsReporterConfig.Csv(
            directory = File(dir).toJava,
            interval = config.NonNegativeFiniteDuration.tryFromDuration(csvReportingInterval),
            filters = Seq(
              "daml.participant.console",
              "indexer.events",
              "sequencer-events",
              "daml.sequencer.block.events",
              "canton.performance.failed",
              "canton.performance.latency",
              "daml.participant.phase",
            ).map(c => MetricsFilterConfig(contains = c)),
          )
        )
        .getOrElse(fail("We need METRICS_DIR environment variable to be set!"))

      envDef
        .addConfigTransforms(
          _.focus(_.monitoring.metrics.reporters).modify(rs => reporter +: rs),
          _.focus(_.monitoring.metrics.qualifiers).replace(MetricQualification.All),
          ConfigTransforms.updateAllParticipantConfigs_(
            _.focus(_.parameters.warnIfOverloadedFor).replace(None) // no overloaded warnings
          ),
          _.focus(_.monitoring.logging.api.warnBeyondLoad).replace(
            None
          ), // no overloaded warnings
        )
    } else envDef
}
