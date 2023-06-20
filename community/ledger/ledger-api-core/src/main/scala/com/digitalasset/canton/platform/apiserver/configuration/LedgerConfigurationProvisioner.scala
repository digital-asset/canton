// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.configuration

import akka.actor.Scheduler
import com.daml.api.util.TimeProvider
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.entries.LoggingEntry
import com.daml.tracing.{SpanKind, SpanName, Telemetry}
import com.digitalasset.canton.ledger.api.SubmissionIdGenerator
import com.digitalasset.canton.ledger.configuration.Configuration
import com.digitalasset.canton.ledger.participant.state.v2 as state
import com.digitalasset.canton.logging.LoggingContextWithTrace.{
  implicitExtractTraceContext,
  withEnrichedLoggingContext,
}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.configuration.InitialLedgerConfiguration
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration as ScalaDuration
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.{Failure, Success}

/** Writes a default ledger configuration to the ledger, after a configurable delay. The
  * configuration is only written if the ledger does not already have a configuration.
  *
  * Used by the participant to initialize a new ledger.
  */
final class LedgerConfigurationProvisioner(
    ledgerConfigurationSubscription: LedgerConfigurationSubscription,
    writeService: state.WriteConfigService,
    timeProvider: TimeProvider,
    submissionIdGenerator: SubmissionIdGenerator,
    scheduler: Scheduler,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  /** Submits the initial configuration after the specified delay.
    *
    * There are several reasons why the change could be rejected:
    *
    *   - The participant is not authorized to set the configuration
    *   - There already is a configuration, it just didn't appear in the index yet
    *
    * This method therefore does not try to re-submit the initial configuration in case of failure.
    */
  def submit(
      initialLedgerConfiguration: InitialLedgerConfiguration
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
  ): ResourceOwner[Unit] =
    ResourceOwner
      .forCancellable(() =>
        scheduler.scheduleOnce(
          ScalaDuration.fromNanos(initialLedgerConfiguration.delayBeforeSubmitting.toNanos),
          new Runnable {
            override def run(): Unit = {
              submitImmediately(initialLedgerConfiguration.toConfiguration)
            }
          },
        )
      )
      .map(_ => ())

  private def submitImmediately(
      initialConfiguration: Configuration
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContextWithTrace): Unit =
    if (ledgerConfigurationSubscription.latestConfiguration().isEmpty) {
      val submissionId = submissionIdGenerator.generate()
      withEnrichedLoggingContext(("submissionId" -> submissionId): LoggingEntry) {
        implicit loggingContext =>
          logger.info("No ledger configuration found, submitting an initial configuration.")
          telemetry
            .runFutureInSpan(
              SpanName.LedgerConfigProviderInitialConfig,
              SpanKind.Internal,
            ) { implicit telemetryContext =>
              val maxRecordTime =
                Timestamp.assertFromInstant(timeProvider.getCurrentTime.plusSeconds(60))
              writeService
                .submitConfiguration(maxRecordTime, submissionId, initialConfiguration)(
                  TraceContext.fromDamlTelemetryContext(telemetryContext)
                )
                .asScala
            }
            .onComplete {
              case Success(state.SubmissionResult.Acknowledged) =>
                logger.info("Initial configuration submission was successful.")
              case Success(result: state.SubmissionResult.SynchronousError) =>
                withEnrichedLoggingContext(("error" -> result): LoggingEntry) {
                  implicit loggingContext =>
                    logger.warn("Initial configuration submission failed.")
                }
              case Failure(exception) =>
                logger.error("Initial configuration submission failed.", exception)
            }
      }
    }
}
