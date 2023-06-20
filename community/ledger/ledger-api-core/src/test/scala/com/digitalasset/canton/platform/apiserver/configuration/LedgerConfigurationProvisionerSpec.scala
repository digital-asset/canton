// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.configuration

import akka.event.NoLogging
import akka.testkit.ExplicitlyTriggeredScheduler
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.resources.ResourceContext
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.SubmissionId
import com.daml.lf.data.Time.Timestamp
import com.daml.tracing.NoOpTelemetry
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ledger.api.SubmissionIdGenerator
import com.digitalasset.canton.ledger.configuration.{Configuration, LedgerTimeModel}
import com.digitalasset.canton.ledger.participant.state.v2 as state
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.configuration.InitialLedgerConfiguration
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.concurrent.Eventually
import org.scalatest.wordspec.AsyncWordSpec

import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.DurationInt

final class LedgerConfigurationProvisionerSpec
    extends AsyncWordSpec
    with Eventually
    with AkkaBeforeAndAfterAll
    with BaseTest {

  private implicit val resourceContext: ResourceContext = ResourceContext(executionContext)
  private implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting

  override implicit val patienceConfig: PatienceConfig =
    super.patienceConfig.copy(timeout = 1.second)

  "provisioning a ledger configuration" should {
    "write a ledger configuration to the index if one is not provided" in {
      val configurationToSubmit =
        Configuration(
          generation = 1,
          timeModel = LedgerTimeModel.reasonableDefault,
          maxDeduplicationDuration = Duration.ofDays(1),
        )
      val initialLedgerConfiguration = InitialLedgerConfiguration(
        maxDeduplicationDuration = configurationToSubmit.maxDeduplicationDuration,
        avgTransactionLatency = configurationToSubmit.timeModel.avgTransactionLatency,
        minSkew = configurationToSubmit.timeModel.minSkew,
        maxSkew = configurationToSubmit.timeModel.maxSkew,
        delayBeforeSubmitting = Duration.ofMillis(100),
      )
      val submissionId = Ref.SubmissionId.assertFromString("the submission ID")

      val ledgerConfigurationSubscription = new LedgerConfigurationSubscription {
        override def latestConfiguration(): Option[Configuration] = None
      }
      val writeService = mock[state.WriteConfigService]
      val timeProvider = TimeProvider.Constant(Instant.EPOCH)
      val submissionIdGenerator = new SubmissionIdGenerator {
        override def generate(): SubmissionId = submissionId
      }
      val scheduler = new ExplicitlyTriggeredScheduler(null, NoLogging, null)

      new LedgerConfigurationProvisioner(
        ledgerConfigurationSubscription = ledgerConfigurationSubscription,
        writeService = writeService,
        timeProvider = timeProvider,
        submissionIdGenerator = submissionIdGenerator,
        scheduler = scheduler,
        telemetry = NoOpTelemetry,
        loggerFactory = loggerFactory,
      ).submit(initialLedgerConfiguration)
        .use { _ =>
          verify(writeService, never).submitConfiguration(
            any[Timestamp],
            any[Ref.SubmissionId],
            any[Configuration],
          )(any[TraceContext])

          scheduler.timePasses(100.millis)
          eventually {
            verify(writeService).submitConfiguration(
              eqTo(Timestamp.assertFromInstant(timeProvider.getCurrentTime.plusSeconds(60))),
              eqTo(submissionId),
              eqTo(configurationToSubmit),
            )(any[TraceContext])
          }
          succeed
        }
    }

    "not write a configuration if one is provided" in {
      val currentConfiguration =
        Configuration(6, LedgerTimeModel.reasonableDefault, Duration.ofHours(12))
      val initialLedgerConfiguration = InitialLedgerConfiguration(
        maxDeduplicationDuration = Duration.ofDays(1),
        avgTransactionLatency = LedgerTimeModel.reasonableDefault.avgTransactionLatency,
        minSkew = LedgerTimeModel.reasonableDefault.minSkew,
        maxSkew = LedgerTimeModel.reasonableDefault.maxSkew,
        delayBeforeSubmitting = Duration.ofMillis(100),
      )

      val ledgerConfigurationSubscription = new LedgerConfigurationSubscription {
        override def latestConfiguration(): Option[Configuration] = Some(currentConfiguration)
      }
      val writeService = mock[state.WriteConfigService]
      val timeProvider = TimeProvider.Constant(Instant.EPOCH)
      val scheduler = new ExplicitlyTriggeredScheduler(null, NoLogging, null)

      new LedgerConfigurationProvisioner(
        ledgerConfigurationSubscription = ledgerConfigurationSubscription,
        writeService = writeService,
        timeProvider = timeProvider,
        submissionIdGenerator = SubmissionIdGenerator.Random,
        scheduler = scheduler,
        telemetry = NoOpTelemetry,
        loggerFactory = loggerFactory,
      ).submit(initialLedgerConfiguration)
        .use { _ =>
          scheduler.timePasses(1.second)
          verify(writeService, after(100).never()).submitConfiguration(
            any[Timestamp],
            any[Ref.SubmissionId],
            any[Configuration],
          )(any[TraceContext])
          succeed
        }
    }
  }

  "not write a configuration if one is provided within the time window" in {
    val eventualConfiguration =
      Configuration(8, LedgerTimeModel.reasonableDefault, Duration.ofDays(3))
    val initialLedgerConfiguration = InitialLedgerConfiguration(
      avgTransactionLatency = LedgerTimeModel.reasonableDefault.avgTransactionLatency,
      minSkew = LedgerTimeModel.reasonableDefault.minSkew,
      maxSkew = LedgerTimeModel.reasonableDefault.maxSkew,
      maxDeduplicationDuration = Duration.ofDays(1),
      delayBeforeSubmitting = Duration.ofSeconds(3),
    )

    val currentConfiguration = new AtomicReference[Option[Configuration]](None)
    val ledgerConfigurationSubscription = new LedgerConfigurationSubscription {
      override def latestConfiguration(): Option[Configuration] = currentConfiguration.get
    }
    val writeService = mock[state.WriteConfigService]
    val timeProvider = TimeProvider.Constant(Instant.EPOCH)
    val scheduler = new ExplicitlyTriggeredScheduler(null, NoLogging, null)

    new LedgerConfigurationProvisioner(
      ledgerConfigurationSubscription = ledgerConfigurationSubscription,
      writeService = writeService,
      timeProvider = timeProvider,
      submissionIdGenerator = SubmissionIdGenerator.Random,
      scheduler = scheduler,
      telemetry = NoOpTelemetry,
      loggerFactory = loggerFactory,
    ).submit(initialLedgerConfiguration)
      .use { _ =>
        scheduler.scheduleOnce(
          2.seconds,
          new Runnable {
            override def run(): Unit = {
              currentConfiguration.set(Some(eventualConfiguration))
            }
          },
        )
        scheduler.timePasses(5.seconds)
        verify(writeService, after(100).never()).submitConfiguration(
          any[Timestamp],
          any[Ref.SubmissionId],
          any[Configuration],
        )(any[TraceContext])
        succeed
      }
  }

  "not write a configuration if the provisioner is shut down" in {
    val initialLedgerConfiguration = InitialLedgerConfiguration(
      avgTransactionLatency = LedgerTimeModel.reasonableDefault.avgTransactionLatency,
      minSkew = LedgerTimeModel.reasonableDefault.minSkew,
      maxSkew = LedgerTimeModel.reasonableDefault.maxSkew,
      maxDeduplicationDuration = Duration.ofDays(1),
      delayBeforeSubmitting = Duration.ofSeconds(1),
    )

    val ledgerConfigurationSubscription = new LedgerConfigurationSubscription {
      override def latestConfiguration(): Option[Configuration] = None
    }
    val writeService = mock[state.WriteConfigService]
    val timeProvider = TimeProvider.Constant(Instant.EPOCH)
    val scheduler = new ExplicitlyTriggeredScheduler(null, NoLogging, null)

    val resource = new LedgerConfigurationProvisioner(
      ledgerConfigurationSubscription = ledgerConfigurationSubscription,
      writeService = writeService,
      timeProvider = timeProvider,
      submissionIdGenerator = SubmissionIdGenerator.Random,
      scheduler = scheduler,
      telemetry = NoOpTelemetry,
      loggerFactory = loggerFactory,
    ).submit(initialLedgerConfiguration).acquire()

    resource.asFuture
      .flatMap { _ => resource.release() }
      .map { _ =>
        scheduler.timePasses(1.second)
        verify(writeService, after(100).never()).submitConfiguration(
          any[Timestamp],
          any[Ref.SubmissionId],
          any[Configuration],
        )(any[TraceContext])
        succeed
      }
  }
}
