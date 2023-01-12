// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.api.MetricName
import com.daml.metrics.api.opentelemetry.OpenTelemetryFactory
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.metrics.MetricHandle.CantonDropwizardMetricsFactory
import io.opentelemetry.sdk.metrics.SdkMeterProvider

object ParticipantTestMetrics
    extends ParticipantMetrics(
      "test_participant",
      MetricName("test"),
      new CantonDropwizardMetricsFactory(new MetricRegistry()),
      new OpenTelemetryFactory(SdkMeterProvider.builder().build().meterBuilder("test").build()),
    ) {

  val domain: SyncDomainMetrics = this.domainMetrics(DomainAlias.tryCreate("test"))

}
