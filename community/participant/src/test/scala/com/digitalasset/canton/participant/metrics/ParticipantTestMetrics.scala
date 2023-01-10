// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.api.MetricName
import com.daml.metrics.api.testing.InMemoryMetricsFactory
import com.digitalasset.canton.DomainAlias
import io.opentelemetry.sdk.metrics.SdkMeterProvider

object ParticipantTestMetrics
    extends ParticipantMetrics(
      "test_participant",
      MetricName("test"),
      new MetricRegistry(),
      SdkMeterProvider.builder().build().meterBuilder("test").build(),
      InMemoryMetricsFactory,
    ) {

  val domain: SyncDomainMetrics = this.domainMetrics(DomainAlias.tryCreate("test"))

}
