// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.MetricName
import com.digitalasset.canton.DomainAlias
import io.opentelemetry.sdk.metrics.SdkMeterProvider

object ParticipantTestMetrics
    extends ParticipantMetrics(
      MetricName("test"),
      new MetricRegistry(),
      SdkMeterProvider.builder().build().meterBuilder("test").build(),
    ) {

  val domain: SyncDomainMetrics = this.domainMetrics(DomainAlias.tryCreate("test"))

}
