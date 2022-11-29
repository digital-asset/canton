// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.api.MetricName
import com.daml.metrics.api.opentelemetry.OpenTelemetryFactory
import com.daml.metrics.grpc.DamlGrpcServerMetrics
import io.opentelemetry.sdk.metrics.SdkMeterProvider

object DomainTestMetrics
    extends DomainMetrics(
      MetricName("test"),
      new MetricRegistry(),
      new DamlGrpcServerMetrics(
        new OpenTelemetryFactory(
          SdkMeterProvider.builder().build().meterBuilder("test").build()
        ),
        "test",
      ),
    ) {}
