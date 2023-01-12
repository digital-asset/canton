// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.metrics

import com.daml.metrics.api.MetricName
import com.digitalasset.canton.metrics.MetricHandle.NoOpMetricsFactory

object DomainTestMetrics
    extends DomainMetrics(
      MetricName("test"),
      NoOpMetricsFactory,
    )
