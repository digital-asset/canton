// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.api.MetricName

object DomainTestMetrics extends DomainMetrics(MetricName("test"), new MetricRegistry()) {}
