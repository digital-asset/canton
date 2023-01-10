// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.api.MetricName

object CommonMockMetrics {

  private val prefix = MetricName("test")
  private lazy val registry = new MetricRegistry()

  object sequencerClient extends SequencerClientMetrics(prefix, registry)
  object dbStorage extends DbStorageMetrics(prefix, registry)

}
