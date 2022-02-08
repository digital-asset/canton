// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

class MetricsFactoryTest extends AnyWordSpec with BaseTest {

  "metrics factory" should {
    "generate valid documentation" in {
      val mf = MetricsFactory.forConfig(MetricsConfig())
      val (participantMetrics, domainMetrics) = mf.metricsDoc()
      domainMetrics should not be empty
      participantMetrics should not be empty
    }
  }

}
