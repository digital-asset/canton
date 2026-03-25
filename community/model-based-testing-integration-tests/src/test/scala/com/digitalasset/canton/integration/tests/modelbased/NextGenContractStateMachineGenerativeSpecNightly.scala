// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.modelbased

import com.digitalasset.daml.lf.transaction.NextGenContractStateMachineGenerativeSpec

import scala.concurrent.duration.DurationInt

class NextGenContractStateMachineGenerativeSpecLarge
    extends NextGenContractStateMachineGenerativeSpec(
      sampleSize = 30,
      maxSamples = Int.MaxValue,
      timeout = 10.minutes,
      sampleBufferSize = 1000,
      generatorParallelism = Runtime.getRuntime.availableProcessors(),
    )
