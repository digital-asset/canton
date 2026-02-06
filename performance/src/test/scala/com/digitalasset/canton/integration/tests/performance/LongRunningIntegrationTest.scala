// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.performance

import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.performance.scenarios.LongRunning

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

final class LongRunningSingleSynchronizerIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numSequencers = 1,
        numMediators = 1,
        numParticipants = 1,
        withRemote = false,
      )
      .withManualStart

  "long-running startup script allows to start (single synchronizer)" in { implicit env =>
    val mc = LongRunning
      .startup(
        totalCycles = 10,
        reportFrequency = 1,
        numAssetsPerIssuer = 10,
        enablePruning = false,
        singleSynchronizer = true,
      )
      ._1
    Await.result(mc.isDoneF, 1.minute)
  }
}

sealed trait LongRunningMultiSynchronizerIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numSequencers = 2,
        numMediators = 2,
        numParticipants = 1,
        withRemote = false,
      )
      .withManualStart

  "long-running startup script allows to start (multi synchronizers)" in { implicit env =>
    val mc = LongRunning
      .startup(
        totalCycles = 10,
        reportFrequency = 1,
        numAssetsPerIssuer = 10,
        enablePruning = false,
        singleSynchronizer = false,
      )
      ._1
    Await.result(mc.isDoneF, 1.minute)
  }
}

class LongRunningMultiSynchronizerIntegrationTestPostgres
    extends LongRunningMultiSynchronizerIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
}

// H2 is not optimized for performance and therefore this test becomes flaky
//class LongRunningMultiSynchronizerIntegrationTestH2
//    extends LongRunningMultiSynchronizerIntegrationTest {
//  registerPlugin(new UseH2(loggerFactory))
//}

class LongRunningMultiSynchronizerIntegrationTestInMemory
    extends LongRunningMultiSynchronizerIntegrationTest
