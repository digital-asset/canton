// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.performance

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.integration.EnvironmentDefinition
import com.digitalasset.canton.performance.PartyRole.{
  Master,
  MasterDynamicConfig,
  TriPartyProvider,
  TriPartyUser,
}
import com.digitalasset.canton.performance.RateSettings.SubmissionRateSettings
import com.digitalasset.canton.performance.model.java.orchestration.runtype.TriParty
import com.digitalasset.canton.performance.{
  PerformanceRunner,
  PerformanceRunnerConfig,
  RateSettings,
}

class TripartyPerformanceIntegrationTest extends BasePerformanceIntegrationTestCommon {

  import BasePerformanceIntegrationTest.*

  override lazy val environmentDefinition: EnvironmentDefinition =
    TransferPerformanceIntegrationTest.environmentDefinition

  TransferPerformanceIntegrationTest.plugins(loggerFactory).foreach(registerPlugin)

  "setup synchronizers" in (env =>
    TransferPerformanceIntegrationTest.setupSynchronizers(testedProtocolVersion)(env, traceContext)
  )

  "run a tri-party test" in { implicit env =>
    import env.*
    val rateSettings =
      RateSettings(SubmissionRateSettings.TargetLatencyNew())
    val (baseSynchronizer, otherSynchronizers) =
      participant1.synchronizers.list_connected().map(_.synchronizerId).toList match {
        case (one :: rest) => (one, rest)
        case syncs => fail(s"invalid setup $syncs, you need to run the whole test!")
      }

    val p1Config = PerformanceRunnerConfig(
      master = s"$Master",
      localRoles = Set(
        Master(
          s"$Master",
          runConfig = MasterDynamicConfig(
            totalCycles = 100,
            reportFrequency = 10,
            runType = new TriParty(
              2, // numAgents
              4, // numAssetsPerIssuer
              2, // numAccountsPerIssuer
              100, // capNumCounterparties
            ),
          ),
          // quorum must be set precisely for this type of run
          // because we will move to trader prepare based on this number
          quorumProviders = 3 * 2 + 3,
        ),
        TriPartyProvider("cash-issuer", rateSettings),
        TriPartyProvider("issuer-01", rateSettings),
        TriPartyProvider("taker-01", rateSettings),
        TriPartyProvider("agent-01", rateSettings),
        TriPartyProvider("subregister-01", rateSettings),
        TriPartyUser("user-01", rateSettings),
      ),
      ledger = toConnectivity(participant1),
      baseSynchronizerId = baseSynchronizer,
      otherSynchronizers = otherSynchronizers,
    )

    val p2Config = PerformanceRunnerConfig(
      master = s"$Master",
      localRoles = Set(
        TriPartyProvider("issuer-02", rateSettings),
        TriPartyProvider("taker-02", rateSettings),
        TriPartyProvider("agent-02", rateSettings),
        TriPartyProvider("subregister-02", rateSettings),
        TriPartyUser("user-02", rateSettings),
      ),
      ledger = toConnectivity(participant2),
      baseSynchronizerId = baseSynchronizer,
      otherSynchronizers = otherSynchronizers,
    )

    val runnerP1 =
      new PerformanceRunner(
        p1Config,
        _ => NoOpMetricsFactory,
        loggerFactory.append("participant", "participant1"),
      )

    val runnerP2 =
      new PerformanceRunner(
        p2Config,
        _ => NoOpMetricsFactory,
        loggerFactory.append("participant", "participant2"),
      )

    env.environment.addUserCloseable(runnerP1)
    env.environment.addUserCloseable(runnerP2)

    loggerFactory
      .assertLoggedWarningsAndErrorsSeq(
        {
          val f2 = runnerP2.startup()
          val f1 = runnerP1.startup()
          waitUntilComplete((runnerP1, f1), (runnerP2, f2))
        },
        forEvery(_)(acceptableLogMessage),
      )

  }

}
