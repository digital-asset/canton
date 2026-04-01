// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.performance

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnections,
  StaticSynchronizerParameters,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DbConfig, NonNegativeDuration}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentDefinition}
import com.digitalasset.canton.performance.PartyRole.{
  DvpIssuer,
  Master,
  MasterDynamicConfig,
  Transfer,
}
import com.digitalasset.canton.performance.RateSettings.SubmissionRateSettings
import com.digitalasset.canton.performance.model.java.orchestration.runtype.TransferRun
import com.digitalasset.canton.performance.{
  PerformanceRunner,
  PerformanceRunnerConfig,
  RateSettings,
}
import com.digitalasset.canton.{BaseTest, config}
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.*

class TransferPerformanceIntegrationTest extends BasePerformanceIntegrationTestCommon {
  import BasePerformanceIntegrationTest.*

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 2,
        numSequencers = 2,
        numMediators = 2,
      )
      .addConfigTransforms(
        _.focus(_.parameters.timeouts.processing.shutdownProcessing)
          .replace(NonNegativeDuration.tryFromDuration(1.minute))
          .focus(_.parameters.enableAdditionalConsistencyChecks)
          .replace(false),
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.reassignmentsConfig.targetTimestampForwardTolerance)
            .replace(config.NonNegativeFiniteDuration.ofSeconds(60))
        ),
        ConfigTransforms.enableUnsafeMutiSynchronizerTopologyFeatureFlag,
      )

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2"))
          .map(_.map(InstanceName.tryCreate))
      ),
    )
  )

  "setup synchronizers" in { implicit env =>
    import env.*

    // bootstrap synchronizeres
    val connections =
      sequencers.all.zip(mediators.all).zipWithIndex.map { case ((sequencer, mediator), idx) =>
        val alias = s"sync$idx"
        logger.info(s"Bootstrapping $alias with ${sequencer.name} and ${mediator.name}")
        bootstrap.synchronizer(
          synchronizerName = alias,
          sequencers = Seq(sequencer),
          mediators = Seq(mediator),
          synchronizerOwners = Seq(sequencer),
          synchronizerThreshold = PositiveInt.one,
          staticSynchronizerParameters =
            StaticSynchronizerParameters.defaultsWithoutKMS(testedProtocolVersion),
        )
        (alias, sequencer.sequencerConnection.withAlias(alias))
      }

    // connect the participants to all sequencers
    participants.all.foreach { participant =>
      connections.foreach { case (alias, connection) =>
        logger.info(s"Connecting ${participant.name} to $alias")
        participant.synchronizers.connect_by_config(
          SynchronizerConnectionConfig(
            alias,
            SequencerConnections.single(connection),
          )
        )
      }

      // upload dar first
      logger.info(s"Uploading dar to ${participant.name}")
      val darId = participant.dars.upload(
        BaseTest.PerformanceTestPath,
        vetAllPackages = false,
        synchronizeVetting = false,
      )
      // vet the dar on all synchronizers
      participant.synchronizers.list_connected().map(_.synchronizerId).foreach { synchronizerId =>
        participant.dars.vetting
          .enable(darId, synchronizerId = Some(synchronizerId), synchronize = false)
      }

      logger.info(s"Completed connecting nodes")

    }

  }

  "run a normal transfer perf test" in { implicit env =>
    import env.*
    val rateSettings =
      RateSettings(SubmissionRateSettings.TargetLatencyNew())
    val (baseSynchronizer, otherSynchronizers) =
      participant1.synchronizers.list_connected().map(_.synchronizerId).toList match {
        case (one :: rest) => (one, rest)
        case _ => fail("invalid setup")
      }
    val p1Config = PerformanceRunnerConfig(
      master = s"$Master",
      localRoles = Set(
        Master(
          s"$Master",
          runConfig = MasterDynamicConfig(
            totalCycles = 100,
            reportFrequency = 10,
            runType = new TransferRun(
              12, // num assets per issuer
              3, // transfer batch size
              0, // payload
            ),
          ),
        ),
        Transfer(s"transfer-01", rateSettings),
        Transfer(s"transfer-02", rateSettings),
        DvpIssuer(s"issuer-01", rateSettings),
      ),
      ledger = toConnectivity(participant1),
      baseSynchronizerId = baseSynchronizer,
      otherSynchronizers = otherSynchronizers,
    )

    val p2Config = PerformanceRunnerConfig(
      master = s"$Master",
      localRoles = Set(
        Transfer(s"transfer-11", rateSettings),
        Transfer(s"transfer-12", rateSettings),
        DvpIssuer(s"issuer-01", rateSettings), // same name to ensure we dedup
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
