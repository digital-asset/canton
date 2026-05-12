// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import cats.syntax.functor.*
import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnectionPoolDelays,
  SequencerConnectionValidation,
  SequencerConnections,
  SubmissionRequestAmplification,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{ExponentialBackoffConfig, NonNegativeDuration}
import com.digitalasset.canton.console.{MediatorReference, SequencerReference}
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.{SequencerAlias, config}
import monocle.macros.syntax.lens.*

/** Checks that the background task that retrieves and stores sequencer ids work correctly.
  */
final class SequencerIdsRetrieverIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S4M4_Config
      .withNetworkBootstrap { implicit env =>
        import env.*

        val sequencers = Seq(sequencer1, sequencer2, sequencer3, sequencer4)
        val mediators = Seq(mediator1, mediator2, mediator3, mediator4)
        /*
         Ensure each mediator is connected to a single sequencer.
         The goal is that if a sequencer is stopped (sequencer4 in this test) it does not impact
         */
        val mediatorToSequencers
            : Map[MediatorReference, (Seq[SequencerReference], PositiveInt, NonNegativeInt)] =
          sequencers
            .zip(mediators)
            .map { case (sequencer, mediator) =>
              (mediator, (Seq(sequencer), PositiveInt.one, NonNegativeInt.zero))
            }
            .toMap

        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = sequencers,
            synchronizerThreshold = PositiveInt.one,
            sequencers = sequencers,
            mediators = mediators,
            overrideMediatorToSequencers = Some(mediatorToSequencers),
          )
        )
      }
      // Retry more aggressively
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.lsu.sequencerIdsRetrievalRetry).replace(
            ExponentialBackoffConfig(
              initialDelay = config.NonNegativeFiniteDuration.ofMillis(100),
              maxDelay = config.NonNegativeDuration.ofMillis(200),
              maxRetries = Int.MaxValue,
            )
          )
        )
      )
      .addConfigTransform(
        // speed up the test
        _.focus(_.parameters.timeouts.processing.sequencerInfo)
          .replace(NonNegativeDuration.ofSeconds(2))
      )

  private def getAliasToId()(implicit
      env: TestConsoleEnvironment
  ): Map[SequencerAlias, Option[SequencerId]] =
    env.participant1.underlying.value.sync.synchronizerConnectionConfigStore
      .getActive(env.daId)
      .value
      .config
      .sequencerConnections
      .aliasToConnection
      .forgetNE
      .fmap(_.sequencerId)

  "sequencer ids are retrieved, with retries" in { implicit env =>
    import env.*

    val sequencers = Seq(sequencer1, sequencer2, sequencer3, sequencer4)

    val fourSequencersThreshold1 = SynchronizerConnectionConfig(
      synchronizerAlias = daName,
      sequencerConnections = SequencerConnections.tryMany(
        connections =
          sequencers.map(s => s.sequencerConnection.withAlias(SequencerAlias.tryCreate(s.name))),
        sequencerTrustThreshold = PositiveInt.tryCreate(1),
        sequencerLivenessMargin = NonNegativeInt.zero,
        submissionRequestAmplification = SubmissionRequestAmplification.NoAmplification,
        sequencerConnectionPoolDelays = SequencerConnectionPoolDelays.default,
      ),
    )

    // Stopping the mediator removes noise in the logs (warning that it cannot connect to its sequencer)
    mediator4.stop()
    sequencer4.stop()

    loggerFactory.assertLogsUnorderedOptional(
      {
        participant1.synchronizers.connect_by_config(
          fourSequencersThreshold1,
          validation = SequencerConnectionValidation.ThresholdActive,
        )

        // Eventually, all ids except for sequencer 4 (stopped) are updated
        eventually() {
          getAliasToId() shouldBe Map(
            sequencer1.sequencerAlias -> Some(sequencer1.id),
            sequencer2.sequencerAlias -> Some(sequencer2.id),
            sequencer3.sequencerAlias -> Some(sequencer3.id),
            sequencer4.sequencerAlias -> None,
          )
        }

        sequencer4.start()
        mediator4.start()

        // Eventually, all ids are updated
        eventually() {
          getAliasToId() shouldBe Map(
            sequencer1.sequencerAlias -> Some(sequencer1.id),
            sequencer2.sequencerAlias -> Some(sequencer2.id),
            sequencer3.sequencerAlias -> Some(sequencer3.id),
            sequencer4.sequencerAlias -> Some(sequencer4.id),
          )
        }
      },
      (
        LogEntryOptionality.OptionalMany,
        _.warningMessage should include("Connection has failed validation"),
      ),
      (
        LogEntryOptionality.OptionalMany,
        _.warningMessage should include("Request failed for server-sequencer4-0"),
      ),
    )
  }
}
