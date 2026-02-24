// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.connection

import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.admin.api.client.data.ComponentHealthState
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.sequencing.{
  SequencerConnectionValidation,
  SubmissionRequestAmplification,
}

/** This test checks that all the health components for a node report a healthy status when the
  * sequencer connection pool is disabled.
  *
  * See https://digitalasset.atlassian.net/jira/servicedesk/projects/DA/queues/custom/380/DA-3701.
  */
sealed trait DisabledConnectionPoolHealthIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S4M1_Config
      .addConfigTransforms(
        ConfigTransforms.setConnectionPool(false)
      )
      .withNetworkBootstrap { implicit env =>
        import env.*
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = sequencers.local,
            mediators = Seq(mediator1),
            overrideMediatorToSequencers = Some(
              Map(
                mediator1 -> (sequencers.local,
                /* trust threshold */ PositiveInt.two, /* liveness margin */ NonNegativeInt.one)
              )
            ),
          )
        )
      }
      .withSetup { implicit env =>
        import env.*

        val connectionsConfig = sequencers.local.map(s =>
          s.config.publicApi.clientConfig
            .asSequencerConnection(SequencerAlias.tryCreate(s.name), sequencerId = None)
        )

        clue("connect participants to all sequencers") {
          participants.local.foreach { participant =>
            participant.start()
            participant.synchronizers.connect_bft(
              connections = connectionsConfig,
              sequencerTrustThreshold = PositiveInt.two,
              sequencerLivenessMargin = NonNegativeInt.one,
              submissionRequestAmplification = SubmissionRequestAmplification.NoAmplification,
              synchronizerAlias = daName,
              physicalSynchronizerId = Some(daId),
              validation = SequencerConnectionValidation.Disabled,
            )
          }
        }

        clue("ping") {
          participant1.health.ping(participant2.id)
        }
      }

  "Member nodes" must {
    "show all health components as healthy when the pool is disabled" in { implicit env =>
      import env.*

      forAll(nodes.local) { node =>
        forAll(node.health.status.trySuccess.components) {
          _.state shouldBe ComponentHealthState.Ok()
        }
      }
    }
  }
}

class DisabledConnectionPoolHealthIntegrationTestDefault
    extends DisabledConnectionPoolHealthIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
