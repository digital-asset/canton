// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.slow

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.admin.api.client.data.ComponentHealthState
import com.digitalasset.canton.config.CantonConfig
import com.digitalasset.canton.environment.CantonEnvironment
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.plugins.toxiproxy.UseToxiproxy.ToxiproxyConfig
import com.digitalasset.canton.integration.plugins.toxiproxy.{
  ParticipantToPostgres,
  ProxyConfig,
  UseToxiproxy,
}
import com.digitalasset.canton.integration.{
  BaseEnvironmentDefinition,
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer

import scala.concurrent.duration.DurationInt

class LedgerApiIndexerToxiproxyIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {
  private def proxyConf: ProxyConfig =
    ParticipantToPostgres("Participant-to-postgres", "participant1")

  override protected def environmentDefinition
      : BaseEnvironmentDefinition[CantonConfig, CantonEnvironment] = EnvironmentDefinition.P1_S1M1

  registerPlugin(new UsePostgres(loggerFactory))
  val toxiproxyPlugin = UseToxiproxy(ToxiproxyConfig(Seq(proxyConf)))
  registerPlugin(toxiproxyPlugin)

  "ledger api indexer should successfully report healthy again after db connection interruption" in {
    implicit env =>
      import env.*

      participant1.synchronizers.connect(sequencer1, daName)
      participant1.health.ping(participant1.id)

      eventually() {
        participant1.health.status.trySuccess.components
          .find(_.name == LedgerApiIndexer.healthComponentName)
          .value
          .state shouldBe (ComponentHealthState.Ok())
      }

      val proxy = toxiproxyPlugin.runningToxiproxy.getProxy(proxyConf.name).value
      proxy.underlying.disable()

      BaseTest.eventuallyForever(timeUntilSuccess = 60.seconds, durationOfSuccess = 60.seconds) {
        // Wait until indexer gives up restarts and closes itself.
        participant1.health.status.trySuccess.components
          .find(_.name == LedgerApiIndexer.healthComponentName)
          .value
          .state shouldBe (ComponentHealthState.Failed(
          ComponentHealthState.UnhealthyState(Some("Component is closed"))()
        ))
      }
      proxy.underlying.enable()

      eventually() {
        participant1.health.status.trySuccess.components
          .find(_.name == LedgerApiIndexer.healthComponentName)
          .value
          .state shouldBe (ComponentHealthState.Ok())
      }
  }
}
