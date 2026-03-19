// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.fast

import com.digitalasset.canton.admin.api.client.data.SequencerConnections
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.toxiproxy.UseToxiproxy.ToxiproxyConfig
import com.digitalasset.canton.integration.plugins.toxiproxy.{SequencerToPostgres, UseToxiproxy}
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseExternalProcess,
  UsePostgres,
  UseSharedStorage,
}
import com.digitalasset.canton.integration.tests.toxiproxy.ToxiproxyHelpers
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import eu.rekawek.toxiproxy.model.ToxicDirection

/** Two simultaneous block-based sequencers must not write to the database. The one that does not
  * acquire the lock (passive) should get killed.
  */
class SimultaneousSequencerNodesIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  protected lazy val externalPlugin = new UseExternalProcess(
    loggerFactory,
    externalSequencers = Set("sequencer1", "sequencer2"),
    fileNameHint = this.getClass.getSimpleName,
  )

  registerPlugin(new UsePostgres(loggerFactory))
  val bftSequencerPlugin = new UseBftSequencer(loggerFactory)
  registerPlugin(bftSequencerPlugin)
  registerPlugin(UseSharedStorage.forSequencers("sequencer1", Seq("sequencer2"), loggerFactory))
  val sequencer2DbProxyConfig = SequencerToPostgres("sequencer2-to-postgres", "sequencer2")
  val toxiproxyPlugin = new UseToxiproxy(
    ToxiproxyConfig(proxies = Seq(sequencer2DbProxyConfig))
  )
  registerPlugin(toxiproxyPlugin)
  registerPlugin(externalPlugin)

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M1_Config.withManualStart
      .withSetup { implicit env =>
        import env.*
        logger.debug(s"Starting sequencer ${remoteSequencer1.name}")
        externalPlugin.start(remoteSequencer1.name)
        remoteSequencer1.health.wait_for_running()
        logger.debug(s"Sequencer ${remoteSequencer1.name} is running")
        mediators.local.start()
        participants.local.start()
      }
      .withNetworkBootstrap { implicit env =>
        import env.*
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq[InstanceReference](remoteSequencer1, mediator1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(remoteSequencer1),
            mediators = Seq(mediator1),
          )
        )
      }
      .withTeardown { _ =>
        ToxiproxyHelpers.removeAllProxies(
          toxiproxyPlugin.runningToxiproxy.controllingToxiproxyClient
        )
      }

  "Starting simultaneous sequencers with the same database config" should {
    "the second sequencer should crash without interfering with the other" in { implicit env =>
      import env.*
      // make sure sequencer1 is functional
      participant1.synchronizers.connect_local(remoteSequencer1, alias = daName)
      participant1.health.ping(participant1)

      externalPlugin.start(remoteSequencer2.name)

      // sequencer1 is still good
      participant1.health.ping(participant1)

      // sequencer2 will fail to acquire the lock and stop the startup process
      remoteSequencer2.health.is_running() shouldBe false
      // however its process will dangle, so we kill it
      externalPlugin.kill(remoteSequencer2.name)
      externalPlugin.isRunning(remoteSequencer2.name) shouldBe false
    }

    "stopping the first sequencer should allow the second to start" in { implicit env =>
      import env.*
      externalPlugin.kill(remoteSequencer1.name)
      externalPlugin.start(remoteSequencer2.name)
      remoteSequencer2.health.wait_for_running()
      remoteSequencer2.health.wait_for_initialized()

      // suppress mediator warnings during sequencer connection change
      loggerFactory.suppressWarningsAndErrors {
        // make sure sequencer2 is functional
        mediator1.sequencer_connection.set(
          SequencerConnections.single(remoteSequencer2.sequencerConnection)
        )
        participant2.synchronizers.connect_local(remoteSequencer2, alias = daName)
        participant2.health.ping(participant2)
      }
    }

    "crash sequencer2 if it loses connections while sequencer1 acquires it" in { implicit env =>
      import env.*
      // sequencer2 is running
      participant2.health.ping(participant2)

      // make sequencer2 lose the db connection
      val proxy =
        toxiproxyPlugin.runningToxiproxy.getProxy(sequencer2DbProxyConfig.name).value.underlying
      val toxic = proxy.toxics().timeout("db-con-timeout", ToxicDirection.UPSTREAM, 1000)

      // start sequencer1 so it acquires the lock on the connection and forces sequencer2 to go passive and crash
      loggerFactory.suppressWarningsAndErrors {
        externalPlugin.start(remoteSequencer1.name)
        remoteSequencer1.health.wait_for_initialized()
      }

      remoteSequencer1.health.is_running() shouldBe true
      remoteSequencer2.health.is_running() shouldBe false
      externalPlugin.kill(remoteSequencer2.name) // stop dangling process
      toxic.remove()

      loggerFactory.suppressWarningsAndErrors {
        // make sure sequencer1 is functional again
        mediator1.sequencer_connection.set(
          SequencerConnections.single(remoteSequencer1.sequencerConnection)
        )
        participant1.health.ping(participant1)
      }
    }
  }

}
