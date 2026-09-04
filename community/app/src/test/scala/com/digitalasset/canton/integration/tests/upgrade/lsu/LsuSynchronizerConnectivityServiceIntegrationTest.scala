// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import cats.syntax.option.*
import com.digitalasset.canton.admin.api.client.data.SynchronizerConnectionConfig
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.upgrade.lsu.LsuBase.Fixture
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.*
import com.digitalasset.canton.topology.{KnownPhysicalSynchronizerId, PhysicalSynchronizerId}

import java.time.Duration

/** Ensure that no synchronizer can be registered during an LSU (when both synchronizers are
  * inactive).
  */
final class LsuSynchronizerConnectivityServiceIntegrationTest extends LsuBase {

  override protected def testName: String = "lsu-synchronizer-connectivity-service"

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer2" -> "sequencer1")
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S2M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        defaultEnvironmentSetup(connectParticipants = false)
      }

  private var fixture: Fixture = _
  private var currentConfig: SynchronizerConnectionConfig = _
  private var successorConfig: SynchronizerConnectionConfig = _

  private def getConfigStore()(implicit
      env: TestConsoleEnvironment
  ): SynchronizerConnectionConfigStore =
    env.participant1.underlying.value.sync.synchronizerConnectionConfigStore

  private def getSynchronizerStatuses()(implicit
      env: TestConsoleEnvironment
  ): Map[Option[PhysicalSynchronizerId], Status] =
    getConfigStore()
      .getAll()
      .map(c => c.configuredPsid.toOption -> c.status)
      .toMap

  "Synchronizer connectivity service" should {
    "prepare scenario" in { implicit env =>
      import env.*

      fixture = fixtureWithDefaults()
      currentConfig = SynchronizerConnectionConfig(
        synchronizerAlias = daName,
        sequencerConnections = sequencer1,
        synchronizerId = Some(fixture.currentPsid),
      )

      participant1.synchronizers.connect_by_config(defaultSynchronizerConnectionConfig())
      participant1.health.ping(participant1)
      performSynchronizerNodesLsu(fixture)

      eventually() {
        getSynchronizerStatuses() shouldBe Map(
          fixture.currentPsid.some -> Active,
          fixture.newPsid.some -> LsuTarget,
        )
      }

      successorConfig = SynchronizerConnectionConfig.fromInternal(
        getConfigStore().get(fixture.newPsid).value.config
      )
    }

    "not allow registering synchronizers during LSU" in { implicit env =>
      import env.*

      // Simulate LSU that crashes in the middle (successor is not yet active)
      getConfigStore()
        .setStatus(daName, KnownPhysicalSynchronizerId(fixture.currentPsid), LsuSource)
        .futureValueUS
        .value

      getSynchronizerStatuses() shouldBe Map(
        fixture.currentPsid.some -> LsuSource,
        fixture.newPsid.some -> LsuTarget,
      )

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.synchronizers.register_by_config(currentConfig),
        _.errorMessage should (include(
          s"The synchronizer with alias $daName cannot be registered: The operation is not allowed when an LSU is ongoing. Found LSU from ${fixture.currentPsid} to ${fixture.newPsid}."
        )),
      )

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.synchronizers.register_by_config(successorConfig),
        _.errorMessage should (include(
          s"The synchronizer with alias $daName cannot be registered: The operation is not allowed when an LSU is ongoing. Found LSU from ${fixture.currentPsid} to ${fixture.newPsid}."
        )),
      )

      getSynchronizerStatuses() shouldBe Map(
        fixture.currentPsid.some -> LsuSource,
        fixture.newPsid.some -> LsuTarget,
      )

      // Reset to sane state
      getConfigStore()
        .setStatus(daName, KnownPhysicalSynchronizerId(fixture.currentPsid), Active)
        .futureValueUS
        .value
    }

    "Do LSU" in { implicit env =>
      import env.*

      environment.simClock.foreach(_.advanceTo(upgradeTime.immediateSuccessor))
      transferTraffic()

      eventually() {
        environment.simClock.foreach(_.advance(Duration.ofSeconds(1)))
        participant1.synchronizers.is_connected(fixture.newPsid) shouldBe true
        participant1.synchronizers.is_connected(fixture.currentPsid) shouldBe false
      }

      waitForTargetTimeOnSequencer(sequencer2, upgradeTime.immediateSuccessor, logger)
      participant1.health.ping(participant1)
    }
  }
}
