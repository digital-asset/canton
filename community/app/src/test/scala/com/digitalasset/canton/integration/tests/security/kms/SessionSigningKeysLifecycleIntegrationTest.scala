// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms

import com.daml.metrics.api.testing.MetricValues.*
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig.Postgres
import com.digitalasset.canton.config.{KmsConfig, SessionSigningKeysConfig}
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.security.kms.mock.MockKmsDriverCryptoIntegrationTestBase
import com.digitalasset.canton.integration.tests.security.kms.mock.MockKmsDriverCryptoIntegrationTestBase.mockKmsDriverConfig
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransform,
  ConfigTransforms,
  SharedEnvironment,
}
import com.digitalasset.canton.version.ProtocolVersion
import monocle.Monocle.toAppliedFocusOps

trait SessionSigningKeysLifecycleIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with KmsCryptoIntegrationTestBase {

  private val sessionSigningKeysConfig = SessionSigningKeysConfig.short
  private val shortDuration = config.NonNegativeFiniteDuration.tryFromDuration(
    SessionSigningKeysConfig.short.keyValidityDuration.duration.div(2.0)
  )

  override protected def otherConfigTransforms: Seq[ConfigTransform] = Seq(
    ConfigTransforms.setSigningKeysIfPV35OrHigher(sessionSigningKeysConfig),
    // adjust the max sequencing time offset so requests remain compatible with the short validity
    // periods of session signing keys.
    ConfigTransforms.updateAllSequencerConfigs_(
      _.focus(_.sequencerClient.defaultMaxSequencingTimeOffset)
        .replace(shortDuration)
    ),
    ConfigTransforms.updateAllMediatorConfigs_(
      _.focus(_.sequencerClient.defaultMaxSequencingTimeOffset)
        .replace(shortDuration)
    ),
    ConfigTransforms.updateAllParticipantConfigs_(
      _.focus(_.sequencerClient.defaultMaxSequencingTimeOffset)
        .replace(shortDuration)
    ),
  )

  "verify correct session key lifecycle with clock advances" onlyRunWhen
    testedProtocolVersion >= ProtocolVersion.v35 in { implicit env =>
      import env.*

      // adjust timeouts to ensure requests and responses fit within the very short validity
      // periods of session signing keys.
      sequencer1.topology.synchronizer_parameters.propose_update(
        synchronizerId = daId,
        _.update(
          confirmationResponseTimeout = shortDuration,
          mediatorReactionTimeout = shortDuration,
          ledgerTimeRecordTimeTolerance = sessionSigningKeysConfig.cutOffDuration,
        ),
      )

      // ensure that all participants have observed the updated synchronizer parameters
      // before starting to record the use of session signing keys
      eventually() {
        participants.local.foreach { participant =>
          val syncParameters = participant.topology.synchronizer_parameters.latest(daId)

          syncParameters.confirmationResponseTimeout shouldBe shortDuration
          syncParameters.mediatorReactionTimeout shouldBe shortDuration
          syncParameters.ledgerTimeRecordTimeTolerance shouldBe sessionSigningKeysConfig.cutOffDuration
        }
      }

      // record initial metrics to establish a baseline, as a fallback to the long-term key
      // may have occurred during bootstrap.
      val participantKmsMetrics = Seq(participant1, participant2).map(
        _.underlying.value.metrics.kmsMetrics.sessionSigningKeysFallback.valuesWithContext
      )

      env.nodes.local.foreach { node =>
        node.config.crypto.sessionSigningKeys shouldBe sessionSigningKeysConfig
      }

      assertPingSucceeds(participant1, participant2)

      // session signing keys created are still valid
      Threading.sleep(sessionSigningKeysConfig.cutOffDuration.duration.div(2.0).toMillis)

      assertPingSucceeds(participant1, participant2)

      // session signing keys have expired; new keys will be generated
      Threading.sleep(sessionSigningKeysConfig.keyValidityDuration.duration.toMillis)

      assertPingSucceeds(participant1, participant2)

      // we expect that no fallback has been triggered for the ping requests and that
      // session signing keys have been used and rotated successfully.
      Seq(participant1, participant2).map(
        _.underlying.value.metrics.kmsMetrics.sessionSigningKeysFallback.valuesWithContext
      ) shouldBe participantKmsMetrics
    }
}

class MockKmsDriverSessionSigningKeysLifecycleIntegrationTestPostgres
    extends SessionSigningKeysLifecycleIntegrationTest
    with MockKmsDriverCryptoIntegrationTestBase {

  override protected val kmsConfig: KmsConfig = mockKmsDriverConfig

  override protected lazy val protectedNodes: Set[String] =
    Set("participant1", "participant2", "mediator1", "sequencer1")

  setupPlugins(
    withAutoInit = true,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseReferenceBlockSequencer[Postgres](loggerFactory),
  )
}
