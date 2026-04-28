// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.daml.metrics.api.testing.MetricValues.*
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.SessionSigningKeysConfig
import com.digitalasset.canton.console.LocalInstanceReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.security.kms.mock.MockKmsDriverCryptoIntegrationTestBase
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.integration.{
  ConfigTransform,
  ConfigTransforms,
  EnvironmentDefinition,
}
import com.digitalasset.canton.version.ProtocolVersion

import java.time.Duration

/** Tests that a logical synchronizer upgrade (LSU) works correctly when KMS and session signing
  * keys are enabled. Session signing keys are only supposed to be used together with an external
  * KMS, so it makes sense to test both together. We use a mock KMS here to save on resources, but
  * the test should be valid for any KMS provider.
  */
final class LsuSessionSigningKeysIntegrationTest
    extends LsuBase
    with MockKmsDriverCryptoIntegrationTestBase {

  setupPlugins(
    withAutoInit = true,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    ),
  )

  // Nodes configured to run with a mock KMS driver.
  override protected lazy val protectedNodes: Set[String] =
    Set("participant1")

  override protected def testName: String = "lsu-session-signing-keys"
  override protected lazy val newOldSequencers: Map[String, String] = Map(
    "sequencer2" -> "sequencer1"
  )
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")

  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(
    SessionSigningKeysConfig.enabled.keyValidityDuration.duration.toSeconds * 5
  )

  override protected def configTransforms: Seq[ConfigTransform] =
    super.configTransforms :+ ConfigTransforms.setSigningKeysIfPV35OrHigher(
      SessionSigningKeysConfig.enabled
    )
  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S2M2_Config
      .withNetworkBootstrap(implicit env => new NetworkBootstrapper(EnvironmentDefinition.S1M1))
      .addConfigTransforms(configTransforms*)
      .withSetup(implicit env => defaultEnvironmentSetup())

  "Logical synchronizer upgrade" should {
    "work with session signing keys" onlyRunWithOrGreaterThan ProtocolVersion.v35 in {
      implicit env =>
        import env.*

        withClue(s"${participant1.id} should have KMS enabled before LSU") {
          participant1.config.crypto.kms.isDefined shouldBe true
        }

        Seq[LocalInstanceReference](participant1, sequencer1, mediator1).foreach { node =>
          withClue(s"${node.id} should have session signing keys enabled before LSU") {
            node.config.crypto.sessionSigningKeys.enabled shouldBe true
          }
        }

        val alice = participant1.parties.enable("Alice")
        val bank = participant1.parties.enable("Bank")
        val fixture = fixtureWithDefaults()

        // current number of times session signing keys are not used,
        // and we fall back to signing with the long-term key
        def fallbackSessionSigningKeyMetrics =
          participant1.underlying.value.metrics.kmsMetrics.sessionSigningKeysFallback.valuesWithContext

        val initialFallbackMetric = fallbackSessionSigningKeyMetrics

        // first ping to trigger usage of the initial session signing key
        participant1.health.ping(participant1)

        // advance the clock so that the first session signing key expires and a second one must be created
        environment.simClock.value.advance(
          SessionSigningKeysConfig.enabled.keyValidityDuration.asJava
        )
        // TODO(#31053): Remove after static clock synchronization is fixed.
        Threading.sleep(500)

        // second ping: this will trigger creation and usage of a new session signing key
        participant1.health.ping(participant1)

        withClue("perform LSU") {
          performSynchronizerNodesLsu(fixture)
          // the clock is advanced past the expiry time of the existing session signing keys,
          // which forces the creation of new session keys on the participant
          environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)
          // TODO(#31053): Remove after static clock synchronization is fixed.
          Threading.sleep(500)

          transferTraffic()
          eventually() {
            environment.simClock.value.advance(Duration.ofSeconds(1))
            participants.all.forall(_.synchronizers.is_connected(fixture.newPsid)) shouldBe true
          }
          oldSynchronizerNodes.all.stop()
          waitForTargetTimeOnSequencer(sequencer2, environment.clock.now, logger)
        }

        // Verify we can still successfully execute transactions.
        IouSyntax.createIou(participant1)(bank, alice)

        Seq[LocalInstanceReference](participant1, sequencer2, mediator2).foreach { node =>
          withClue(s"${node.id} should have session signing keys enabled after LSU") {
            node.config.crypto.sessionSigningKeys.enabled shouldBe true
          }
        }

        // even after more time has elapsed we can still use session signing keys correctly
        environment.simClock.value.advance(
          SessionSigningKeysConfig.enabled.keyValidityDuration.asJava
        )
        // TODO(#31053): Remove after static clock synchronization is fixed.
        Threading.sleep(500)
        participant1.health.ping(participant1)

        // Verify that no fallback to long-term signing keys occurred during the test.
        // The metric should remain unchanged, meaning all operations used session signing keys.
        initialFallbackMetric shouldBe fallbackSessionSigningKeyMetrics
    }
  }
}
