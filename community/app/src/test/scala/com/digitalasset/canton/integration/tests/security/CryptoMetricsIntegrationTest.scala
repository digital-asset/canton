// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.metrics.api.testing.MetricValues.*
import com.digitalasset.canton.annotations.RequiresExternalKms
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.crypto.provider.jce.JcePrivateCrypto
import com.digitalasset.canton.crypto.provider.kms.KmsPrivateCrypto
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.security.kms.gcp.GcpKmsCryptoIntegrationTestBase
import com.digitalasset.canton.integration.tests.security.kms.mock.MockKmsDriverCryptoIntegrationTestBase
import com.digitalasset.canton.integration.{
  CantonEnvironmentSetup,
  CommunityIntegrationTest,
  ConfigTransform,
  ConfigTransforms,
  EnvironmentSetupPlugin,
  SharedEnvironment,
}
import monocle.macros.syntax.lens.*

/** Integration tests verifying that cryptographic operations using KMS providers are correctly
  * instrumented to correctly record crypto-related metrics.
  *
  * In particular, this suite ensures that signing and decryption latencies are captured, as well as
  * KMS-related metrics.
  */
trait CryptoMetricsIntegrationTest {
  self: CommunityIntegrationTest & CantonEnvironmentSetup =>

  val disableSessionKeysConfigTransform: ConfigTransform =
    ConfigTransforms.updateAllParticipantConfigs_(
      _.focus(_.parameters.caching.sessionEncryptionKeyCache.enabled)
        .replace(false)
        .focus(_.crypto.sessionSigningKeys.enabled)
        .replace(false)
    )

  "signing, decryption latencies, and KMS metrics are recorded" in { implicit env =>
    import env.*

    // By default, participant1 is configured to use a KMS provider, while participant2 uses a JCE provider.
    participant1.crypto.privateCrypto.isInstanceOf[KmsPrivateCrypto] shouldBe true
    participant2.crypto.privateCrypto.isInstanceOf[JcePrivateCrypto] shouldBe true

    case class MetricCounts(signingCount: Int, decryptCount: Int)

    def metricCounts(p: LocalParticipantReference): MetricCounts = {
      val crypto = p.underlying.value.metrics.cryptoMetrics
      MetricCounts(
        signingCount = crypto.signingMetrics.signingLatency.valuesWithContext.values.flatten.size,
        decryptCount = crypto.decryptionMetrics.decryptLatency.valuesWithContext.values.flatten.size,
      )
    }

    val participants = Seq(participant1, participant2)
    val initialCounts = participants.map(p => p -> metricCounts(p)).toMap

    assertPingSucceeds(participant1, participant2)

    participant1.underlying.value.metrics.cryptoMetrics.kmsMetricsO shouldBe defined

    // The ping command consists of 2 transactions:
    // 1. The first transaction is a request from participant1 to create the ping contract that requires the
    // confirmation of participant1. It involves the following signing and decryption operations:
    //    - participant1 signs the submission request and adds one submitting participant signature to the messages
    //      that make up the confirmation request (informee message and encrypted view messages) - total 2 signing
    //      operations
    //    - participants decrypt the encrypted view message - total 1 decryption operation each
    //    - participant1 signs a confirmation response and the wrapper submission request - total 2 signing operations
    // 2. The second transaction is a request from participant2 to archive the previous ping contract that requires
    //    the confirmation of both participants. It involves the following signing and decryption operations:
    //    - participant2 signs the submission request and adds one submitting participant signature to the messages
    //      that make up the confirmation request (informee message and encrypted view messages) - total 2 signing
    //      operations
    //    - participants decrypt the encrypted view message - total 1 decryption operation each
    //    - participant1 and participant2 sign a confirmation response and the wrapper submission request - total 2
    //      signing operations for each participant
    //
    // Total signing operations: participant1 = 2 + 2 + 2 = 6, participant2 = 2 + 2 = 4
    // Total decryption operations: participant1 = 1 + 0 = 1, participant2 = 1
    val expectedDeltas = Map(
      participant1 -> MetricCounts(signingCount = 6, decryptCount = 2),
      participant2 -> MetricCounts(signingCount = 4, decryptCount = 2),
    )

    // Even when KMS is not used, signing and decryption operations always record latency metrics.
    forAll(participants) { p =>
      val MetricCounts(initialSigning, initialDecrypt) = initialCounts(p)
      val MetricCounts(finalSigning, finalDecrypt) = metricCounts(p)

      val MetricCounts(expectedSigningDelta, expectedDecryptDelta) = expectedDeltas(p)

      (finalSigning - initialSigning) shouldBe expectedSigningDelta
      (finalDecrypt - initialDecrypt) shouldBe expectedDecryptDelta
    }
  }

}

@RequiresExternalKms
class GcpKmsCryptoMetricsIntegrationTestPostgres
    extends CommunityIntegrationTest
    with SharedEnvironment
    with CryptoMetricsIntegrationTest
    with GcpKmsCryptoIntegrationTestBase {
  override protected def otherConfigTransforms: Seq[ConfigTransform] =
    super.otherConfigTransforms ++ Seq(disableSessionKeysConfigTransform)
  setupPlugins(
    withAutoInit = false,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseBftSequencer(loggerFactory),
  )
}

class MockKmsCryptoMetricsIntegrationTestPostgres
    extends CommunityIntegrationTest
    with SharedEnvironment
    with CryptoMetricsIntegrationTest
    with MockKmsDriverCryptoIntegrationTestBase {
  override protected def otherConfigTransforms: Seq[ConfigTransform] =
    super.otherConfigTransforms ++ Seq(disableSessionKeysConfigTransform)
  setupPlugins(
    withAutoInit = true,
    storagePlugin = Option.empty[EnvironmentSetupPlugin],
    sequencerPlugin = new UseBftSequencer(loggerFactory),
  )
}
