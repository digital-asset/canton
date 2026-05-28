// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.metrics.api.testing.MetricValues.*
import com.digitalasset.canton.crypto.provider.jce.JcePrivateCrypto
import com.digitalasset.canton.crypto.provider.kms.KmsPrivateCrypto
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.security.kms.KmsCryptoWithPreDefinedKeysIntegrationTest
import com.digitalasset.canton.integration.tests.security.kms.gcp.GcpKmsCryptoIntegrationTestBase
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentSetup,
  SharedEnvironment,
}

/** Integration tests verifying that cryptographic operations using KMS providers are correctly
  * instrumented to correctly record crypto-related metrics.
  *
  * In particular, this suite ensures that signing and decryption latencies are captured, as well as
  * KMS-related metrics.
  */
trait CryptoMetricsIntegrationTest extends KmsCryptoWithPreDefinedKeysIntegrationTest {
  self: CommunityIntegrationTest & EnvironmentSetup =>

  override lazy val protectedNodes: Set[String] = Set("participant1")

  "signing, decryption latencies, and KMS metrics are recorded" in { implicit env =>
    import env.*

    participant1.crypto.privateCrypto.isInstanceOf[KmsPrivateCrypto] shouldBe true
    participant2.crypto.privateCrypto.isInstanceOf[JcePrivateCrypto] shouldBe true

    assertPingSucceeds(participant1, participant1)

    participant1.underlying.value.metrics.cryptoMetrics.kmsMetricsO shouldBe defined

    // Even when KMS is not used, signing and decryption operations always record latency metrics.
    forAll(Seq(participant1, participant2)) { p =>
      p.underlying.value.metrics.cryptoMetrics.signingMetrics.signingLatency.valuesWithContext should not be empty
      p.underlying.value.metrics.cryptoMetrics.decryptionMetrics.decryptLatency.valuesWithContext should not be empty
    }
  }

}

class GcpKmsCryptoMetricsIntegrationTestPostgres
    extends CommunityIntegrationTest
    with SharedEnvironment
    with GcpKmsCryptoIntegrationTestBase
    with KmsCryptoWithPreDefinedKeysIntegrationTest {
  setupPlugins(
    withAutoInit = false,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseBftSequencer(loggerFactory),
  )
}
