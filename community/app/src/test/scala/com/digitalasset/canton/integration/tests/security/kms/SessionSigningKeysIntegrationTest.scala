// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms

import com.daml.metrics.api.MetricsContext
import com.daml.metrics.api.testing.MetricValues.*
import com.digitalasset.canton.config
import com.digitalasset.canton.config.SessionSigningKeysConfig
import com.digitalasset.canton.console.{
  LocalInstanceReference,
  LocalMediatorReference,
  LocalParticipantReference,
  LocalSequencerReference,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.security.kms.aws.AwsKmsCryptoIntegrationTestBase
import com.digitalasset.canton.integration.tests.security.kms.gcp.GcpKmsCryptoIntegrationTestBase
import com.digitalasset.canton.integration.tests.security.kms.mock.MockKmsDriverCryptoIntegrationTestBase
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransform,
  ConfigTransforms,
  EnvironmentSetupPlugin,
  SharedEnvironment,
}
import com.digitalasset.canton.version.ProtocolVersion

/** Test a scenario where we have a combination of non-KMS, KMS and KMS with session signing keys'
  * nodes and make sure communication is correct among all of them.
  */
trait SessionSigningKeysIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with KmsCryptoIntegrationTestBase {

  protected lazy val nodesWithSessionSigningKeysDisabled: Set[String] = Set.empty

  override protected def otherConfigTransforms: Seq[ConfigTransform] = Seq(
    ConfigTransforms.setSigningKeysIfPV35OrHigher(
      SessionSigningKeysConfig.default,
      nodeFilter = name => !nodesWithSessionSigningKeysDisabled.contains(name),
    )
  )

  private def getSessionSigningKeysFallback(
      node: LocalInstanceReference
  ): Map[MetricsContext, Long] = {
    val kmsMetrics = node match {
      case p: LocalParticipantReference =>
        p.underlying.value.metrics.kmsMetrics

      case m: LocalMediatorReference =>
        m.underlying.value.replicaManager.mediatorRuntime.value.mediator.metrics.kmsMetrics

      case s: LocalSequencerReference =>
        s.underlying.value.sequencer.metrics.kmsMetrics

      case _ => fail("unexpected node")
    }
    kmsMetrics.sessionSigningKeysFallback.valuesWithContext
  }

  s"ping succeeds with nodes $protectedNodes using session signing keys" onlyRunWithOrGreaterThan
    ProtocolVersion.v35 in { implicit env =>
      import env.*

      env.nodes.local.foreach { node =>
        if (nodesWithSessionSigningKeysDisabled.contains(node.name))
          node.config.crypto.sessionSigningKeys shouldBe SessionSigningKeysConfig.disabled
        else
          node.config.crypto.sessionSigningKeys shouldBe SessionSigningKeysConfig.default
      }

      assertPingSucceeds(participant1, participant2)

      val kmsMetrics = nodes.local.map(getSessionSigningKeysFallback)

      // with our default validity parameters, we expect all protocol messages to be signed using session signing keys.
      forAll(kmsMetrics)(_ shouldBe empty)
    }

  "a longer `confirmationResponseTimeout` triggers a fallback to long-term signing keys" onlyRunWithOrGreaterThan
    ProtocolVersion.v35 in { implicit env =>
      import env.*

      // Increasing the `confirmationResponseTimeout` to 5 minutes increases the max sequencing time assigned
      // to confirmation responses (`requestId` + `confirmationResponseTimeout`). With the default
      // session signing key validity parameters, this new 5-minute window cannot be covered by a session
      // key, so the system falls back to using the long-term key. This fallback is recorded
      // in the `sessionSigningKeysFallback` metric.
      sequencer1.topology.synchronizer_parameters.propose_update(
        synchronizerId = daId,
        _.update(
          confirmationResponseTimeout = config.NonNegativeFiniteDuration.ofMinutes(5)
        ),
        mustFullyAuthorize = true,
      )

      assertPingSucceeds(participant1, participant2)

      val kmsMetrics = nodes.local.map(getSessionSigningKeysFallback)
      forAtLeast(2, kmsMetrics)(_ should not be empty)
    }
}

class AwsKmsSessionSigningKeysIntegrationTestPostgres
    extends SessionSigningKeysIntegrationTest
    with AwsKmsCryptoIntegrationTestBase {
  override protected lazy val nodesWithSessionSigningKeysDisabled: Set[String] =
    Set("participant2")

  override protected lazy val protectedNodes: Set[String] =
    Set("participant1", "participant2", "mediator1")

  setupPlugins(
    withAutoInit = false,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseBftSequencer(loggerFactory),
  )
}

class GcpKmsSessionSigningKeysIntegrationTestPostgres
    extends SessionSigningKeysIntegrationTest
    with GcpKmsCryptoIntegrationTestBase {
  override protected lazy val nodesWithSessionSigningKeysDisabled: Set[String] =
    Set("participant2")

  override protected lazy val protectedNodes: Set[String] =
    Set("participant1", "participant2", "mediator1")

  setupPlugins(
    withAutoInit = false,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseBftSequencer(loggerFactory),
  )
}

class MockKmsDriverSessionSigningKeysIntegrationTestPostgres
    extends SessionSigningKeysIntegrationTest
    with MockKmsDriverCryptoIntegrationTestBase {
  override protected lazy val nodesWithSessionSigningKeysDisabled: Set[String] =
    Set.empty

  override protected lazy val protectedNodes: Set[String] =
    Set("participant1", "participant2", "mediator1", "sequencer1")

  setupPlugins(
    // TODO(#25069): Add persistence to mock KMS driver to support auto-init = false
    withAutoInit = true,
    storagePlugin = Option.empty[EnvironmentSetupPlugin],
    sequencerPlugin = new UseBftSequencer(loggerFactory),
  )
}
