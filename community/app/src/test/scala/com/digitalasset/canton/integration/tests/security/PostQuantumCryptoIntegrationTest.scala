// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.nonempty.NonEmpty
import com.daml.test.evidence.tag.Security.SecurityTestSuite
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.{
  CryptoConfig,
  CryptoProvider,
  CryptoSchemeConfig,
  SigningSchemeConfig,
}
import com.digitalasset.canton.crypto.{SigningAlgorithmSpec, SigningKeySpec}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.synchronizer.config.SynchronizerParametersConfig
import monocle.macros.syntax.lens.*

/** Integration test for post-quantum cryptography support in nodes.
  *
  * Covers happy cases (participants and synchronizers with PQC enabled can ping) and unhappy cases
  * to cover the crypto handshake with experimental schemes.
  */
class PostQuantumCryptoIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SecurityTestSuite
    with CryptoHandshakeIntegrationTestBase {

  // A traditional (EC crypto) synchronizer
  private val syncTrad: String = "synchronizer1"
  private val seqTrad: String = "sequencer1"
  private val medTrad: String = "mediator1"

  // A synchronizer that allows PQC
  private val syncPqcSupport: String = "synchronizer2"
  protected val seqPqcSupport: String = "sequencer2"
  private val medPqcSupport: String = "mediator2"

  // A synchronizer with only PQC
  private val syncPqcOnly: String = "synchronizer3"
  protected val seqPqcOnly: String = "sequencer3"
  private val medPqcOnly: String = "mediator3"

  // A participant with only PQC
  private val partPqcOnly: String = "participant1"

  // A participant with default PQC
  private val partPqcDefault: String = "participant2"

  // A participant that supports PQC
  private val partPqcSupport: String = "participant3"

  // A participant that only supports traditional crypto
  private val partTrad: String = "participant4"

  // Default JCE
  private val jce: CryptoConfig = CryptoConfig(provider = CryptoProvider.Jce)

  // JCE with experimental PQC schemes enabled but not used by default
  private val jceWithExperimental: CryptoConfig = jce.copy(enableExperimental = true)

  // JCE with default ML-DSA-65
  private val jceWithMlDsaDefault: CryptoConfig = jceWithExperimental.copy(
    signing = SigningSchemeConfig(
      algorithms = CryptoSchemeConfig(
        default = Some(SigningAlgorithmSpec.MlDsa65)
      ),
      keys = CryptoSchemeConfig(
        default = Some(SigningKeySpec.MlDsa65)
      ),
    )
  )

  // JCE with only ML-DSA-65
  private val jceWithMlDsaOnly: CryptoConfig = jceWithExperimental.copy(
    signing = SigningSchemeConfig(
      algorithms = CryptoSchemeConfig(
        default = Some(SigningAlgorithmSpec.MlDsa65),
        allowed = Some(NonEmpty.mk(Set, SigningAlgorithmSpec.MlDsa65)),
      ),
      keys = CryptoSchemeConfig(
        default = Some(SigningKeySpec.MlDsa65),
        allowed = Some(NonEmpty.mk(Set, SigningKeySpec.MlDsa65)),
      ),
    )
  )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .P4_S1M1_S1M1_S1M1(
        Map(
          syncTrad -> StaticSynchronizerParameters.defaults(jce, testedProtocolVersion),
          syncPqcSupport -> StaticSynchronizerParameters
            .defaults(jceWithExperimental, testedProtocolVersion),
          syncPqcOnly -> StaticSynchronizerParameters.fromConfig(
            SynchronizerParametersConfig()
              .copy(
                requiredSigningAlgorithmSpecs =
                  Some(NonEmpty.mk(Set, SigningAlgorithmSpec.MlDsa65)),
                requiredSigningKeySpecs = Some(NonEmpty.mk(Set, SigningKeySpec.MlDsa65)),
              ),
            jceWithExperimental,
            testedProtocolVersion,
          ),
        )
      )
      .addConfigTransforms(
        ConfigTransforms.updateAllParticipantConfigs {
          case (`partPqcOnly`, config) => config.focus(_.crypto).replace(jceWithMlDsaOnly)
          case (`partPqcDefault`, config) => config.focus(_.crypto).replace(jceWithMlDsaDefault)
          case (`partPqcSupport`, config) => config.focus(_.crypto).replace(jceWithExperimental)
          case (`partTrad`, config) => config.focus(_.crypto).replace(jce)
          case (_, config) => config
        },
        ConfigTransforms.updateAllSequencerConfigs {
          case (`seqPqcOnly`, config) => config.focus(_.crypto).replace(jceWithMlDsaOnly)
          case (`seqPqcSupport`, config) => config.focus(_.crypto).replace(jceWithExperimental)
          case (`seqTrad`, config) => config.focus(_.crypto).replace(jce)
          case (_, config) => config
        },
        ConfigTransforms.updateAllMediatorConfigs {
          case (`medPqcOnly`, config) => config.focus(_.crypto).replace(jceWithMlDsaOnly)
          case (`medPqcSupport`, config) => config.focus(_.crypto).replace(jceWithExperimental)
          case (`medTrad`, config) => config.focus(_.crypto).replace(jce)
          case (_, config) => config
        },
      )

  s"A PQC-only participant $partPqcOnly with PQC-only synchronizer $syncPqcOnly" can {
    testConnectAndPing(
      partPqcOnly,
      partPqcOnly, // Ping itself
      seqPqcOnly,
      syncPqcOnly,
      happyCase = "Connect with PQC-only configurations on participant and synchronizer.",
    )
  }

  s"A PQC-default participant $partPqcDefault with PQC-only synchronizer $syncPqcOnly" can {
    testConnectAndPing(
      partPqcDefault,
      partPqcOnly, // Ping partPqcOnly
      seqPqcOnly,
      syncPqcOnly,
      happyCase = "Connect with PQC default configuration on participant to PQC only synchronizer.",
    )
  }

  s"A PQC-default participant $partPqcDefault with PQC-support synchronizer $syncPqcSupport" can {
    testConnectAndPing(
      partPqcDefault,
      partPqcDefault, // Ping itself
      seqPqcSupport,
      syncPqcSupport,
      happyCase =
        "Connect with PQC default configuration on participant to PQC supporting synchronizer.",
    )
  }

  s"A PQC-support participant $partPqcSupport with PQC-support synchronizer $syncPqcSupport" can {
    testConnectAndPing(
      partPqcSupport,
      partPqcDefault, // Ping partPqcDefault
      seqPqcSupport,
      syncPqcSupport,
      happyCase = "Connect with PQC supporting configurations on participant and synchronizer.",
    )
  }

  s"A PQC-only participant $partPqcOnly with PQC-support synchronizer $syncPqcSupport" can {
    failConnectAndPing(
      partPqcOnly,
      seqPqcSupport,
      syncPqcSupport,
      attack(threat =
        "Exploit the use of weak elliptic curve signing scheme allowed on the synchronizer when the participant requires PQC"
      ),
    )
  }

  s"A PQC-only participant $partPqcOnly with traditional synchronizer $syncTrad" can {
    failConnectAndPing(
      partPqcOnly,
      seqTrad,
      syncTrad,
      attack(threat =
        "Exploit the use of weak elliptic curve signing scheme when the participant requires PQC"
      ),
    )
  }

  s"A PQC-default participant $partPqcDefault with traditional synchronizer $syncTrad" can {
    failConnectAndPing(
      partPqcDefault,
      seqTrad,
      syncTrad,
      attack(threat =
        "Exploit the use of weak elliptic curve signing scheme when the participant requires PQC"
      ),
    )
  }

  s"A traditional participant $partTrad with PQC-only synchronizer $syncPqcOnly" can {
    failConnectAndPing(
      partTrad,
      seqPqcOnly,
      syncPqcOnly,
      attack(threat =
        "Exploit the use of weak elliptic curve signing scheme when the synchronizer requires PQC"
      ),
    )
  }

}
