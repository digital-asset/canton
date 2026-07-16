// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.SecureConfiguration
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.admin.api.client.data.{
  StaticSynchronizerParameters,
  crypto as dataCrypto,
}
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{
  CryptoConfig,
  CryptoProvider,
  CryptoSchemeConfig,
  SigningSchemeConfig,
}
import com.digitalasset.canton.console.{MediatorReference, SequencerReference}
import com.digitalasset.canton.crypto.{
  EncryptionAlgorithmSpec,
  SigningAlgorithmSpec,
  SigningKeySpec,
}
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.synchronizer.config.SynchronizerParametersConfig
import com.digitalasset.nonempty.NonEmpty
import monocle.macros.syntax.lens.*

trait CryptoHandshakeIntegrationTestBase {
  self: CommunityIntegrationTest & SharedEnvironment & SecurityTestSuite =>

  protected val securityAsset: SecurityTest =
    SecurityTest(property = SecureConfiguration, asset = "participant node")

  protected def attack(threat: String): Attack = Attack(
    actor = "A network participant that can reach the public api",
    threat,
    mitigation = "Refuse to connect the participant to the synchronizer",
  )

  protected def testConnectAndPing(
      participantName: String,
      pingParticipantName: String,
      compatibleSequencerName: String,
      compatibleSynchronizerName: String,
      happyCase: String,
  ): Unit =
    s"connect to a compatible synchronizer and ping $pingParticipantName" taggedAs securityAsset
      .setHappyCase(happyCase) in { implicit env =>
      import env.*

      p(participantName).synchronizers
        .connect_local(s(compatibleSequencerName), alias = compatibleSynchronizerName)

      val compatibleSynchronizerId = eventually() {
        withClue("synchronizer should be connected after restart") {
          val compatibleSynchronizer = p(participantName).synchronizers
            .list_connected()
            .find(_.synchronizerAlias.unwrap == compatibleSynchronizerName)
          compatibleSynchronizer should not be empty
          compatibleSynchronizer.value.physicalSynchronizerId
        }
      }

      p(participantName).health
        .ping(p(pingParticipantName).id, synchronizerId = Some(compatibleSynchronizerId))
    }

  protected def failConnectAndPing(
      participantName: String,
      incompatibleSequencerName: String,
      incompatibleSynchronizerName: String,
      attack: Attack,
  ): Unit =
    "not connect to an incompatible synchronizer" taggedAs securityAsset.setAttack(attack) in {
      implicit env =>
        import env.*

        assertThrowsAndLogsCommandFailures(
          p(participantName).synchronizers
            .connect_local(s(incompatibleSequencerName), alias = incompatibleSynchronizerName),
          out => {
            out.commandFailureMessage should (include("Required schemes") and include(
              "are not supported/allowed"
            ) or include("scheme is not a required scheme"))
          },
        )
    }

}

trait CryptoHandshakeIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SecurityTestSuite
    with CryptoHandshakeIntegrationTestBase {

  private val sync1: String = "synchronizer1"
  private val sync2: String = "synchronizer2"
  private val sync3: String = "synchronizer3"
  protected val seq1: String = "sequencer1"
  protected val seq2: String = "sequencer2"
  protected val seq3: String = "sequencer3"
  private val med1: String = "mediator1"
  private val med2: String = "mediator2"
  private val med3: String = "mediator3"

  private val part1: String = "participant1"
  private val part2: String = "participant2"
  private val part3: String = "participant3"
  private val part4: String = "participant4"

  // Default JCE
  private val jce: CryptoConfig = CryptoConfig(provider = CryptoProvider.Jce)

  // JCE with limit of Ed25519 signing
  private val jceWithOnlySigEd25519: CryptoConfig = jce.copy(
    signing = SigningSchemeConfig(
      algorithms = CryptoSchemeConfig(
        default = Some(SigningAlgorithmSpec.Ed25519),
        allowed = Some(NonEmpty.mk(Set, SigningAlgorithmSpec.Ed25519)),
      ),
      keys = CryptoSchemeConfig(
        default = Some(SigningKeySpec.EcCurve25519),
        allowed = Some(NonEmpty.mk(Set, SigningKeySpec.EcCurve25519)),
      ),
    )
  )

  // JCE with default ECDSA P-256
  private val jceWithOnlySigEcDsaP256: CryptoConfig = jce.copy(
    signing = SigningSchemeConfig(
      algorithms = CryptoSchemeConfig(
        default = Some(SigningAlgorithmSpec.EcDsaSha256),
        allowed = Some(NonEmpty.mk(Set, SigningAlgorithmSpec.EcDsaSha256)),
      ),
      keys = CryptoSchemeConfig(
        default = Some(SigningKeySpec.EcP256),
        allowed = Some(NonEmpty.mk(Set, SigningKeySpec.EcP256)),
      ),
    )
  )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .P4_S1M1_S1M1_S1M1(
        Map(
          sync1 -> StaticSynchronizerParameters
            .defaults(cryptoConfig = jce, protocolVersion = testedProtocolVersion),
          sync2 -> StaticSynchronizerParameters.fromConfig(
            SynchronizerParametersConfig()
              .copy(
                requiredSigningAlgorithmSpecs =
                  Some(NonEmpty.mk(Set, SigningAlgorithmSpec.EcDsaSha256)),
                requiredSigningKeySpecs = Some(NonEmpty.mk(Set, SigningKeySpec.EcP256)),
              ),
            jce,
            testedProtocolVersion,
          ),
          sync3 ->
            StaticSynchronizerParameters.fromConfig(
              SynchronizerParametersConfig().copy(requiredEncryptionAlgorithmSpecs =
                Some(NonEmpty.mk(Set, EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc))
              ),
              jce,
              testedProtocolVersion,
            ),
        )
      )
      .addConfigTransforms(
        ConfigTransforms.updateAllParticipantConfigs {
          case (`part1`, config) => config.focus(_.crypto).replace(jce)
          case (`part2`, config) => config.focus(_.crypto).replace(jceWithOnlySigEd25519)
          case (`part3`, config) => config.focus(_.crypto).replace(jceWithOnlySigEcDsaP256)
          case (`part4`, config) => config.focus(_.crypto).replace(jce)
          case (_, config) => config
        },
        ConfigTransforms.updateAllSequencerConfigs {
          case (`seq1`, config) => config.focus(_.crypto).replace(jce)
          case (`seq2`, config) => config.focus(_.crypto).replace(jceWithOnlySigEcDsaP256)
          case (`seq3`, config) => config.focus(_.crypto).replace(jce)
          case (_, config) => config
        },
        ConfigTransforms.updateAllMediatorConfigs {
          case (`med1`, config) => config.focus(_.crypto).replace(jce)
          case (`med2`, config) => config.focus(_.crypto).replace(jceWithOnlySigEcDsaP256)
          case (`med3`, config) => config.focus(_.crypto).replace(jce)
          case (_, config) => config
        },
      )

  s"Participant $part1 with synchronizer $sync1" can {
    testConnectAndPing(
      part1,
      part1, // Ping itself
      seq1,
      sync1,
      happyCase = "Connect with JCE crypto provider and default configuration.",
    )
  }

  // p2 cannot connect because it only allows Ed25519 and not all signing schemes of the synchronizer
  s"Participant $part2 with synchronizer $sync1" can {
    failConnectAndPing(
      part2,
      seq1,
      sync1,
      attack(threat = "Exploit a weak signing scheme"),
    )
  }

  // p2 cannot connect because it only allows Ed25519 and not all signing schemes of the synchronizer
  s"Participant $part2 with synchronizer $sync2" can {
    failConnectAndPing(
      part2,
      seq2,
      sync2,
      attack(threat = "Exploit a weak signing scheme"),
    )
  }

  s"Participants $part3 with synchronizer $sync2" can {
    testConnectAndPing(
      part3,
      part3,
      seq2,
      sync2,
      happyCase = "Connect with JCE crypto provider and enforcing ECDSA P-256 as signing scheme.",
    )
  }

  s"Participant $part4 with synchronizer $sync3" can {
    testConnectAndPing(
      part4,
      part4, // Ping itself
      seq3,
      sync3,
      happyCase = "Connect with JCE crypto provider and default configuration.",
    )
  }

}

trait CryptoHandshakeInBootstrapIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SecurityTestSuite {

  private val onlyEcP256Config = SigningSchemeConfig(
    algorithms = CryptoSchemeConfig(
      default = Some(SigningAlgorithmSpec.EcDsaSha256),
      allowed = Some(NonEmpty.mk(Set, SigningAlgorithmSpec.EcDsaSha256)),
    ),
    keys = CryptoSchemeConfig(
      default = Some(SigningKeySpec.EcP256),
      allowed = Some(NonEmpty.mk(Set, SigningKeySpec.EcP256)),
    ),
  )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P0S2M2_Manual
      .addConfigTransforms(
        ConfigTransforms.updateAllSequencerConfigs_(
          _.focus(_.crypto.signing)
            .replace(onlyEcP256Config)
        ),
        ConfigTransforms.updateAllMediatorConfigs_(
          _.focus(_.crypto.signing)
            .replace(onlyEcP256Config)
        ),
      )

  private def bootstrapWithStaticParameters(
      sequencer: SequencerReference,
      mediator: MediatorReference,
      staticSynchronizerParameters: StaticSynchronizerParameters,
  )(implicit env: TestConsoleEnvironment): Unit =
    NetworkBootstrapper(
      Seq(
        NetworkTopologyDescription(
          env.daName,
          synchronizerOwners = Seq(sequencer),
          synchronizerThreshold = PositiveInt.one,
          sequencers = Seq(sequencer),
          mediators = Seq(mediator),
          overrideStaticSynchronizerParameters = Some(staticSynchronizerParameters),
        )(env)
      )
    )(env).bootstrap()

  private def requiredSigningSpecs(
      algorithm: dataCrypto.SigningAlgorithmSpec,
      key: dataCrypto.SigningKeySpec,
  ) =
    dataCrypto.RequiredSigningSpecs(
      algorithms = NonEmpty.mk(Set, algorithm),
      keys = NonEmpty.mk(Set, key),
    )

  "Bootstrap fails if sequencer or mediator has incompatible crypto configuration" taggedAs SecurityTest(
    property = SecureConfiguration,
    asset = "synchronizer",
  ) in { implicit env =>
    import env.*

    sequencer1.start()
    mediator1.start()

    assertThrowsAndLogsCommandFailures(
      bootstrapWithStaticParameters(
        sequencer1,
        mediator1,
        // Override the default static synchronizer parameters to require Ed25519 signing scheme,
        // which is incompatible with the sequencer and mediator's configuration
        EnvironmentDefinition.defaultStaticSynchronizerParameters.copy(
          requiredSigningSpecs = requiredSigningSpecs(
            dataCrypto.SigningAlgorithmSpec.Ed25519,
            dataCrypto.SigningKeySpec.EcCurve25519,
          )
        ),
      ),
      out => {
        out.commandFailureMessage should include("Required schemes")
        out.commandFailureMessage should include("are not supported/allowed")
      },
    )
  }

  "Bootstrap succeeds if the crypto configuration is compatible with the sequencer and mediator" in {
    implicit env =>
      import env.*

      sequencer2.start()
      mediator2.start()

      bootstrapWithStaticParameters(
        sequencer2,
        mediator2,
        // Override the default static synchronizer parameters to require EcDsaSha256 signing scheme,
        // which is compatible with the sequencer and mediator's configuration
        EnvironmentDefinition.defaultStaticSynchronizerParameters.copy(
          requiredSigningSpecs = requiredSigningSpecs(
            dataCrypto.SigningAlgorithmSpec.EcDsaSha256,
            dataCrypto.SigningKeySpec.EcP256,
          )
        ),
      )
  }
}

class CryptoHandshakeInBootstrapIntegrationTestDefault
    extends CryptoHandshakeInBootstrapIntegrationTest

class CryptoHandshakeIntegrationTestPostgres extends CryptoHandshakeIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set(seq1), Set(seq2), Set(seq3))
          .map(_.map(InstanceName.tryCreate))
      ),
    )
  )
}
