// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.sync

import com.daml.metrics.ExecutorServiceMetrics
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.config.KmsConfig.Driver
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{
  BatchingConfig,
  CachingConfigs,
  CryptoConfig,
  CryptoProvider,
  SessionEncryptionKeyCacheConfig,
  SessionSigningKeysConfig,
}
import com.digitalasset.canton.crypto.signer.SyncCryptoSigner
import com.digitalasset.canton.crypto.verifier.SyncCryptoVerifier
import com.digitalasset.canton.crypto.{
  Crypto,
  Hash,
  RequiredEncryptionSpecs,
  RequiredSigningSpecs,
  SignatureCheckError,
  SignatureWithoutSigner,
  SigningAlgorithmSpec,
  SigningKeySpec,
  SigningKeyUsage,
  SigningKeysWithThreshold,
  SigningPublicKey,
  SynchronizerCryptoClient,
  TestHash,
}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.replica.ReplicaManager
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.topology.DefaultTestIdentities.{participant1, participant2}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  PhysicalSynchronizerId,
  SynchronizerId,
  TestingIdentityFactory,
  TestingTopology,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.NoReportingTracerProvider
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString
import com.typesafe.config.ConfigValueFactory
import monocle.Monocle.toAppliedFocusOps
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec

trait SyncCryptoTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with BeforeAndAfterAll {

  protected val sessionSigningKeysConfig: SessionSigningKeysConfig

  // Use JceCrypto for the configured crypto schemes
  private lazy val jceStaticSynchronizerParameters: StaticSynchronizerParameters =
    StaticSynchronizerParameters(
      requiredSigningSpecs = RequiredSigningSpecs(
        CryptoProvider.Jce.signingAlgorithms.supported,
        CryptoProvider.Jce.signingKeys.supported,
      ),
      requiredEncryptionSpecs = RequiredEncryptionSpecs(
        CryptoProvider.Jce.encryptionAlgorithms.supported,
        CryptoProvider.Jce.encryptionKeys.supported,
      ),
      requiredSymmetricKeySchemes = CryptoProvider.Jce.symmetric.supported,
      requiredHashAlgorithms = CryptoProvider.Jce.hash.supported,
      requiredCryptoKeyFormats = CryptoProvider.Jce.supportedCryptoKeyFormats,
      requiredSignatureFormats = CryptoProvider.Jce.supportedSignatureFormats,
      topologyChangeDelay = StaticSynchronizerParameters.defaultTopologyChangeDelay,
      enableTransparencyChecks = false,
      protocolVersion = testedProtocolVersion,
      serial = NonNegativeInt.zero,
    )

  protected lazy val otherSynchronizerId: PhysicalSynchronizerId = PhysicalSynchronizerId(
    SynchronizerId(
      UniqueIdentifier.tryFromProtoPrimitive("other::default")
    ),
    jceStaticSynchronizerParameters,
  )

  protected def createTestingTopologyWith(
      sessionSigningKeysConfig: SessionSigningKeysConfig
  ): TestingIdentityFactory =
    TestingTopology()
      .withSynchronizers(
        synchronizers = DefaultTestIdentities.physicalSynchronizerId,
        otherSynchronizerId,
      )
      .withSimpleParticipants(participant1, participant2)
      .withStaticSynchronizerParams(jceStaticSynchronizerParameters)
      .withCryptoConfig(cryptoConfigWithSessionSigningKeysConfig(sessionSigningKeysConfig))
      .withExternalParty(
        partyId = externalParty1Id,
        participantId = participant1,
        signingKeys = SigningKeysWithThreshold(
          keys = NonEmpty.mk(Set, externalParty1Key),
          threshold = PositiveInt.tryCreate(1),
        ),
      )
      .withExternalParty(
        partyId = externalParty2Id,
        participantId = participant1,
        signingKeys = SigningKeysWithThreshold(
          keys = NonEmpty.mk(Set, externalParty2Key),
          threshold = PositiveInt.tryCreate(1),
        ),
      )
      .withExternalParty(
        partyId = externalDaoPartyId,
        participantId = participant1,
        signingKeys = SigningKeysWithThreshold(
          keys = NonEmpty.mk(Set, externalParty1Key, externalParty2Key),
          threshold = PositiveInt.tryCreate(2),
        ),
      )
      .build(crypto, loggerFactory)

  protected lazy val testingTopology: TestingIdentityFactory =
    createTestingTopologyWith(sessionSigningKeysConfig)

  protected lazy val defaultUsage: NonEmpty[Set[SigningKeyUsage]] = SigningKeyUsage.ProtocolOnly

  private val cryptoConfig: CryptoConfig = CryptoConfig()

  // we define a "fake" [[CryptoConfig]] with a session signing keys configuration to control whether
  // the testing environment uses session signing keys or not. Although the actual `crypto` implementation
  // used in the tests is a JCE provider (and not a real KMS-backed environment), this "fake" configuration
  // allows us to simulate a KMS-like setup. By enabling session signing keys within this config, we can trick
  // the system into behaving as if it's running in a KMS environment, which is useful for testing code paths
  // that depend on the presence of session signing keys without needing a real KMS infrastructure.
  protected def cryptoConfigWithSessionSigningKeysConfig(
      sessionSigningKeys: SessionSigningKeysConfig
  ): CryptoConfig =
    cryptoConfig
      .focus(_.sessionSigningKeys)
      .replace(sessionSigningKeys)
      .focus(_.kms)
      .replace(
        Some(
          Driver(
            "mock",
            ConfigValueFactory.fromAnyRef(0),
          )
        )
      )
      .focus(_.provider)
      .replace(CryptoProvider.Kms)

  protected lazy val crypto: Crypto = Crypto
    .create(
      cryptoConfig,
      CachingConfigs.defaultKmsMetadataCache,
      SessionEncryptionKeyCacheConfig(),
      CachingConfigs.defaultPublicKeyConversionCache,
      new MemoryStorage(loggerFactory, timeouts),
      Option.empty[ReplicaManager],
      testedReleaseProtocolVersion,
      futureSupervisor,
      wallClock,
      CommonMockMetrics.cryptoMetrics,
      executorService,
      timeouts,
      BatchingConfig(),
      loggerFactory,
      NoReportingTracerProvider,
      new ExecutorServiceMetrics(NoOpMetricsFactory),
    )
    .valueOrFailShutdown("Failed to create crypto object")
    .futureValue

  protected lazy val testSnapshot: TopologySnapshot = testingTopology.topologySnapshot()

  protected lazy val hash: Hash = TestHash.digest(0)

  protected lazy val p1: SynchronizerCryptoClient =
    testingTopology.forOwnerAndSynchronizer(participant1)
  protected lazy val p2: SynchronizerCryptoClient =
    testingTopology.forOwnerAndSynchronizer(participant2)

  protected lazy val syncCryptoSignerP1: SyncCryptoSigner = p1.syncCryptoSigner
  protected lazy val syncCryptoSignerP2: SyncCryptoSigner = p2.syncCryptoSigner

  protected lazy val syncCryptoVerifierP1: SyncCryptoVerifier = p1.syncCryptoVerifier
  protected lazy val syncCryptoVerifierP2: SyncCryptoVerifier = p2.syncCryptoVerifier

  protected lazy val externalParty1Id = DefaultTestIdentities.party1
  protected lazy val externalParty1Key: SigningPublicKey = crypto
    .generateSigningKey(
      keySpec = SigningKeySpec.EcCurve25519,
      usage = SigningKeyUsage.ProtocolOnly,
    )
    .futureValueUS
    .value
  protected lazy val externalParty2Id = DefaultTestIdentities.party2
  protected lazy val externalParty2Key: SigningPublicKey = crypto
    .generateSigningKey(
      keySpec = SigningKeySpec.EcCurve25519,
      usage = SigningKeyUsage.ProtocolOnly,
    )
    .futureValueUS
    .value
  protected lazy val externalDaoPartyId = DefaultTestIdentities.party3

  def syncCryptoSignerTest(): Unit = {
    "correctly sign and verify a message" in {
      val signature = syncCryptoSignerP1
        .sign(
          testSnapshot,
          None,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      syncCryptoVerifierP1
        .verifyKeyUsage(
          testSnapshot,
          participant1.member,
          signature.signedBy,
          signature.signatureDelegation,
          defaultUsage,
        )
        .futureValueUS
        .valueOrFail("key usage failed")

      syncCryptoVerifierP1
        .verifyKeyUsage(
          testSnapshot,
          participant2.member,
          signature.signedBy,
          signature.signatureDelegation,
          defaultUsage,
        )
        .futureValueUS
        .isLeft shouldBe true

      syncCryptoVerifierP1
        .verifySignature(
          testSnapshot,
          hash,
          participant1.member,
          signature,
          defaultUsage,
        )
        .futureValueUS
        .valueOrFail("verification failed")
    }

    "correctly sign and verify message from distinct participants" in {
      val signature = syncCryptoSignerP1
        .sign(
          testSnapshot,
          None,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      syncCryptoVerifierP2
        .verifySignature(
          testSnapshot,
          hash,
          participant1.member,
          signature,
          defaultUsage,
        )
        .valueOrFail("verification failed")
        .futureValueUS
    }

    "correctly sign and verify multiple messages" in {

      val signature_1 = syncCryptoSignerP1
        .sign(
          testSnapshot,
          None,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      val signature_2 = syncCryptoSignerP1
        .sign(
          testSnapshot,
          None,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      syncCryptoVerifierP1
        .verifySignatures(
          testSnapshot,
          hash,
          p1.member,
          NonEmpty.mk(Seq, signature_1, signature_2),
          defaultUsage,
        )
        .valueOrFail("verification failed")
        .futureValueUS

    }

    "correctly sign and verify group signatures" in {

      val signature1 = syncCryptoSignerP1
        .sign(
          testSnapshot,
          None,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      val signature2 = syncCryptoSignerP2
        .sign(
          testSnapshot,
          None,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      syncCryptoVerifierP1
        .verifyGroupSignatures(
          testSnapshot,
          hash,
          Seq(participant1.member, participant2.member),
          PositiveInt.two,
          "group",
          NonEmpty.mk(Seq, signature1, signature2),
          defaultUsage,
        )
        .valueOrFail("verification failed")
        .futureValueUS

    }

    "correctly verify party signatures" in {

      val signature = crypto.privateCrypto
        .signBytes(
          bytes = ByteString.copyFromUtf8("hello, world!"),
          signingKeyId = externalParty1Key.fingerprint,
          usage = SigningKeyUsage.ProtocolOnly,
          signingAlgorithmSpec = SigningAlgorithmSpec.Ed25519,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      val signatureWithoutSigner = SignatureWithoutSigner(
        format = signature.format,
        signature = signature.unwrap,
        signingAlgorithmSpec = signature.signingAlgorithmSpec.valueOrFail("no algorithm spec"),
      )

      List(syncCryptoVerifierP1, syncCryptoVerifierP2).foreach { verifier =>
        verifier
          .verifyPartyJwtSignature(
            topologySnapshot = testSnapshot,
            bytes = ByteString.copyFromUtf8("hello, world!"),
            signer = externalParty1Id,
            signature = signatureWithoutSigner,
            usage = SigningKeyUsage.ProtocolOnly,
          )
          .valueOrFail("verification failed")
          .futureValueUS
      }

      syncCryptoVerifierP1
        .verifyPartyJwtSignature(
          topologySnapshot = testSnapshot,
          bytes = ByteString.copyFromUtf8("goodbye, cruel world!"), // Wrong
          signer = externalParty1Id,
          signature = signatureWithoutSigner,
          usage = SigningKeyUsage.ProtocolOnly,
        )
        .futureValueUS
        .left
        .value shouldBe a[SignatureCheckError.InvalidSignature]

      syncCryptoVerifierP1
        .verifyPartyJwtSignature(
          topologySnapshot = testSnapshot,
          bytes = ByteString.copyFromUtf8("hello, world!"),
          signer = externalParty2Id, // Wrong
          signature = signatureWithoutSigner,
          usage = SigningKeyUsage.ProtocolOnly,
        )
        .futureValueUS
        .left
        .value shouldBe a[SignatureCheckError.InvalidSignature]

      syncCryptoVerifierP1
        .verifyPartyJwtSignature(
          topologySnapshot = testSnapshot,
          bytes = ByteString.copyFromUtf8("hello, world!"),
          signer = externalDaoPartyId, // Dao with threshold
          signature = signatureWithoutSigner,
          usage = SigningKeyUsage.ProtocolOnly,
        )
        .futureValueUS
        .left
        .value shouldBe a[SignatureCheckError.PartyKeysInvalidThreshold]
    }
  }

  override def afterAll(): Unit = {
    LifeCycle.close(
      syncCryptoSignerP1,
      syncCryptoSignerP2,
    )(logger)
    super.afterAll()
  }

}
