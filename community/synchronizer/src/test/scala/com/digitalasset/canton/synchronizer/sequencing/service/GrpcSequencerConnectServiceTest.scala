// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencer.api.v30.SequencerConnect
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{SynchronizerTopologyClient, TopologySnapshot}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AnyWordSpec

class GrpcSequencerConnectServiceTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with MockitoSugar {

  private class Env(initialTimeBound: Option[CantonTimestamp] = None) {
    val synchronizerId = DefaultTestIdentities.synchronizerId
    val psid: PhysicalSynchronizerId = synchronizerId.toPhysical
    val sequencerId = DefaultTestIdentities.sequencerId
    val participantId = DefaultTestIdentities.participant1

    // methods for key generation, signing, etc. for the participant
    val factory = new TestingOwnerWithKeys(participantId, loggerFactory, parallelExecutionContext)

    // Mocks: external dependencies
    val topologyManager = mock[SynchronizerTopologyManager]
    val cryptoClient = mock[SynchronizerCryptoClient]
    val topologyClient = mock[SynchronizerTopologyClient]
    val mockSnapshot = mock[TopologySnapshot]

    val clock = new SimClock(CantonTimestamp.Epoch, loggerFactory = loggerFactory)

    val staticSynchronizerParameters = BaseTest.defaultStaticSynchronizerParametersWith()

    // "wire" the cryptoClient and topologyClient mocks together
    when(cryptoClient.ips).thenReturn(topologyClient)
    // "wire" the topologyClient and mockSnapshot mocks together
    when(topologyClient.headSnapshot).thenReturn(mockSnapshot)

    val service = new GrpcSequencerConnectService(
      psid,
      sequencerId,
      staticSynchronizerParameters,
      topologyManager,
      cryptoClient,
      clock,
      initialTimeBound,
      loggerFactory,
    )(parallelExecutionContext)

    // --- Helper methods using the TestingOwnerWithKeys factory ---

    // mock transactions for participant1
    def mockTx(
        mapping: TopologyMapping
    ): SignedTopologyTransaction[TopologyChangeOp, TopologyMapping] =
      factory.mkAdd(mapping, signingKey = factory.SigningKeys.key1)

    def createSTC(pId: ParticipantId = participantId): SynchronizerTrustCertificate =
      SynchronizerTrustCertificate(pId, synchronizerId)

    def createOTK(
        pId: ParticipantId = participantId,
        hasSigning: Boolean = true,
        hasEncryption: Boolean = true,
    ): OwnerToKeyMapping = {
      // use a builder to decide which keys to include
      val keys = Seq.newBuilder[PublicKey]
      if (hasSigning) keys += factory.SigningKeys.key1
      if (hasEncryption) keys += factory.EncryptionKeys.key1
      OwnerToKeyMapping.tryCreate(
        pId,
        NonEmpty.from(keys.result()).getOrElse(NonEmpty(Seq, factory.SigningKeys.key1)),
      )
    }
  }

  "GrpcSequencerConnectService" should {
    val totalTxnLimit = PositiveInt.tryCreate(7)

    "reject requests without memberId in the context" in {
      val env = new Env()

      val defaultParams = com.digitalasset.canton.protocol.DynamicSynchronizerParameters
        .defaultValues(env.staticSynchronizerParameters.protocolVersion)

      val versionedParams = com.digitalasset.canton.protocol.SynchronizerParameters.WithValidity(
        validFrom = CantonTimestamp.Epoch,
        validUntil = None,
        parameter = defaultParams,
      )

      // stub the findDynamicSynchronizerParameters method of the mock snapshot
      // to return the default parameter state setup above
      val _ = doReturn(FutureUnlessShutdown.pure(versionedParams))
        .when(env.mockSnapshot)
        .findDynamicSynchronizerParameters()(anyTraceContext)

      // prepare the protobuf message to be sent to the service (empty)
      val request = SequencerConnect.RegisterOnboardingTopologyTransactionsRequest(Seq.empty)

      // Send the message to the service, it should be refused because there is
      // no memberId in the grpc context
      val result = env.service.registerOnboardingTopologyTransactions(request).failed.futureValue

      result.getMessage should (include("Unable to find member id"))
    }

    // --- validateOnboardingTransactions logic  ---

    "enforce the transaction batch limit" in {
      val env = new Env()
      val tooManyTxs = (1 to (totalTxnLimit.value + 1)).map(_ => env.mockTx(env.createSTC()))
      val result = env.service
        .validateOnboardingTransactions(env.participantId, tooManyTxs, totalTxnLimit)
        .value
        .futureValue
      result.left.value.getStatus.getDescription should include("Too many topology transactions")
    }

    "enforce exactly one SynchronizerTrustCertificate" in {
      val env = new Env()

      // Test Missing
      env.service
        .validateOnboardingTransactions(
          env.participantId,
          Seq(env.mockTx(env.createOTK())),
          totalTxnLimit,
        )
        .value
        .futureValue
        .left
        .value
        .getStatus
        .getDescription should include("Exactly one SynchronizerTrustCertificate is required")

      // Test Multiple
      val multiStc =
        Seq(env.mockTx(env.createSTC()), env.mockTx(env.createSTC()), env.mockTx(env.createOTK()))
      env.service
        .validateOnboardingTransactions(env.participantId, multiStc, totalTxnLimit)
        .value
        .futureValue
        .left
        .value
        .getStatus
        .getDescription should include("Exactly one SynchronizerTrustCertificate is required")
    }

    "reject if NamespaceDelegation is missing" in {
      val env = new Env()
      val txs = Seq(env.mockTx(env.createSTC()), env.mockTx(env.createOTK()))
      val result = env.service
        .validateOnboardingTransactions(env.participantId, txs, PositiveInt.tryCreate(10))
        .value
        .futureValue
      result.left.value.getStatus.getDescription should include("Missing mappings")
    }

    "enforce identity and authority constraints" in {
      val env = new Env()
      val otherPId = DefaultTestIdentities.participant2
      val nsDelegation = NamespaceDelegation.tryCreate(
        otherPId.namespace,
        env.factory.SigningKeys.key1,
        DelegationRestriction.CanSignAllMappings,
      )
      env.service
        .validateOnboardingTransactions(
          env.participantId,
          Seq(
            env.mockTx(env.createSTC(otherPId)),
            env.mockTx(env.createOTK(otherPId)),
            env.factory.mkAdd(nsDelegation),
          ),
          totalTxnLimit,
        )
        .value
        .futureValue
        .left
        .value
        .getStatus
        .getDescription should include("Mappings for unexpected UIDs")

      val wrongNs = NamespaceDelegation.tryCreate(
        Namespace(Fingerprint.tryFromString("default")),
        env.factory.SigningKeys.key1,
        DelegationRestriction.CanSignAllMappings,
      )
      env.service
        .validateOnboardingTransactions(
          env.participantId,
          Seq(env.mockTx(env.createSTC()), env.mockTx(env.createOTK()), env.factory.mkAdd(wrongNs)),
          totalTxnLimit,
        )
        .value
        .futureValue
        .left
        .value
        .getStatus
        .getDescription should include("Mappings for unexpected namespaces")

      val proposal = env.factory.mkAdd(env.createSTC(), isProposal = true)
      env.service
        .validateOnboardingTransactions(
          env.participantId,
          Seq(proposal, env.mockTx(env.createOTK()), env.factory.mkAdd(nsDelegation)),
          totalTxnLimit,
        )
        .value
        .futureValue
        .left
        .value
        .getStatus
        .getDescription should include("Unexpected proposals")

      val removal = env.factory.mkRemove(env.createSTC())
      env.service
        .validateOnboardingTransactions(
          env.participantId,
          Seq(removal, env.mockTx(env.createOTK()), env.factory.mkAdd(nsDelegation)),
          totalTxnLimit,
        )
        .value
        .futureValue
        .left
        .value
        .getStatus
        .getDescription should include("Unexpected removals")
    }

    "enforce exactly one OwnerToKeyMapping" in {
      val env = new Env()
      val stc = env.mockTx(env.createSTC())

      // Missing
      val missingOtkResult = env.service
        .validateOnboardingTransactions(
          env.participantId,
          Seq(stc),
          totalTxnLimit,
        )
        .value
        .futureValue

      missingOtkResult.left.value.getStatus.getDescription should include(
        "Exactly one OwnerToKeyMapping is required"
      )

      // Multiple
      val multipleOtkResult = env.service
        .validateOnboardingTransactions(
          env.participantId,
          Seq(stc, env.mockTx(env.createOTK()), env.mockTx(env.createOTK())),
          totalTxnLimit,
        )
        .value
        .futureValue

      multipleOtkResult.left.value.getStatus.getDescription should include(
        "Exactly one OwnerToKeyMapping is required. Found: 2"
      )
    }

    "successfully validate a correct set of onboarding transactions (happy path)" in {
      val env = new Env()
      val stc = env.createSTC()
      val otk = env.createOTK(hasSigning = true, hasEncryption = true)
      val nsDelegation = NamespaceDelegation.tryCreate(
        env.participantId.namespace,
        env.factory.SigningKeys.key1,
        DelegationRestriction.CanSignAllMappings,
      )
      val txs = Seq(
        env.mockTx(stc),
        env.mockTx(otk),
        env.factory.mkAdd(nsDelegation),
      )
      val result = env.service
        .validateOnboardingTransactions(
          env.participantId,
          txs,
          PositiveInt.tryCreate(10),
        )
        .value
        .futureValue
      result shouldBe Right(())
    }
  }
}
