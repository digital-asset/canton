// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service

import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencer.api.v30.SequencerConnect
import com.digitalasset.canton.synchronizer.metrics.SequencerTestMetrics
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerLimits
import com.digitalasset.canton.synchronizer.sequencer.time.LsuSequencingBounds
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{SynchronizerTopologyClient, TopologySnapshot}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext, ProtocolVersionChecksAnyWordSpec}
import com.digitalasset.nonempty.NonEmpty
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AnyWordSpec

class GrpcSequencerConnectServiceTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAnyWordSpec
    with MockitoSugar {

  lazy private val sequencerLimits = SequencerLimits()

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
      initialTimeBound.map(ts =>
        LsuSequencingBounds.unsafeCreate(upgradeTime = ts, lowerBoundSequencingTimeExclusive = ts)
      ),
      sequencerLimits,
      sanitizePublicErrorMessages = false,
      disableReleaseVersionHandshakeCheck = false,
      SequencerTestMetrics(this.getClass.getSimpleName),
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
      val request = SequencerConnect.RegisterOnboardingTopologyTransactionsRequest(
        Seq.empty[com.digitalasset.canton.protocol.v30.SignedTopologyTransaction]
      )

      // Send the message to the service, it should be refused because there is
      // no memberId in the grpc context
      inside(env.service.registerOnboardingTopologyTransactions(request).failed.futureValue) {
        case ex: StatusRuntimeException =>
          ex.getStatus.getDescription shouldBe "Unable to find participant id in gRPC context"
          ex.getStatus.getCode shouldBe Code.INVALID_ARGUMENT
      }
    }

    // --- validateOnboardingTransactions logic  ---

    "enforce the transaction batch limit" in {
      val env = new Env()

      val tooMany = GrpcSequencerConnectService.maxOnboardingTransactions.value + 1
      val tooManyTxs =
        (1 to tooMany).map(_ => env.mockTx(env.createSTC()))

      val result = env.service
        .parseAndValidateOnboardingTransactions(env.participantId, tooManyTxs.map(_.toProtoV30))

      // Below boundsCheck the manual length check applies, with a different error message than the
      // check from the proto validation tooling
      if (testedProtocolVersion < ProtocolVersion.boundsCheck) {
        result.left.value.getDescription should include("Too many topology transactions")
      } else {
        result.left.value.getDescription should include(
          s"repeated field has $tooMany elements, exceeding the maximum of ${GrpcSequencerConnectService.maxOnboardingTransactions.value}"
        )
      }
    }

    "enforce exactly one SynchronizerTrustCertificate" in {
      val env = new Env()

      // Test Missing
      env.service
        .validateOnboardingTransactions(
          env.participantId,
          Seq(env.mockTx(env.createOTK())),
        )
        .left
        .value
        .getDescription should include("Exactly one SynchronizerTrustCertificate is required")

      // Test Multiple
      val multiStc =
        Seq(env.mockTx(env.createSTC()), env.mockTx(env.createSTC()), env.mockTx(env.createOTK()))
      env.service
        .validateOnboardingTransactions(env.participantId, multiStc)
        .left
        .value
        .getDescription should include("Exactly one SynchronizerTrustCertificate is required")
    }

    "reject if NamespaceDelegation is missing" in {
      val env = new Env()
      val txs = Seq(env.mockTx(env.createSTC()), env.mockTx(env.createOTK()))
      val result = env.service
        .validateOnboardingTransactions(env.participantId, txs)
      result.left.value.getDescription should include("Missing mappings")
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
        )
        .left
        .value
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
        )
        .left
        .value
        .getDescription should include("Mappings for unexpected namespaces")

      val proposal = env.factory.mkAdd(env.createSTC(), isProposal = true)
      env.service
        .validateOnboardingTransactions(
          env.participantId,
          Seq(proposal, env.mockTx(env.createOTK()), env.factory.mkAdd(nsDelegation)),
        )
        .left
        .value
        .getDescription should include("Unexpected proposals")

      val removal = env.factory.mkRemove(env.createSTC())
      env.service
        .validateOnboardingTransactions(
          env.participantId,
          Seq(removal, env.mockTx(env.createOTK()), env.factory.mkAdd(nsDelegation)),
        )
        .left
        .value
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
        )

      missingOtkResult.left.value.getDescription should include(
        "Exactly one OwnerToKeyMapping is required"
      )

      // Multiple
      val multipleOtkResult = env.service
        .validateOnboardingTransactions(
          env.participantId,
          Seq(stc, env.mockTx(env.createOTK()), env.mockTx(env.createOTK())),
        )

      multipleOtkResult.left.value.getDescription should include(
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
        )
      result shouldBe Right(())
    }

    // --- handshake clientProtocolVersions size limit ---

    "reject a handshake request that exceeds the clientProtocolVersions limit" onlyRunWithOrGreaterThan ProtocolVersion.boundsCheck in {
      val env = new Env()
      val maxClientProtocolVersions = sequencerLimits.maxClientProtocolVersions.value
      val exceedingClientProtocolVersions =
        maxClientProtocolVersions + 1 // One over the configured limit
      val request = SequencerConnect.HandshakeRequest(
        clientProtocolVersions = Seq.fill(exceedingClientProtocolVersions)(30),
        minimumProtocolVersion = None,
        clientVersion = "",
      )
      inside(env.service.handshake(request).failed.futureValue) { case ex: StatusRuntimeException =>
        ex.getStatus.getCode shouldBe Code.INVALID_ARGUMENT
        ex.getStatus.getDescription should include(
          s"exceeding the maximum of $maxClientProtocolVersions"
        )
      }
    }

    "accept a handshake request with exactly the clientProtocolVersions limit" in {
      val env = new Env()
      val protocolVersion = env.staticSynchronizerParameters.protocolVersion.v
      val maxClientProtocolVersions = SequencerLimits().maxClientProtocolVersions.value

      val request = SequencerConnect.HandshakeRequest(
        clientProtocolVersions = Seq.fill(maxClientProtocolVersions)(protocolVersion),
        minimumProtocolVersion = None,
        clientVersion = com.digitalasset.canton.version.ReleaseVersion.current.toProtoPrimitive,
      )

      val response = env.service.handshake(request).futureValue
      response.serverProtocolVersion shouldBe protocolVersion
    }
  }
}
