// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import com.digitalasset.canton.common.sequencer.SequencerConnectClient
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.TopologyConfig
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{SigningKeyUsage, SynchronizerCrypto}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.synchronizer.SynchronizerRegistryError
import com.digitalasset.canton.sequencing.protocol.HandshakeRequest
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyStoreId,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllMappings
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  Member,
  Namespace,
  ParticipantId,
  PartyId,
  PhysicalSynchronizerId,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext, SynchronizerAlias}
import com.digitalasset.nonempty.NonEmpty
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.AtomicReference

final class SynchronizerOnboardingOutboxTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown {

  private lazy val crypto =
    SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory)
  private lazy val defaultSynchronizerCrypto =
    SynchronizerCrypto(crypto, defaultStaticSynchronizerParameters)

  private lazy val namespaceKey =
    crypto.generateSymbolicSigningKey(usage = SigningKeyUsage.NamespaceOnly)
  private lazy val namespace = Namespace(namespaceKey.id)
  private lazy val participant =
    ParticipantId(UniqueIdentifier.tryCreate("onboarding", namespace))
  private lazy val signingKey =
    crypto.generateSymbolicSigningKey(usage = SigningKeyUsage.ProtocolOnly)
  private lazy val encryptionKey = crypto.generateSymbolicEncryptionKey()

  private lazy val synchronizer = SynchronizerAlias.tryCreate("da")

  // A synchronizer with protocol version 36 doesn't admit topology transactions of protocol
  // version 37, so onboarding must re-sign the stored ones. The reverse needs no re-signing,
  // because protocol version 37 admits those of 36.
  private lazy val storePv = ProtocolVersion.v37
  private lazy val synchronizerPv = ProtocolVersion.v36

  private lazy val synchronizerStaticParameters =
    BaseTest.defaultStaticSynchronizerParametersWith(protocolVersion = synchronizerPv)
  private lazy val synchronizerPsid =
    PhysicalSynchronizerId(DefaultTestIdentities.synchronizerId, synchronizerStaticParameters)
  private lazy val synchronizerCrypto = SynchronizerCrypto(crypto, synchronizerStaticParameters)

  private def signAt(
      mapping: TopologyMapping,
      protocolVersion: ProtocolVersion,
  ): GenericSignedTopologyTransaction =
    SignedTopologyTransaction
      .signAndCreate(
        TopologyTransaction.tryCreate(
          TopologyChangeOp.Replace,
          serial = PositiveInt.one,
          mapping,
          protocolVersion,
        ),
        signingKeys = NonEmpty(Set, namespaceKey.fingerprint),
        isProposal = false,
        crypto.privateCrypto,
        protocolVersion,
      )
      .value
      .futureValueUS
      .valueOrFail("failed to sign onboarding transaction")

  private def sign(mapping: TopologyMapping): GenericSignedTopologyTransaction =
    signAt(mapping, testedProtocolVersion)

  private lazy val nsdMapping =
    NamespaceDelegation.tryCreate(namespace, namespaceKey, CanSignAllMappings)
  private lazy val otkMapping =
    OwnerToKeyMapping.tryCreate(participant, NonEmpty(Seq, signingKey, encryptionKey))
  private lazy val stcMapping =
    SynchronizerTrustCertificate(participant, DefaultTestIdentities.synchronizerId, Set.empty)
  private lazy val onboardingMappings = Seq(nsdMapping, otkMapping, stcMapping)

  private lazy val namespaceDelegation = sign(nsdMapping)
  private lazy val ownerToKeyMapping = sign(otkMapping)
  private lazy val trustCertificate = sign(stcMapping)

  private lazy val storeOnboardingTransactions = onboardingMappings.map(signAt(_, storePv))

  private def mkStore() = new InMemoryTopologyStore(
    TopologyStoreId.AuthorizedStore,
    predecessor = None,
    testedProtocolVersion,
    loggerFactory.append("store", "Authorized"),
    timeouts,
  )

  private def initiate(
      psid: PhysicalSynchronizerId,
      onboardCrypto: SynchronizerCrypto,
      store: TopologyStore[TopologyStoreId.AuthorizedStore],
      provided: Seq[GenericSignedTopologyTransaction],
  ): (Either[SynchronizerRegistryError, Boolean], RecordingSequencerConnectClient) = {
    val connectClient = new RecordingSequencerConnectClient
    val result = SynchronizerOnboardingOutbox
      .initiateOnboarding(
        synchronizer,
        psid,
        participant,
        connectClient,
        store,
        TopologyConfig(),
        timeouts,
        loggerFactory,
        onboardCrypto,
        NonEmpty.from(provided),
      )
      .value
      .futureValueUS
    (result, connectClient)
  }

  private def onboard(
      provided: Seq[GenericSignedTopologyTransaction]
  ): (Either[SynchronizerRegistryError, Boolean], RecordingSequencerConnectClient) =
    initiate(
      DefaultTestIdentities.physicalSynchronizerId,
      defaultSynchronizerCrypto,
      mkStore(),
      provided,
    )

  private def onboardFromStore(
      psid: PhysicalSynchronizerId,
      onboardCrypto: SynchronizerCrypto,
      storeTxs: Seq[GenericSignedTopologyTransaction],
  ): (Either[SynchronizerRegistryError, Boolean], RecordingSequencerConnectClient) = {
    val store = mkStore()
    val ts = CantonTimestamp.Epoch
    store
      .update(
        SequencedTime(ts),
        EffectiveTime(ts),
        additions = storeTxs.map(ValidatedTopologyTransaction(_, None)).toList,
        removals = Map.empty,
      )
      .futureValueUS
    initiate(psid, onboardCrypto, store, provided = Seq.empty)
  }

  "SynchronizerOnboardingOutbox" should {
    "dispatch provided onboarding transactions as-is" in {
      val provided = Seq(namespaceDelegation, ownerToKeyMapping, trustCertificate)
      val (result, client) = onboard(provided)
      result.valueOrFail("onboarding failed") shouldBe true
      client.recorded.get() shouldBe Some(provided)
    }

    "reject provided onboarding transactions with an unexpected mapping" in {
      val partyToParticipant = sign(
        PartyToParticipant.tryCreate(
          PartyId(UniqueIdentifier.tryCreate("alice", namespace)),
          PositiveInt.one,
          Seq.empty,
        )
      )
      val (result, client) =
        onboard(Seq(namespaceDelegation, ownerToKeyMapping, trustCertificate, partyToParticipant))
      result.left.value.cause should include("unexpected mappings")
      client.recorded.get() shouldBe None
    }

    "reject provided onboarding transactions missing the encryption key" in {
      val otkWithoutEncryptionKey =
        sign(OwnerToKeyMapping.tryCreate(participant, NonEmpty(Seq, signingKey)))
      val (result, client) =
        onboard(Seq(namespaceDelegation, otkWithoutEncryptionKey, trustCertificate))
      result.left.value.cause should include("encryption key")
      client.recorded.get() shouldBe None
    }

    "reject provided onboarding transactions without an owner-to-key mapping" in {
      val (result, client) = onboard(Seq(namespaceDelegation, trustCertificate))
      result.left.value.cause should include("exactly one owner-to-key mapping")
      client.recorded.get() shouldBe None
    }

    "reject provided onboarding transactions with several owner-to-key mappings" in {
      val (result, client) =
        onboard(Seq(namespaceDelegation, ownerToKeyMapping, ownerToKeyMapping, trustCertificate))
      result.left.value.cause should include("exactly one owner-to-key mapping")
      client.recorded.get() shouldBe None
    }

    "order provided onboarding transactions before dispatching them" in {
      val provided = Seq(namespaceDelegation, ownerToKeyMapping, trustCertificate)
      val (result, client) = onboard(provided.reverse)
      result.valueOrFail("onboarding failed") shouldBe true
      client.recorded.get() shouldBe Some(provided)
    }

    "re-sign store onboarding transactions for the synchronizer's protocol version" in {
      // sanity: the stored transactions are not admissible on the synchronizer
      forEvery(storeOnboardingTransactions)(
        _.transaction.isAdmissibleFor(synchronizerPv) shouldBe false
      )

      val (result, client) =
        onboardFromStore(synchronizerPsid, synchronizerCrypto, storeOnboardingTransactions)
      result.valueOrFail("onboarding failed") shouldBe true
      val dispatched = client.recorded.get().value
      dispatched should have size 3
      forEvery(dispatched)(_.transaction.isEquivalentTo(synchronizerPv) shouldBe true)
    }

    "fail to re-sign store onboarding transactions when the signing key is unavailable" in {
      val unavailableKeyCrypto = SynchronizerCrypto(
        SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory),
        synchronizerStaticParameters,
      )
      val (result, client) =
        onboardFromStore(synchronizerPsid, unavailableKeyCrypto, storeOnboardingTransactions)
      result.left.value shouldBe a[
        SynchronizerRegistryError.TopologyConversionError.Error
      ]
      client.recorded.get() shouldBe None
    }
  }

  private class RecordingSequencerConnectClient extends SequencerConnectClient {
    override protected def loggerFactory: NamedLoggerFactory =
      SynchronizerOnboardingOutboxTest.this.loggerFactory

    val recorded: AtomicReference[Option[Seq[GenericSignedTopologyTransaction]]] =
      new AtomicReference(None)

    override def registerOnboardingTopologyTransactions(
        member: Member,
        topologyTransactions: Seq[GenericSignedTopologyTransaction],
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, SequencerConnectClient.Error, Unit] = {
      recorded.set(Some(topologyTransactions))
      EitherT.rightT(())
    }

    override def getSynchronizerClientBootstrapInfo()(implicit traceContext: TraceContext) = ???
    override def getSynchronizerParameters()(implicit traceContext: TraceContext) = ???
    override def handshake(request: HandshakeRequest, dontWarnOnDeprecatedPV: Boolean)(implicit
        traceContext: TraceContext
    ) = ???
    override def isActive(waitForActive: Boolean)(implicit traceContext: TraceContext) = ???
    override def close(): Unit = ()
  }
}
