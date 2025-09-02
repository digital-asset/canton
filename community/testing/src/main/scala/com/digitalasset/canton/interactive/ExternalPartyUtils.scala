// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.interactive

import com.daml.ledger.api.v2.admin.party_management_service.AllocateExternalPartyRequest
import com.daml.ledger.api.v2.interactive.interactive_submission_service as iss
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest.{testedProtocolVersion, testedReleaseProtocolVersion}
import com.digitalasset.canton.FutureHelpers
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{CryptoConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.kms.CommunityKmsFactory
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreFactory
import com.digitalasset.canton.interactive.ExternalPartyUtils.{
  ExternalParty,
  OnboardingTransactions,
}
import com.digitalasset.canton.logging.SuppressingLogger
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.time.WallClock
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllMappings
import com.digitalasset.canton.topology.transaction.TopologyTransaction.GenericTopologyTransaction
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TraceContext}
import com.google.protobuf.ByteString
import io.scalaland.chimney.dsl.*
import org.scalatest.EitherValues

import scala.concurrent.ExecutionContext

object ExternalPartyUtils {
  final case class ExternalParty(
      partyId: PartyId,
      signingFingerprints: NonEmpty[Seq[Fingerprint]],
  ) {
    def primitiveId: String = partyId.toProtoPrimitive
  }
  final case class OnboardingTransactions(
      namespaceDelegation: TopologyTransaction[TopologyChangeOp.Replace, NamespaceDelegation],
      partyToParticipant: TopologyTransaction[TopologyChangeOp.Replace, PartyToParticipant],
      partyToKeyMapping: TopologyTransaction[TopologyChangeOp.Replace, PartyToKeyMapping],
      multiHashSignatures: Seq[Signature],
      singleTransactionSignatures: Seq[(GenericTopologyTransaction, Seq[Signature])],
  ) {
    def toAllocateExternalPartyRequest(
        synchronizerId: SynchronizerId,
        identityProviderId: String = "",
    ): AllocateExternalPartyRequest =
      AllocateExternalPartyRequest(
        synchronizerId = synchronizerId.toProtoPrimitive,
        onboardingTransactions = singleTransactionSignatures.map { case (transaction, signatures) =>
          AllocateExternalPartyRequest.SignedTransaction(
            transaction.getCryptographicEvidence,
            signatures.map(_.toProtoV30.transformInto[iss.Signature]),
          )
        },
        multiHashSignatures = multiHashSignatures.map(
          _.toProtoV30.transformInto[iss.Signature]
        ),
        identityProviderId = identityProviderId,
      )
  }
}

trait ExternalPartyUtils extends FutureHelpers with EitherValues {

  def loggerFactory: SuppressingLogger
  def futureSupervisor: FutureSupervisor
  protected def timeouts: ProcessingTimeout
  def wallClock: WallClock

  implicit def externalPartyExecutionContext: ExecutionContext
  implicit protected def traceContext: TraceContext

  private val storage = new MemoryStorage(loggerFactory, timeouts)

  lazy val crypto: Crypto = Crypto
    .create(
      CryptoConfig(),
      storage,
      CryptoPrivateStoreFactory.withoutKms(wallClock, externalPartyExecutionContext),
      CommunityKmsFactory,
      testedReleaseProtocolVersion,
      nonStandardConfig = false,
      futureSupervisor,
      wallClock,
      externalPartyExecutionContext,
      timeouts,
      loggerFactory,
      NoReportingTracerProvider,
    )
    .valueOrFailShutdown("Failed to create crypto object")
    .futureValue

  def generateProtocolSigningKeys(numberOfKeys: PositiveInt) =
    NonEmpty
      .from(
        Seq.fill(numberOfKeys.value)(
          crypto.generateSigningKey(usage = SigningKeyUsage.ProtocolOnly).futureValueUS.value
        )
      )
      .getOrElse(
        fail("Expected at least one protocol signing key")
      )

  def generateExternalPartyOnboardingTransactions(
      name: String,
      confirming: ParticipantId,
      extraConfirming: Seq[ParticipantId] = Seq.empty,
      observing: Seq[ParticipantId] = Seq.empty,
      confirmationThreshold: PositiveInt = PositiveInt.one,
      numberOfKeys: PositiveInt = PositiveInt.one,
      keyThreshold: PositiveInt = PositiveInt.one,
  ): (OnboardingTransactions, ExternalParty) = {

    val namespaceKey: SigningPublicKey =
      crypto.generateSigningKey(usage = SigningKeyUsage.NamespaceOnly).futureValueUS.value
    val partyId: PartyId = PartyId.tryCreate(name, namespaceKey.fingerprint)
    val protocolSigningKeys: NonEmpty[Seq[SigningPublicKey]] = generateProtocolSigningKeys(
      numberOfKeys
    )

    val namespaceDelegationTx =
      TopologyTransaction(
        TopologyChangeOp.Replace,
        serial = PositiveInt.one,
        NamespaceDelegation.tryCreate(
          namespace = partyId.uid.namespace,
          target = namespaceKey,
          CanSignAllMappings,
        ),
        testedProtocolVersion,
      )

    val allConfirming = NonEmpty.mk(Seq, confirming, extraConfirming*)
    val confirmingHostingParticipants = allConfirming.forgetNE.map { cp =>
      HostingParticipant(
        cp,
        ParticipantPermission.Confirmation,
      )
    }
    val observingHostingParticipants = observing.map { op =>
      HostingParticipant(
        op,
        ParticipantPermission.Observation,
      )
    }
    val partyToParticipantTx =
      TopologyTransaction(
        TopologyChangeOp.Replace,
        serial = PositiveInt.one,
        PartyToParticipant.tryCreate(
          partyId = partyId,
          threshold = confirmationThreshold,
          participants = confirmingHostingParticipants ++ observingHostingParticipants,
        ),
        testedProtocolVersion,
      )

    val partyToKeyTx =
      TopologyTransaction(
        TopologyChangeOp.Replace,
        serial = PositiveInt.one,
        PartyToKeyMapping.tryCreate(
          partyId = partyId,
          threshold = keyThreshold,
          signingKeys = protocolSigningKeys,
        ),
        testedProtocolVersion,
      )

    val transactionHashes =
      NonEmpty.mk(Set, namespaceDelegationTx.hash, partyToParticipantTx.hash, partyToKeyTx.hash)
    val combinedMultiTxHash =
      MultiTransactionSignature.computeCombinedHash(transactionHashes, crypto.pureCrypto)

    // Sign the multi hash with the namespace key, as it is needed to authorize all 3 transactions
    val namespaceSignature =
      crypto.privateCrypto
        .sign(
          combinedMultiTxHash,
          namespaceKey.fingerprint,
          NonEmpty.mk(Set, SigningKeyUsage.Namespace),
        )
        .futureValueUS
        .value

    // The protocol key signature is only needed on the party to key mapping, so we can sign only that, and combine it with the
    // namespace signature
    val protocolSignatures = protocolSigningKeys.map { key =>
      crypto.privateCrypto
        .sign(
          partyToKeyTx.hash.hash,
          key.fingerprint,
          NonEmpty.mk(Set, SigningKeyUsage.Protocol),
        )
        .futureValueUS
        .value
    }

    (
      OnboardingTransactions(
        namespaceDelegationTx,
        partyToParticipantTx,
        partyToKeyTx,
        Seq(namespaceSignature),
        Seq(
          namespaceDelegationTx -> Seq.empty,
          partyToParticipantTx -> Seq.empty,
          partyToKeyTx -> protocolSignatures.forgetNE,
        ),
      ),
      ExternalParty(partyId, protocolSigningKeys.map(_.fingerprint)),
    )
  }

  def signTopologyTransaction[Op <: TopologyChangeOp, M <: TopologyMapping](
      party: PartyId,
      topologyTransaction: TopologyTransaction[Op, M],
  ): SignedTopologyTransaction[Op, M] =
    SignedTopologyTransaction
      .signAndCreate(
        topologyTransaction,
        NonEmpty.mk(Set, party.fingerprint),
        isProposal = false,
        crypto.privateCrypto,
        testedProtocolVersion,
      )
      .futureValueUS
      .value

  def signTxAs(
      hash: ByteString,
      p: ExternalParty,
  ): Map[PartyId, Seq[Signature]] = {
    val signatures =
      p.signingFingerprints.map { fingerprint =>
        crypto.privateCrypto
          .signBytes(
            hash,
            fingerprint,
            SigningKeyUsage.ProtocolOnly,
          )
          .valueOrFailShutdown("Failed to sign transaction hash")
          .futureValue
      }

    Map(p.partyId -> signatures.forgetNE)
  }

}
