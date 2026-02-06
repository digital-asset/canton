// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.util

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.signer.SyncCryptoSignerWithLongTermKeys
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerSuccessor}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.{
  DynamicSequencingParametersWithValidity,
  DynamicSynchronizerParametersWithValidity,
  StaticSynchronizerParameters,
}
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.client.{
  BaseTopologySnapshotClient,
  PartyTopologySnapshotClient,
  TopologySnapshot,
}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.UnknownOrUnvettedPackages
import com.digitalasset.canton.topology.transaction.{
  LsuSequencerConnectionSuccessor,
  ParticipantAttributes,
  SynchronizerTrustCertificate,
  VettedPackage,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.HasToByteString
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.google.protobuf.ByteString

import scala.collection.immutable
import scala.concurrent.ExecutionContext

class FixedSyncCryptoApiForSigning(
    member: Member,
    crypto: Crypto,
    staticSynchronizerParameters: StaticSynchronizerParameters,
    signingKey: SigningPublicKey,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends SyncCryptoApi
    with NamedLogging {

  override def pureCrypto: SynchronizerCryptoPureApi =
    new SynchronizerCryptoPureApi(staticSynchronizerParameters, crypto.pureCrypto)

  private val signer = new SyncCryptoSignerWithLongTermKeys(
    member,
    crypto.privateCrypto,
    crypto.cryptoPrivateStore,
    loggerFactory,
  )

  override def sign(
      hash: Hash,
      usage: NonEmpty[Set[SigningKeyUsage]],
      approximateTimestampOverride: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, Signature] =
    signer.sign(ipsSnapshot, approximateTimestampOverride: Option[CantonTimestamp], hash, usage)

  override val ipsSnapshot: TopologySnapshot = new TopologySnapshot
    with BaseTopologySnapshotClient
    with NamedLogging {
    override protected implicit def executionContext: ExecutionContext = ec

    override protected def loggerFactory: NamedLoggerFactory =
      FixedSyncCryptoApiForSigning.this.loggerFactory

    override def timestamp: CantonTimestamp = CantonTimestamp.MinValue

    override def signingKeys(owner: Member, filterUsage: NonEmpty[Set[SigningKeyUsage]])(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Seq[SigningPublicKey]] = FutureUnlessShutdown.pure(Seq(signingKey))

    override def signingKeys(members: Seq[Member], filterUsage: NonEmpty[Set[SigningKeyUsage]])(
        implicit traceContext: TraceContext
    ): FutureUnlessShutdown[Map[Member, Seq[SigningPublicKey]]] =
      FutureUnlessShutdown.pure(members.map(_ -> Seq(signingKey)).toMap)

    // =============================================
    // NOTHING ELSE IS IMPLEMENTED BEYOND THIS POINT
    // =============================================

    override def encryptionKey(owner: Member)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Option[EncryptionPublicKey]] = notImplementedUS

    override def encryptionKey(members: Seq[Member])(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Map[Member, EncryptionPublicKey]] = notImplementedUS

    override def encryptionKeys(owner: Member)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Seq[EncryptionPublicKey]] = notImplementedUS

    override def encryptionKeys(members: Seq[Member])(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Map[Member, Seq[EncryptionPublicKey]]] = notImplementedUS

    override def inspectKeys(
        filterOwner: String,
        filterOwnerType: Option[MemberCode],
        limit: Int,
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[Member, KeyCollection]] =
      notImplementedUS

    override def signingKeysWithThreshold(party: PartyId)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Option[SigningKeysWithThreshold]] = notImplementedUS

    override def mediatorGroups()(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Seq[MediatorGroup]] = notImplementedUS

    override def allMembers()(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Set[Member]] = notImplementedUS

    override def isMemberKnown(member: Member)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Boolean] = notImplementedUS

    override def areMembersKnown(members: Set[Member])(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Set[Member]] = notImplementedUS

    override def memberFirstKnownAt(member: Member)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]] = notImplementedUS

    override def isParticipantActive(participantId: ParticipantId)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Boolean] = notImplementedUS

    override def participantsWithSupportedFeature(
        participants: Set[ParticipantId],
        feature: SynchronizerTrustCertificate.ParticipantTopologyFeatureFlag,
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[ParticipantId]] =
      notImplementedUS

    override def isParticipantActiveAndCanLoginAt(
        participantId: ParticipantId,
        timestamp: CantonTimestamp,
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean] = notImplementedUS

    override def activeParticipantsOfParties(parties: Seq[LfPartyId])(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Map[LfPartyId, Set[ParticipantId]]] = notImplementedUS

    override def activeParticipantsOfPartiesWithInfo(parties: Seq[LfPartyId])(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Map[LfPartyId, PartyTopologySnapshotClient.PartyInfo]] =
      notImplementedUS

    override def activeParticipantsOf(party: LfPartyId)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Map[ParticipantId, ParticipantAttributes]] = notImplementedUS

    override def allHaveActiveParticipants(
        parties: Set[LfPartyId],
        check: ParticipantAttributes => Boolean,
    )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, Set[LfPartyId], Unit] =
      notImplementedE

    override def isHostedByAtLeastOneParticipantF(
        parties: Set[LfPartyId],
        check: (LfPartyId, ParticipantAttributes) => Boolean,
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[LfPartyId]] = notImplementedUS

    override def hostedOn(partyIds: Set[LfPartyId], participantId: ParticipantId)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Map[LfPartyId, ParticipantAttributes]] = notImplementedUS

    override def allHostedOn(
        partyIds: Set[LfPartyId],
        participantId: ParticipantId,
        permissionCheck: ParticipantAttributes => Boolean,
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean] = notImplementedUS

    override def canConfirm(participant: ParticipantId, parties: Set[LfPartyId])(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Set[LfPartyId]] = notImplementedUS

    override def hasNoConfirmer(parties: Set[LfPartyId])(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Set[LfPartyId]] = notImplementedUS

    override def canNotSubmit(participant: ParticipantId, parties: Seq[LfPartyId])(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[immutable.Iterable[LfPartyId]] = notImplementedUS

    override def activeParticipantsOfAll(parties: List[LfPartyId])(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, Set[LfPartyId], Set[ParticipantId]] = notImplementedE

    override def inspectKnownParties(filterParty: String, filterParticipant: String, limit: Int)(
        implicit traceContext: TraceContext
    ): FutureUnlessShutdown[Set[PartyId]] = notImplementedUS

    override def sequencerGroup()(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Option[SequencerGroup]] = notImplementedUS

    override def findDynamicSynchronizerParameters()(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Either[String, DynamicSynchronizerParametersWithValidity]] =
      notImplementedUS

    override def findDynamicSequencingParameters()(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Either[String, DynamicSequencingParametersWithValidity]] =
      notImplementedUS

    override def listDynamicSynchronizerParametersChanges()(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Seq[DynamicSynchronizerParametersWithValidity]] = notImplementedUS

    override def synchronizerUpgradeOngoing()(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Option[(SynchronizerSuccessor, EffectiveTime)]] = notImplementedUS

    override def sequencerConnectionSuccessors()(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Map[SequencerId, LsuSequencerConnectionSuccessor]] = notImplementedUS

    override def loadUnvettedPackagesOrDependencies(
        participantId: ParticipantId,
        packages: Set[PackageId],
        ledgerTime: CantonTimestamp,
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[UnknownOrUnvettedPackages] =
      notImplementedUS

    override def determinePackagesWithNoVettingEntry(
        participantId: ParticipantId,
        packageIds: Set[PackageId],
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PackageId]] = notImplementedUS

    override def vettedPackages(participantId: ParticipantId)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Set[VettedPackage]] = notImplementedUS
  }

  override def verifySignature(
      hash: Hash,
      signer: Member,
      signature: Signature,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] = notImplementedE

  override def verifySignatures(
      hash: Hash,
      signer: Member,
      signatures: NonEmpty[Seq[Signature]],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] = notImplementedE

  override def verifyMediatorSignatures(
      hash: Hash,
      mediatorGroupIndex: MediatorGroupIndex,
      signatures: NonEmpty[Seq[Signature]],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] = notImplementedE

  override def verifySequencerSignatures(
      hash: Hash,
      signatures: NonEmpty[Seq[Signature]],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] = notImplementedE

  override def decrypt[M](encryptedMessage: AsymmetricEncrypted[M])(
      deserialize: ByteString => Either[DeserializationError, M]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SyncCryptoError, M] =
    notImplementedE

  override def encryptFor[M <: HasToByteString, MemberType <: Member](
      message: M,
      members: Seq[MemberType],
      deterministicEncryption: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, (MemberType, SyncCryptoError), Map[
    MemberType,
    AsymmetricEncrypted[M],
  ]] =
    notImplementedE

  def notImplementedUS[A]: FutureUnlessShutdown[A] =
    FutureUnlessShutdown.failed(new NotImplementedError())

  def notImplementedE[A, B]: EitherT[FutureUnlessShutdown, A, B] = EitherT.right(notImplementedUS)
}
