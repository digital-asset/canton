// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.traverse._
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.ContractIdSyntax._
import com.digitalasset.canton.protocol.messages.TransferOutMediatorMessage
import com.digitalasset.canton.protocol.{LfContractId, RootHash, ViewHash, v0, v1}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.time.TimeProof
import com.digitalasset.canton.topology.{DomainId, MediatorId}
import com.digitalasset.canton.util.{EitherUtil, NoCopy}
import com.digitalasset.canton.version.{
  HasMemoizedProtocolVersionedWithContextCompanion,
  HasProtocolVersionedWithContextCompanion,
  HasProtocolVersionedWrapper,
  HasRepresentativeProtocolVersion,
  HasVersionedToByteString,
  ProtobufVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
  SourceProtocolVersion,
  TargetProtocolVersion,
}
import com.google.protobuf.ByteString

import java.util.UUID

/** A blindable Merkle tree for transfer-out requests */
sealed abstract case class TransferOutViewTree(
    commonData: MerkleTree[TransferOutCommonData],
    view: MerkleTree[TransferOutView],
)(
    val representativeProtocolVersion: RepresentativeProtocolVersion[TransferOutViewTree],
    hashOps: HashOps,
) extends GenTransferViewTree[
      TransferOutCommonData,
      TransferOutView,
      TransferOutViewTree,
      TransferOutMediatorMessage,
    ](commonData, view)(hashOps)
    with HasRepresentativeProtocolVersion {

  override private[data] def withBlindedSubtrees(
      optimizedBlindingPolicy: PartialFunction[RootHash, MerkleTree.BlindingCommand]
  ): MerkleTree[TransferOutViewTree] =
    new TransferOutViewTree(
      commonData.doBlind(optimizedBlindingPolicy),
      view.doBlind(optimizedBlindingPolicy),
    )(representativeProtocolVersion, hashOps) {}

  protected[this] override def createMediatorMessage(
      blindedTree: TransferOutViewTree
  ): TransferOutMediatorMessage =
    TransferOutMediatorMessage(blindedTree)

  override def pretty: Pretty[TransferOutViewTree] = prettyOfClass(
    param("common data", _.commonData),
    param("view", _.view),
  )
}

object TransferOutViewTree
    extends HasProtocolVersionedWithContextCompanion[TransferOutViewTree, HashOps] {

  override val name: String = "TransferOutViewTree"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtobufVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v2_0_0,
      supportedProtoVersion(v0.TransferViewTree)((hashOps, proto) => fromProtoV0(hashOps)(proto)),
      _.toProtoV0.toByteString,
    )
  )

  def apply(
      commonData: MerkleTree[TransferOutCommonData],
      view: MerkleTree[TransferOutView],
  )(protocolVersion: ProtocolVersion, hashOps: HashOps) = new TransferOutViewTree(commonData, view)(
    TransferOutViewTree.protocolVersionRepresentativeFor(protocolVersion),
    hashOps,
  ) {}

  def fromProtoV0(hashOps: HashOps)(
      transferOutViewTreeP: v0.TransferViewTree
  ): ParsingResult[TransferOutViewTree] =
    GenTransferViewTree.fromProtoV0(
      TransferOutCommonData.fromByteString(hashOps),
      TransferOutView.fromByteString(hashOps),
    )((commonData, view) =>
      new TransferOutViewTree(commonData, view)(
        protocolVersionRepresentativeFor(ProtobufVersion(0)),
        hashOps,
      ) {}
    )(
      transferOutViewTreeP
    )
}

/** Aggregates the data of a transfer-out request that is sent to the mediator and the involved participants.
  *
  * @param salt Salt for blinding the Merkle hash
  * @param sourceDomain The domain to which the transfer-out request is sent
  * @param sourceMediator The mediator that coordinates the transfer-out request on the source domain
  * @param stakeholders The stakeholders of the contract to be transferred
  * @param adminParties The admin parties of transferring transfer-out participants
  * @param uuid The request UUID of the transfer-out
  */
sealed abstract case class TransferOutCommonData private (
    override val salt: Salt,
    sourceDomain: DomainId,
    sourceMediator: MediatorId,
    stakeholders: Set[LfPartyId],
    adminParties: Set[LfPartyId],
    uuid: UUID,
)(
    hashOps: HashOps,
    val protocolVersion: SourceProtocolVersion,
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[TransferOutCommonData](hashOps)
    with HasProtocolVersionedWrapper[TransferOutCommonData]
    with ProtocolVersionedMemoizedEvidence
    with NoCopy {

  override def companionObj = TransferOutCommonData

  val representativeProtocolVersion: RepresentativeProtocolVersion[TransferOutCommonData] =
    TransferOutCommonData.protocolVersionRepresentativeFor(protocolVersion.v)

  protected def toProtoV0: v0.TransferOutCommonData =
    v0.TransferOutCommonData(
      salt = Some(salt.toProtoV0),
      originDomain = sourceDomain.toProtoPrimitive,
      originMediator = sourceMediator.toProtoPrimitive,
      stakeholders = stakeholders.toSeq,
      adminParties = adminParties.toSeq,
      uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
    )

  protected def toProtoV1: v1.TransferOutCommonData =
    v1.TransferOutCommonData(
      salt = Some(salt.toProtoV0),
      sourceDomain = sourceDomain.toProtoPrimitive,
      sourceMediator = sourceMediator.toProtoPrimitive,
      stakeholders = stakeholders.toSeq,
      adminParties = adminParties.toSeq,
      uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
      sourceProtocolVersion = protocolVersion.v.toProtoPrimitive,
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def hashPurpose: HashPurpose = HashPurpose.TransferOutCommonData

  def confirmingParties: Set[Informee] = (stakeholders ++ adminParties).map(ConfirmingParty(_, 1))

  override def pretty: Pretty[TransferOutCommonData] = prettyOfClass(
    param("source domain", _.sourceDomain),
    param("source mediator", _.sourceMediator),
    param("stakeholders", _.stakeholders),
    param("admin parties", _.adminParties),
    param("uuid", _.uuid),
    param("salt", _.salt),
  )
}

object TransferOutCommonData
    extends HasMemoizedProtocolVersionedWithContextCompanion[
      TransferOutCommonData,
      HashOps,
    ] {
  override val name: String = "TransferOutCommonData"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtobufVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v2_0_0,
      supportedProtoVersionMemoized(v0.TransferOutCommonData)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    // TODO(i9423): Migrate to next protocol version
    ProtobufVersion(1) -> VersionedProtoConverter(
      ProtocolVersion.unstable_development,
      supportedProtoVersionMemoized(v1.TransferOutCommonData)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  def create(hashOps: HashOps)(
      salt: Salt,
      sourceDomain: DomainId,
      sourceMediator: MediatorId,
      stakeholders: Set[LfPartyId],
      adminParties: Set[LfPartyId],
      uuid: UUID,
      protocolVersion: SourceProtocolVersion,
  ): TransferOutCommonData =
    new TransferOutCommonData(
      salt,
      sourceDomain,
      sourceMediator,
      stakeholders,
      adminParties,
      uuid,
    )(hashOps, protocolVersion, None) {}

  private[this] def fromProtoV0(hashOps: HashOps, transferOutCommonDataP: v0.TransferOutCommonData)(
      bytes: ByteString
  ): ParsingResult[TransferOutCommonData] = {
    val v0.TransferOutCommonData(
      saltP,
      sourceDomainP,
      stakeholdersP,
      adminPartiesP,
      uuidP,
      mediatorIdP,
    ) = transferOutCommonDataP
    for {
      salt <- ProtoConverter.parseRequired(Salt.fromProtoV0, "salt", saltP)
      sourceDomain <- DomainId.fromProtoPrimitive(sourceDomainP, "origin_domain")
      sourceMediator <- MediatorId.fromProtoPrimitive(mediatorIdP, "origin_mediator")
      stakeholders <- stakeholdersP.traverse(ProtoConverter.parseLfPartyId)
      adminParties <- adminPartiesP.traverse(ProtoConverter.parseLfPartyId)
      uuid <- ProtoConverter.UuidConverter.fromProtoPrimitive(uuidP)
    } yield new TransferOutCommonData(
      salt,
      sourceDomain,
      sourceMediator,
      stakeholders.toSet,
      adminParties.toSet,
      uuid,
    )(
      hashOps,
      SourceProtocolVersion(
        protocolVersionRepresentativeFor(ProtobufVersion(0)).representative
      ),
      Some(bytes),
    ) {}
  }

  private[this] def fromProtoV1(hashOps: HashOps, transferOutCommonDataP: v1.TransferOutCommonData)(
      bytes: ByteString
  ): ParsingResult[TransferOutCommonData] = {
    val v1.TransferOutCommonData(
      saltP,
      sourceDomainP,
      stakeholdersP,
      adminPartiesP,
      uuidP,
      mediatorIdP,
      protocolVersionP,
    ) = transferOutCommonDataP
    for {
      salt <- ProtoConverter.parseRequired(Salt.fromProtoV0, "salt", saltP)
      sourceDomain <- DomainId.fromProtoPrimitive(sourceDomainP, "source_domain")
      sourceMediator <- MediatorId.fromProtoPrimitive(mediatorIdP, "source_mediator")
      stakeholders <- stakeholdersP.traverse(ProtoConverter.parseLfPartyId)
      adminParties <- adminPartiesP.traverse(ProtoConverter.parseLfPartyId)
      uuid <- ProtoConverter.UuidConverter.fromProtoPrimitive(uuidP)
      protocolVersion <- ProtocolVersion.fromProtoPrimitive(protocolVersionP)
    } yield new TransferOutCommonData(
      salt,
      sourceDomain,
      sourceMediator,
      stakeholders.toSet,
      adminParties.toSet,
      uuid,
    )(hashOps, SourceProtocolVersion(protocolVersion), Some(bytes)) {}
  }
}

/** Aggregates the data of a transfer-out request that is only sent to the involved participants
  *
  * @param salt The salt to blind the Merkle hash
  * @param submitter The submitter of the transfer-out request
  * @param contractId The contract ID to be transferred
  * @param targetDomain The target domain to which the contract is to be transferred
  * @param targetTimeProof The sequenced event from the target domain
  *                        whose timestamp defines the baseline for measuring time periods on the target domain
  */
sealed abstract case class TransferOutView private (
    override val salt: Salt,
    submitter: LfPartyId,
    contractId: LfContractId,
    targetDomain: DomainId,
    targetTimeProof: TimeProof,
    targetProtocolVersion: TargetProtocolVersion,
)(
    hashOps: HashOps,
    val representativeProtocolVersion: RepresentativeProtocolVersion[TransferOutView],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[TransferOutView](hashOps)
    with HasProtocolVersionedWrapper[TransferOutView]
    with ProtocolVersionedMemoizedEvidence
    with NoCopy {

  override def hashPurpose: HashPurpose = HashPurpose.TransferOutView

  override def companionObj = TransferOutView

  protected def toProtoV0: v0.TransferOutView =
    v0.TransferOutView(
      salt = Some(salt.toProtoV0),
      submitter = submitter,
      contractId = contractId.toProtoPrimitive,
      targetDomain = targetDomain.toProtoPrimitive,
      targetTimeProof = Some(targetTimeProof.toProtoV0),
    )

  protected def toProtoV1: v1.TransferOutView =
    v1.TransferOutView(
      salt = Some(salt.toProtoV0),
      submitter = submitter,
      contractId = contractId.toProtoPrimitive,
      targetDomain = targetDomain.toProtoPrimitive,
      targetTimeProof = Some(targetTimeProof.toProtoV0),
      targetProtocolVersion = targetProtocolVersion.v.toProtoPrimitive,
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def pretty: Pretty[TransferOutView] = prettyOfClass(
    param("submitter", _.submitter),
    param("contract id", _.contractId),
    param("target domain", _.targetDomain),
    param("target time proof", _.targetTimeProof),
    param("salt", _.salt),
  )
}

object TransferOutView
    extends HasMemoizedProtocolVersionedWithContextCompanion[TransferOutView, HashOps] {
  override val name: String = "TransferOutView"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtobufVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v2_0_0,
      supportedProtoVersionMemoized(v0.TransferOutView)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    // TODO(i9423): Migrate to next protocol version
    ProtobufVersion(1) -> VersionedProtoConverter(
      ProtocolVersion.unstable_development,
      supportedProtoVersionMemoized(v1.TransferOutView)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  def create(hashOps: HashOps)(
      salt: Salt,
      submitter: LfPartyId,
      contractId: LfContractId,
      targetDomain: DomainId,
      targetTimeProof: TimeProof,
      sourceProtocolVersion: SourceProtocolVersion,
      targetProtocolVersion: TargetProtocolVersion,
  ): TransferOutView =
    new TransferOutView(
      salt,
      submitter,
      contractId,
      targetDomain,
      targetTimeProof,
      targetProtocolVersion,
    )(
      hashOps,
      protocolVersionRepresentativeFor(sourceProtocolVersion.v),
      None,
    ) {}

  private[this] def fromProtoV0(hashOps: HashOps, transferOutViewP: v0.TransferOutView)(
      bytes: ByteString
  ): ParsingResult[TransferOutView] = {
    val v0.TransferOutView(saltP, submitterP, contractIdP, targetDomainP, targetTimeProofP) =
      transferOutViewP
    for {
      salt <- ProtoConverter.parseRequired(Salt.fromProtoV0, "salt", saltP)
      submitter <- ProtoConverter.parseLfPartyId(submitterP)
      contractId <- LfContractId.fromProtoPrimitive(contractIdP)
      targetDomain <- DomainId.fromProtoPrimitive(targetDomainP, "targetDomain")

      protocolVersionRepresentative = protocolVersionRepresentativeFor(ProtobufVersion(0))
      targetDomainPV = protocolVersionRepresentative.representative

      targetTimeProof <- ProtoConverter
        .required("targetTimeProof", targetTimeProofP)
        .flatMap(TimeProof.fromProtoV0(targetDomainPV, hashOps))
    } yield new TransferOutView(
      salt,
      submitter,
      contractId,
      targetDomain,
      targetTimeProof,
      TargetProtocolVersion(targetDomainPV),
    )(hashOps, protocolVersionRepresentative, Some(bytes)) {}
  }

  private[this] def fromProtoV1(hashOps: HashOps, transferOutViewP: v1.TransferOutView)(
      bytes: ByteString
  ): ParsingResult[TransferOutView] = {
    val v1.TransferOutView(
      saltP,
      submitterP,
      contractIdP,
      targetDomainP,
      targetTimeProofP,
      targetProtocolVersionP,
    ) = transferOutViewP

    for {
      salt <- ProtoConverter.parseRequired(Salt.fromProtoV0, "salt", saltP)
      submitter <- ProtoConverter.parseLfPartyId(submitterP)
      contractId <- LfContractId.fromProtoPrimitive(contractIdP)
      targetDomain <- DomainId.fromProtoPrimitive(targetDomainP, "targetDomain")

      targetProtocolVersion <- ProtocolVersion
        .fromProtoPrimitive(targetProtocolVersionP)
        .map(TargetProtocolVersion(_))

      targetTimeProof <- ProtoConverter
        .required("targetTimeProof", targetTimeProofP)
        .flatMap(TimeProof.fromProtoV0(targetProtocolVersion.v, hashOps))
    } yield new TransferOutView(
      salt,
      submitter,
      contractId,
      targetDomain,
      targetTimeProof,
      targetProtocolVersion,
    )(
      hashOps,
      protocolVersionRepresentativeFor(ProtobufVersion(1)),
      Some(bytes),
    ) {}
  }
}

/** A fully unblinded [[TransferOutViewTree]]
  *
  * @throws java.lang.IllegalArgumentException if the [[tree]] is not fully unblinded
  */
case class FullTransferOutTree(tree: TransferOutViewTree)
    extends ViewTree
    with HasVersionedToByteString
    with PrettyPrinting {
  require(tree.isFullyUnblinded, "A transfer-out request must be fully unblinded")

  private[this] val commonData = tree.commonData.tryUnwrap
  private[this] val view = tree.view.tryUnwrap

  def submitter: LfPartyId = view.submitter

  def stakeholders: Set[LfPartyId] = commonData.stakeholders

  def adminParties: Set[LfPartyId] = commonData.adminParties

  def contractId: LfContractId = view.contractId

  def sourceDomain: DomainId = commonData.sourceDomain

  def targetDomain: DomainId = view.targetDomain

  def targetTimeProof: TimeProof = view.targetTimeProof

  def mediatorMessage: TransferOutMediatorMessage = tree.mediatorMessage

  override def domainId: DomainId = sourceDomain

  override def mediatorId: MediatorId = commonData.sourceMediator

  override def informees: Set[Informee] = commonData.confirmingParties

  override def toBeSigned: Option[RootHash] = Some(tree.rootHash)

  override def viewHash: ViewHash = tree.viewHash

  override def rootHash: RootHash = tree.rootHash

  override def pretty: Pretty[FullTransferOutTree] = prettyOfClass(unnamedParam(_.tree))

  override def toByteString(version: ProtocolVersion): ByteString = tree.toByteString(version)
}

object FullTransferOutTree {
  def fromByteString(
      crypto: CryptoPureApi
  )(bytes: ByteString): ParsingResult[FullTransferOutTree] =
    for {
      tree <- TransferOutViewTree.fromByteString(crypto)(bytes)
      _ <- EitherUtil.condUnitE(
        tree.isFullyUnblinded,
        OtherError(s"Transfer-out request ${tree.rootHash} is not fully unblinded"),
      )
    } yield FullTransferOutTree(tree)
}
