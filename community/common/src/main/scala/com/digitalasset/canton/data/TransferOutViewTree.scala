// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.traverse._
import com.digitalasset.canton.ProtoDeserializationError.{FieldNotSet, OtherError}
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.ContractIdSyntax._
import com.digitalasset.canton.protocol.messages.TransferOutMediatorMessage
import com.digitalasset.canton.protocol.version.{
  VersionedTransferOutCommonData,
  VersionedTransferOutView,
  VersionedTransferViewTree,
}
import com.digitalasset.canton.protocol.{LfContractId, RootHash, ViewHash, v0}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{MemoizedEvidence, ProtoConverter}
import com.digitalasset.canton.time.TimeProof
import com.digitalasset.canton.topology.{DomainId, MediatorId}
import com.digitalasset.canton.util.{
  EitherUtil,
  HasVersionedToByteString,
  HasVersionedWrapper,
  NoCopy,
}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.LfPartyId
import com.google.protobuf.ByteString

import java.util.UUID

/** A blindable Merkle tree for transfer-out requests */
case class TransferOutViewTree(
    commonData: MerkleTree[TransferOutCommonData],
    view: MerkleTree[TransferOutView],
)(hashOps: HashOps)
    extends GenTransferViewTree[
      TransferOutCommonData,
      TransferOutView,
      TransferOutViewTree,
      TransferOutMediatorMessage,
    ](commonData, view)(hashOps) {

  override private[data] def withBlindedSubtrees(
      optimizedBlindingPolicy: PartialFunction[RootHash, MerkleTree.BlindingCommand]
  ): MerkleTree[TransferOutViewTree] =
    TransferOutViewTree(
      commonData.doBlind(optimizedBlindingPolicy),
      view.doBlind(optimizedBlindingPolicy),
    )(hashOps)

  protected[this] override def createMediatorMessage(
      blindedTree: TransferOutViewTree
  ): TransferOutMediatorMessage =
    TransferOutMediatorMessage(blindedTree)

  override def pretty: Pretty[TransferOutViewTree] = prettyOfClass(
    param("common data", _.commonData),
    param("view", _.view),
  )
}

object TransferOutViewTree {

  def fromProtoVersioned(crypto: CryptoPureApi)(
      transferOutViewTreeP: VersionedTransferViewTree
  ): ParsingResult[TransferOutViewTree] =
    transferOutViewTreeP.version match {
      case VersionedTransferViewTree.Version.Empty =>
        Left(FieldNotSet("VersionedTransferViewTree.version"))
      case VersionedTransferViewTree.Version.V0(tree) => fromProtoV0(crypto)(tree)
    }

  def fromProtoV0(hashOps: HashOps)(
      transferOutViewTreeP: v0.TransferViewTree
  ): ParsingResult[TransferOutViewTree] =
    GenTransferViewTree.fromProtoV0(
      TransferOutCommonData.fromByteString(hashOps),
      TransferOutView.fromByteString(hashOps),
    )((commonData, view) => new TransferOutViewTree(commonData, view)(hashOps))(
      transferOutViewTreeP
    )

  def fromByteString(
      crypto: CryptoPureApi
  )(bytes: ByteString): ParsingResult[TransferOutViewTree] =
    ProtoConverter
      .protoParser(VersionedTransferViewTree.parseFrom)(bytes)
      .flatMap(fromProtoVersioned(crypto))

}

/** Aggregates the data of a transfer-out request that is sent to the mediator and the involved participants.
  *
  * @param salt Salt for blinding the Merkle hash
  * @param originDomain The domain to which the transfer-out request is sent
  * @param originMediator The mediator that coordinates the transfer-out request on the origin domain
  * @param stakeholders The stakeholders of the contract to be transferred
  * @param adminParties The admin parties of transferring transfer-out participants
  * @param uuid The request UUID of the transfer-out
  */
case class TransferOutCommonData private (
    override val salt: Salt,
    originDomain: DomainId,
    originMediator: MediatorId,
    stakeholders: Set[LfPartyId],
    adminParties: Set[LfPartyId],
    uuid: UUID,
)(hashOps: HashOps, override val deserializedFrom: Option[ByteString])
    extends MerkleTreeLeaf[TransferOutCommonData](hashOps)
    with HasVersionedWrapper[VersionedTransferOutCommonData]
    with MemoizedEvidence
    with NoCopy {

  override protected def toProtoVersioned(
      version: ProtocolVersion
  ): VersionedTransferOutCommonData =
    VersionedTransferOutCommonData(VersionedTransferOutCommonData.Version.V0(toProtoV0))

  protected def toProtoV0: v0.TransferOutCommonData =
    v0.TransferOutCommonData(
      salt = Some(salt.toProtoV0),
      originDomain = originDomain.toProtoPrimitive,
      originMediator = originMediator.toProtoPrimitive,
      stakeholders = stakeholders.toSeq,
      adminParties = adminParties.toSeq,
      uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
    )

  override protected[this] def toByteStringUnmemoized(version: ProtocolVersion): ByteString =
    super[HasVersionedWrapper].toByteString(version)

  override def hashPurpose: HashPurpose = HashPurpose.TransferOutCommonData

  def confirmingParties: Set[Informee] = (stakeholders ++ adminParties).map(ConfirmingParty(_, 1))

  override def pretty: Pretty[TransferOutCommonData] = prettyOfClass(
    param("origin domain", _.originDomain),
    param("origin mediator", _.originMediator),
    param("stakeholders", _.stakeholders),
    param("admin parties", _.adminParties),
    param("uuid", _.uuid),
    param("salt", _.salt),
  )
}

object TransferOutCommonData {

  private[this] def apply(
      salt: Salt,
      originDomain: DomainId,
      originMediator: MediatorId,
      stakeholders: Set[LfPartyId],
      adminParties: Set[LfPartyId],
      uuid: UUID,
  )(hashOps: HashOps, deserializedFrom: Option[ByteString]): TransferOutCommonData =
    throw new UnsupportedOperationException("Use the create method instead")

  def create(hashOps: HashOps)(
      salt: Salt,
      originDomain: DomainId,
      originMediator: MediatorId,
      stakeholders: Set[LfPartyId],
      adminParties: Set[LfPartyId],
      uuid: UUID,
  ): TransferOutCommonData =
    new TransferOutCommonData(
      salt,
      originDomain,
      originMediator,
      stakeholders,
      adminParties,
      uuid,
    )(hashOps, None)

  private[this] def fromProtoVersioned(hashOps: HashOps, bytes: ByteString)(
      transferOutCommonDataP: VersionedTransferOutCommonData
  ): ParsingResult[TransferOutCommonData] =
    transferOutCommonDataP.version match {
      case VersionedTransferOutCommonData.Version.Empty =>
        Left(FieldNotSet("TransferOutCommonData.version"))
      case VersionedTransferOutCommonData.Version.V0(data) => fromProtoV0(hashOps, bytes)(data)
    }

  private[this] def fromProtoV0(hashOps: HashOps, bytes: ByteString)(
      transferOutCommonDataP: v0.TransferOutCommonData
  ): ParsingResult[TransferOutCommonData] = {
    val v0.TransferOutCommonData(
      saltP,
      originDomainP,
      stakeholdersP,
      adminPartiesP,
      uuidP,
      mediatorIdP,
    ) = transferOutCommonDataP
    for {
      salt <- ProtoConverter.parseRequired(Salt.fromProtoV0, "salt", saltP)
      originDomain <- DomainId.fromProtoPrimitive(originDomainP, "origin_domain")
      originMediator <- MediatorId.fromProtoPrimitive(mediatorIdP, "origin_mediator")
      stakeholders <- stakeholdersP.traverse(ProtoConverter.parseLfPartyId)
      adminParties <- adminPartiesP.traverse(ProtoConverter.parseLfPartyId)
      uuid <- ProtoConverter.UuidConverter.fromProtoPrimitive(uuidP)
    } yield new TransferOutCommonData(
      salt,
      originDomain,
      originMediator,
      stakeholders.toSet,
      adminParties.toSet,
      uuid,
    )(hashOps, Some(bytes))
  }

  def fromByteString(
      hashOps: HashOps
  )(bytes: ByteString): ParsingResult[TransferOutCommonData] =
    ProtoConverter
      .protoParser(VersionedTransferOutCommonData.parseFrom)(bytes)
      .flatMap(fromProtoVersioned(hashOps, bytes))

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
case class TransferOutView private (
    override val salt: Salt,
    submitter: LfPartyId,
    contractId: LfContractId,
    targetDomain: DomainId,
    targetTimeProof: TimeProof,
)(hashOps: HashOps, override val deserializedFrom: Option[ByteString])
    extends MerkleTreeLeaf[TransferOutView](hashOps)
    with HasVersionedWrapper[VersionedTransferOutView]
    with MemoizedEvidence
    with NoCopy {

  override def hashPurpose: HashPurpose = HashPurpose.TransferOutView

  override protected def toProtoVersioned(version: ProtocolVersion): VersionedTransferOutView =
    VersionedTransferOutView(VersionedTransferOutView.Version.V0(toProtoV0))
  protected def toProtoV0: v0.TransferOutView =
    v0.TransferOutView(
      salt = Some(salt.toProtoV0),
      submitter = submitter,
      contractId = contractId.toProtoPrimitive,
      targetDomain = targetDomain.toProtoPrimitive,
      targetTimeProof = Some(targetTimeProof.toProtoV0),
    )

  override protected[this] def toByteStringUnmemoized(version: ProtocolVersion): ByteString =
    super[HasVersionedWrapper].toByteString(version)

  override def pretty: Pretty[TransferOutView] = prettyOfClass(
    param("submitter", _.submitter),
    param("contract id", _.contractId),
    param("target domain", _.targetDomain),
    param("target time proof", _.targetTimeProof),
    param("salt", _.salt),
  )
}

object TransferOutView {

  private[this] def apply(
      salt: Salt,
      submitter: LfPartyId,
      contractId: LfContractId,
      targetDomain: DomainId,
      targetTimeEvent: TimeProof,
  )(hashOps: HashOps, deserializedFrom: Option[ByteString]): TransferOutView =
    throw new UnsupportedOperationException("Use the create method instead")

  def create(hashOps: HashOps)(
      salt: Salt,
      submitter: LfPartyId,
      contractId: LfContractId,
      targetDomain: DomainId,
      targetTimeProof: TimeProof,
  ): TransferOutView =
    new TransferOutView(salt, submitter, contractId, targetDomain, targetTimeProof)(hashOps, None)

  private[this] def fromProtoVersioned(hashOps: HashOps, bytes: ByteString)(
      transferOutViewP: VersionedTransferOutView
  ): ParsingResult[TransferOutView] =
    transferOutViewP.version match {
      case VersionedTransferOutView.Version.Empty =>
        Left(FieldNotSet("VersionedTransferOutView.version"))
      case VersionedTransferOutView.Version.V0(view) => fromProtoV0(hashOps, bytes)(view)
    }

  private[this] def fromProtoV0(hashOps: HashOps, bytes: ByteString)(
      transferOutViewP: v0.TransferOutView
  ): ParsingResult[TransferOutView] = {
    val v0.TransferOutView(saltP, submitterP, contractIdP, targetDomainP, targetTimeProofP) =
      transferOutViewP
    for {
      salt <- ProtoConverter.parseRequired(Salt.fromProtoV0, "salt", saltP)
      submitter <- ProtoConverter.parseLfPartyId(submitterP)
      contractId <- LfContractId.fromProtoPrimitive(contractIdP)
      targetDomain <- DomainId.fromProtoPrimitive(targetDomainP, "targetDomain")
      targetTimeProof <- ProtoConverter
        .required("targetTimeProof", targetTimeProofP)
        .flatMap(TimeProof.fromProtoV0(hashOps))
    } yield new TransferOutView(salt, submitter, contractId, targetDomain, targetTimeProof)(
      hashOps,
      Some(bytes),
    )
  }

  def fromByteString(
      hashOps: HashOps
  )(bytes: ByteString): ParsingResult[TransferOutView] =
    ProtoConverter
      .protoParser(VersionedTransferOutView.parseFrom)(bytes)
      .flatMap(fromProtoVersioned(hashOps, bytes))

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

  def originDomain: DomainId = commonData.originDomain

  def targetDomain: DomainId = view.targetDomain

  def targetTimeProof: TimeProof = view.targetTimeProof

  def mediatorMessage: TransferOutMediatorMessage = tree.mediatorMessage

  override def domainId: DomainId = originDomain

  override def mediatorId: MediatorId = commonData.originMediator

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
