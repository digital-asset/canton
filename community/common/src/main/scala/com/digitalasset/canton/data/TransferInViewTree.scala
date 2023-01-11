// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.bifunctor.*
import cats.syntax.traverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.{
  DeliveredTransferOutResult,
  EnvelopeContent,
  TransferInMediatorMessage,
}
import com.digitalasset.canton.protocol.{
  RootHash,
  SerializableContract,
  TransactionId,
  ViewHash,
  v0,
  v1,
}
import com.digitalasset.canton.sequencing.protocol.{OpenEnvelope, SequencedEvent, SignedContent}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.{DomainId, MediatorId}
import com.digitalasset.canton.util.EitherUtil
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.version.{
  HasMemoizedProtocolVersionedWithContextCompanion,
  HasProtocolVersionedWrapper,
  HasVersionedMessageWithContextCompanion,
  HasVersionedToByteString,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.protobuf.ByteString

import java.util.UUID

/** A blindable Merkle tree for transfer-in requests */

// TODO(#11196): enrich with the participant that submitted the transfer
case class TransferInViewTree(
    commonData: MerkleTree[TransferInCommonData],
    view: MerkleTree[TransferInView],
)(hashOps: HashOps)
    extends GenTransferViewTree[
      TransferInCommonData,
      TransferInView,
      TransferInViewTree,
      TransferInMediatorMessage,
    ](commonData, view)(hashOps) {

  override def createMediatorMessage(blindedTree: TransferInViewTree): TransferInMediatorMessage =
    TransferInMediatorMessage(blindedTree)

  override private[data] def withBlindedSubtrees(
      optimizedBlindingPolicy: PartialFunction[RootHash, MerkleTree.BlindingCommand]
  ): MerkleTree[TransferInViewTree] =
    TransferInViewTree(
      commonData.doBlind(optimizedBlindingPolicy),
      view.doBlind(optimizedBlindingPolicy),
    )(hashOps)

  override def pretty: Pretty[TransferInViewTree] = prettyOfClass(
    param("common data", _.commonData),
    param("view", _.view),
  )
}

object TransferInViewTree
    extends HasVersionedMessageWithContextCompanion[TransferInViewTree, HashOps] {
  override val name: String = "TransferInViewTree"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> ProtoCodec(
      ProtocolVersion.v3,
      supportedProtoVersion(v0.TransferViewTree)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> ProtoCodec(
      ProtocolVersion.v4,
      supportedProtoVersion(v1.TransferViewTree)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  def fromProtoV0(
      hashOps: HashOps,
      transferInViewTreeP: v0.TransferViewTree,
  ): ParsingResult[TransferInViewTree] =
    GenTransferViewTree.fromProtoV0(
      TransferInCommonData.fromByteString(hashOps),
      TransferInView.fromByteString(hashOps),
    )((commonData, view) => new TransferInViewTree(commonData, view)(hashOps))(transferInViewTreeP)

  def fromProtoV1(
      hashOps: HashOps,
      transferInViewTreeP: v1.TransferViewTree,
  ): ParsingResult[TransferInViewTree] =
    GenTransferViewTree.fromProtoV1(
      TransferInCommonData.fromByteString(hashOps),
      TransferInView.fromByteString(hashOps),
    )((commonData, view) => new TransferInViewTree(commonData, view)(hashOps))(transferInViewTreeP)
}

/** Aggregates the data of a transfer-in request that is sent to the mediator and the involved participants.
  *
  * @param salt Salt for blinding the Merkle hash
  * @param targetDomain The domain on which the contract is transferred in
  * @param targetMediator The mediator that coordinates the transfer-in request on the target domain
  * @param stakeholders The stakeholders of the transferred contract
  * @param uuid The uuid of the transfer-in request
  */
final case class TransferInCommonData private (
    override val salt: Salt,
    targetDomain: DomainId,
    targetMediator: MediatorId,
    stakeholders: Set[LfPartyId],
    uuid: UUID,
)(
    hashOps: HashOps,
    val targetProtocolVersion: TargetProtocolVersion,
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[TransferInCommonData](hashOps)
    with HasProtocolVersionedWrapper[TransferInCommonData]
    with ProtocolVersionedMemoizedEvidence {

  val representativeProtocolVersion: RepresentativeProtocolVersion[TransferInCommonData] =
    TransferInCommonData.protocolVersionRepresentativeFor(targetProtocolVersion.v)

  def confirmingParties: Set[Informee] = stakeholders.map(ConfirmingParty(_, 1))

  override def companionObj = TransferInCommonData

  protected def toProtoV0: v0.TransferInCommonData = v0.TransferInCommonData(
    salt = Some(salt.toProtoV0),
    targetDomain = targetDomain.toProtoPrimitive,
    targetMediator = targetMediator.toProtoPrimitive,
    stakeholders = stakeholders.toSeq,
    uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
  )

  protected def toProtoV1: v1.TransferInCommonData = v1.TransferInCommonData(
    salt = Some(salt.toProtoV0),
    targetDomain = targetDomain.toProtoPrimitive,
    targetMediator = targetMediator.toProtoPrimitive,
    stakeholders = stakeholders.toSeq,
    uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
    targetProtocolVersion = targetProtocolVersion.v.toProtoPrimitive,
  )

  override def hashPurpose: HashPurpose = HashPurpose.TransferInCommonData

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def pretty: Pretty[TransferInCommonData] = prettyOfClass(
    param("target domain", _.targetDomain),
    param("target mediator", _.targetMediator),
    param("stakeholders", _.stakeholders),
    param("uuid", _.uuid),
    param("salt", _.salt),
  )
}

object TransferInCommonData
    extends HasMemoizedProtocolVersionedWithContextCompanion[TransferInCommonData, HashOps] {
  override val name: String = "TransferInCommonData"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v3,
      supportedProtoVersionMemoized(v0.TransferInCommonData)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(
      ProtocolVersion.v4,
      supportedProtoVersionMemoized(v1.TransferInCommonData)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  def create(hashOps: HashOps)(
      salt: Salt,
      targetDomain: DomainId,
      targetMediator: MediatorId,
      stakeholders: Set[LfPartyId],
      uuid: UUID,
      targetProtocolVersion: TargetProtocolVersion,
  ): TransferInCommonData =
    TransferInCommonData(salt, targetDomain, targetMediator, stakeholders, uuid)(
      hashOps,
      targetProtocolVersion,
      None,
    )

  private[this] def fromProtoV0(hashOps: HashOps, transferInCommonDataP: v0.TransferInCommonData)(
      bytes: ByteString
  ): ParsingResult[TransferInCommonData] = {
    val v0.TransferInCommonData(saltP, targetDomainP, stakeholdersP, uuidP, targetMediatorP) =
      transferInCommonDataP
    for {
      salt <- ProtoConverter.parseRequired(Salt.fromProtoV0, "salt", saltP)
      targetDomain <- DomainId.fromProtoPrimitive(targetDomainP, "target_domain")
      targetMediator <- MediatorId.fromProtoPrimitive(targetMediatorP, "target_mediator")
      stakeholders <- stakeholdersP.traverse(ProtoConverter.parseLfPartyId)
      uuid <- ProtoConverter.UuidConverter.fromProtoPrimitive(uuidP)
    } yield TransferInCommonData(salt, targetDomain, targetMediator, stakeholders.toSet, uuid)(
      hashOps,
      TargetProtocolVersion(
        protocolVersionRepresentativeFor(ProtoVersion(0)).representative
      ),
      Some(bytes),
    )
  }

  private[this] def fromProtoV1(hashOps: HashOps, transferInCommonDataP: v1.TransferInCommonData)(
      bytes: ByteString
  ): ParsingResult[TransferInCommonData] = {
    val v1.TransferInCommonData(
      saltP,
      targetDomainP,
      stakeholdersP,
      uuidP,
      targetMediatorP,
      protocolVersionP,
    ) =
      transferInCommonDataP
    for {
      salt <- ProtoConverter.parseRequired(Salt.fromProtoV0, "salt", saltP)
      targetDomain <- DomainId.fromProtoPrimitive(targetDomainP, "target_domain")
      targetMediator <- MediatorId.fromProtoPrimitive(targetMediatorP, "target_mediator")
      stakeholders <- stakeholdersP.traverse(ProtoConverter.parseLfPartyId)
      uuid <- ProtoConverter.UuidConverter.fromProtoPrimitive(uuidP)
      protocolVersion = ProtocolVersion.fromProtoPrimitive(protocolVersionP)
    } yield TransferInCommonData(salt, targetDomain, targetMediator, stakeholders.toSet, uuid)(
      hashOps,
      TargetProtocolVersion(protocolVersion),
      Some(bytes),
    )
  }
}

/** Aggregates the data of a transfer-in request that is only sent to the involved participants
  *
  * @param salt The salt to blind the Merkle hash
  * @param submitter The submitter of the transfer-in request
  * @param creatingTransactionId The id of the transaction that created the contract
  * @param contract The contract to be transferred including the instance
  * @param transferOutResultEvent The signed deliver event of the transfer-out result message
  */
final case class TransferInView private (
    override val salt: Salt,
    submitter: LfPartyId,
    contract: SerializableContract,
    creatingTransactionId: TransactionId,
    transferOutResultEvent: DeliveredTransferOutResult,
    sourceProtocolVersion: SourceProtocolVersion,
)(
    hashOps: HashOps,
    val representativeProtocolVersion: RepresentativeProtocolVersion[TransferInView],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[TransferInView](hashOps)
    with HasProtocolVersionedWrapper[TransferInView]
    with ProtocolVersionedMemoizedEvidence {

  override def hashPurpose: HashPurpose = HashPurpose.TransferInView

  override def companionObj = TransferInView

  protected def toProtoV0: v0.TransferInView =
    v0.TransferInView(
      salt = Some(salt.toProtoV0),
      submitter = submitter,
      contract = Some(contract.toProtoV0),
      creatingTransactionId = creatingTransactionId.toProtoPrimitive,
      transferOutResultEvent = Some(transferOutResultEvent.result.toProtoV0),
    )

  protected def toProtoV1: v1.TransferInView =
    v1.TransferInView(
      salt = Some(salt.toProtoV0),
      submitter = submitter,
      contract = Some(contract.toProtoV1),
      creatingTransactionId = creatingTransactionId.toProtoPrimitive,
      transferOutResultEvent = Some(transferOutResultEvent.result.toProtoV0),
      sourceProtocolVersion = sourceProtocolVersion.v.toProtoPrimitive,
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def pretty: Pretty[TransferInView] = prettyOfClass(
    param("submitter", _.submitter),
    param("contract", _.contract), // TODO(#3269) this may contain confidential data
    param("creating transaction id", _.creatingTransactionId),
    param("transfer out result", _.transferOutResultEvent),
    param("salt", _.salt),
  )
}

object TransferInView
    extends HasMemoizedProtocolVersionedWithContextCompanion[TransferInView, HashOps] {
  override val name: String = "TransferInView"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v3,
      supportedProtoVersionMemoized(v0.TransferInView)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(
      ProtocolVersion.v4,
      supportedProtoVersionMemoized(v1.TransferInView)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  def create(hashOps: HashOps)(
      salt: Salt,
      submitter: LfPartyId,
      contract: SerializableContract,
      creatingTransactionId: TransactionId,
      transferOutResultEvent: DeliveredTransferOutResult,
      sourceProtocolVersion: SourceProtocolVersion,
      targetProtocolVersion: TargetProtocolVersion,
  ): TransferInView =
    TransferInView(
      salt,
      submitter,
      contract,
      creatingTransactionId,
      transferOutResultEvent,
      sourceProtocolVersion,
    )(
      hashOps,
      protocolVersionRepresentativeFor(targetProtocolVersion.v),
      None,
    )

  private[this] def fromProtoV0(hashOps: HashOps, transferInViewP: v0.TransferInView)(
      bytes: ByteString
  ): ParsingResult[TransferInView] = {
    val v0.TransferInView(
      saltP,
      submitterP,
      contractP,
      transferOutResultEventPO,
      creatingTransactionIdP,
    ) =
      transferInViewP
    for {
      salt <- ProtoConverter.parseRequired(Salt.fromProtoV0, "salt", saltP)
      submitter <- ProtoConverter.parseLfPartyId(submitterP)
      contract <- ProtoConverter
        .required("contract", contractP)
        .flatMap(SerializableContract.fromProtoV0)

      // TransferOutResultEvent deserialization
      transferOutResultEventP <- ProtoConverter
        .required("TransferInView.transferOutResultEvent", transferOutResultEventPO)

      protocolVersionRepresentative = protocolVersionRepresentativeFor(ProtoVersion(0))
      sourceDomainPV = protocolVersionRepresentative.representative

      envelopeDeserializer = (envelopeP: v0.Envelope) =>
        OpenEnvelope.fromProtoV0(
          EnvelopeContent.messageFromByteString(sourceDomainPV, hashOps)(
            _
          ),
          sourceDomainPV,
        )(envelopeP)

      transferOutResultEventMC <- SignedContent.fromProtoV0(
        contentDeserializer = SequencedEvent.fromByteString(envelopeDeserializer),
        transferOutResultEventP,
      )

      transferOutResultEvent <- DeliveredTransferOutResult
        .create(transferOutResultEventMC)
        .leftMap(err => OtherError(err.toString))
      creatingTransactionId <- TransactionId.fromProtoPrimitive(creatingTransactionIdP)
    } yield TransferInView(
      salt,
      submitter,
      contract,
      creatingTransactionId,
      transferOutResultEvent,
      SourceProtocolVersion(sourceDomainPV),
    )(hashOps, protocolVersionRepresentative, Some(bytes))
  }

  private[this] def fromProtoV1(hashOps: HashOps, transferInViewP: v1.TransferInView)(
      bytes: ByteString
  ): ParsingResult[TransferInView] = {
    val v1.TransferInView(
      saltP,
      submitterP,
      contractP,
      transferOutResultEventPO,
      creatingTransactionIdP,
      sourceProtocolVersionP,
    ) =
      transferInViewP
    for {
      salt <- ProtoConverter.parseRequired(Salt.fromProtoV0, "salt", saltP)
      submitter <- ProtoConverter.parseLfPartyId(submitterP)
      contract <- ProtoConverter
        .required("contract", contractP)
        .flatMap(SerializableContract.fromProtoV1)

      sourceProtocolVersion = SourceProtocolVersion(
        ProtocolVersion.fromProtoPrimitive(sourceProtocolVersionP)
      )

      // TransferOutResultEvent deserialization
      transferOutResultEventP <- ProtoConverter
        .required("TransferInView.transferOutResultEvent", transferOutResultEventPO)

      envelopeDeserializer = (envelopeP: v0.Envelope) =>
        OpenEnvelope.fromProtoV0(
          EnvelopeContent.messageFromByteString(sourceProtocolVersion.v, hashOps)(
            _
          ),
          sourceProtocolVersion.v,
        )(envelopeP)

      transferOutResultEventMC <- SignedContent.fromProtoV0(
        contentDeserializer = SequencedEvent.fromByteString(envelopeDeserializer),
        transferOutResultEventP,
      )

      transferOutResultEvent <- DeliveredTransferOutResult
        .create(transferOutResultEventMC)
        .leftMap(err => OtherError(err.toString))
      creatingTransactionId <- TransactionId.fromProtoPrimitive(creatingTransactionIdP)
    } yield TransferInView(
      salt,
      submitter,
      contract,
      creatingTransactionId,
      transferOutResultEvent,
      sourceProtocolVersion,
    )(hashOps, protocolVersionRepresentativeFor(ProtoVersion(1)), Some(bytes))
  }
}

/** A fully unblinded [[TransferInViewTree]]
  *
  * @throws java.lang.IllegalArgumentException if the [[tree]] is not fully unblinded
  */
case class FullTransferInTree(tree: TransferInViewTree)
    extends ViewTree
    with HasVersionedToByteString
    with PrettyPrinting {
  require(tree.isFullyUnblinded, "A transfer-in request must be fully unblinded")

  private[this] val commonData = tree.commonData.tryUnwrap
  private[this] val view = tree.view.tryUnwrap

  def submitter: LfPartyId = view.submitter

  def stakeholders: Set[LfPartyId] = commonData.stakeholders

  def contract: SerializableContract = view.contract

  def creatingTransactionId: TransactionId = view.creatingTransactionId

  def transferOutResultEvent: DeliveredTransferOutResult = view.transferOutResultEvent

  def mediatorMessage: TransferInMediatorMessage = tree.mediatorMessage

  override def domainId: DomainId = commonData.targetDomain

  override def mediatorId: MediatorId = commonData.targetMediator

  override def informees: Set[Informee] = commonData.confirmingParties

  override def toBeSigned: Option[RootHash] = Some(tree.rootHash)

  override def viewHash: ViewHash = tree.viewHash

  override def toByteString(version: ProtocolVersion): ByteString = tree.toByteString(version)

  override def rootHash: RootHash = tree.rootHash

  override def pretty: Pretty[FullTransferInTree] = prettyOfClass(unnamedParam(_.tree))
}

object FullTransferInTree {
  def fromByteString(
      crypto: CryptoPureApi
  )(bytes: ByteString): ParsingResult[FullTransferInTree] =
    for {
      tree <- TransferInViewTree.fromByteString(crypto)(bytes)
      _ <- EitherUtil.condUnitE(
        tree.isFullyUnblinded,
        OtherError(s"Transfer-in request ${tree.rootHash} is not fully unblinded"),
      )
    } yield FullTransferInTree(tree)
}
