// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import java.util.UUID
import cats.syntax.bifunctor._
import cats.syntax.traverse._
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.{
  DeliveredTransferOutResult,
  ProtocolMessage,
  TransferInMediatorMessage,
}
import com.digitalasset.canton.protocol.{
  RootHash,
  SerializableContract,
  TransactionId,
  ViewHash,
  v0,
}
import com.digitalasset.canton.sequencing.protocol.{OpenEnvelope, SequencedEvent, SignedContent}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.{DomainId, MediatorId}
import com.digitalasset.canton.util.{EitherUtil, NoCopy}
import com.digitalasset.canton.version.{
  HasMemoizedProtocolVersionedWithContextCompanion,
  HasProtocolVersionedWrapper,
  HasVersionedMessageWithContextCompanion,
  HasVersionedToByteString,
  ProtocolVersion,
  VersionedMessage,
}
import com.digitalasset.canton.LfPartyId
import com.google.protobuf.ByteString

/** A blindable Merkle tree for transfer-in requests */
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

  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersion(v0.TransferViewTree)(fromProtoV0)
  )

  def fromProtoV0(
      hashOps: HashOps,
      transferInViewTreeP: v0.TransferViewTree,
  ): ParsingResult[TransferInViewTree] =
    GenTransferViewTree.fromProtoV0(
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
case class TransferInCommonData private (
    override val salt: Salt,
    targetDomain: DomainId,
    targetMediator: MediatorId,
    stakeholders: Set[LfPartyId],
    uuid: UUID,
)(
    hashOps: HashOps,
    val representativeProtocolVersion: ProtocolVersion,
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[TransferInCommonData](hashOps)
    with HasProtocolVersionedWrapper[VersionedMessage[TransferInCommonData]]
    with ProtocolVersionedMemoizedEvidence
    with NoCopy {

  def confirmingParties: Set[Informee] = stakeholders.map(ConfirmingParty(_, 1))

  override protected def toProtoVersioned: VersionedMessage[TransferInCommonData] =
    TransferInCommonData.toProtoVersioned(this)

  protected def toProtoV0: v0.TransferInCommonData = v0.TransferInCommonData(
    salt = Some(salt.toProtoV0),
    targetDomain = targetDomain.toProtoPrimitive,
    targetMediator = targetMediator.toProtoPrimitive,
    stakeholders = stakeholders.toSeq,
    uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
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
    0 -> VersionedProtoConverter(
      ProtocolVersion.v2_0_0,
      supportedProtoVersionMemoized(v0.TransferInCommonData)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  private[this] def apply(
      salt: Salt,
      targetDomain: DomainId,
      targetMediator: MediatorId,
      stakeholders: Set[LfPartyId],
      uuid: UUID,
  )(
      hashOps: HashOps,
      representativeProtocolVersion: ProtocolVersion,
      deserializedFrom: Option[ByteString],
  ): TransferInCommonData =
    throw new UnsupportedOperationException("Use the create method instead")

  def create(hashOps: HashOps)(
      salt: Salt,
      targetDomain: DomainId,
      targetMediator: MediatorId,
      stakeholders: Set[LfPartyId],
      uuid: UUID,
      protocolVersion: ProtocolVersion,
  ): TransferInCommonData =
    new TransferInCommonData(salt, targetDomain, targetMediator, stakeholders, uuid)(
      hashOps,
      protocolVersionRepresentativeFor(protocolVersion),
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
    } yield new TransferInCommonData(salt, targetDomain, targetMediator, stakeholders.toSet, uuid)(
      hashOps,
      protocolVersionRepresentativeFor(0),
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
case class TransferInView private (
    override val salt: Salt,
    submitter: LfPartyId,
    contract: SerializableContract,
    creatingTransactionId: TransactionId,
    transferOutResultEvent: DeliveredTransferOutResult,
)(
    hashOps: HashOps,
    val representativeProtocolVersion: ProtocolVersion,
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[TransferInView](hashOps)
    with HasProtocolVersionedWrapper[VersionedMessage[TransferInView]]
    with ProtocolVersionedMemoizedEvidence
    with NoCopy {

  override def hashPurpose: HashPurpose = HashPurpose.TransferInView

  override protected def toProtoVersioned: VersionedMessage[TransferInView] =
    TransferInView.toProtoVersioned(this)

  protected def toProtoV0: v0.TransferInView =
    v0.TransferInView(
      salt = Some(salt.toProtoV0),
      submitter = submitter,
      contract = Some(contract.toProtoV0),
      creatingTransactionId = creatingTransactionId.toProtoPrimitive,
      transferOutResultEvent = Some(transferOutResultEvent.result.toProtoV0),
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
    0 -> VersionedProtoConverter(
      ProtocolVersion.v2_0_0,
      supportedProtoVersionMemoized(v0.TransferInView)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  private[this] def apply(
      salt: Salt,
      submitter: LfPartyId,
      contract: SerializableContract,
      creatingTransactionid: TransactionId,
      transferOutResultEvent: DeliveredTransferOutResult,
  )(
      hashOps: HashOps,
      representativeProtocolVersion: ProtocolVersion,
      deserializedFrom: Option[ByteString],
  ): TransferInView =
    throw new UnsupportedOperationException("Use the create method instead")

  def create(hashOps: HashOps)(
      salt: Salt,
      submitter: LfPartyId,
      contract: SerializableContract,
      creatingTransactionId: TransactionId,
      transferOutResultEvent: DeliveredTransferOutResult,
      protocolVersion: ProtocolVersion,
  ): TransferInView =
    new TransferInView(salt, submitter, contract, creatingTransactionId, transferOutResultEvent)(
      hashOps,
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    )

  private[this] def fromProtoV0(hashOps: HashOps, transferInViewP: v0.TransferInView)(
      bytes: ByteString
  ): ParsingResult[TransferInView] = {
    val v0.TransferInView(
      saltP,
      submitterP,
      contractP,
      transferOutResultEventP,
      creatingTransactionIdP,
    ) =
      transferInViewP
    for {
      salt <- ProtoConverter.parseRequired(Salt.fromProtoV0, "salt", saltP)
      submitter <- ProtoConverter.parseLfPartyId(submitterP)
      contract <- ProtoConverter
        .required("contract", contractP)
        .flatMap(SerializableContract.fromProtoV0)
      transferOutResultEventMC <- ProtoConverter
        .required("TransferInView.transferOutResultEvent", transferOutResultEventP)
        .flatMap(
          SignedContent.fromProtoV0(
            SequencedEvent.fromByteString(
              OpenEnvelope.fromProtoV0(ProtocolMessage.fromEnvelopeContentByteStringV0(hashOps))
            )
          )
        )
      transferOutResultEvent <- DeliveredTransferOutResult
        .create(transferOutResultEventMC)
        .leftMap(err => OtherError(err.toString))
      creatingTransactionId <- TransactionId.fromProtoPrimitive(creatingTransactionIdP)
    } yield new TransferInView(
      salt,
      submitter,
      contract,
      creatingTransactionId,
      transferOutResultEvent,
    )(hashOps, protocolVersionRepresentativeFor(0), Some(bytes))
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
