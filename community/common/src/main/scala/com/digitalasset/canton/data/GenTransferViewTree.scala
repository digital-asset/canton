// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either._
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.MerkleTree.{BlindSubtree, RevealIfNeedBe, RevealSubtree}
import com.digitalasset.canton.protocol.ViewHash
import com.digitalasset.canton.protocol.v0.TransferViewTree
import com.digitalasset.canton.serialization.{HasCryptographicEvidence, ProtoConverter}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.{
  HasProtoV0,
  HasVersionedWrapper,
  ProtocolVersion,
  UntypedVersionedMessage,
  VersionedMessage,
}
import com.google.protobuf.ByteString

/** A transfer request tree has two children:
  * The `commonData` for the mediator and the involved participants
  * and the `view` only for the involved participants.
  */
abstract class GenTransferViewTree[
    CommonData <: HasCryptographicEvidence,
    View <: HasCryptographicEvidence,
    Tree,
    MediatorMessage,
] protected (commonData: MerkleTree[CommonData], participantData: MerkleTree[View])(
    hashOps: HashOps
) extends MerkleTreeInnerNode[Tree](hashOps)
    with HasVersionedWrapper[VersionedMessage[TransferViewTree]]
    with HasProtoV0[v0.TransferViewTree] { this: Tree =>

  override def subtrees: Seq[MerkleTree[_]] = Seq(commonData, participantData)

  // This method is visible because we need the non-deterministic serialization only when we encrypt the tree,
  // but the message to the mediator is sent unencrypted.
  override def toProtoVersioned(version: ProtocolVersion): VersionedMessage[TransferViewTree] =
    VersionedMessage(toProtoV0.toByteString, 0)

  override def toProtoV0: v0.TransferViewTree =
    v0.TransferViewTree(
      commonData = Some(MerkleTree.toBlindableNode(commonData)),
      participantData = Some(MerkleTree.toBlindableNode(participantData)),
    )

  def viewHash = ViewHash.fromRootHash(rootHash)

  /** Blinds the transfer view tree such that the `view` is blinded and the `commonData` remains revealed. */
  def mediatorMessage: MediatorMessage = {
    val blinded = blind {
      case root if root eq this => RevealIfNeedBe
      case `commonData` => RevealSubtree
      case `participantData` => BlindSubtree
    }
    createMediatorMessage(blinded.tryUnwrap)
  }

  /** Creates the mediator message from an appropriately blinded transfer view tree. */
  protected[this] def createMediatorMessage(blindedTree: Tree): MediatorMessage
}

object GenTransferViewTree {

  private[data] def fromProtoVersioned[CommonData, View, Tree](
      deserializeCommonData: ByteString => ParsingResult[MerkleTree[
        CommonData
      ]],
      deserializeView: ByteString => ParsingResult[MerkleTree[View]],
  )(
      createTree: (MerkleTree[CommonData], MerkleTree[View]) => Tree
  )(treeP: UntypedVersionedMessage): ParsingResult[Tree] =
    if (treeP.version == 0)
      treeP.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet("data")).flatMap { data =>
        ProtoConverter
          .protoParser(v0.TransferViewTree.parseFrom)(data)
          .flatMap(fromProtoV0(deserializeCommonData, deserializeView)(createTree))
      }
    else Left(ProtoDeserializationError.VersionError("TransferViewTree", treeP.version))

  private[data] def fromProtoV0[CommonData, View, Tree](
      deserializeCommonData: ByteString => ParsingResult[MerkleTree[
        CommonData
      ]],
      deserializeView: ByteString => ParsingResult[MerkleTree[View]],
  )(
      createTree: (MerkleTree[CommonData], MerkleTree[View]) => Tree
  )(treeP: v0.TransferViewTree): ParsingResult[Tree] = {
    val v0.TransferViewTree(commonDataP, viewP) = treeP
    for {
      commonData <- MerkleTree
        .fromProtoOption(commonDataP, deserializeCommonData(_))
        .leftMap(error => OtherError(s"transferCommonData: $error"))
      view <- MerkleTree
        .fromProtoOption(viewP, deserializeView(_))
        .leftMap(error => OtherError(s"transferView: $error"))
    } yield createTree(commonData, view)
  }

}
