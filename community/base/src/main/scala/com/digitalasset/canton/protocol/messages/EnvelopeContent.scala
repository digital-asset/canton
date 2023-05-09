// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.{v0, v1, v2}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.{
  HasProtocolVersionedWithContextCompanion,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.protobuf.ByteString

sealed trait EnvelopeContent
    extends HasProtocolVersionedWrapper[EnvelopeContent]
    with Product
    with Serializable {
  def message: ProtocolMessage

  @transient override protected lazy val companionObj: EnvelopeContent.type = EnvelopeContent

  def toByteStringUnversioned: ByteString
}

final case class EnvelopeContentV0(override val message: ProtocolMessageV0)
    extends EnvelopeContent {
  override def toByteStringUnversioned: ByteString = message.toProtoEnvelopeContentV0.toByteString

  override def representativeProtocolVersion: RepresentativeProtocolVersion[EnvelopeContent.type] =
    EnvelopeContent.representativeV0
}

final case class EnvelopeContentV1(override val message: ProtocolMessageV1)
    extends EnvelopeContent {
  override def toByteStringUnversioned: ByteString = message.toProtoEnvelopeContentV1.toByteString

  override def representativeProtocolVersion: RepresentativeProtocolVersion[EnvelopeContent.type] =
    EnvelopeContent.representativeV1
}

final case class EnvelopeContentV2(override val message: UnsignedProtocolMessageV2)
    extends EnvelopeContent {
  override def toByteStringUnversioned: ByteString =
    v2.EnvelopeContent(message.toProtoSomeEnvelopeContentV2).toByteString

  override def representativeProtocolVersion: RepresentativeProtocolVersion[EnvelopeContent.type] =
    EnvelopeContent.representativeV2
}

object EnvelopeContent extends HasProtocolVersionedWithContextCompanion[EnvelopeContent, HashOps] {

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> LegacyProtoConverter(ProtocolVersion.v3)(v0.EnvelopeContent)(
      supportedProtoVersion(_)(fromProtoV0),
      _.toByteStringUnversioned,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(v1.EnvelopeContent)(
      supportedProtoVersion(_)(fromProtoV1),
      _.toByteStringUnversioned,
    ),
    ProtoVersion(2) -> VersionedProtoConverter(
      // TODO(#12373) Adapt when releasing BFT
      ProtocolVersion.dev
    )(v2.EnvelopeContent)(
      supportedProtoVersion(_)(fromProtoV2),
      _.toByteStringUnversioned,
    ),
  )

  private[messages] val representativeV0: RepresentativeProtocolVersion[EnvelopeContent.type] =
    protocolVersionRepresentativeFor(ProtoVersion(0))
  private[messages] val representativeV1: RepresentativeProtocolVersion[EnvelopeContent.type] =
    protocolVersionRepresentativeFor(ProtoVersion(1))
  private[messages] val representativeV2: RepresentativeProtocolVersion[EnvelopeContent.type] =
    protocolVersionRepresentativeFor(ProtoVersion(2))

  def create(
      message: ProtocolMessage,
      protocolVersion: ProtocolVersion,
  ): Either[String, EnvelopeContent] = {
    val representativeProtocolVersion = protocolVersionRepresentativeFor(protocolVersion)
    message match {
      case messageV2: UnsignedProtocolMessageV2
          if representativeProtocolVersion == EnvelopeContent.representativeV2 =>
        Right(EnvelopeContentV2(messageV2))
      case messageV1: ProtocolMessageV1
          if representativeProtocolVersion == EnvelopeContent.representativeV1 =>
        Right(EnvelopeContentV1(messageV1))
      case messageV0: ProtocolMessageV0
          if representativeProtocolVersion == EnvelopeContent.representativeV0 =>
        Right(EnvelopeContentV0(messageV0))
      case _ => Left(s"Cannot use message $message in protocol version $protocolVersion")
    }
  }

  def tryCreate(
      message: ProtocolMessage,
      protocolVersion: ProtocolVersion,
  ): EnvelopeContent =
    create(message, protocolVersion).valueOr(err => throw new IllegalArgumentException(err))

  def fromProtoV0(
      hashOps: HashOps,
      envelopeContent: v0.EnvelopeContent,
  ): ParsingResult[EnvelopeContent] = {
    import v0.EnvelopeContent.{SomeEnvelopeContent as Content}
    val messageE = (envelopeContent.someEnvelopeContent match {
      case Content.InformeeMessage(messageP) =>
        InformeeMessage.fromProtoV0(hashOps)(messageP)
      case Content.DomainTopologyTransactionMessage(messageP) =>
        DomainTopologyTransactionMessage.fromProtoV0(messageP)
      case Content.EncryptedViewMessage(messageP) =>
        EncryptedViewMessageV0.fromProto(messageP)
      case Content.SignedMessage(messageP) =>
        SignedProtocolMessage.fromProtoV0(hashOps, messageP)
      case Content.TransferOutMediatorMessage(messageP) =>
        TransferOutMediatorMessage.fromProtoV0(hashOps)(messageP)
      case Content.TransferInMediatorMessage(messageP) =>
        TransferInMediatorMessage.fromProtoV0(hashOps)(messageP)
      case Content.RootHashMessage(messageP) =>
        RootHashMessage.fromProtoV0(SerializedRootHashMessagePayload.fromByteString)(messageP)
      case Content.RegisterTopologyTransactionRequest(messageP) =>
        RegisterTopologyTransactionRequest.fromProtoV0(messageP)
      case Content.RegisterTopologyTransactionResponse(messageP) =>
        RegisterTopologyTransactionResponse.fromProtoV0(messageP)
      case Content.CausalityMessage(messageP) => CausalityMessage.fromProtoV0(messageP)
      case Content.Empty => Left(OtherError("Cannot deserialize an empty message content"))
    }): ParsingResult[ProtocolMessageV0]
    messageE.map(message => EnvelopeContentV0(message))
  }

  def fromProtoV1(
      hashOps: HashOps,
      envelopeContent: v1.EnvelopeContent,
  ): ParsingResult[EnvelopeContent] = {
    import v1.EnvelopeContent.{SomeEnvelopeContent as Content}
    val messageE = (envelopeContent.someEnvelopeContent match {
      case Content.InformeeMessage(messageP) =>
        InformeeMessage.fromProtoV1(hashOps)(messageP)
      case Content.DomainTopologyTransactionMessage(messageP) =>
        DomainTopologyTransactionMessage.fromProtoV0(messageP)
      case Content.EncryptedViewMessage(messageP) =>
        EncryptedViewMessageV1.fromProto(messageP)
      case Content.SignedMessage(messageP) =>
        SignedProtocolMessage.fromProtoV0(hashOps, messageP)
      case Content.TransferOutMediatorMessage(messageP) =>
        TransferOutMediatorMessage.fromProtoV1(hashOps)(messageP)
      case Content.TransferInMediatorMessage(messageP) =>
        TransferInMediatorMessage.fromProtoV1(hashOps)(messageP)
      case Content.RootHashMessage(messageP) =>
        RootHashMessage.fromProtoV0(SerializedRootHashMessagePayload.fromByteString)(messageP)
      case Content.RegisterTopologyTransactionRequest(messageP) =>
        RegisterTopologyTransactionRequest.fromProtoV0(messageP)
      case Content.RegisterTopologyTransactionResponse(messageP) =>
        RegisterTopologyTransactionResponse.fromProtoV1(messageP)
      case Content.CausalityMessage(messageP) => CausalityMessage.fromProtoV0(messageP)
      case Content.Empty => Left(OtherError("Cannot deserialize an empty message content"))
    }): ParsingResult[ProtocolMessageV1]
    messageE.map(message => EnvelopeContentV1(message))
  }

  private def fromProtoV2(
      hashOps: HashOps,
      contentP: v2.EnvelopeContent,
  ): ParsingResult[EnvelopeContent] = {
    import v2.EnvelopeContent.{SomeEnvelopeContent as Content}
    for {
      content <- (contentP.someEnvelopeContent match {
        case Content.InformeeMessage(messageP) =>
          InformeeMessage.fromProtoV1(hashOps)(messageP)
        case Content.DomainTopologyTransactionMessage(messageP) =>
          DomainTopologyTransactionMessage.fromProtoV0(messageP)
        case Content.EncryptedViewMessage(messageP) =>
          EncryptedViewMessageV1.fromProto(messageP)
        case Content.TransferOutMediatorMessage(messageP) =>
          TransferOutMediatorMessage.fromProtoV1(hashOps)(messageP)
        case Content.TransferInMediatorMessage(messageP) =>
          TransferInMediatorMessage.fromProtoV1(hashOps)(messageP)
        case Content.RootHashMessage(messageP) =>
          RootHashMessage.fromProtoV0(SerializedRootHashMessagePayload.fromByteString)(messageP)
        case Content.RegisterTopologyTransactionRequest(messageP) =>
          RegisterTopologyTransactionRequest.fromProtoV0(messageP)
        case Content.RegisterTopologyTransactionResponse(messageP) =>
          RegisterTopologyTransactionResponse.fromProtoV1(messageP)
        case Content.CausalityMessage(messageP) => CausalityMessage.fromProtoV0(messageP)
        case Content.RegisterTopologyTransactionRequestX(messageP) =>
          RegisterTopologyTransactionRequestX.fromProtoV2(messageP)
        case Content.RegisterTopologyTransactionResponseX(messageP) =>
          RegisterTopologyTransactionResponseX.fromProtoV2(messageP)
        case Content.AcceptedTopologyTransactions(messageP) =>
          AcceptedTopologyTransactionsX.fromProtoV2(messageP)
        case Content.Empty => Left(OtherError("Cannot deserialize an empty message content"))
      }): ParsingResult[UnsignedProtocolMessageV2]
    } yield EnvelopeContentV2(content)
  }

  override protected def name: String = "EnvelopeContent"

  def messageFromByteArray[M <: UnsignedProtocolMessage](
      protocolVersion: ProtocolVersion,
      hashOps: HashOps,
  )(
      bytes: Array[Byte]
  )(implicit cast: ProtocolMessageContentCast[M]): ParsingResult[M] = {
    for {
      envelopeContent <- fromByteString(protocolVersion)(hashOps)(ByteString.copyFrom(bytes))
      message <- cast
        .toKind(envelopeContent.message)
        .toRight(
          ProtoDeserializationError.OtherError(
            s"Cannot deserialize ${envelopeContent.message} as a ${cast.targetKind}"
          )
        )
    } yield message
  }
}
