// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.protocol.{v0, v1}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.{
  HasProtocolVersionedWithContextCompanion,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.protobuf.ByteString

final case class EnvelopeContent(message: ProtocolMessage)(
    val representativeProtocolVersion: RepresentativeProtocolVersion[EnvelopeContent]
) extends HasProtocolVersionedWrapper[EnvelopeContent] {

  override def companionObj = EnvelopeContent
}

object EnvelopeContent extends HasProtocolVersionedWithContextCompanion[EnvelopeContent, HashOps] {
  // Serializer defined for the EnvelopeContent can throw
  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> LegacyProtoConverter(
      ProtocolVersion.v3,
      supportedProtoVersion(v0.EnvelopeContent)(fromProtoV0),
      _.message match {
        case messageV0: ProtocolMessageV0 => messageV0.toProtoEnvelopeContentV0.toByteString
        case message =>
          throw new IllegalArgumentException(
            s"Trying to serialize message $message for incompatible protocol version ${ProtocolVersion.v3}"
          )
      },
    ),
    ProtoVersion(1) -> VersionedProtoConverter(
      ProtocolVersion.v4,
      supportedProtoVersion(v1.EnvelopeContent)(fromProtoV1),
      _.message match {
        case messageV1: ProtocolMessageV1 => messageV1.toProtoEnvelopeContentV1.toByteString
        case message =>
          throw new IllegalArgumentException(
            s"Trying to serialize message $message for incompatible protocol version ${ProtocolVersion.v4}"
          )
      },
    ),
  )

  def fromProtoV0(
      hashOps: HashOps,
      envelopeContent: v0.EnvelopeContent,
  ): ParsingResult[EnvelopeContent] = {
    import v0.EnvelopeContent.{SomeEnvelopeContent as Content}
    val message = envelopeContent.someEnvelopeContent match {
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
    }
    message.map(
      EnvelopeContent.apply(_)(EnvelopeContent.protocolVersionRepresentativeFor(ProtoVersion(0)))
    )
  }

  def fromProtoV1(
      hashOps: HashOps,
      envelopeContent: v1.EnvelopeContent,
  ): ParsingResult[EnvelopeContent] = {
    import v1.EnvelopeContent.{SomeEnvelopeContent as Content}
    val message = envelopeContent.someEnvelopeContent match {
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
    }
    message.map(
      EnvelopeContent.apply(_)(EnvelopeContent.protocolVersionRepresentativeFor(ProtoVersion(1)))
    )
  }

  override protected def name: String = "EnvelopeContent"

  def apply(message: ProtocolMessage, protocolVersion: ProtocolVersion): EnvelopeContent = {
    EnvelopeContent(message)(protocolVersionRepresentativeFor(protocolVersion))
  }

  def messageFromByteString(protocolVersion: ProtocolVersion, hashOps: HashOps)(
      bytes: ByteString
  ): ParsingResult[ProtocolMessage] = {
    fromByteString(protocolVersion)(hashOps)(bytes).map(_.message)
  }
}
