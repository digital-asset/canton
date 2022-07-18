// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.protocol.{v0, v1}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.{
  HasProtocolVersionedWithContextCompanion,
  HasProtocolVersionedWrapper,
  ProtobufVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.protobuf.ByteString

final case class EnvelopeContent(message: ProtocolMessage)(
    val representativeProtocolVersion: RepresentativeProtocolVersion[EnvelopeContent]
) extends HasProtocolVersionedWrapper[EnvelopeContent] {

  // TODO(i9627): Remove this distinction and define an unwrapped serialization for PV2 in the companion object
  // TODO(i9423): Migrate to next protocol version
  override def toByteString: ByteString =
    if (isEquivalentTo(ProtocolVersion.unstable_development))
      toProtoVersioned.toByteString
    else toProtoVersioned.getData

  override def companionObj = EnvelopeContent
}

object EnvelopeContent extends HasProtocolVersionedWithContextCompanion[EnvelopeContent, HashOps] {
  // Serializer defined for the EnvelopeContent can throw
  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtobufVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v2_0_0,
      supportedProtoVersion(v0.EnvelopeContent)(fromProtoV0),
      _.message match {
        case messageV0: ProtocolMessageV0 => messageV0.toProtoEnvelopeContentV0.toByteString
        case message =>
          throw new IllegalArgumentException(
            s"Trying to serialize message $message for incompatible protocol version ${ProtocolVersion.v2_0_0}"
          )
      },
    ),
    ProtobufVersion(1) -> VersionedProtoConverter(
      ProtocolVersion.unstable_development, // TODO(i9423): Migrate to next protocol version
      supportedProtoVersion(v1.EnvelopeContent)(fromProtoV1),
      _.message match {
        case messageV1: ProtocolMessageV1 => messageV1.toProtoEnvelopeContentV1.toByteString
        case message =>
          throw new IllegalArgumentException(
            s"Trying to serialize message $message for incompatible protocol version ${ProtocolVersion.unstable_development}"
          )
      },
    ),
  )

  def fromProtoV0(
      hashOps: HashOps,
      envelopeContent: v0.EnvelopeContent,
  ): ParsingResult[EnvelopeContent] = {
    import v0.EnvelopeContent.{SomeEnvelopeContent => Content}
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
      EnvelopeContent.apply(_)(EnvelopeContent.protocolVersionRepresentativeFor(ProtobufVersion(0)))
    )
  }

  def fromProtoV1(
      hashOps: HashOps,
      envelopeContent: v1.EnvelopeContent,
  ): ParsingResult[EnvelopeContent] = {
    import v1.EnvelopeContent.{SomeEnvelopeContent => Content}
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
        TransferOutMediatorMessage.fromProtoV0(hashOps)(messageP)
      case Content.TransferInMediatorMessage(messageP) =>
        TransferInMediatorMessage.fromProtoV0(hashOps)(messageP)
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
      EnvelopeContent.apply(_)(EnvelopeContent.protocolVersionRepresentativeFor(ProtobufVersion(1)))
    )
  }

  override protected def name: String = "EnvelopeContent"

  def apply(message: ProtocolMessage, protocolVersion: ProtocolVersion): EnvelopeContent = {
    EnvelopeContent(message)(protocolVersionRepresentativeFor(protocolVersion))
  }

  def messageFromByteStringV0(hashOps: HashOps)(
      bytes: ByteString
  ): ParsingResult[ProtocolMessage] =
    ProtoConverter
      .protoParser(v0.EnvelopeContent.parseFrom)(bytes)
      .flatMap(fromProtoV0(hashOps, _))
      .map(_.message)

  def messageFromByteString(protocolVersion: ProtocolVersion, hashOps: HashOps)(
      bytes: ByteString
  ): Either[ProtoDeserializationError, ProtocolMessage] = {
    // Previously the envelope content was not wrapped in a VersionedMessage, therefore we have to explicitly decide to
    // deserialize from a versioned wrapper message or not.
    protocolVersion match {
      // TODO(i9423): Migrate to next protocol version
      case ProtocolVersion.unstable_development =>
        fromByteString(hashOps)(bytes).map(_.message)
      case _ =>
        messageFromByteStringV0(hashOps)(bytes)
    }
  }
}
