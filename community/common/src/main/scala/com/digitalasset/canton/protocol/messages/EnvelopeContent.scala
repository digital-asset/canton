// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.protocol.{v0, v1}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.{
  HasProtocolVersionedWithContextCompanion,
  HasProtocolVersionedWrapper,
  ProtobufVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedMessage,
}
import com.google.protobuf.ByteString

final case class EnvelopeContent(message: ProtocolMessage)
    extends HasProtocolVersionedWrapper[EnvelopeContent] {

  def representativeProtocolVersion: RepresentativeProtocolVersion =
    message.representativeProtocolVersion

  override def toByteString: ByteString = toProtoVersioned.toByteString

  // This method can throw (see companion object)
  override def toProtoVersioned: VersionedMessage[EnvelopeContent] =
    EnvelopeContent.toProtoVersioned(this)
}

object EnvelopeContent extends HasProtocolVersionedWithContextCompanion[EnvelopeContent, HashOps] {
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
    message.map(EnvelopeContent.apply)
  }

  def fromProtoV1(
      hashOps: HashOps,
      envelopeContent: v1.EnvelopeContent,
  ): ParsingResult[EnvelopeContent] = {
    import v1.EnvelopeContent.{SomeEnvelopeContent => Content}
    val message = envelopeContent.someEnvelopeContent match {
      case Content.InformeeMessage(messageP) =>
        InformeeMessage.fromProtoV0(hashOps)(messageP)
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
        RegisterTopologyTransactionResponse.fromProtoV0(messageP)
      case Content.CausalityMessage(messageP) => CausalityMessage.fromProtoV0(messageP)
      case Content.Empty => Left(OtherError("Cannot deserialize an empty message content"))
    }
    message.map(EnvelopeContent.apply)
  }

  override protected def name: String = "EnvelopeContent"

  def messageFromByteString(hashOps: HashOps)(
      bytes: ByteString
  ): Either[ProtoDeserializationError, ProtocolMessage] =
    fromByteString(hashOps)(bytes).map(_.message)
}
