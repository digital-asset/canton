// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{v0, v1}
import com.digitalasset.canton.sequencing.protocol.{Batch, OpenEnvelope}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.version.{
  HasProtocolVersionedWithContextCompanion,
  HasRepresentativeProtocolVersion,
  ProtobufVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId
import com.google.protobuf.ByteString

/** Parent trait of messages that are sent through the sequencer
  */
trait ProtocolMessage
    extends Product
    with Serializable
    with HasDomainId
    with PrettyPrinting
    with HasRepresentativeProtocolVersion {
  def representativeProtocolVersion: RepresentativeProtocolVersion

  /** The ID of the domain over which this message is supposed to be sent. */
  def domainId: DomainId

  /** By default prints only the object name as a trade-off for shorter long lines and not leaking confidential data.
    * Sub-classes may override the pretty instance to print more information.
    */
  override def pretty: Pretty[this.type] = prettyOfObject[ProtocolMessage]
}

trait ProtocolMessageV0 extends ProtocolMessage {
  protected[messages] def toProtoEnvelopeContentV0: v0.EnvelopeContent
}

trait ProtocolMessageV1 extends ProtocolMessage {
  protected[messages] def toProtoEnvelopeContentV1: v1.EnvelopeContent
}

object ProtocolMessage extends HasProtocolVersionedWithContextCompanion[ProtocolMessage, HashOps] {
  val name = "ProtocolMessage"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtobufVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v2_0_0,
      supportedProtoVersion(v0.EnvelopeContent)(fromProtoV0),
      protocolMessage =>
        protocolMessage match {
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
      protocolMessage =>
        protocolMessage match {
          case messageV1: ProtocolMessageV1 => messageV1.toProtoEnvelopeContentV1.toByteString
          case message =>
            throw new IllegalArgumentException(
              s"Trying to serialize message $message for incompatible protocol version ${ProtocolVersion.unstable_development}"
            )
        },
    ),
  )

  def toEnvelopeContentByteString(
      message: ProtocolMessage
  ): ByteString =
    supportedProtoVersions.serializerFor(message.representativeProtocolVersion)(message)

  def fromEnvelopeContentByteString(protocolVersion: ProtocolVersion, hashOps: HashOps)(
      bytes: ByteString
  ): ParsingResult[ProtocolMessage] = protocolVersion match {
    // TODO(i9423): Migrate to next protocol version
    case ProtocolVersion.unstable_development =>
      fromEnvelopeContentByteStringV1(hashOps)(bytes)
    case _ =>
      fromEnvelopeContentByteStringV0(hashOps)(bytes)
  }

  /** Returns the envelopes from the batch that match the given domain ID. If any other messages exist, it gives them
    * to the provided callback
    */
  def filterDomainsEnvelopes[M <: ProtocolMessage](
      batch: Batch[OpenEnvelope[M]],
      domainId: DomainId,
      onWrongDomain: List[OpenEnvelope[M]] => Unit,
  ): List[OpenEnvelope[M]] = {
    val (withCorrectDomainId, withWrongDomainId) =
      batch.envelopes.partition(_.protocolMessage.domainId == domainId)
    if (withWrongDomainId.nonEmpty)
      onWrongDomain(withWrongDomainId)
    withCorrectDomainId
  }

  def fromProtoV0(
      hashOps: HashOps,
      envelopeContent: v0.EnvelopeContent,
  ): ParsingResult[ProtocolMessage] = {
    import v0.EnvelopeContent.{SomeEnvelopeContent => Content}
    envelopeContent.someEnvelopeContent match {
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
  }

  def fromProtoV1(
      hashOps: HashOps,
      envelopeContent: v1.EnvelopeContent,
  ): ParsingResult[ProtocolMessage] = {
    import v1.EnvelopeContent.{SomeEnvelopeContent => Content}
    envelopeContent.someEnvelopeContent match {
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
  }

  def fromEnvelopeContentByteStringV0(hashOps: HashOps)(
      bytes: ByteString
  ): ParsingResult[ProtocolMessage] =
    ProtoConverter.protoParser(v0.EnvelopeContent.parseFrom)(bytes).flatMap(fromProtoV0(hashOps, _))

  private def fromEnvelopeContentByteStringV1(hashOps: HashOps)(
      bytes: ByteString
  ): ParsingResult[ProtocolMessage] =
    ProtoConverter.protoParser(v1.EnvelopeContent.parseFrom)(bytes).flatMap(fromProtoV1(hashOps, _))

  trait ProtocolMessageContentCast[A <: ProtocolMessage] {
    def toKind(message: ProtocolMessage): Option[A]
  }

  def toKind[M <: ProtocolMessage](envelope: DefaultOpenEnvelope)(implicit
      cast: ProtocolMessageContentCast[M]
  ): Option[M] =
    cast.toKind(envelope.protocolMessage)

  def select[M <: ProtocolMessage](envelope: DefaultOpenEnvelope)(implicit
      cast: ProtocolMessageContentCast[M]
  ): Option[OpenEnvelope[M]] =
    envelope.traverse(cast.toKind)
}
