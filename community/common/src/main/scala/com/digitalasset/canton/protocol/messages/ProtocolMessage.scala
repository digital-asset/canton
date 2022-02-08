// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.sequencing.protocol.{Batch, OpenEnvelope}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.DomainId
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.google.protobuf.ByteString

/** Parent trait of messages that are sent through the sequencer
  */
trait ProtocolMessage extends Product with Serializable with HasDomainId with PrettyPrinting {

  /** The ID of the domain over which this message is supposed to be sent. */
  def domainId: DomainId

  def toProtoEnvelopeContentV0(version: ProtocolVersion): v0.EnvelopeContent

  def toEnvelopeContentByteString(version: ProtocolVersion): ByteString =
    toProtoEnvelopeContentV0(version).toByteString

  /** By default prints only the object name as a trade-off for shorter long lines and not leaking confidential data.
    * Sub-classes may override the pretty instance to print more information.
    */
  override def pretty: Pretty[this.type] = prettyOfObject[ProtocolMessage]
}

object ProtocolMessage {

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
      hashOps: HashOps
  )(envelopeContent: v0.EnvelopeContent): ParsingResult[ProtocolMessage] = {
    import v0.EnvelopeContent.{SomeEnvelopeContent => Content}
    envelopeContent.someEnvelopeContent match {
      case Content.InformeeMessage(messageP) =>
        InformeeMessage.fromProtoV0(hashOps)(messageP)
      case Content.DomainTopologyTransactionMessage(messageP) =>
        DomainTopologyTransactionMessage.fromProtoV0(messageP)
      case Content.EncryptedViewMessage(messageP) =>
        EncryptedViewMessage.fromProtoV0(messageP)
      case Content.SignedMessage(messageP) =>
        SignedProtocolMessage.fromProtoV0(hashOps)(messageP)
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
    ProtoConverter.protoParser(v0.EnvelopeContent.parseFrom)(bytes).flatMap(fromProtoV0(hashOps))

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
