// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.{HashOps, Signature}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  EnvelopeContent,
  ProtocolMessage,
  SignedProtocolMessage,
  TypedSignedProtocolMessageContent,
}
import com.digitalasset.canton.protocol.{v0, v1}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.version.{
  HasRepresentativeProtocolVersion,
  HasSupportedProtoVersions,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.protobuf.ByteString
import monocle.Lens

import scala.math.Ordered.orderingToOrdered

/** A [[ClosedEnvelope]]'s contents are serialized as a [[com.google.protobuf.ByteString]].
  *
  * The serialization is interpreted as a [[com.digitalasset.canton.protocol.messages.EnvelopeContent]]
  * if `signatures` are empty, and as a [[com.digitalasset.canton.protocol.messages.TypedSignedProtocolMessageContent]] otherwise.
  * It itself is serialized without version wrappers inside a [[Batch]].
  */
final case class ClosedEnvelope(
    bytes: ByteString,
    override val recipients: Recipients,
    signatures: Seq[Signature],
)(override val representativeProtocolVersion: RepresentativeProtocolVersion[ClosedEnvelope])
    extends Envelope[ByteString]
    with HasRepresentativeProtocolVersion {

  // TODO(#11862) proper factory api
  locally {
    val signaturesSupportedFrom = ClosedEnvelope.signaturesSupportedSince
    require(
      signatures.isEmpty || representativeProtocolVersion >= signaturesSupportedFrom,
      s"Signatures on closed envelopes are supported only from protocol version ${signaturesSupportedFrom} on.",
    )
  }

  def openEnvelope(
      hashOps: HashOps,
      protocolVersion: ProtocolVersion,
  ): ParsingResult[DefaultOpenEnvelope] =
    NonEmpty.from(signatures) match {
      case None =>
        EnvelopeContent.fromByteString(protocolVersion)(hashOps)(bytes).map { envelopeContent =>
          OpenEnvelope(envelopeContent.message, recipients)(protocolVersion)
        }
      case Some(signaturesNE) =>
        TypedSignedProtocolMessageContent.fromByteString(hashOps)(bytes).map { typedMessage =>
          OpenEnvelope(
            SignedProtocolMessage(typedMessage, signaturesNE, protocolVersion),
            recipients,
          )(protocolVersion)
        }
    }

  override def pretty: Pretty[ClosedEnvelope] = prettyOfClass(param("recipients", _.recipients))

  override def forRecipient(member: Member): Option[ClosedEnvelope] =
    recipients.forMember(member).map(r => this.copy(recipients = r))

  override def closeEnvelope: this.type = this

  def toProtoV0: v0.Envelope = v0.Envelope(
    content = bytes,
    recipients = Some(recipients.toProtoV0),
  )

  def toProtoV1: v1.Envelope = v1.Envelope(
    content = bytes,
    recipients = Some(recipients.toProtoV0),
    signatures = signatures.map(_.toProtoV0),
  )

  def copy(
      bytes: ByteString = this.bytes,
      recipients: Recipients = this.recipients,
      signatures: Seq[Signature] = this.signatures,
  ): ClosedEnvelope =
    ClosedEnvelope(bytes, recipients, signatures)(representativeProtocolVersion)
}

object ClosedEnvelope extends HasSupportedProtoVersions[ClosedEnvelope] {

  override type Deserializer = ByteString => ParsingResult[ClosedEnvelope]

  override protected def name: String = "ClosedEnvelope"

  override def supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter.mk(ProtocolVersion.v3)(v0.Envelope)(
      protoCompanion =>
        ProtoConverter.protoParser(protoCompanion.parseFrom)(_).flatMap(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter.mk(
      // TODO(#12373) Adapt when releasing BFT
      ProtocolVersion.dev
    )(v1.Envelope)(
      protoCompanion =>
        ProtoConverter.protoParser(protoCompanion.parseFrom)(_).flatMap(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  val signaturesSupportedSince =
    protocolVersionRepresentativeFor(ProtoVersion(1))

  def apply(
      bytes: ByteString,
      recipients: Recipients,
      signatures: Seq[Signature],
      protocolVersion: ProtocolVersion,
  ): ClosedEnvelope = ClosedEnvelope(bytes, recipients, signatures)(
    protocolVersionRepresentativeFor(protocolVersion)
  )

  def apply(
      bytes: ByteString,
      recipients: Recipients,
      signatures: Seq[Signature],
      protoVersion: ProtoVersion,
  ): ClosedEnvelope = ClosedEnvelope(bytes, recipients, signatures)(
    protocolVersionRepresentativeFor(protoVersion)
  )

  private[protocol] def fromProtoV0(envelopeP: v0.Envelope): ParsingResult[ClosedEnvelope] = {
    val v0.Envelope(contentP, recipientsP) = envelopeP
    for {
      recipients <- ProtoConverter.parseRequired(Recipients.fromProtoV0, "recipients", recipientsP)
    } yield ClosedEnvelope(contentP, recipients, Seq.empty, ProtoVersion(0))
  }

  private[protocol] def fromProtoV1(envelopeP: v1.Envelope): ParsingResult[ClosedEnvelope] = {
    val v1.Envelope(contentP, recipientsP, signaturesP) = envelopeP
    for {
      recipients <- ProtoConverter.parseRequired(Recipients.fromProtoV0, "recipients", recipientsP)
      signatures <- signaturesP.traverse(Signature.fromProtoV0)
    } yield ClosedEnvelope(contentP, recipients, signatures, ProtoVersion(1))
  }

  def tryDefaultOpenEnvelope(
      hashOps: HashOps,
      protocolVersion: ProtocolVersion,
  ): Lens[ClosedEnvelope, DefaultOpenEnvelope] =
    Lens[ClosedEnvelope, DefaultOpenEnvelope](
      _.openEnvelope(hashOps, protocolVersion).valueOr(err =>
        throw new IllegalArgumentException(s"Failed to open envelope: $err")
      )
    )(newOpenEnvelope => _ => newOpenEnvelope.closeEnvelope)

  override protected def deserializationErrorK(
      error: ProtoDeserializationError
  ): ByteString => ParsingResult[ClosedEnvelope] = _ => Left(error)

  def fromProtocolMessage(
      protocolMessage: ProtocolMessage,
      recipients: Recipients,
      protocolVersion: ProtocolVersion,
  ): ClosedEnvelope = {
    if (protocolVersionRepresentativeFor(protocolVersion) >= signaturesSupportedSince) {
      protocolMessage match {
        case SignedProtocolMessage(typedMessageContent, signatures) =>
          ClosedEnvelope(
            typedMessageContent.toByteString,
            recipients,
            signatures,
            protocolVersion,
          )
        case _ =>
          ClosedEnvelope(
            EnvelopeContent.tryCreate(protocolMessage, protocolVersion).toByteString,
            recipients,
            Seq.empty,
            protocolVersion,
          )
      }
    } else {
      ClosedEnvelope(
        EnvelopeContent.tryCreate(protocolMessage, protocolVersion).toByteString,
        recipients,
        Seq.empty,
        protocolVersion,
      )
    }

  }
}
