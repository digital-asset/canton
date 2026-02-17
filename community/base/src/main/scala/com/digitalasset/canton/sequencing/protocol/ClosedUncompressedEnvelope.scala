// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.checkedToByteString
import com.digitalasset.canton.crypto.{HashOps, Signature, SignatureCheckError, SyncCryptoApi}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  EnvelopeContent,
  ProtocolMessage,
  SignedProtocolMessage,
  TypedSignedProtocolMessageContent,
  UnsignedProtocolMessage,
}
import com.digitalasset.canton.protocol.{v30, v31}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ByteStringUtil, MaxBytesToDecompress}
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionValidation,
  RepresentativeProtocolVersion,
  UnsupportedProtoCodec,
  VersionedProtoCodec,
  VersioningCompanion,
}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

/** A [[ClosedUncompressedEnvelope]]'s contents are serialized as a
  * [[com.google.protobuf.ByteString]].
  *
  * The serialization is interpreted as a
  * [[com.digitalasset.canton.protocol.messages.EnvelopeContent]] if `signatures` are empty, and as
  * a [[com.digitalasset.canton.protocol.messages.TypedSignedProtocolMessageContent]] otherwise. It
  * itself is serialized without version wrappers inside a [[Batch]].
  */
final case class ClosedUncompressedEnvelope private[protocol] (
    override val bytes: ByteString,
    override val recipients: Recipients,
    val signatures: Seq[Signature],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      ClosedUncompressedEnvelope.type
    ]
) extends ClosedEnvelope
    with HasProtocolVersionedWrapper[ClosedUncompressedEnvelope] {

  @transient override protected lazy val companionObj: ClosedUncompressedEnvelope.type =
    ClosedUncompressedEnvelope

  override def toOpenEnvelope(
      hashOps: HashOps,
      protocolVersion: ProtocolVersion,
  ): ParsingResult[DefaultOpenEnvelope] =
    NonEmpty.from(signatures) match {
      case None =>
        EnvelopeContent
          .fromByteString(hashOps, protocolVersion)(bytes)
          .map { envelopeContent =>
            OpenEnvelope(envelopeContent.message, recipients)(protocolVersion)
          }
      case Some(signaturesNE) =>
        TypedSignedProtocolMessageContent
          .fromByteStringPVV(ProtocolVersionValidation.PV(protocolVersion), bytes)
          .map { typedMessage =>
            OpenEnvelope(
              SignedProtocolMessage(typedMessage, signaturesNE),
              recipients,
            )(protocolVersion)
          }
    }

  override protected def pretty: Pretty[ClosedUncompressedEnvelope] = prettyOfClass(
    param("recipients", _.recipients),
    paramIfNonEmpty("signatures", _.signatures),
  )

  override def forRecipient(
      member: Member,
      groupRecipients: Set[GroupRecipient],
  ): Option[ClosedEnvelope] =
    recipients.forMember(member, groupRecipients).map(r => this.copy(recipients = r))

  override def toClosedUncompressedEnvelopeResult: ParsingResult[ClosedUncompressedEnvelope] =
    this.asRight

  override def toClosedCompressedEnvelope: ClosedCompressedEnvelope =
    ClosedCompressedEnvelope.create(
      bytes = ByteStringUtil.compressGzip(
        checkedToByteString(
          v31.EnvelopeWithoutRecipients(
            content = bytes,
            signatures = signatures.map(_.toProtoV30),
          )
        )
      ),
      recipients = recipients,
      algorithm = CompressionAlgorithm.GZIP,
    )(maxBytesToDecompress = MaxBytesToDecompress.HardcodedDefault)

  def toProtoV30: v30.Envelope = v30.Envelope(
    content = bytes,
    recipients = Some(recipients.toProtoV30),
    signatures = signatures.map(_.toProtoV30),
  )

  def updateSignatures(signatures: Seq[Signature]): ClosedUncompressedEnvelope =
    copy(signatures = signatures)

  @VisibleForTesting
  def copy(
      bytes: ByteString = this.bytes,
      recipients: Recipients = this.recipients,
      signatures: Seq[Signature] = this.signatures,
  ): ClosedUncompressedEnvelope =
    ClosedUncompressedEnvelope.create(bytes, recipients, signatures, representativeProtocolVersion)

  def verifySignatures(
      snapshot: SyncCryptoApi,
      sender: Member,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    NonEmpty
      .from(signatures)
      .traverse_(ClosedEnvelope.verifySignatures(snapshot, sender, bytes, _))

  @VisibleForTesting
  override def withRecipients(newRecipients: Recipients): ClosedUncompressedEnvelope =
    copy(recipients = newRecipients)
}

object ClosedUncompressedEnvelope extends VersioningCompanion[ClosedUncompressedEnvelope] {
  override def name: String = "ClosedUncompressedEnvelope"

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(
      ProtocolVersion.v34
    )(v30.Envelope)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    ),
    ProtoVersion(31) -> UnsupportedProtoCodec(ProtocolVersion.v35),
  )

  private[protocol] def fromProtoV30(
      envelopeP: v30.Envelope
  ): ParsingResult[ClosedUncompressedEnvelope] = {
    val v30.Envelope(contentP, recipientsP, signaturesP) = envelopeP
    for {
      recipients <- ProtoConverter.parseRequired(Recipients.fromProtoV30, "recipients", recipientsP)
      signatures <- signaturesP.traverse(Signature.fromProtoV30)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      closedEnvelope = create(
        contentP,
        recipients,
        signatures,
        rpv,
      )
    } yield closedEnvelope
  }

  def fromProtocolMessage(
      protocolMessage: ProtocolMessage,
      recipients: Recipients,
      protocolVersion: ProtocolVersion,
  ): ClosedUncompressedEnvelope =
    protocolMessage match {
      case SignedProtocolMessage(typedMessageContent, signatures) =>
        ClosedUncompressedEnvelope.create(
          typedMessageContent.toByteString,
          recipients,
          signatures,
          protocolVersion,
        )
      case unsignedProtocolMessage: UnsignedProtocolMessage =>
        ClosedUncompressedEnvelope.create(
          EnvelopeContent(unsignedProtocolMessage, protocolVersion).toByteString,
          recipients,
          Seq.empty,
          protocolVersion,
        )
    }

  def create(
      bytes: ByteString,
      recipients: Recipients,
      signatures: Seq[Signature],
      representativeProtocolVersion: RepresentativeProtocolVersion[ClosedUncompressedEnvelope.type],
  ): ClosedUncompressedEnvelope =
    ClosedUncompressedEnvelope(bytes, recipients, signatures)(representativeProtocolVersion)

  def create(
      bytes: ByteString,
      recipients: Recipients,
      signatures: Seq[Signature],
      protocolVersion: ProtocolVersion,
  ): ClosedUncompressedEnvelope =
    create(bytes, recipients, signatures, protocolVersionRepresentativeFor(protocolVersion))
}
