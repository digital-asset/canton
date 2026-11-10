// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.Functor
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.protocol.{SynchronizerLimits, v30}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{
  BytestringWithCryptographicEvidence,
  HasCryptographicEvidence,
  ProtoConverter,
}
import com.digitalasset.canton.validation.ProtoValidation
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionValidation,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanion2,
}
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString

/** Content together with the signatures of several sequencers on it, as produced by the sequencer
  * client when it aggregates the copies of the same event received from several sequencers. The
  * signatures are not verified after aggregation, so this class offers no way to verify them.
  *
  * Shares the wire format with [[SignedContent]], without the timestamp of the signing key: the
  * signing key of a sequenced event is always determined by the event itself.
  *
  * @param signatures
  *   Signatures of the content provided by the different sequencers.
  */
final case class AggregatedSignedContent[+A <: HasCryptographicEvidence] private (
    content: A,
    signatures: NonEmpty[Seq[Signature]],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      AggregatedSignedContent.type
    ]
) extends HasProtocolVersionedWrapper[AggregatedSignedContent[HasCryptographicEvidence]] {

  @transient override protected lazy val companionObj: AggregatedSignedContent.type =
    AggregatedSignedContent

  def toProtoV30: v30.SignedContent =
    v30.SignedContent(
      Some(content.getCryptographicEvidence),
      signatures.map(_.toProtoV30),
      timestampOfSigningKey = None,
    )

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def traverse[F[_], B <: HasCryptographicEvidence](
      f: A => F[B]
  )(implicit F: Functor[F]): F[AggregatedSignedContent[B]] =
    F.map(f(content)) { newContent =>
      if (newContent eq content) this.asInstanceOf[AggregatedSignedContent[B]]
      else this.copy(content = newContent)
    }

  def deserializeContent[B <: HasCryptographicEvidence](
      contentDeserializer: ByteString => ParsingResult[B]
  )(implicit
      ev: A <:< BytestringWithCryptographicEvidence
  ): ParsingResult[AggregatedSignedContent[B]] =
    this.traverse(content => contentDeserializer(ev(content).bytes))

  def copy[B <: HasCryptographicEvidence](
      content: B = this.content,
      signatures: NonEmpty[Seq[Signature]] = this.signatures,
  ): AggregatedSignedContent[B] =
    new AggregatedSignedContent(content, signatures)(representativeProtocolVersion)
}

object AggregatedSignedContent
    extends VersioningCompanion2[
      AggregatedSignedContent[HasCryptographicEvidence],
      AggregatedSignedContent[BytestringWithCryptographicEvidence],
    ] {

  override def name: String = "AggregatedSignedContent"

  override val versioningTable: VersioningTable = VersioningTable(
    // TODO(i37895): Split the wire format of SignedContent and AggregatedSignedContent
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v36)(v30.SignedContent)(
      supportedProtoVersionPVV(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  /** Creates an [[AggregatedSignedContent]] from the content and the signatures. */
  def apply[A <: HasCryptographicEvidence](
      content: A,
      signatures: NonEmpty[Seq[Signature]],
      protocolVersion: ProtocolVersion,
  ): AggregatedSignedContent[A] =
    new AggregatedSignedContent(content, signatures)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  /** Creates an [[AggregatedSignedContent]] from a single signature. */
  def apply[A <: HasCryptographicEvidence](
      content: A,
      signature: Signature,
      protocolVersion: ProtocolVersion,
  ): AggregatedSignedContent[A] =
    apply(content, NonEmpty(Seq, signature), protocolVersion)

  private def fromProtoV30(
      pvv: ProtocolVersionValidation,
      signedValueP: v30.SignedContent,
  ): ParsingResult[AggregatedSignedContent[BytestringWithCryptographicEvidence]] = {
    val v30.SignedContent(content, signatures, timestampOfSigningKey) = signedValueP
    for {
      contentB <- ProtoConverter.required("content", content)
      signaturesSeq <- ProtoValidation
        .validateLength(signatures, "signature", pvv, ProtoValidation.MaxCollectionSize)
      signatures <- ProtoConverter.parseRequiredNonEmpty(
        Signature.fromProtoV30,
        "signature",
        signaturesSeq,
      )
      _ <- Either.cond(
        timestampOfSigningKey.isEmpty,
        (),
        InvariantViolation(
          "timestamp_of_signing_key",
          "must not be set on aggregated signed content",
        ),
      )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield new AggregatedSignedContent(BytestringWithCryptographicEvidence(contentB), signatures)(
      rpv
    )
  }

  implicit def prettyAggregatedSignedContent[A <: HasCryptographicEvidence](implicit
      prettyA: Pretty[A]
  ): Pretty[AggregatedSignedContent[A]] = {
    import com.digitalasset.canton.logging.pretty.PrettyUtil.*
    prettyOfClass(
      unnamedParam(_.content),
      param("signatures", _.signatures),
    )
  }

  def openEnvelopes(event: AggregatedSignedContent[DecompressedSequencedEvent[ClosedEnvelope]])(
      protocolVersion: ProtocolVersion,
      hashOps: HashOps,
      synchronizerLimits: SynchronizerLimits,
  ): WithOpeningErrors[AggregatedSignedContent[DecompressedSequencedEvent[DefaultOpenEnvelope]]] = {
    val (openSequencedEvent, openingErrors) =
      SequencedEvent.openEnvelopes(event.content)(protocolVersion, hashOps, synchronizerLimits)
    WithOpeningErrors(
      event.copy(content = openSequencedEvent),
      openingErrors,
    )
  }
}
