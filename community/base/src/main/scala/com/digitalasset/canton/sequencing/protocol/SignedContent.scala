// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.Functor
import cats.data.EitherT
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{
  BytestringWithCryptographicEvidence,
  HasCryptographicEvidence,
  ProtoConverter,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{
  HasProtocolVersionedCompanion2,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

/** @param timestampOfSigningKey The timestamp of the topology snapshot that was used for signing the content.
  *                              [[scala.None$]] if the signing timestamp can be derived from the content.
  * @param signatures            Signatures of the content provided by the different sequencers. For protocol versions
  *                              before [[com.digitalasset.canton.version.ProtocolVersion.dev]] must not look at signatures except for the last one.
  */
// TODO(#12373) Adapt comment regarding PV=dev when releasing BFT
final case class SignedContent[+A <: HasCryptographicEvidence](
    content: A,
    signatures: NonEmpty[Seq[Signature]],
    timestampOfSigningKey: Option[CantonTimestamp],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[SignedContent.type]
) extends HasProtocolVersionedWrapper[SignedContent[HasCryptographicEvidence]]
    with Serializable
    with Product {
  @transient override protected lazy val companionObj: SignedContent.type = SignedContent

  def toProtoV0: v0.SignedContent =
    v0.SignedContent(
      Some(content.getCryptographicEvidence),
      signatures.map(_.toProtoV0),
      timestampOfSigningKey.map(_.toProtoPrimitive),
    )

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def traverse[F[_], B <: HasCryptographicEvidence](
      f: A => F[B]
  )(implicit F: Functor[F]): F[SignedContent[B]] =
    F.map(f(content)) { newContent =>
      if (newContent eq content) this.asInstanceOf[SignedContent[B]]
      else this.copy(content = newContent)
    }

  def verifySignature(
      snapshot: SyncCryptoApi,
      member: Member,
      purpose: HashPurpose,
  ): EitherT[Future, SignatureCheckError, Unit] = {
    val hash = SignedContent.hashContent(snapshot.pureCrypto, content, purpose)
    snapshot.verifySignature(hash, member, signature)
  }

  def deserializeContent[B <: HasCryptographicEvidence](
      contentDeserializer: ByteString => ParsingResult[B]
  ): ParsingResult[SignedContent[B]] =
    this.traverse(content => contentDeserializer(content.getCryptographicEvidence))

  // TODO(i12076): Start using multiple signatures
  val signature: Signature = signatures.last1

  def copy[B <: HasCryptographicEvidence](
      content: B = this.content,
      signatures: NonEmpty[Seq[Signature]] = this.signatures,
      timestampOfSigningKey: Option[CantonTimestamp] = this.timestampOfSigningKey,
  ): SignedContent[B] =
    SignedContent(content, signatures, timestampOfSigningKey)(representativeProtocolVersion)
}

object SignedContent
    extends HasProtocolVersionedCompanion2[
      SignedContent[HasCryptographicEvidence],
      SignedContent[BytestringWithCryptographicEvidence],
    ] {

  override protected def name: String = "SignedContent"

  override def supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(1) -> VersionedProtoConverter.mk(ProtocolVersion.v3)(v0.SignedContent)(
      supportedProtoVersion(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  // TODO(i12076): Start using multiple signatures
  def apply[A <: HasCryptographicEvidence](
      content: A,
      signature: Signature,
      timestampOfSigningKey: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
  ): SignedContent[A] = SignedContent(content, NonEmpty(Seq, signature), timestampOfSigningKey)(
    protocolVersionRepresentativeFor(protocolVersion)
  )

  def apply[A <: HasCryptographicEvidence](
      content: A,
      signatures: NonEmpty[Seq[Signature]],
      timestampOfSigningKey: Option[CantonTimestamp],
      protoVersion: ProtoVersion,
  ): SignedContent[A] = SignedContent(content, signatures, timestampOfSigningKey)(
    protocolVersionRepresentativeFor(protoVersion)
  )

  def create[A <: HasCryptographicEvidence](
      cryptoApi: CryptoPureApi,
      cryptoPrivateApi: SyncCryptoApi,
      content: A,
      timestampOfSigningKey: Option[CantonTimestamp],
      purpose: HashPurpose,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, SyncCryptoError, SignedContent[A]] = {
    // as deliverEvent implements MemoizedEvidence repeated calls to serialize will return the same bytes
    // so fine to call once for the hash here and then again when serializing to protobuf
    val hash = hashContent(cryptoApi, content, purpose)
    cryptoPrivateApi
      .sign(hash)
      .map(signature => SignedContent(content, signature, timestampOfSigningKey, protocolVersion))
  }

  private def hashContent(
      cryptoApi: CryptoPureApi,
      content: HasCryptographicEvidence,
      purpose: HashPurpose,
  ): Hash =
    cryptoApi.digest(purpose, content.getCryptographicEvidence)

  def tryCreate[A <: HasCryptographicEvidence](
      cryptoApi: CryptoPureApi,
      cryptoPrivateApi: SyncCryptoApi,
      content: A,
      timestampOfSigningKey: Option[CantonTimestamp],
      purpose: HashPurpose,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[SignedContent[A]] =
    create(cryptoApi, cryptoPrivateApi, content, timestampOfSigningKey, purpose, protocolVersion)
      .valueOr(err => throw new IllegalStateException(s"Failed to create signed content: $err"))

  def fromProtoV0(
      signedValueP: v0.SignedContent
  ): ParsingResult[SignedContent[BytestringWithCryptographicEvidence]] = {
    val v0.SignedContent(content, signatures, timestampOfSigningKey) = signedValueP
    for {
      contentB <- ProtoConverter.required("content", content)
      signatures <- ProtoConverter.parseRequiredNonEmpty(
        Signature.fromProtoV0,
        "signature",
        signatures,
      )
      ts <- timestampOfSigningKey.traverse(CantonTimestamp.fromProtoPrimitive)
    } yield SignedContent(
      BytestringWithCryptographicEvidence(contentB),
      signatures,
      ts,
      ProtoVersion(0),
    )
  }

  implicit def prettySignedContent[A <: HasCryptographicEvidence](implicit
      prettyA: Pretty[A]
  ): Pretty[SignedContent[A]] = {
    import com.digitalasset.canton.logging.pretty.PrettyUtil.*
    prettyOfClass(
      unnamedParam(_.content),
      param("signatures", _.signatures),
      paramIfDefined("timestamp of signing key", _.timestampOfSigningKey),
    )
  }

  def openEnvelopes(
      event: SignedContent[SequencedEvent[ClosedEnvelope]]
  )(
      protocolVersion: ProtocolVersion,
      hashOps: HashOps,
  ): Either[
    EventWithErrors[SequencedEvent[DefaultOpenEnvelope]],
    SignedContent[SequencedEvent[DefaultOpenEnvelope]],
  ] = {
    val (openSequencedEvent, openingErrors) =
      SequencedEvent.openEnvelopes(event.content)(protocolVersion, hashOps)

    Either.cond(
      openingErrors.isEmpty,
      event.copy(content = openSequencedEvent), // The signature is still valid
      EventWithErrors(openSequencedEvent, openingErrors, isIgnored = false),
    )
  }

}

final case class EventWithErrors[Event <: SequencedEvent[_]](
    content: Event,
    openingErrors: Seq[ProtoDeserializationError],
    isIgnored: Boolean,
)
