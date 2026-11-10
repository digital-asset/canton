// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.Functor
import cats.data.EitherT
import cats.syntax.traverse.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.signer.SyncCryptoSigner.SigningTimestampOverrides
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{
  BytestringWithCryptographicEvidence,
  HasCryptographicEvidence,
  ProtoConverter,
  ProtocolVersionedMemoizedEvidence,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.validation.{ProtoValidation, ValidationParam}
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionValidation,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanionContextMemoization2,
}
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

/** Content signed by a single member. Content signed by several sequencers is a
  * [[AggregatedSignedContent]].
  *
  * @param signature
  *   The signature of the content.
  * @param timestampOfSigningKey
  *   The timestamp of the topology snapshot that was used for signing the content. [[scala.None$]]
  *   if the signing timestamp can be derived from the content.
  */
final case class SignedContent[+A <: HasCryptographicEvidence] private (
    content: A,
    signature: Signature,
    timestampOfSigningKey: Option[CantonTimestamp],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[SignedContent.type],
    override val deserializedFrom: Option[ByteString],
) extends HasProtocolVersionedWrapper[SignedContent[HasCryptographicEvidence]]
    with ProtocolVersionedMemoizedEvidence {

  @transient override protected lazy val companionObj: SignedContent.type = SignedContent

  def toProtoV30: v30.SignedContent =
    v30.SignedContent(
      Some(content.getCryptographicEvidence),
      Seq(signature.toProtoV30),
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
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] = {
    val hash = SignedContent.hashContent(snapshot.pureCrypto, content, purpose)
    snapshot.verifySignature(hash, member, signature, SigningKeyUsage.ProtocolOnly)
  }

  def verifyKeyUsage(
      snapshot: SyncCryptoApi,
      member: Member,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    snapshot.verifyKeyUsage(
      member,
      signature.authorizingLongTermKey,
      signature.signatureDelegation,
      SigningKeyUsage.ProtocolOnly,
    )

  def deserializeContent[B <: HasCryptographicEvidence](
      contentDeserializer: ByteString => ParsingResult[B]
  )(implicit
      ev: A <:< BytestringWithCryptographicEvidence
  ): ParsingResult[SignedContent[B]] =
    this.traverse(content => contentDeserializer(ev(content).bytes))

  def copy[B <: HasCryptographicEvidence](
      content: B = this.content,
      signature: Signature = this.signature,
      timestampOfSigningKey: Option[CantonTimestamp] = this.timestampOfSigningKey,
  ): SignedContent[B] =
    SignedContent.create(
      content,
      signature,
      timestampOfSigningKey,
      representativeProtocolVersion,
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString
}

object SignedContent
    extends VersioningCompanionContextMemoization2[
      SignedContent[HasCryptographicEvidence],
      SignatureCountCheck,
      SignedContent[BytestringWithCryptographicEvidence],
      Unit,
    ] {

  override def name: String = "SignedContent"

  override val versioningTable: VersioningTable = VersioningTable(
    // TODO(i37895): Split the wire format of SignedContent and AggregatedSignedContent
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v36)(v30.SignedContent)(
      supportedProtoVersionMemoizedPVV(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  def apply[A <: HasCryptographicEvidence](
      content: A,
      signature: Signature,
      timestampOfSigningKey: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
  ): SignedContent[A] =
    SignedContent.create(
      content,
      signature,
      timestampOfSigningKey,
      protocolVersionRepresentativeFor(protocolVersion),
    )

  /** Creates a new signed content from scratch. Use when creating without having an existing
    * serialized SignedContent.
    */
  def create[A <: HasCryptographicEvidence](
      content: A,
      signature: Signature,
      timestampOfSigningKey: Option[CantonTimestamp],
      representativeProtocolVersion: RepresentativeProtocolVersion[SignedContent.type],
  ): SignedContent[A] =
    new SignedContent(content, signature, timestampOfSigningKey)(
      representativeProtocolVersion,
      None,
    )

  /** @param timestampOfSigningKey
    *   indicates which topology snapshot’s timestamp was used when producing the signature. Leave
    *   as [[scala.None$]] if the content itself provides enough information to deduce this
    *   timestamp. This timestamp is currently only used by the traffic-cost primitives. During
    *   traffic-cost validation, it is compared against the most recent known synchronizer timestamp
    *   to decide whether to reject the request, use the current snapshot, or wait for a future one.
    *   The value reflects the last perceived topology state rather than the "current" time, which
    *   is why using `approximateTimestampForSigning` (that takes the value 'clock.now') does not
    *   make sense and would require adjusting the validation logic to also rely on the current time
    *   of the validator node.
    * @param signingTimestampOverrides
    *   Optional overrides for selecting an approximate signing timestamp and validity end, used to
    *   select the correct session signing key whenever session signing keys are enabled.
    */
  def create[A <: HasCryptographicEvidence](
      cryptoApi: CryptoPureApi,
      cryptoPrivateApi: SyncCryptoApi,
      content: A,
      timestampOfSigningKey: Option[CantonTimestamp],
      signingTimestampOverrides: Option[SigningTimestampOverrides],
      purpose: HashPurpose,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, SignedContent[A]] = {
    // as deliverEvent implements MemoizedEvidence repeated calls to serialize will return the same bytes
    // so fine to call once for the hash here and then again when serializing to protobuf
    val hash = hashContent(cryptoApi, content, purpose)
    cryptoPrivateApi
      .sign(
        hash,
        SigningKeyUsage.ProtocolOnly,
        signingTimestampOverrides,
      )
      .map(signature => SignedContent(content, signature, timestampOfSigningKey, protocolVersion))
  }

  def hashContent(
      hashOps: HashOps,
      content: HasCryptographicEvidence,
      purpose: HashPurpose,
  ): Hash =
    hashOps.digest(purpose, content.getCryptographicEvidence)

  def tryCreate[A <: HasCryptographicEvidence](
      cryptoApi: CryptoPureApi,
      cryptoPrivateApi: SyncCryptoApi,
      content: A,
      timestampOfSigningKey: Option[CantonTimestamp],
      signingTimestampOverrides: Option[SigningTimestampOverrides],
      purpose: HashPurpose,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[SignedContent[A]] =
    create(
      cryptoApi,
      cryptoPrivateApi,
      content,
      timestampOfSigningKey,
      signingTimestampOverrides: Option[SigningTimestampOverrides],
      purpose,
      protocolVersion,
    )
      .valueOr(err => throw new IllegalStateException(s"Failed to create signed content: $err"))

  private def fromProtoV30(
      pvv: ProtocolVersionValidation,
      signatureCountCheck: SignatureCountCheck,
      signedValueP: v30.SignedContent,
  )(
      bytes: ByteString
  ): ParsingResult[SignedContent[BytestringWithCryptographicEvidence]] = {
    val v30.SignedContent(content, signaturesP, timestampOfSigningKey) = signedValueP
    val (signatureCountPvv, maxSignatures) = signatureCountCheck match {
      case SignatureCountCheck.Strict =>
        (
          ProtocolVersionValidation.AlwaysValidation,
          ValidationParam(ProtocolVersion.minimum -> PositiveInt.one),
        )
      case SignatureCountCheck.ByProtocolVersion =>
        (pvv, ValidationParam(ProtocolVersion.nextValidationBoundPV -> PositiveInt.one))
    }
    for {
      contentB <- ProtoConverter.required("content", content)
      signaturesSeq <- ProtoValidation
        .validateLength(signaturesP, "signatures", signatureCountPvv, maxSignatures)
      signatures <- ProtoConverter.parseRequiredNonEmpty(
        Signature.fromProtoV30,
        "signatures",
        signaturesSeq,
      )
      ts <- timestampOfSigningKey.traverse(CantonTimestamp.fromProtoPrimitive)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      // Protocol versions before the bound accept several signatures, of which only the last one is
      // verified. Keep only that one so that all nodes see the same signature.
    } yield new SignedContent(BytestringWithCryptographicEvidence(contentB), signatures.last1, ts)(
      rpv,
      Some(bytes),
    )
  }

  implicit def prettySignedContent[A <: HasCryptographicEvidence](implicit
      prettyA: Pretty[A]
  ): Pretty[SignedContent[A]] = {
    import com.digitalasset.canton.logging.pretty.PrettyUtil.*
    prettyOfClass(
      unnamedParam(_.content),
      param("signature", _.signature),
      paramIfDefined("timestamp of signing key", _.timestampOfSigningKey),
    )
  }

}

/** How [[SignedContent]] deserialization treats a message with more than one signature. */
sealed trait SignatureCountCheck extends Product with Serializable

object SignatureCountCheck {

  /** Reject more than one signature on every protocol version. Use where a node decides alone
    * whether to accept the message, e.g. a sequencer receiving a request before ordering it.
    */
  case object Strict extends SignatureCountCheck

  /** Reject more than one signature from
    * [[com.digitalasset.canton.version.ProtocolVersion.nextValidationBoundPV]] on, and keep only
    * the last signature on earlier protocol versions. Use where all nodes must reach the same
    * result, e.g. after ordering, including nodes on different releases in cross-version
    * deployments.
    */
  case object ByProtocolVersion extends SignatureCountCheck
}
