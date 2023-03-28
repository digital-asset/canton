// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.Functor
import cats.data.EitherT
import cats.syntax.option.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.crypto.{
  HashOps,
  HashPurpose,
  Signature,
  SignatureCheckError,
  SyncCryptoApi,
  SyncCryptoError,
}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.{v0, v1}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{
  HasProtocolVersionedWithContextCompanion,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordered.orderingToOrdered

/** In protocol versions prior to [[com.digitalasset.canton.version.ProtocolVersion.dev]],
  * the `signatures` field contains a single signature over the `typeMessage`'s
  * [[com.digitalasset.canton.protocol.messages.TypedSignedProtocolMessageContent.content]].
  * From [[com.digitalasset.canton.version.ProtocolVersion.dev]] on, there can be any number of signatures
  * and each signature covers the serialization of the `typedMessage` itself rather than just its
  * [[com.digitalasset.canton.protocol.messages.TypedSignedProtocolMessageContent.content]].
  */
// TODO(#12373) Adapt comment regarding PV=dev when releasing BFT
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class SignedProtocolMessage[+M <: SignedProtocolMessageContent](
    typedMessage: TypedSignedProtocolMessageContent[M],
    signatures: NonEmpty[Seq[Signature]],
)(
    val representativeProtocolVersion: RepresentativeProtocolVersion[
      SignedProtocolMessage[SignedProtocolMessageContent]
    ]
) extends ProtocolMessage
    with ProtocolMessageV0
    with ProtocolMessageV1
    with HasProtocolVersionedWrapper[SignedProtocolMessage[SignedProtocolMessageContent]] {

  // TODO(#11862) proper factory APIs
  require(
    representativeProtocolVersion >=
      // TODO(#12373) Adapt when releasing BFT
      companionObj.protocolVersionRepresentativeFor(ProtocolVersion.dev) ||
      signatures.sizeCompare(1) == 0,
    s"SignedProtocolMessage supports only a single signatures in protocol versions below ${ProtocolVersion.dev}. Got ${signatures.size} signatures",
  )

  def message: M = typedMessage.content

  def verifySignature(
      snapshot: SyncCryptoApi,
      member: Member,
  ): EitherT[Future, SignatureCheckError, Unit] = {
    val (hashPurpose, signedBytes) =
      if (
        representativeProtocolVersion >=
          // TODO(#12373) Adapt when releasing BFT
          companionObj.protocolVersionRepresentativeFor(ProtocolVersion.dev)
      ) (HashPurpose.SignedProtocolMessageSignature, typedMessage.getCryptographicEvidence)
      else (message.hashPurpose, message.getCryptographicEvidence)
    val hash = snapshot.pureCrypto.digest(hashPurpose, signedBytes)
    snapshot.verifySignatures(hash, member, signatures)
  }

  def copy[MM <: SignedProtocolMessageContent](
      typedMessage: TypedSignedProtocolMessageContent[MM] = this.typedMessage,
      signatures: NonEmpty[Seq[Signature]] = this.signatures,
  ): SignedProtocolMessage[MM] =
    SignedProtocolMessage(typedMessage, signatures)(representativeProtocolVersion)

  override def domainId: DomainId = message.domainId

  override def companionObj = SignedProtocolMessage

  protected def toProtoV0: v0.SignedProtocolMessage = {
    val content = typedMessage.content.toProtoSomeSignedProtocolMessage
    v0.SignedProtocolMessage(
      signature = signatures.head1.toProtoV0.some,
      someSignedProtocolMessage = content,
    )
  }

  protected def toProtoV1: v1.SignedProtocolMessage = {
    v1.SignedProtocolMessage(
      signature = signatures.map(_.toProtoV0),
      typedSignedProtocolMessageContent = typedMessage.toByteString,
    )
  }

  override def toProtoEnvelopeContentV0: v0.EnvelopeContent =
    v0.EnvelopeContent(v0.EnvelopeContent.SomeEnvelopeContent.SignedMessage(toProtoV0))

  override def toProtoEnvelopeContentV1: v1.EnvelopeContent =
    v1.EnvelopeContent(v1.EnvelopeContent.SomeEnvelopeContent.SignedMessage(toProtoV0))

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private[SignedProtocolMessage] def traverse[F[_], MM <: SignedProtocolMessageContent](
      f: M => F[MM]
  )(implicit F: Functor[F]): F[SignedProtocolMessage[MM]] = {
    F.map(typedMessage.traverse(f)) { newTypedMessage =>
      if (newTypedMessage eq typedMessage) this.asInstanceOf[SignedProtocolMessage[MM]]
      else this.copy(typedMessage = newTypedMessage)
    }
  }

  override def pretty: Pretty[this.type] =
    prettyOfClass(unnamedParam(_.message), param("signatures", _.signatures))
}

object SignedProtocolMessage
    extends HasProtocolVersionedWithContextCompanion[SignedProtocolMessage[
      SignedProtocolMessageContent
    ], HashOps] {
  override val name: String = "SignedProtocolMessage"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter.mk(ProtocolVersion.v3)(v0.SignedProtocolMessage)(
      supportedProtoVersion(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter.mk(
      // TODO(#12373) Adapt when releasing BFT
      ProtocolVersion.dev
    )(v1.SignedProtocolMessage)(
      supportedProtoVersion(_)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  def apply[M <: SignedProtocolMessageContent](
      typedMessage: TypedSignedProtocolMessageContent[M],
      signatures: NonEmpty[Seq[Signature]],
      protocolVersion: ProtocolVersion,
  ): SignedProtocolMessage[M] = SignedProtocolMessage(typedMessage, signatures)(
    protocolVersionRepresentativeFor(protocolVersion)
  )

  def apply[M <: SignedProtocolMessageContent](
      typedMessage: TypedSignedProtocolMessageContent[M],
      signatures: NonEmpty[Seq[Signature]],
      protoVersion: ProtoVersion,
  ): SignedProtocolMessage[M] = SignedProtocolMessage(typedMessage, signatures)(
    protocolVersionRepresentativeFor(protoVersion)
  )

  @VisibleForTesting
  def from[M <: SignedProtocolMessageContent](
      message: M,
      protocolVersion: ProtocolVersion,
      signature: Signature,
      moreSignatures: Signature*
  ): SignedProtocolMessage[M] = SignedProtocolMessage(
    TypedSignedProtocolMessageContent(message, protocolVersion),
    NonEmpty(Seq, signature, moreSignatures: _*),
    protocolVersion,
  )

  def create[M <: SignedProtocolMessageContent](
      message: M,
      cryptoApi: SyncCryptoApi,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, SyncCryptoError, SignedProtocolMessage[M]] = {
    val typedMessage = TypedSignedProtocolMessageContent(message, protocolVersion)
    for {
      signature <- mkSignature(typedMessage, cryptoApi, protocolVersion)
    } yield SignedProtocolMessage(typedMessage, NonEmpty(Seq, signature), protocolVersion)
  }

  def mkSignature[M <: SignedProtocolMessageContent](
      typedMessage: TypedSignedProtocolMessageContent[M],
      cryptoApi: SyncCryptoApi,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncCryptoError, Signature] =
    mkSignature(typedMessage, cryptoApi, protocolVersionRepresentativeFor(protocolVersion))

  @VisibleForTesting
  private[canton] def mkSignature[M <: SignedProtocolMessageContent](
      typedMessage: TypedSignedProtocolMessageContent[M],
      cryptoApi: SyncCryptoApi,
      protocolVersion: RepresentativeProtocolVersion[SignedProtocolMessage[M]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncCryptoError, Signature] = {
    val (hashPurpose, serialization) =
      if (protocolVersion == protocolVersionRepresentativeFor(ProtoVersion(0))) {
        (typedMessage.content.hashPurpose, typedMessage.content.getCryptographicEvidence)
      } else {
        (HashPurpose.SignedProtocolMessageSignature, typedMessage.getCryptographicEvidence)
      }
    val hash = cryptoApi.pureCrypto.digest(hashPurpose, serialization)
    cryptoApi.sign(hash)
  }

  def tryCreate[M <: SignedProtocolMessageContent](
      message: M,
      cryptoApi: SyncCryptoApi,
      protocolVersion: ProtocolVersion,
  )(implicit traceContext: TraceContext, ec: ExecutionContext): Future[SignedProtocolMessage[M]] =
    create(message, cryptoApi, protocolVersion)
      .valueOr(err =>
        throw new IllegalStateException(s"Failed to create signed protocol message: $err")
      )

  def fromProtoV0(
      hashOps: HashOps,
      signedMessageP: v0.SignedProtocolMessage,
  ): ParsingResult[SignedProtocolMessage[SignedProtocolMessageContent]] = {
    import v0.SignedProtocolMessage.{SomeSignedProtocolMessage as Sm}
    val v0.SignedProtocolMessage(maybeSignatureP, messageBytes) = signedMessageP
    for {
      message <- (messageBytes match {
        case Sm.MediatorResponse(mediatorResponseBytes) =>
          MediatorResponse.fromByteString(mediatorResponseBytes)
        case Sm.TransactionResult(transactionResultMessageBytes) =>
          TransactionResultMessage.fromByteString(hashOps)(transactionResultMessageBytes)
        case Sm.TransferResult(transferResultBytes) =>
          TransferResult.fromByteString(transferResultBytes)
        case Sm.AcsCommitment(acsCommitmentBytes) =>
          AcsCommitment.fromByteString(acsCommitmentBytes)
        case Sm.MalformedMediatorRequestResult(malformedMediatorRequestResultBytes) =>
          MalformedMediatorRequestResult.fromByteString(malformedMediatorRequestResultBytes)
        case Sm.Empty =>
          Left(OtherError("Deserialization of a SignedMessage failed due to a missing message"))
      }): ParsingResult[SignedProtocolMessageContent]
      signature <- ProtoConverter.parseRequired(Signature.fromProtoV0, "signature", maybeSignatureP)
    } yield SignedProtocolMessage(
      TypedSignedProtocolMessageContent(message, ProtoVersion(0)),
      NonEmpty(Seq, signature),
      ProtoVersion(0),
    )
  }

  def fromProtoV1(
      hashOps: HashOps,
      signedMessageP: v1.SignedProtocolMessage,
  ): ParsingResult[SignedProtocolMessage[SignedProtocolMessageContent]] = {
    val v1.SignedProtocolMessage(signaturesP, typedMessageBytes) = signedMessageP
    for {
      typedMessage <- TypedSignedProtocolMessageContent.fromByteString(hashOps)(typedMessageBytes)
      signatures <- ProtoConverter.parseRequiredNonEmpty(
        Signature.fromProtoV0,
        "signatures",
        signaturesP,
      )
    } yield SignedProtocolMessage(typedMessage, signatures, ProtoVersion(1))
  }

  implicit def signedMessageCast[M <: SignedProtocolMessageContent](implicit
      cast: SignedMessageContentCast[M]
  ): ProtocolMessageContentCast[SignedProtocolMessage[M]] = {
    case sm: SignedProtocolMessage[SignedProtocolMessageContent] => sm.traverse(cast.toKind)
    case _ => None
  }
}
