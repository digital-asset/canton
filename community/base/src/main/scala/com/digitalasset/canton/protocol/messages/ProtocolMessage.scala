// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.Functor
import cats.data.EitherT
import com.digitalasset.canton.crypto.signer.SyncCryptoSigner.SigningTimestampOverrides
import com.digitalasset.canton.crypto.{
  HashPurpose,
  Signature,
  SignatureCheckError,
  SigningKeyUsage,
  SyncCryptoApi,
  SyncCryptoError,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.{v30, v32}
import com.digitalasset.canton.sequencing.protocol.{ClosedEnvelope, OpenEnvelope}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.validation.ProtoValidation
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  HasRepresentativeProtocolVersion,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionValidation,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanion,
}
import com.digitalasset.nonempty.NonEmpty
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext

/** Parent trait of messages that are sent through the sequencer
  */
sealed trait ProtocolMessage
    extends Product
    with Serializable
    with HasPhysicalSynchronizerId
    with PrettyPrintingFromCompanion
    with HasRepresentativeProtocolVersion {

  override def representativeProtocolVersion: RepresentativeProtocolVersion[companionObj.type]

  /** The ID of the synchronizer over which this message is supposed to be sent. */
  def psid: PhysicalSynchronizerId

  /** By default prints only the object name as a trade-off for shorter long lines and not leaking
    * confidential data. Sub-classes may override the pretty instance to print more information.
    */
  @VisibleForTesting
  override def prettyCompanion: PrettyPrintingCompanion[this.type] = ProtocolMessage
}

object ProtocolMessage extends PrettyPrintingCompanion[ProtocolMessage] {

  override protected val pretty: Pretty[ProtocolMessage] = prettyOfObject[ProtocolMessage]

  /** Returns the envelopes from the batch that match the given synchronizer id. If any other
    * messages exist, it gives them to the provided callback
    */
  def filterSynchronizerEnvelopes[M <: ProtocolMessage](
      envelopes: Seq[OpenEnvelope[M]],
      psid: PhysicalSynchronizerId,
  )(
      onWrongSynchronizer: Seq[OpenEnvelope[M]] => Unit
  ): Seq[OpenEnvelope[M]] = {
    val (withCorrectPsid, withWrongPsid) =
      envelopes.partition(_.protocolMessage.psid == psid)
    if (withWrongPsid.nonEmpty)
      onWrongSynchronizer(withWrongPsid)
    withCorrectPsid
  }

  trait ProtocolMessageContentCast[A <: ProtocolMessage] {
    def toKind(message: ProtocolMessage): Option[A]
    def targetKind: String
  }

  object ProtocolMessageContentCast {
    def create[A <: ProtocolMessage](name: String)(
        cast: ProtocolMessage => Option[A]
    ): ProtocolMessageContentCast[A] = new ProtocolMessageContentCast[A] {
      override def toKind(message: ProtocolMessage): Option[A] = cast(message)

      override def targetKind: String = name
    }
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

/** Marker trait for [[ProtocolMessage]]s that are not a [[SignedProtocolMessage]]
  *
  * Unlike [[SignedProtocolMessage]], the sequencer does not check the signature on a
  * [[UnsignedProtocolMessage]]. The message itself can contain signatures which are then verified
  * by the recipient(s).
  */
trait UnsignedProtocolMessage extends ProtocolMessage {
  protected[messages] def toProtoSomeEnvelopeContentV32: v32.EnvelopeContent.SomeEnvelopeContent
}

/** There can be any number of signatures. Every signature covers the serialization of the
  * `typedMessage` and needs to be valid.
  */
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class SignedProtocolMessage[+M <: SignedProtocolMessageContent] private (
    typedMessage: TypedSignedProtocolMessageContent[M],
    signatures: NonEmpty[Seq[Signature]],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SignedProtocolMessage.type
    ]
) extends ProtocolMessage
    with HasProtocolVersionedWrapper[SignedProtocolMessage[SignedProtocolMessageContent]] {

  @transient override protected lazy val companionObj: SignedProtocolMessage.type =
    SignedProtocolMessage

  def message: M = typedMessage.content

  def verifySignature(
      snapshot: SyncCryptoApi,
      member: Member,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    ClosedEnvelope.verifySignatures(
      snapshot,
      member,
      typedMessage.getCryptographicEvidence,
      signatures,
    )

  def verifyMediatorSignatures(
      snapshot: SyncCryptoApi,
      mediatorGroupIndex: MediatorGroupIndex,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    ClosedEnvelope.verifyMediatorSignatures(
      snapshot,
      mediatorGroupIndex,
      typedMessage.getCryptographicEvidence,
      signatures,
    )

  def verifySequencerSignatures(
      snapshot: SyncCryptoApi
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    ClosedEnvelope.verifySequencerSignatures(
      snapshot,
      typedMessage.getCryptographicEvidence,
      signatures,
    )

  def copy[MM <: SignedProtocolMessageContent](
      typedMessage: TypedSignedProtocolMessageContent[MM] = this.typedMessage,
      signatures: NonEmpty[Seq[Signature]] = this.signatures,
  ): SignedProtocolMessage[MM] =
    SignedProtocolMessage(typedMessage, signatures)(representativeProtocolVersion)

  override def psid: PhysicalSynchronizerId = message.psid

  protected def toProtoV30: v30.SignedProtocolMessage =
    v30.SignedProtocolMessage(
      signature = signatures.map(_.toProtoV30),
      typedSignedProtocolMessageContent = typedMessage.toByteString,
    )

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private[SignedProtocolMessage] def traverse[F[_], MM <: SignedProtocolMessageContent](
      f: M => F[MM]
  )(implicit F: Functor[F]): F[SignedProtocolMessage[MM]] =
    F.map(typedMessage.traverse(f)) { newTypedMessage =>
      if (newTypedMessage eq typedMessage) this.asInstanceOf[SignedProtocolMessage[MM]]
      else this.copy(typedMessage = newTypedMessage)
    }

  override def prettyCompanion
      : PrettyPrintingCompanion[SignedProtocolMessage[SignedProtocolMessageContent]] =
    SignedProtocolMessage
}

object SignedProtocolMessage
    extends VersioningCompanion[SignedProtocolMessage[SignedProtocolMessageContent]]
    with PrettyPrintingCompanion[SignedProtocolMessage[SignedProtocolMessageContent]] {
  override val name: String = "SignedProtocolMessage"

  override protected val pretty: Pretty[SignedProtocolMessage[SignedProtocolMessageContent]] =
    prettyOfClass(unnamedParam(_.message), param("signatures", _.signatures))

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(
      ProtocolVersion.v36
    )(v30.SignedProtocolMessage)(
      supportedProtoVersionPVV(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  def apply[M <: SignedProtocolMessageContent](
      typedMessage: TypedSignedProtocolMessageContent[M],
      signatures: NonEmpty[Seq[Signature]],
      protocolVersion: ProtocolVersion,
  ): SignedProtocolMessage[M] =
    new SignedProtocolMessage(typedMessage, signatures)(
      SignedProtocolMessage.protocolVersionRepresentativeFor(protocolVersion)
    )

  @VisibleForTesting
  def from[M <: SignedProtocolMessageContent](
      message: M,
      protocolVersion: ProtocolVersion,
      signature: Signature,
      moreSignatures: Signature*
  ): SignedProtocolMessage[M] = SignedProtocolMessage(
    TypedSignedProtocolMessageContent(message, protocolVersion),
    NonEmpty(Seq, signature, moreSignatures*),
    protocolVersion,
  )

  /** @param signingTimestampOverrides
    *   Optional overrides for selecting an approximate signing timestamp and validity end, used to
    *   select the correct session signing key whenever session signing keys are enabled.
    */
  def signAndCreate[M <: SignedProtocolMessageContent](
      message: M,
      cryptoApi: SyncCryptoApi,
      signingTimestampOverrides: Option[SigningTimestampOverrides],
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, SignedProtocolMessage[M]] = {
    val typedMessage = TypedSignedProtocolMessageContent(message, protocolVersion)

    mkSignature(typedMessage, cryptoApi, signingTimestampOverrides).map { signature =>
      SignedProtocolMessage(typedMessage, NonEmpty(Seq, signature), protocolVersion)
    }
  }

  /** @param signingTimestampOverrides
    *   Optional overrides for selecting an approximate signing timestamp and validity end, used to
    *   select the correct session signing key whenever session signing keys are enabled.
    */
  @VisibleForTesting
  private[canton] def mkSignature[M <: SignedProtocolMessageContent](
      typedMessage: TypedSignedProtocolMessageContent[M],
      cryptoApi: SyncCryptoApi,
      signingTimestampOverrides: Option[SigningTimestampOverrides],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, Signature] = {
    val hashPurpose = HashPurpose.SignedProtocolMessageSignature
    val serialization = typedMessage.getCryptographicEvidence

    val hash = cryptoApi.pureCrypto.digest(hashPurpose, serialization)
    cryptoApi.sign(
      hash,
      SigningKeyUsage.ProtocolOnly,
      signingTimestampOverrides,
    )
  }

  def trySignAndCreate[M <: SignedProtocolMessageContent](
      message: M,
      cryptoApi: SyncCryptoApi,
      signingTimestampOverrides: Option[SigningTimestampOverrides],
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[SignedProtocolMessage[M]] =
    signAndCreate(message, cryptoApi, signingTimestampOverrides, protocolVersion)
      .valueOr(err =>
        throw new IllegalStateException(s"Failed to create signed protocol message: $err")
      )

  private def fromProtoV30(
      pvv: ProtocolVersionValidation,
      signedMessageP: v30.SignedProtocolMessage,
  ): ParsingResult[SignedProtocolMessage[SignedProtocolMessageContent]] = {
    val v30.SignedProtocolMessage(signaturesP, typedMessageBytes) = signedMessageP

    for {
      typedMessage <- TypedSignedProtocolMessageContent
        .fromByteString(pvv, typedMessageBytes)
      signaturesSeqP <- ProtoValidation
        .validateLength(signaturesP, "signatures", pvv, ProtoValidation.MaxCollectionSize)
      signatures <- ProtoConverter.parseRequiredNonEmpty(
        Signature.fromProtoV30,
        "signatures",
        signaturesSeqP,
      )
      rpv <- SignedProtocolMessage.protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield SignedProtocolMessage(typedMessage, signatures)(rpv)
  }

  implicit def signedMessageCast[M <: SignedProtocolMessageContent](implicit
      cast: SignedMessageContentCast[M]
  ): ProtocolMessageContentCast[SignedProtocolMessage[M]] =
    ProtocolMessageContentCast.create[SignedProtocolMessage[M]](cast.targetKind) {
      case sm: SignedProtocolMessage[SignedProtocolMessageContent] => sm.traverse(cast.toKind)
      case _ => None
    }
}
