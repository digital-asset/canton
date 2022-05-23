// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.Functor
import cats.data.EitherT
import cats.syntax.option._
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.crypto.{HashOps, Signature, SyncCryptoApi, SyncCryptoError}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.{v0, v1}
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{
  HasProtoV0,
  HasVersionedMessageWithContextCompanion,
  HasVersionedWrapper,
  ProtocolVersion,
  VersionedMessage,
}

import scala.concurrent.{ExecutionContext, Future}

case class SignedProtocolMessage[+M <: SignedProtocolMessageContent](
    message: M,
    signature: Signature,
) extends ProtocolMessage
    with ProtocolMessageV0
    with ProtocolMessageV1
    with HasProtoV0[v0.SignedProtocolMessage]
    with HasVersionedWrapper[VersionedMessage[SignedProtocolMessage[M]]] {

  override def domainId: DomainId = message.domainId

  override protected def toProtoVersioned(
      version: ProtocolVersion
  ): VersionedMessage[SignedProtocolMessage[M]] = VersionedMessage(toProtoV0.toByteString, 0)

  override protected def toProtoV0: v0.SignedProtocolMessage = {
    val content = message.toProtoSomeSignedProtocolMessage
    v0.SignedProtocolMessage(
      signature = signature.toProtoV0.some,
      someSignedProtocolMessage = content,
    )
  }

  override def toProtoEnvelopeContentV0(version: ProtocolVersion): v0.EnvelopeContent =
    v0.EnvelopeContent(v0.EnvelopeContent.SomeEnvelopeContent.SignedMessage(toProtoV0))

  override def toProtoEnvelopeContentV1(version: ProtocolVersion): v1.EnvelopeContent =
    v1.EnvelopeContent(v1.EnvelopeContent.SomeEnvelopeContent.SignedMessage(toProtoV0))

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private[SignedProtocolMessage] def traverse[F[_], MM <: SignedProtocolMessageContent](
      f: M => F[MM]
  )(implicit F: Functor[F]): F[SignedProtocolMessage[MM]] = {
    F.map(f(message)) { newMessage =>
      if (newMessage eq message) this.asInstanceOf[SignedProtocolMessage[MM]]
      else this.copy(message = newMessage)
    }
  }

  override def pretty: Pretty[this.type] =
    prettyOfClass(unnamedParam(_.message), param("signature", _.signature))
}

object SignedProtocolMessage
    extends HasVersionedMessageWithContextCompanion[SignedProtocolMessage[_], HashOps] {
  override val name: String = "SignedProtocolMessage"

  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersion(v0.SignedProtocolMessage) { (hashOps, proto) =>
      fromProtoV0(hashOps)(proto)
    }
  )

  def create[M <: SignedProtocolMessageContent](
      message: M,
      cryptoApi: SyncCryptoApi,
      hashOps: HashOps,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, SyncCryptoError, SignedProtocolMessage[M]] = {
    val serialization = message.getCryptographicEvidence
    val hash = hashOps.digest(message.hashPurpose, serialization)
    cryptoApi.sign(hash).map(signature => SignedProtocolMessage(message, signature))
  }

  def tryCreate[M <: SignedProtocolMessageContent](
      message: M,
      cryptoApi: SyncCryptoApi,
      hashOps: HashOps,
  )(implicit traceContext: TraceContext, ec: ExecutionContext): Future[SignedProtocolMessage[M]] =
    create(message, cryptoApi, hashOps)
      .fold(
        err => throw new IllegalStateException(s"Failed to create signed protocol message: $err"),
        identity,
      )

  def fromProtoV0(hashOps: HashOps)(
      signedMessageP: v0.SignedProtocolMessage
  ): ParsingResult[SignedProtocolMessage[_]] = {
    import v0.SignedProtocolMessage.{SomeSignedProtocolMessage => Sm}
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
    } yield SignedProtocolMessage(message, signature)
  }

  implicit def signedMessageCast[M <: SignedProtocolMessageContent](implicit
      cast: SignedMessageContentCast[M]
  ): ProtocolMessageContentCast[SignedProtocolMessage[M]] = {
    case sm: SignedProtocolMessage[SignedProtocolMessageContent] => sm.traverse(cast.toKind)
    case _ => None

  }
}
