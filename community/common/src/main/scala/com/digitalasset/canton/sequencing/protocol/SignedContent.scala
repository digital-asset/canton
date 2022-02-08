// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.Functor
import cats.data.EitherT
import cats.syntax.traverse._
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.protocol.version.VersionedSignedContent
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{MemoizedEvidence, ProtoConverter}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{HasProtoV0, HasVersionedWrapper}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

case class SignedContent[+A <: MemoizedEvidence](
    content: A,
    signature: Signature,
    timestampOfSigningKey: Option[CantonTimestamp],
) extends HasVersionedWrapper[VersionedSignedContent]
    with HasProtoV0[v0.SignedContent] {
  override def toProtoVersioned(version: ProtocolVersion): VersionedSignedContent =
    VersionedSignedContent(VersionedSignedContent.Version.V0(toProtoV0))

  override def toProtoV0: v0.SignedContent =
    v0.SignedContent(
      Some(content.getCryptographicEvidence),
      Some(signature.toProtoV0),
      timestampOfSigningKey.map(_.toProtoPrimitive),
    )

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def traverse[F[_], B <: MemoizedEvidence](
      f: A => F[B]
  )(implicit F: Functor[F]): F[SignedContent[B]] =
    F.map(f(content)) { newContent =>
      if (newContent eq content) this.asInstanceOf[SignedContent[B]]
      else this.copy(content = newContent)
    }
}

object SignedContent {

  def create[Env <: Envelope[_]](
      cryptoApi: CryptoPureApi,
      cryptoPrivateApi: SyncCryptoApi,
      event: SequencedEvent[Env],
      timestampOfSigningKey: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, SyncCryptoError, SignedContent[SequencedEvent[Env]]] = {
    // as deliverEvent implements MemoizedEvidence repeated calls to serialize will return the same bytes
    // so fine to call once for the hash here and then again when serializing to protobuf
    val hash = hashContent(cryptoApi, event)
    cryptoPrivateApi
      .sign(hash)
      .map(signature => SignedContent(event, signature, timestampOfSigningKey))
  }

  def hashContent(cryptoApi: CryptoPureApi, sequencedEvent: SequencedEvent[_]): Hash =
    cryptoApi.digest(HashPurpose.SequencedEventSignature, sequencedEvent.getCryptographicEvidence)

  def tryCreate[Env <: Envelope[_]](
      cryptoApi: CryptoPureApi,
      cryptoPrivateApi: SyncCryptoApi,
      event: SequencedEvent[Env],
      timestampOfSigningKey: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[SignedContent[SequencedEvent[Env]]] =
    create(cryptoApi, cryptoPrivateApi, event, timestampOfSigningKey)
      .fold(
        err => throw new IllegalStateException(s"Failed to create signed content: $err"),
        identity,
      )

  def fromProtoVersioned[A <: MemoizedEvidence](
      deserializer: ByteString => ParsingResult[A]
  )(signedValueP: VersionedSignedContent): ParsingResult[SignedContent[A]] =
    signedValueP.version match {
      case VersionedSignedContent.Version.Empty =>
        Left(FieldNotSet("VersionedSignedContent.version"))
      case VersionedSignedContent.Version.V0(content) => fromProtoV0(deserializer)(content)
    }

  def fromProtoV0[A <: MemoizedEvidence](
      deserializer: ByteString => ParsingResult[A]
  )(signedValueP: v0.SignedContent): ParsingResult[SignedContent[A]] =
    signedValueP match {
      case v0.SignedContent(content, maybeSignatureP, timestampOfSigningKey) =>
        for {
          contentB <- ProtoConverter.required("content", content)
          content <- deserializer(contentB)
          signature <- ProtoConverter.parseRequired(
            Signature.fromProtoV0,
            "signature",
            maybeSignatureP,
          )
          ts <- timestampOfSigningKey.traverse(CantonTimestamp.fromProtoPrimitive)
        } yield SignedContent(content, signature, ts)
    }

  implicit def prettySignedContent[A <: MemoizedEvidence](implicit
      prettyA: Pretty[A]
  ): Pretty[SignedContent[A]] = {
    import com.digitalasset.canton.logging.pretty.PrettyUtil._
    prettyOfClass(
      unnamedParam(_.content),
      param("signature", _.signature),
      paramIfDefined("timestamp of signing key", _.timestampOfSigningKey),
    )
  }
}
