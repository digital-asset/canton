// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.Applicative
import cats.implicits._
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.ProtocolMessage
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.ByteStringUtil
import com.digitalasset.canton.version.{
  HasProtoV0WithVersion,
  HasVersionedMessageWithContextCompanion,
  HasVersionedWrapper,
  ProtocolVersion,
  UntypedVersionedMessage,
  VersionedMessage,
}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

/** A '''batch''' is a a list of `n` tuples `(m`,,i,,` , recipients`,,i,,),
  * where `m`,,i,, is a message, and
  *  `recipients`,,i,, is the list of recipients of m,,i,,,
  *  for `0 <= i < n`.
  */
case class Batch[+Env <: Envelope[_]] private (envelopes: List[Env])
    extends HasVersionedWrapper[VersionedMessage[Batch[Env]]]
    with HasProtoV0WithVersion[v0.CompressedBatch]
    with PrettyPrinting {

  /** builds a set of recipients from all messages in this message batch
    */
  lazy val allRecipients: Set[Member] = envelopes.flatMap { e =>
    e.recipients.allRecipients
  }.toSet

  /** builds a message batch containing only messages including the given recipient
    */
  def filterEnvelopesFor(recipient: Member): Batch[_] = {
    val forRecipient: List[Envelope[_]] = envelopes.mapFilter { env =>
      env.forRecipient(recipient)
    }
    Batch(forRecipient)
  }

  override def toProtoVersioned(version: ProtocolVersion): VersionedMessage[Batch[Env]] =
    VersionedMessage(toProtoV0(version).toByteString, 0)

  override def toProtoV0(version: ProtocolVersion): v0.CompressedBatch = {
    val batch = v0.Batch(envelopes = envelopes.map(_.toProtoV0(version)))
    val compressed = ByteStringUtil.compressGzip(batch.toByteString)
    v0.CompressedBatch(
      algorithm = v0.CompressedBatch.CompressionAlgorithm.Gzip,
      compressedBatch = compressed,
    )
  }

  def map[Env2 <: Envelope[_]](f: Env => Env2): Batch[Env2] = Batch(envelopes.map(f))

  def envelopesCount: Int = envelopes.size

  private[sequencing] def traverse[F[_], Env2 <: Envelope[_]](f: Env => F[Env2])(implicit
      F: Applicative[F]
  ): F[Batch[Env2]] =
    F.map(envelopes.traverse(f))(Batch(_))

  override def pretty: Pretty[Batch[Envelope[_]]] = prettyOfClass(unnamedParam(_.envelopes))
}

object Batch {
  def versionedProtoConverter[Env <: Envelope[_]](envelopeType: String) =
    new HasVersionedMessageWithContextCompanion[Batch[Env], v0.Envelope => ParsingResult[Env]] {
      override val name: String = s"Batch[$envelopeType]"

      val supportedProtoVersions: Map[Int, Parser] = Map(
        0 -> supportedProtoVersion(v0.CompressedBatch) { (deserializer, proto) =>
          fromProtoV0(deserializer)(proto)
        }
      )
    }

  private lazy val protoConverterBatchClosedEnvelopes =
    versionedProtoConverter[ClosedEnvelope]("ClosedEnvelope")

  def batchClosedEnvelopesFromByteString(bytes: ByteString): ParsingResult[Batch[ClosedEnvelope]] =
    ProtoConverter
      .protoParser(UntypedVersionedMessage.parseFrom)(bytes)
      .map(VersionedMessage.apply)
      .flatMap(batchClosedEnvelopesFromProtoVersioned(_))

  def batchClosedEnvelopesFromProtoVersioned(
      batchProto: VersionedMessage[Batch[ClosedEnvelope]]
  ): ParsingResult[Batch[ClosedEnvelope]] =
    protoConverterBatchClosedEnvelopes.fromProtoVersioned(ClosedEnvelope.fromProtoV0)(batchProto)

  def of[M <: ProtocolMessage](envs: (M, Recipients)*): Batch[OpenEnvelope[M]] = {
    val envelopes = envs.map { case (m, addresses) => OpenEnvelope[M](m, addresses) }.toList
    Batch[OpenEnvelope[M]](envelopes)
  }

  @VisibleForTesting
  def fromClosed(envelopes: ClosedEnvelope*): Batch[ClosedEnvelope] = Batch(envelopes.toList)

  def fromProtoV0[Env <: Envelope[_]](
      envelopeDeserializer: v0.Envelope => ParsingResult[Env]
  )(batchProto: v0.CompressedBatch): ParsingResult[Batch[Env]] = {
    val v0.CompressedBatch(algorithm, compressed) = batchProto

    for {
      // TODO(M40): Add safeguard against zip bombs
      uncompressed <- decompress(algorithm, compressed)
      uncompressedBatchProto <- ProtoConverter.protoParser(v0.Batch.parseFrom)(uncompressed)
      v0.Batch(envelopesProto) = uncompressedBatchProto
      res <- envelopesProto.toList
        .traverse(envelopeDeserializer)
        .map(Batch[Env])
    } yield res
  }

  private def decompress(
      algorithm: v0.CompressedBatch.CompressionAlgorithm,
      compressed: ByteString,
  ): Either[ProtoDeserializationError, ByteString] = {
    algorithm match {
      case v0.CompressedBatch.CompressionAlgorithm.None => Right(compressed)
      case v0.CompressedBatch.CompressionAlgorithm.Gzip =>
        ByteStringUtil
          .decompressGzip(compressed)
          .leftMap(err => ProtoDeserializationError.OtherError(err.toString))
      case _ => Left(FieldNotSet("CompressedBatch.Algorithm"))
    }
  }

  /** Constructs a batch with no envelopes */
  def empty[Env <: Envelope[_]]: Batch[Env] = Batch(List.empty[Env])

  def filterClosedEnvelopesFor(
      batch: Batch[ClosedEnvelope],
      member: Member,
  ): Batch[ClosedEnvelope] = {
    val newEnvs = batch.envelopes.mapFilter(e => e.forRecipient(member))
    Batch(newEnvs)
  }

  def filterOpenEnvelopesFor[T <: ProtocolMessage](
      batch: Batch[OpenEnvelope[T]],
      member: Member,
  ): Batch[OpenEnvelope[T]] = {
    val newEnvs = batch.envelopes.mapFilter(e => e.forRecipient(member))
    Batch(newEnvs)
  }

  def closeEnvelopes[T <: ProtocolMessage](batch: Batch[OpenEnvelope[T]]): Batch[ClosedEnvelope] = {
    val closedEnvs = batch.envelopes.map(env => env.closeEnvelope)
    Batch(closedEnvs)
  }
}
