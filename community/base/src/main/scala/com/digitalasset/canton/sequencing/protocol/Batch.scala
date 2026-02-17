// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.Applicative
import cats.implicits.*
import com.digitalasset.canton.ProtoDeserializationError.{FieldNotSet, InvariantViolation}
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.ProtocolMessage
import com.digitalasset.canton.protocol.{v30, v31}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{MediatorId, Member}
import com.digitalasset.canton.util.{ByteStringUtil, MaxBytesToDecompress}
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanionContext2,
}
import com.digitalasset.canton.{ProtoDeserializationError, checkedToByteString}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

/** A '''batch''' is a a list of `n` tuples `(m`,,i,,` , recipients`,,i,,), where `m`,,i,, is a
  * message, and `recipients`,,i,, is the list of recipients of m,,i,,, for `0 <= i < n`.
  */
final case class Batch[+Env <: Envelope[?]] private (envelopes: List[Env])(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[Batch.type]
) extends HasProtocolVersionedWrapper[Batch[Envelope[?]]]
    with PrettyPrinting {

  @transient override protected lazy val companionObj: Batch.type = Batch

  /** builds a set of recipients from all messages in this message batch
    */
  lazy val allMembers: Set[Member] = allRecipients.collect { case MemberRecipient(member) =>
    member
  }

  lazy val allRecipients: Set[Recipient] = envelopes.flatMap { e =>
    e.recipients.allRecipients
  }.toSet

  lazy val allMediatorRecipients: Set[Recipient] =
    allRecipients.collect {
      case r @ MemberRecipient(_: MediatorId) => r
      case r: MediatorGroupRecipient => r
      case AllMembersOfSynchronizer => AllMembersOfSynchronizer
    }

  lazy val isBroadcast: Boolean = allRecipients.contains(AllMembersOfSynchronizer)

  private[protocol] def toProtoV30: v30.CompressedBatch = {
    // We can call the unsafe method here, because for v30 the envelopes are not compressed
    val batch =
      v30.Batch(envelopes = envelopes.map(_.toClosedUncompressedEnvelopeUnsafe.toProtoV30))
    val uncompressed = checkedToByteString(batch)
    // TODO(i10428): if (uncompressed.size > maxBytesToDecompress) we should fail
    val compressed = ByteStringUtil.compressGzip(uncompressed)
    v30.CompressedBatch(
      algorithm = v30.CompressedBatch.CompressionAlgorithm.COMPRESSION_ALGORITHM_GZIP,
      compressedBatch = compressed,
    )
  }

  private[protocol] def toProtoV31: v31.CompressedBatch = {
    val decompressedRecipients =
      v31.CompressedBatch.DecompressedRecipients(envelopes.map(_.recipients.toProtoV30))

    v31.CompressedBatch(
      algorithm = v30.CompressedBatch.CompressionAlgorithm.COMPRESSION_ALGORITHM_GZIP,
      compressedRecipients = ByteStringUtil.compressGzip(
        checkedToByteString(decompressedRecipients)
      ),
      compressedEnvelopes = envelopes.map(_.toClosedCompressedEnvelope.bytes),
    )
  }

  def map[Env2 <: Envelope[?]](f: Env => Env2): Batch[Env2] =
    Batch(envelopes.map(f))(representativeProtocolVersion)

  def copy[Env2 <: Envelope[?]](envelopes: List[Env2]): Batch[Env2] =
    Batch(envelopes)(representativeProtocolVersion)

  def envelopesCount: Int = envelopes.size

  private[sequencing] def traverse[F[_], Env2 <: Envelope[?]](f: Env => F[Env2])(implicit
      F: Applicative[F]
  ): F[Batch[Env2]] =
    F.map(envelopes.traverse(f))(Batch(_)(representativeProtocolVersion))

  override protected def pretty: Pretty[Batch[Envelope[?]]] = prettyOfClass(
    unnamedParam(_.envelopes)
  )

  def toClosedUncompressedBatchResult: ParsingResult[Batch[ClosedUncompressedEnvelope]] = for {
    uncompressedEnvelopes <- envelopes.traverse(_.toClosedUncompressedEnvelopeResult)
  } yield Batch(uncompressedEnvelopes)(representativeProtocolVersion)
}

object Batch
    extends VersioningCompanionContext2[Batch[Envelope[?]], Batch[
      ClosedEnvelope
    ], MaxBytesToDecompress] {

  override def name: String = "Batch"

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(
      ProtocolVersion.v34
    )(v30.CompressedBatch)(
      supportedProtoVersion(_)(Batch.fromProtoV30),
      _.toProtoV30,
    ),
    ProtoVersion(31) -> VersionedProtoCodec(ProtocolVersion.v35)(v31.CompressedBatch)(
      supportedProtoVersion(_)(Batch.fromProtoV31),
      _.toProtoV31,
    ),
  )

  def apply[Env <: Envelope[?]](
      envelopes: List[Env],
      protocolVersion: ProtocolVersion,
  ): Batch[Env] = Batch(envelopes)(protocolVersionRepresentativeFor(protocolVersion))

  def of[M <: ProtocolMessage](
      protocolVersion: ProtocolVersion,
      envs: (M, Recipients)*
  ): Batch[OpenEnvelope[M]] = {
    val envelopes = envs.map { case (m, recipients) =>
      OpenEnvelope[M](m, recipients)(protocolVersion)
    }.toList
    Batch[OpenEnvelope[M]](envelopes)(protocolVersionRepresentativeFor(protocolVersion))
  }

  @VisibleForTesting def fromClosed(
      protocolVersion: ProtocolVersion,
      envelopes: ClosedUncompressedEnvelope*
  ): Batch[ClosedUncompressedEnvelope] =
    Batch(envelopes.toList)(protocolVersionRepresentativeFor(protocolVersion))

  private[protocol] def fromProtoV30(
      maxBytesToDecompress: MaxBytesToDecompress,
      batchProto: v30.CompressedBatch,
  ): ParsingResult[Batch[ClosedEnvelope]] = {
    val v30.CompressedBatch(algorithm, compressed) = batchProto
    for {
      uncompressed <- decompress(algorithm, compressed, maxBytesToDecompress)
      uncompressedBatchProto <- ProtoConverter.protoParser(v30.Batch.parseFrom)(uncompressed)
      v30.Batch(envelopesProto) = uncompressedBatchProto
      envelopes <- envelopesProto.toList.traverse(ClosedUncompressedEnvelope.fromProtoV30)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield Batch[ClosedEnvelope](envelopes)(rpv)
  }

  private[protocol] def fromProtoV31(
      maxBytesToDecompress: MaxBytesToDecompress,
      batchProto: v31.CompressedBatch,
  ): ParsingResult[Batch[ClosedEnvelope]] = {
    val v31.CompressedBatch(protoAlgorithm, compressedRecipients, compressedEnvelopes) = batchProto

    for {
      decompressedRecipientsBytes <- decompress(
        protoAlgorithm,
        compressedRecipients,
        maxBytesToDecompress,
      )
      decompressedRecipientsProto <- ProtoConverter.protoParser(
        v31.CompressedBatch.DecompressedRecipients.parseFrom
      )(
        decompressedRecipientsBytes
      )

      recipientsList <- decompressedRecipientsProto.recipients.toList.traverse(
        Recipients.fromProtoV30
      )
      algorithm <- CompressionAlgorithm.fromProtoV30(protoAlgorithm)

      envelopes <- Either.cond(
        recipientsList.lengthIs == compressedEnvelopes.length,
        recipientsList.zip(compressedEnvelopes).map { case (recipients, envelopes) =>
          ClosedCompressedEnvelope.create(
            envelopes,
            recipients,
            algorithm,
          )(maxBytesToDecompress)
        },
        InvariantViolation(
          None,
          "The number of recipients is different from the number of envelopes.",
        ),
      )

      rpv <- protocolVersionRepresentativeFor(ProtoVersion(31))
    } yield Batch[ClosedEnvelope](envelopes)(rpv)
  }

  private[protocol] def decompress(
      algorithm: v30.CompressedBatch.CompressionAlgorithm,
      compressed: ByteString,
      maxRequestSize: MaxBytesToDecompress,
  ): ParsingResult[ByteString] =
    algorithm match {
      case v30.CompressedBatch.CompressionAlgorithm.COMPRESSION_ALGORITHM_UNSPECIFIED =>
        Right(compressed)
      case v30.CompressedBatch.CompressionAlgorithm.COMPRESSION_ALGORITHM_GZIP =>
        ByteStringUtil
          .decompressGzip(compressed, maxBytesLimit = maxRequestSize)
          .leftMap(_.toProtoDeserializationError)
      case _ => Left(FieldNotSet("CompressedBatch.Algorithm"))
    }

  /** Constructs a batch with no envelopes */
  def empty[Env <: Envelope[?]](protocolVersion: ProtocolVersion): Batch[Env] =
    Batch(List.empty[Env])(protocolVersionRepresentativeFor(protocolVersion))

  def filterClosedEnvelopesFor(
      batch: Batch[ClosedEnvelope],
      member: Member,
      groupRecipients: Set[GroupRecipient],
  ): Batch[ClosedEnvelope] = {
    val newEnvs = batch.envelopes.mapFilter(e => e.forRecipient(member, groupRecipients))
    Batch(newEnvs)(batch.representativeProtocolVersion)
  }

  /** Drops everything definitely NOT addressed to the given member
    * @return
    */
  def trimForMember(
      batch: Batch[ClosedEnvelope],
      member: Member,
  ): Batch[ClosedEnvelope] = {
    val newEnvs = batch.envelopes.mapFilter(e =>
      Option.when(e.recipients.allRecipients.exists {
        case MemberRecipient(member_) if member_ == member => true
        case _: GroupRecipient => true
        case _: MemberRecipient => false
      })(e)
    )
    Batch(newEnvs)(batch.representativeProtocolVersion)
  }

  def filterOpenEnvelopesFor[T <: ProtocolMessage](
      batch: Batch[OpenEnvelope[T]],
      member: Member,
      groupRecipients: Set[GroupRecipient],
  ): Batch[OpenEnvelope[T]] = {
    val newEnvs = batch.envelopes.mapFilter(e => e.forRecipient(member, groupRecipients))
    Batch(newEnvs)(batch.representativeProtocolVersion)
  }

  def closeEnvelopes[T <: ProtocolMessage](
      batch: Batch[OpenEnvelope[T]]
  ): Batch[ClosedUncompressedEnvelope] = {
    val closedEnvs = batch.envelopes.map(env => env.toClosedUncompressedEnvelope)
    Batch(closedEnvs)(batch.representativeProtocolVersion)
  }

  def openEnvelopes(batch: Batch[ClosedEnvelope])(
      protocolVersion: ProtocolVersion,
      hashOps: HashOps,
  ): (Batch[OpenEnvelope[ProtocolMessage]], Seq[ProtoDeserializationError]) = {
    val (openingErrors, openEnvelopes) =
      batch.envelopes.map(_.toOpenEnvelope(hashOps, protocolVersion)).separate

    (Batch(openEnvelopes)(batch.representativeProtocolVersion), openingErrors)
  }
}
