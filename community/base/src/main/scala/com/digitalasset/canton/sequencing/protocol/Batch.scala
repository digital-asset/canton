// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.Applicative
import cats.implicits.*
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.ProtocolMessage
import com.digitalasset.canton.protocol.{SynchronizerLimits, v30, v31, v32}
import com.digitalasset.canton.sequencing.protocol.CompressionAlgorithm.ZSTD
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{MediatorId, Member}
import com.digitalasset.canton.util.ByteStringUtil
import com.digitalasset.canton.util.CompressionAlgo.{Gzip as AlgoGzip, Zstd as AlgoZstd}
import com.digitalasset.canton.validation.{ProtoUnvalidatedSeq, ProtoValidation}
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionValidation,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanionContext2,
}
import com.digitalasset.canton.{ProtoDeserializationError, checkedToByteString}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

/** Deserialization context for compressed batches, carrying the decompression policy and
  * synchronizer limits.
  */
final case class BatchDeserializationContext(
    decompressionPolicy: DecompressionPolicy,
    synchronizerLimits: SynchronizerLimits,
)

/** Common interface for a decompressed [[Batch]] and a still-compressed [[CompressedBatch]]. */
sealed trait GenBatch[+Env <: Envelope[?]] extends Product with Serializable with PrettyPrinting {
  private[protocol] def toProtoV31: v31.CompressedBatch
  private[protocol] def toProtoV32: v32.CompressedBatch
}

/** A '''batch''' is a list of `n` tuples `(m`,,i,,` , recipients`,,i,,), where `m`,,i,, is a
  * message, and `recipients`,,i,, is the list of recipients of m,,i,,, for `0 <= i < n`.
  */
final case class Batch[+Env <: Envelope[?]] private (envelopes: List[Env])(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[Batch.type]
) extends HasProtocolVersionedWrapper[Batch[Envelope[?]]]
    with GenBatch[Env]
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

  override private[protocol] def toProtoV31: v31.CompressedBatch = {
    val decompressedRecipients =
      v31.CompressedBatch.DecompressedRecipients(envelopes.map(_.recipients.toProtoV30))

    v31.CompressedBatch(
      algorithm = v30.CompressedBatch.CompressionAlgorithm.COMPRESSION_ALGORITHM_GZIP,
      compressedRecipients = ByteStringUtil.compressGzip(
        checkedToByteString(decompressedRecipients)
      ),
      compressedEnvelopes = envelopes.map(
        _.toClosedCompressedEnvelope(AlgoGzip).bytes
      ),
    )
  }

  override private[protocol] def toProtoV32: v32.CompressedBatch = {
    val decompressedRecipients =
      v32.CompressedBatch.DecompressedRecipients(envelopes.map(_.recipients.toProtoV30))

    v32.CompressedBatch(
      compressedRecipients = ByteStringUtil.compressZstd(
        checkedToByteString(decompressedRecipients)
      ),
      compressedEnvelopes = envelopes.map(
        _.toClosedCompressedEnvelope(AlgoZstd).bytes
      ),
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

/** A batch that has been received but not yet decompressed. It intentionally retains the original
  * wire proto so that decompression can be deferred until a [[DecompressionPolicy]], derived from
  * the topology snapshot at the event's timestamp, is available. Re-serialization simply hands the
  * retained proto back.
  */
final case class CompressedBatch(proto: ProtoBatch) extends GenBatch[Nothing] {

  override private[protocol] def toProtoV31: v31.CompressedBatch =
    proto match {
      case ProtoBatchV31(wrapped) => wrapped
      case ProtoBatchV32(_) =>
        throw new IllegalStateException("CompressedBatch v32 cannot be serialized as v31")
    }

  override private[protocol] def toProtoV32: v32.CompressedBatch = proto match {
    case ProtoBatchV31(_) =>
      throw new IllegalStateException("CompressedBatch v31 cannot be serialized as v32")
    case ProtoBatchV32(wrapped) => wrapped
  }

  def decompress(
      pvv: ProtocolVersionValidation,
      context: BatchDeserializationContext,
  ): ParsingResult[Batch[ClosedEnvelope]] = proto match {
    case ProtoBatchV31(wrapped) => Batch.fromProtoV31(pvv, context, wrapped)
    case ProtoBatchV32(wrapped) => Batch.fromProtoV32(pvv, context, wrapped)
  }

  override protected def pretty: Pretty[CompressedBatch.this.type] = prettyOfClass()
}

object Batch
    extends VersioningCompanionContext2[Batch[Envelope[?]], Batch[
      ClosedEnvelope
    ], BatchDeserializationContext] {

  override def name: String = "Batch"

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(31) -> VersionedProtoCodec(ProtocolVersion.v35)(v31.CompressedBatch)(
      supportedProtoVersionPVV(_)(Batch.fromProtoV31),
      _.toProtoV31,
    ),
    ProtoVersion(32) -> VersionedProtoCodec(ProtocolVersion.v36)(v32.CompressedBatch)(
      supportedProtoVersionPVV(_)(Batch.fromProtoV32),
      _.toProtoV32,
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

  private[protocol] def fromProtoV31(
      pvv: ProtocolVersionValidation,
      context: BatchDeserializationContext,
      batchProto: v31.CompressedBatch,
  ): ParsingResult[Batch[ClosedEnvelope]] = {
    val BatchDeserializationContext(decompressionPolicy, synchronizerLimits) = context
    val v31.CompressedBatch(protoAlgorithm, compressedRecipientsP, compressedEnvelopesP) =
      batchProto

    for {
      algorithm <- CompressionAlgorithm.fromProtoV30(protoAlgorithm)

      rpv <- protocolVersionRepresentativeFor(ProtoVersion(31))

      batch <- fromProtoV31V32(
        pvv = pvv,
        decompressionPolicy = decompressionPolicy,
        synchronizerLimits = synchronizerLimits,
        compressedRecipientsP = compressedRecipientsP,
        compressedEnvelopesP = compressedEnvelopesP,
        algorithm = algorithm,
        rpv = rpv,
      )
    } yield batch
  }

  private[protocol] def fromProtoV32(
      pvv: ProtocolVersionValidation,
      context: BatchDeserializationContext,
      batchProto: v32.CompressedBatch,
  ): ParsingResult[Batch[ClosedEnvelope]] = {
    val BatchDeserializationContext(decompressionPolicy, synchronizerLimits) = context
    val v32.CompressedBatch(compressedRecipientsP, compressedEnvelopesP) = batchProto

    for {
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(32))

      batch <- fromProtoV31V32(
        pvv = pvv,
        decompressionPolicy = decompressionPolicy,
        synchronizerLimits = synchronizerLimits,
        compressedRecipientsP = compressedRecipientsP,
        compressedEnvelopesP = compressedEnvelopesP,
        algorithm = ZSTD,
        rpv = rpv,
      )
    } yield batch
  }

  /** Generic behavior for v31 and v32
    */
  private[protocol] def fromProtoV31V32(
      pvv: ProtocolVersionValidation,
      decompressionPolicy: DecompressionPolicy,
      synchronizerLimits: SynchronizerLimits,
      compressedRecipientsP: ByteString,
      compressedEnvelopesP: ProtoUnvalidatedSeq[ByteString],
      algorithm: CompressionAlgorithm,
      rpv: RepresentativeProtocolVersion[this.type],
  ): ParsingResult[Batch[ClosedEnvelope]] = {

    val allocator = decompressionPolicy.newBatchAllocator()

    for {

      // The recipients blob is always bounded on its own.
      decompressedRecipientsBytes <- decompress(
        algorithm,
        compressedRecipientsP,
        DecompressionBudget(decompressionPolicy.limit),
      )
      decompressedRecipientsProto <- ProtoConverter.protoParser(
        v31.CompressedBatch.DecompressedRecipients.parseFrom
      )(
        decompressedRecipientsBytes
      )

      recipientsList <- ProtoValidation
        .validateLength(
          decompressedRecipientsProto.recipients,
          "recipients",
          pvv,
          synchronizerLimits.transactionProtocolLimits.maxRecipientsPerBatch.unwrap,
        )
        .flatMap(_.toList.traverse(Recipients.fromProtoV30(pvv, synchronizerLimits, _)))

      compressedEnvelopesSeq <- ProtoValidation.validateLength(
        compressedEnvelopesP,
        "compressed_envelopes",
        pvv,
        synchronizerLimits.transactionProtocolLimits.maxEnvelopes.unwrap,
      )

      envelopes <- Either.cond(
        recipientsList.lengthIs == compressedEnvelopesSeq.length,
        recipientsList.zip(compressedEnvelopesSeq).map { case (recipients, envelopes) =>
          ClosedCompressedEnvelope.create(
            envelopes,
            recipients,
            algorithm,
          )(allocator.nextEnvelopeBudget(), pvv)
        },
        InvariantViolation(
          None,
          "The number of recipients is different from the number of envelopes.",
        ),
      )

    } yield Batch[ClosedEnvelope](envelopes)(rpv)
  }

  private[protocol] def decompress(
      algorithm: CompressionAlgorithm,
      compressed: ByteString,
      decompressionBudget: DecompressionBudget,
  ): ParsingResult[ByteString] =
    algorithm match {
      case CompressionAlgorithm.Unspecified => Right(compressed)
      case CompressionAlgorithm.GZIP => decompressionBudget.decompressGzip(compressed)
      case CompressionAlgorithm.ZSTD => decompressionBudget.decompressZstd(compressed)
    }

  /** Rebinds the deferred decompression of the batch's envelopes to the given policy. */
  def withDecompressionPolicy(
      batch: Batch[ClosedEnvelope],
      decompressionPolicy: DecompressionPolicy,
  ): Batch[ClosedEnvelope] = {
    val allocator = decompressionPolicy.newBatchAllocator()
    Batch(batch.envelopes.map(_.withDecompressionBudget(allocator.nextEnvelopeBudget())))(
      batch.representativeProtocolVersion
    )
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
      synchronizerLimits: SynchronizerLimits,
  ): (Batch[OpenEnvelope[ProtocolMessage]], Seq[ProtoDeserializationError]) = {
    val (openingErrors, openEnvelopes) =
      batch.envelopes.map(_.toOpenEnvelope(hashOps, synchronizerLimits, protocolVersion)).separate

    (Batch(openEnvelopes)(batch.representativeProtocolVersion), openingErrors)
  }
}
