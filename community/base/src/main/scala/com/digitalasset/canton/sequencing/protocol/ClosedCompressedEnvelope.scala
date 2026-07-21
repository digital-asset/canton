// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.traverse.*
import com.digitalasset.canton.crypto.{HashOps, Signature}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.protocol.{v30, v31}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import monocle.Lens

final case class ClosedCompressedEnvelope(
    override val bytes: ByteString,
    override val recipients: Recipients,
    algorithm: CompressionAlgorithm,
)(
    // Moved the deferred decompression to a separate argument group, so it doesn't affect "equals"
    deferredDecompression: DeferredDecompression
) extends ClosedEnvelope {
  // Internal cache in case we need to uncompress more than once.
  private lazy val uncompressedEnvelopeResult: ParsingResult[ClosedUncompressedEnvelope] =
    prepareUncompressedEnvelopeResult

  override def toOpenEnvelope(
      hashOps: HashOps,
      protocolVersion: ProtocolVersion,
  ): ParsingResult[DefaultOpenEnvelope] =
    uncompressedEnvelopeResult.flatMap(_.toOpenEnvelope(hashOps, protocolVersion))

  override def toClosedUncompressedEnvelopeResult: ParsingResult[ClosedUncompressedEnvelope] =
    uncompressedEnvelopeResult

  override def toClosedCompressedEnvelope: ClosedCompressedEnvelope = this

  private def prepareUncompressedEnvelopeResult: ParsingResult[ClosedUncompressedEnvelope] = for {
    decompressed <- deferredDecompression.decompressed
    protoEnvelope <- ProtoConverter.protoParser(v31.EnvelopeWithoutRecipients.parseFrom)(
      decompressed
    )
    signatures <- protoEnvelope.signatures.traverse(Signature.fromProtoV30)
  } yield ClosedUncompressedEnvelope.create(
    protoEnvelope.content,
    recipients,
    signatures,
    ProtocolVersion.v34,
  )

  override def forRecipient(
      member: Member,
      groupAddresses: Set[GroupRecipient],
  ): Option[ClosedCompressedEnvelope] =
    recipients.forMember(member, groupAddresses).map(withRecipients)

  override protected def pretty: Pretty[ClosedCompressedEnvelope] = prettyOfClass(
    param("recipients", _.recipients)
  )

  @VisibleForTesting
  override def withRecipients(newRecipients: Recipients): ClosedCompressedEnvelope =
    // Share the deferred decompression, so that copies draw the budget at most once
    ClosedCompressedEnvelope(bytes, newRecipients, algorithm)(deferredDecompression)

  override private[protocol] def withDecompressionBudget(
      decompressionBudget: DecompressionBudget
  ): ClosedCompressedEnvelope =
    ClosedCompressedEnvelope(bytes, recipients, algorithm)(
      DeferredDecompression(bytes, decompressionBudget)
    )
}

object ClosedCompressedEnvelope {
  @VisibleForTesting
  val recipientsLens: Lens[ClosedCompressedEnvelope, Recipients] =
    Lens[ClosedCompressedEnvelope, Recipients](_.recipients)(newRecipients =>
      envelope => envelope.withRecipients(newRecipients)
    )

  def create(bytes: ByteString, recipients: Recipients, algorithm: CompressionAlgorithm)(
      decompressionBudget: DecompressionBudget
  ): ClosedCompressedEnvelope =
    ClosedCompressedEnvelope(bytes, recipients, algorithm)(
      DeferredDecompression(bytes, decompressionBudget)
    )
}

/** Deferred, memoized decompression of a [[ClosedCompressedEnvelope]] payload. Per-recipient copies
  * of an envelope share the same instance, so the payload is decompressed once for all of them.
  */
private[protocol] final class DeferredDecompression(
    bytes: ByteString,
    budget: DecompressionBudget,
) {
  lazy val decompressed: ParsingResult[ByteString] =
    Batch.decompress(
      algorithm = v30.CompressedBatch.CompressionAlgorithm.COMPRESSION_ALGORITHM_GZIP,
      compressed = bytes,
      decompressionBudget = budget,
    )
}

private[protocol] object DeferredDecompression {
  def apply(bytes: ByteString, budget: DecompressionBudget): DeferredDecompression =
    new DeferredDecompression(bytes, budget)
}
