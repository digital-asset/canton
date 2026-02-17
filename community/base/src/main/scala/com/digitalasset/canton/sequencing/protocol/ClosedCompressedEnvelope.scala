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
import com.digitalasset.canton.util.MaxBytesToDecompress
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

final case class ClosedCompressedEnvelope(
    override val bytes: ByteString,
    override val recipients: Recipients,
    algorithm: CompressionAlgorithm,
)(
    // Moved maxBytesToCompress to a separate argument group, so it doesn't affect "equals"
    maxBytesToDecompress: MaxBytesToDecompress
) extends ClosedEnvelope {
  // Internal cache in case we need to uncompress more than once
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
    decompressed <- Batch.decompress(
      algorithm = v30.CompressedBatch.CompressionAlgorithm.COMPRESSION_ALGORITHM_GZIP,
      compressed = bytes,
      maxRequestSize = maxBytesToDecompress,
    )
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
    ClosedCompressedEnvelope(bytes, newRecipients, algorithm)(maxBytesToDecompress)
}

object ClosedCompressedEnvelope {
  def create(bytes: ByteString, recipients: Recipients, algorithm: CompressionAlgorithm)(
      maxBytesToDecompress: MaxBytesToDecompress
  ): ClosedCompressedEnvelope =
    ClosedCompressedEnvelope(bytes, recipients, algorithm)(maxBytesToDecompress)
}
