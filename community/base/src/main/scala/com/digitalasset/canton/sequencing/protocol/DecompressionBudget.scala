// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.serialization.MaxByteToDecompressExceeded
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.{ByteStringUtil, MaxBytesToDecompress}
import com.google.protobuf.ByteString

import java.util.concurrent.atomic.AtomicLong

/** A budget bounding the total number of bytes produced by decompressing one or more blobs. Each
  * blob is decompressed via [[decompressGzip]], which draws the decompressed size from the budget;
  * once the cumulative size would exceed the initial
  * [[com.digitalasset.canton.util.MaxBytesToDecompress]] limit, further decompressions fail with
  * [[com.digitalasset.canton.serialization.MaxByteToDecompressExceeded]].
  */
final class DecompressionBudget(limit: MaxBytesToDecompress) {

  /** Wider than the limit: concurrent draws each subtract up to `Int.MaxValue`, so the counter can
    * dip below `Int.MinValue` before any of them lands.
    */
  private val remaining = new AtomicLong(limit.limit.value.toLong)

  /** Decompresses the blob, drawing its decompressed size from the remaining budget. Fails if the
    * cumulative decompressed size exceeds the initial limit.
    *
    * A failed draw still consumes the budget, so every later decompression on it fails too.
    */
  def decompressGzip(compressed: ByteString): ParsingResult[ByteString] =
    for {
      decompressed <- ByteStringUtil
        .decompressGzip(compressed, maxBytesLimit = remainingLimit)
        .leftMap(_.toProtoDeserializationError)
      _ <- take(decompressed.size).leftMap(_.toProtoDeserializationError)
    } yield decompressed

  /** The remaining budget, floored at 0.
    */
  private def remainingLimit: MaxBytesToDecompress =
    MaxBytesToDecompress(NonNegativeInt.tryCreate(remaining.get().max(0L).toInt))

  /** Draws `size` bytes from the remaining budget. Fails if the cumulative draw exceeds the initial
    * limit.
    */
  private def take(size: Int): Either[MaxByteToDecompressExceeded, Unit] = {
    val after = remaining.addAndGet(-size.toLong)
    Either.cond(
      after >= 0L,
      (),
      MaxByteToDecompressExceeded(
        s"Decompressing $size bytes exceeds the remaining budget of ${after + size} bytes (limit ${limit.limit.value})."
      ),
    )
  }
}

object DecompressionBudget {
  def apply(limit: MaxBytesToDecompress): DecompressionBudget =
    new DecompressionBudget(limit)
}
