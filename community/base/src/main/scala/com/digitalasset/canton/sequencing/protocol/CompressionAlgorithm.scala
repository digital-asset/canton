// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.UnrecognizedEnum
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.protocol.v30.CompressedBatch
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

sealed trait CompressionAlgorithm {
  def toProtoV30: v30.CompressedBatch.CompressionAlgorithm
}

object CompressionAlgorithm {

  case object Unspecified extends CompressionAlgorithm {
    override def toProtoV30: CompressedBatch.CompressionAlgorithm =
      v30.CompressedBatch.CompressionAlgorithm.COMPRESSION_ALGORITHM_UNSPECIFIED
  }

  case object GZIP extends CompressionAlgorithm {
    override def toProtoV30: CompressedBatch.CompressionAlgorithm =
      v30.CompressedBatch.CompressionAlgorithm.COMPRESSION_ALGORITHM_GZIP
  }

  def fromProtoV30(
      proto: v30.CompressedBatch.CompressionAlgorithm
  ): ParsingResult[CompressionAlgorithm] = proto match {
    case v30.CompressedBatch.CompressionAlgorithm.COMPRESSION_ALGORITHM_UNSPECIFIED =>
      Unspecified.asRight
    case v30.CompressedBatch.CompressionAlgorithm.COMPRESSION_ALGORITHM_GZIP => GZIP.asRight
    case v30.CompressedBatch.CompressionAlgorithm.Unrecognized(unrecognizedValue) =>
      UnrecognizedEnum(
        "index",
        unrecognizedValue.toString,
        Seq(
          v30.CompressedBatch.CompressionAlgorithm.COMPRESSION_ALGORITHM_UNSPECIFIED.index.toString,
          v30.CompressedBatch.CompressionAlgorithm.COMPRESSION_ALGORITHM_GZIP.index.toString,
        ),
      ).asLeft
  }
}
