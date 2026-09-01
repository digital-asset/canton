// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.UnrecognizedEnum
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.protocol.v30.CompressedBatch
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.CompressionAlgo

sealed trait CompressionAlgorithm extends Product with Serializable with PrettyPrinting {
  def toProtoV30: v30.CompressedBatch.CompressionAlgorithm
}

object CompressionAlgorithm {
  def apply(algo: com.digitalasset.canton.util.CompressionAlgo): CompressionAlgorithm =
    algo match {
      case CompressionAlgo.Gzip => GZIP
      case CompressionAlgo.Zstd => ZSTD
    }

  case object Unspecified extends CompressionAlgorithm {
    override def toProtoV30: CompressedBatch.CompressionAlgorithm =
      v30.CompressedBatch.CompressionAlgorithm.COMPRESSION_ALGORITHM_UNSPECIFIED

    override protected def pretty: Pretty[Unspecified.type] =
      prettyOfObject[Unspecified.type]
  }

  case object GZIP extends CompressionAlgorithm {
    override def toProtoV30: CompressedBatch.CompressionAlgorithm =
      v30.CompressedBatch.CompressionAlgorithm.COMPRESSION_ALGORITHM_GZIP

    override protected def pretty: Pretty[GZIP.type] = prettyOfObject[GZIP.type]
  }

  case object ZSTD extends CompressionAlgorithm {
    override def toProtoV30: CompressedBatch.CompressionAlgorithm = throw new IllegalStateException(
      "Cannot serialize zstd to v30.CompressionAlgorithm"
    )

    /** Indicates how to pretty print this instance. See `PrettyPrintingTest` for examples on how to
      * implement this method.
      */
    override protected def pretty: Pretty[ZSTD.type] = prettyOfObject[ZSTD.type]
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
