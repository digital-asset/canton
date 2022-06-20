// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.serialization.DeserializationError
import com.google.protobuf.ByteString
import org.apache.commons.io.IOUtils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, EOFException}
import java.util.zip.{GZIPInputStream, GZIPOutputStream, ZipException}

object ByteArrayUtil {
  // See also the analogue methods in `ByteStringUtil`
  def compressGzip(bytes: ByteString): Array[Byte] = {
    val rawSize = bytes.size()
    val compressed = new ByteArrayOutputStream(rawSize)
    val gzipper = new GZIPOutputStream(compressed)
    bytes.writeTo(gzipper)
    gzipper.close()
    compressed.toByteArray
  }

  def decompressGzip(bytes: Array[Byte]): Either[DeserializationError, Array[Byte]] = {
    try {
      val gunzipper = new GZIPInputStream(new ByteArrayInputStream(bytes))
      val decompressed = IOUtils.toByteArray(gunzipper)
      gunzipper.close()
      Right(decompressed)
    } catch {
      // all exceptions that were observed when testing these methods (see also `GzipCompressionTests`)
      case ex: ZipException => Left(DeserializationError(ex.getMessage, ByteString.copyFrom(bytes)))
      case _: EOFException =>
        Left(
          DeserializationError("Compressed byte input ended too early", ByteString.copyFrom(bytes))
        )
    }
  }
}
