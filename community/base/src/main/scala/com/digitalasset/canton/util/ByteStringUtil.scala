// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.Order
import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.serialization.{
  DefaultDeserializationError,
  DeserializationError,
  MaxByteToDecompressExceeded,
}
import com.github.luben.zstd.Zstd
import com.google.protobuf.ByteString
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream

import java.io.{
  ByteArrayOutputStream,
  EOFException,
  FilterInputStream,
  IOException,
  InputStream,
  OutputStream,
}
import java.util.zip.{GZIPOutputStream, ZipException}
import scala.annotation.tailrec

object ByteStringUtil {

  /** Lexicographic ordering on [[com.google.protobuf.ByteString]]s */
  val orderByteString: Order[ByteString] = new Order[ByteString] {
    override def compare(x: ByteString, y: ByteString): Int = {
      val iterX = x.iterator()
      val iterY = y.iterator()

      @tailrec def go(): Int =
        if (iterX.hasNext) {
          if (iterY.hasNext) {
            val cmp = iterX.next().compareTo(iterY.next())
            if (cmp == 0) go() else cmp
          } else 1
        } else if (iterY.hasNext) -1
        else 0

      go()
    }
  }

  val orderingByteString: Ordering[ByteString] = orderByteString.toOrdering

  /** Default zstd compression level (5), see analysis and outcomes in
    * [[https://github.com/DACH-NY/canton/issues/34653]]. Valid levels range from negative "fast"
    * levels up to 22: higher levels compress better but slower.
    */
  val DefaultZstdCompressionLevel: Int = 5

  def compress(
      bytes: ByteString,
      algo: CompressionAlgo,
      zstdLevel: Int = DefaultZstdCompressionLevel,
  ): ByteString =
    algo match {
      case CompressionAlgo.Gzip => compressGzip(bytes)
      case CompressionAlgo.Zstd => compressZstd(bytes, zstdLevel)
    }

  def compressGzip(bytes: ByteString): ByteString =
    compressWith(bytes, new GZIPOutputStream(_))

  def compressZstd(bytes: ByteString, level: Int = DefaultZstdCompressionLevel): ByteString =
    ByteString.copyFrom(Zstd.compress(bytes.toByteArray, level))

  private def compressWith(
      bytes: ByteString,
      compressor: OutputStream => OutputStream,
  ): ByteString = {
    val rawSize = bytes.size()
    val compressed = new ByteArrayOutputStream(rawSize)
    ResourceUtil.withResource(compressor(compressed)) { output =>
      bytes.writeTo(output)
    }
    ByteString.copyFrom(compressed.toByteArray)
  }

  /** We decompress maximum maxBytesLimit bytes, and if the input is larger we throw
    * MaxBytesToDecompressExceeded error.
    */
  def decompress(
      bytes: ByteString,
      algo: CompressionAlgo,
      maxBytesLimit: MaxBytesToDecompress,
  ): Either[DeserializationError, ByteString] =
    algo match {
      case CompressionAlgo.Gzip => decompressGzip(bytes, maxBytesLimit)
      case CompressionAlgo.Zstd => decompressZstd(bytes, maxBytesLimit)
    }

  def decompressGzip(
      bytes: ByteString,
      maxBytesLimit: MaxBytesToDecompress,
  ): Either[DeserializationError, ByteString] =
    // prefer GzipCompressorInputStream over GZIPInputStream, because it doesn't use exceptions for internal
    // control flow, as GZIPInputStream does.
    decompressCompressed(new GzipCompressorInputStream(bytes.newInput()), maxBytesLimit)

  /** Decompresses a zstd frame produced by [[compressZstd]]. All such frames declare the content
    * size in the frame header, so the limit is enforced before any allocation and the output buffer
    * is sized exactly. Frames without a declared content size (e.g. produced by streaming
    * compressors) are not supported and fail with a deserialization error unless empty.
    */
  def decompressZstd(
      bytes: ByteString,
      maxBytesLimit: MaxBytesToDecompress,
  ): Either[DeserializationError, ByteString] = {
    val limit = maxBytesLimit.limit.value
    val input = bytes.toByteArray
    Either
      .catchNonFatal {
        // Negative values signal an unknown content size (absent from the frame header); clamp
        // to 0 so that the decompress call below fails cleanly on non-empty content.
        val declaredSize = Zstd.getFrameContentSize(input).max(0L)
        if (declaredSize > limit.toLong)
          Left(maxBytesExceededError(limit))
        else
          Right(ByteString.copyFrom(Zstd.decompress(input, declaredSize.toInt)))
      }
      .leftMap(errorMapping)
      .flatten
  }

  private def maxBytesExceededError(limit: Int): MaxByteToDecompressExceeded =
    MaxByteToDecompressExceeded(
      s"Max bytes to decompress is exceeded. The limit is $limit bytes."
    )

  private def decompressCompressed(
      decompressor: => InputStream,
      maxBytesLimit: MaxBytesToDecompress,
  ): Either[DeserializationError, ByteString] =
    ResourceUtil
      .withResourceEither(decompressor) { decompressorStream =>
        val out = ByteString.newOutput()
        val buf = new Array[Byte](8 * 1024) // 8k is the default used by BufferedInputStream.
        if (copyNBuffered(maxBytesLimit.limit.value, buf, decompressorStream, out)) {
          Right(out.toByteString()) // No need to close as data is in-memory.
        } else {
          Left(maxBytesExceededError(maxBytesLimit.limit.value))
        }
      }
      .leftMap(errorMapping)
      .flatten

  /** Copies `n` bytes from in to out. Returns false if the input contained more than `n` bytes.
    *
    * Up to (n + 1) bytes may in fact be copied.
    */
  @tailrec
  def copyNBuffered(n: Int, buffer: Array[Byte], in: InputStream, out: OutputStream): Boolean =
    if (n < 0) false
    else {
      val readCount = (n + 1).max(1) // +1 to detect input exhaustion, max(1) to avoid int overflow
      in.read(buffer, 0, buffer.length.min(readCount)) match {
        case -1 => true
        case count =>
          out.write(buffer, 0, count)
          copyNBuffered(n - count, buffer, in, out)
      }
    }

  /** Wraps `in` so that reading strictly more than `maxBytes` total bytes throws an
    * [[java.io.IOException]]. Streaming is preserved: bytes are counted, not buffered. Suitable for
    * bounding the decompressed size of an untrusted stream without materializing it.
    */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def boundedInputStream(in: InputStream, maxBytes: Long): InputStream =
    new FilterInputStream(in) {
      // No need for AtomicLong since InputStreams are inherently single-threaded
      private var count = 0L
      private def check(read: Int): Unit =
        if (read > 0) {
          count += read
          if (count > maxBytes)
            throw new IOException(s"Decompressed size exceeds the limit of $maxBytes bytes")
        }
      override def read(): Int = {
        val b = super.read()
        check(if (b >= 0) 1 else 0)
        b
      }
      override def read(b: Array[Byte], off: Int, len: Int): Int = {
        val read = super.read(b, off, len)
        check(read)
        read
      }
    }

  /** Based on the final size we either truncate the bytes to fit in that size or pad with 0s
    */
  def padOrTruncate(bytes: ByteString, finalSize: NonNegativeInt): ByteString =
    if (finalSize == NonNegativeInt.zero)
      ByteString.EMPTY
    else {
      val padSize = finalSize.value - bytes.size()
      if (padSize > 0)
        bytes.concat(ByteString.copyFrom(new Array[Byte](padSize)))
      else if (padSize == 0) bytes
      else bytes.substring(0, bytes.size() + padSize)
    }

  private def errorMapping(err: Throwable): DeserializationError =
    err match {
      // all exceptions that were observed when testing these methods (see also `GzipCompressionTests`)
      case ex: ZipException => DefaultDeserializationError(ex.getMessage)
      case _: EOFException =>
        DefaultDeserializationError("Compressed byte input ended too early")
      case error =>
        DefaultDeserializationError(error.getMessage)
    }
}
