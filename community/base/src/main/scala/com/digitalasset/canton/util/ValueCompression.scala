// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.google.protobuf.ByteString

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

/** Transparent compression for small structured values (Daml-LF Value protobufs).
  *
  * Prepends a 1-byte flag to indicate whether the payload is compressed:
  *   0x00 = uncompressed (original bytes follow)
  *   0x01 = gzip compressed
  *   0x02 = zstd with dictionary (reserved for future use)
  *
  * Compression is only applied when it actually reduces size. For very small
  * values where compression overhead exceeds savings, the original bytes are
  * stored with the 0x00 flag.
  *
  * Design: the flag byte is cheap (1 byte overhead on every value) but enables
  * transparent migration — old uncompressed data can coexist with new compressed
  * data. Readers check the flag and decompress only when needed.
  *
  * For Daml-LF Value protobufs, which are structurally repetitive (same field
  * tags, same template shapes), gzip achieves ~30-50% compression on values
  * >100 bytes. For values <50 bytes, compression rarely wins, so the flag byte
  * is the only overhead.
  *
  * Future: zstd with a pre-trained dictionary (flag 0x02) can achieve 2-4x
  * compression even on small values by capturing the common protobuf structure
  * across all contracts of the same template.
  */
object ValueCompression {

  private val FLAG_UNCOMPRESSED: Byte = 0x00
  private val FLAG_GZIP: Byte = 0x01
  // private val FLAG_ZSTD_DICT: Byte = 0x02 // reserved

  /** Minimum payload size to attempt compression. Below this, gzip overhead
    * (10-byte header + 8-byte trailer) guarantees expansion.
    */
  private val MIN_COMPRESS_SIZE = 64

  /** Compress a value if compression reduces size. Returns flag-prefixed bytes. */
  def compress(value: ByteString): ByteString = {
    val raw = value.toByteArray
    if (raw.length < MIN_COMPRESS_SIZE) {
      // Too small — gzip will expand it
      return ByteString.copyFrom(Array(FLAG_UNCOMPRESSED) ++ raw)
    }

    val compressed = gzipCompress(raw)
    if (compressed.length < raw.length) {
      // Compression won — use it
      ByteString.copyFrom(Array(FLAG_GZIP) ++ compressed)
    } else {
      // Compression lost or tied — store uncompressed
      ByteString.copyFrom(Array(FLAG_UNCOMPRESSED) ++ raw)
    }
  }

  /** Decompress a flag-prefixed value. */
  def decompress(data: ByteString): Either[String, ByteString] = {
    val bytes = data.toByteArray
    if (bytes.isEmpty) return Left("Empty value")

    bytes(0) match {
      case FLAG_UNCOMPRESSED =>
        Right(ByteString.copyFrom(bytes, 1, bytes.length - 1))
      case FLAG_GZIP =>
        try {
          Right(ByteString.copyFrom(gzipDecompress(bytes, 1, bytes.length - 1)))
        } catch {
          case e: Exception => Left(s"Gzip decompression failed: ${e.getMessage}")
        }
      case flag =>
        Left(s"Unknown compression flag: $flag")
    }
  }

  /** Returns true if the data is already compressed (has a compression flag). */
  def isCompressed(data: ByteString): Boolean = {
    val bytes = data.toByteArray
    bytes.nonEmpty && bytes(0) != FLAG_UNCOMPRESSED
  }

  /** Returns true if the data starts with a recognized flag byte (0x00 or 0x01).
    * False means legacy uncompressed data (first byte is a protobuf field tag >= 0x08).
    */
  def hasFlagByte(data: ByteString): Boolean = {
    val bytes = data.toByteArray
    bytes.nonEmpty && (bytes(0) == FLAG_UNCOMPRESSED || bytes(0) == FLAG_GZIP)
  }

  /** Decompress if the data has a compression flag byte. If the first byte is
    * not a recognized flag (i.e., it's a valid protobuf field tag), return the
    * data as-is. This handles legacy uncompressed data transparently.
    *
    * Protobuf field tags are always >= 0x08 (field 1, wire type 0). Our flags
    * are 0x00 (uncompressed) and 0x01 (gzip), which never appear as the first
    * byte of valid protobuf.
    */
  def decompressIfFlagged(data: ByteString): ByteString = {
    val bytes = data.toByteArray
    if (bytes.isEmpty) return data

    bytes(0) match {
      case FLAG_UNCOMPRESSED =>
        ByteString.copyFrom(bytes, 1, bytes.length - 1)
      case FLAG_GZIP =>
        try {
          ByteString.copyFrom(gzipDecompress(bytes, 1, bytes.length - 1))
        } catch {
          case _: Exception =>
            // If decompression fails, the data might be legacy uncompressed
            // protobuf that happens to start with 0x01 (extremely unlikely but safe)
            data
        }
      case _ =>
        // No flag byte — legacy uncompressed data, return as-is
        data
    }
  }

  /** Compression ratio for monitoring. Returns compressed/original size. */
  def compressionRatio(original: ByteString, stored: ByteString): Double =
    if (original.isEmpty) 1.0
    else stored.size().toDouble / original.size().toDouble

  private def gzipCompress(data: Array[Byte]): Array[Byte] = {
    val baos = new ByteArrayOutputStream(data.length)
    val gzip = new GZIPOutputStream(baos)
    try {
      gzip.write(data)
      gzip.finish()
    } finally {
      gzip.close()
    }
    baos.toByteArray
  }

  private def gzipDecompress(data: Array[Byte], offset: Int, length: Int): Array[Byte] = {
    val bais = new ByteArrayInputStream(data, offset, length)
    val gzip = new GZIPInputStream(bais)
    try {
      val baos = new ByteArrayOutputStream(length * 2)
      val buf = new Array[Byte](4096)
      var n = gzip.read(buf)
      while (n > 0) {
        baos.write(buf, 0, n)
        n = gzip.read(buf)
      }
      baos.toByteArray
    } finally {
      gzip.close()
    }
  }
}
