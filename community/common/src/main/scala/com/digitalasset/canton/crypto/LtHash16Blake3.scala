// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.util.HexString
import com.google.protobuf.ByteString
import org.bouncycastle.crypto.digests.Blake3Digest

import java.nio.{ByteBuffer, ByteOrder, ShortBuffer}

/** A mutable digest of a set of bytes that supports the following operations:
  *   - Adding and removing elements
  *   - Computing the union with digest of a disjoint set of bytes
  *   - Removing the digest of a subset of bytes
  *
  * Note that it's the caller's responsibility to ensure that the collection defined by the sequence
  * of additions/removals/unions/removals is really a set. In particular:
  *   1. the digest accepts a call to [[remove]] before the corresponding call to [[add]] and
  *      similarly for [[union]] and [[removeAll]]
  *   1. the digest will change if the same element is added twice. Note, however, that the digest
  *      rolls over if you add an element 2^16 times; i.e., taking a digest d, then adding the same
  *      element 2^16 times results in d again.
  *
  * All set operations modify the current set. This class is not thread-safe, but thread-compatible.
  * It is the caller's responsibility to ensure proper synchronization.
  */
class LtHash16Blake3 private (private val buffer: Array[Byte]) {
  import LtHash16Blake3.*

  require(
    buffer.length == BYTE_LENGTH,
    s"Can't initialize LtHash16Blake3 from the given ${buffer.length} bytes",
  )

  def setBytes(bytes: ByteString): Unit = {
    require(
      bytes.size() == BYTE_LENGTH,
      s"Can't set LtHash16Blake3 from the given ${bytes.size()} bytes",
    )
    bytes.copyTo(buffer, 0)
  }

  private val shortBuffer: ShortBuffer = asShortBuffer(buffer)

  private def vectorOp(other: ShortBuffer, f: (Short, Short) => Int): Unit = {
    val sBuf = shortBuffer
    for (i <- 0 until VECTOR_LENGTH) {
      val newVal = f(sBuf.get(i), other.get(i))
      // Note that the potential loss of the highest bit due to conversion to short is intentional here, as this
      // gives us the desired semantics of addition modulo 2^16.
      sBuf.put(i, newVal.toShort).discard[ShortBuffer]
    }
  }

  private def hashInput(bytes: Array[Byte]): ShortBuffer = {
    val hash = Blake3Xof.digest(bytes, BYTE_LENGTH)
    asShortBuffer(hash)
  }

  /** Adds the given bytes to this digest. */
  def add(bytes: Array[Byte]): Unit =
    vectorOp(hashInput(bytes), _ + _)

  /** Removes the given bytes from this digest. */
  def remove(bytes: Array[Byte]): Unit =
    vectorOp(hashInput(bytes), _ - _)

  /** Adds all elements in the other digest to this digest. The caller must ensure that the other
    * digest's set of bytes is disjoint from this digest's set of bytes.
    */
  def union(other: LtHash16Blake3): Unit =
    vectorOp(other.shortBuffer, _ + _)

  /** Removes all elements in the other digest from this digest. The caller must ensure that the
    * other digest's set of bytes is a subset of this digest's set of bytes.
    */
  def removeAll(other: LtHash16Blake3): Unit =
    vectorOp(other.shortBuffer, _ - _)

  def getByteString: ByteString =
    ByteString.copyFrom(buffer)

  def isEmpty: Boolean =
    buffer.forall(_ == 0)

  def hexString(): String = HexString.toHexString(buffer)
}

object LtHash16Blake3 {
  private val VECTOR_LENGTH = 1024
  private val SIZEOF_SHORT = 2
  private val BYTE_LENGTH = VECTOR_LENGTH * SIZEOF_SHORT

  def empty: LtHash16Blake3 =
    new LtHash16Blake3(new Array[Byte](BYTE_LENGTH)) // initialized to zeros

  def tryCreate(bytes: Array[Byte]): LtHash16Blake3 = new LtHash16Blake3(bytes)

  def tryCreate(bytes: ByteString): LtHash16Blake3 = new LtHash16Blake3(bytes.toByteArray)

  private def asShortBuffer(bytes: Array[Byte]): ShortBuffer =
    ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer
}

private object Blake3Xof {
  def digest(bytes: Array[Byte], outputBytes: Int): Array[Byte] = {
    val digest = new Blake3Digest()
    digest.update(bytes, 0, bytes.length)
    val out = new Array[Byte](outputBytes)
    digest.doOutput(out, 0, outputBytes)
    out
  }
}
