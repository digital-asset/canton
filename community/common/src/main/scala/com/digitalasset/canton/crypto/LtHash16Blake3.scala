// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.util.{HexString, Mutex}
import com.google.protobuf.ByteString
import org.bouncycastle.crypto.digests.Blake3Digest

import java.nio.{ByteBuffer, ByteOrder, ShortBuffer}
import scala.jdk.CollectionConverters.*

/** A running digest of a set of bytes, where elements can be added and removed.
  *
  * Note that it's the caller's responsibility to ensure that the collection defined by the sequence
  * of additions/removals is really a set. In particular:
  *   1. the digest accepts a call to [[remove]] before the corresponding call to [[add]]
  *   1. the digest will change if the same element is added twice. Note, however, that the digest
  *      rolls over if you add an element 2^16 times; i.e., taking a digest d, then adding the same
  *      element 2^16 times results in d again.
  */
class LtHash16Blake3 private (private val buffer: Array[Byte]) {
  import LtHash16Blake3.*

  require(
    buffer.length == BYTE_LENGTH,
    s"Can't initialize LtHash16Blake3 from the given ${buffer.length} bytes",
  )

  private def shortBuffer(bytes: Array[Byte]): ShortBuffer =
    ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer

  private val lock = new Mutex()

  private def blockSync[T]: T => T = x => lock.exclusive(x)

  private def performOp(bytes: Array[Byte], f: (Short, Short) => Int): Unit = blockSync {
    val hash = Blake3Xof.digest(bytes, BYTE_LENGTH)
    val hBuf = shortBuffer(hash)
    val iBuf = shortBuffer(buffer)
    for (i <- 0 until VECTOR_LENGTH) {
      val newVal = f(iBuf.get(i), hBuf.get(i))
      // Conversion to short intentionally keeps modulo 2^16 semantics.
      iBuf.put(i, newVal.toShort).discard[ShortBuffer]
    }
  }

  def add(bytes: Array[Byte]): Unit = performOp(bytes, _ + _)

  def remove(bytes: Array[Byte]): Unit = performOp(bytes, _ - _)

  def get(): Array[Byte] = blockSync {
    buffer.clone()
  }

  def getByteString(): ByteString = blockSync {
    ByteString.copyFrom(buffer)
  }

  def isEmpty: Boolean = blockSync {
    buffer.forall(_ == 0)
  }

  def hexString(): String = HexString.toHexString(buffer)
}

object LtHash16Blake3 {
  private val VECTOR_LENGTH = 1024
  private val SIZEOF_SHORT = 2
  private val BYTE_LENGTH = VECTOR_LENGTH * SIZEOF_SHORT

  def apply(): LtHash16Blake3 =
    new LtHash16Blake3(new Array[Byte](BYTE_LENGTH)) // initialized to zeros

  def tryCreate(bytes: Array[Byte]): LtHash16Blake3 = new LtHash16Blake3(bytes)

  def tryCreate(bytes: ByteString): LtHash16Blake3 = new LtHash16Blake3(bytes.toByteArray)

  def isNonEmptyCommitment(bytes: ByteString): Boolean =
    bytes.size() == BYTE_LENGTH && bytes.asScala.exists(_ != 0)
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
