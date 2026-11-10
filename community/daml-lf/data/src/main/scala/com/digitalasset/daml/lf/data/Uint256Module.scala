// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import com.google.protobuf.ByteString

import java.math.BigInteger

/** The model of EVM-compatible unsigned 256-bit integers.
  *
  * Values are in the range [0, 2^256 − 1]. Two families of arithmetic are provided:
  *
  *   - '''Unsafe (modular)''' – every result is taken mod 2^256, matching EVM semantics. Division
  *     by zero yields ZERO (EVM behaviour).
  *
  *   - '''Safe''' – operations that would overflow / underflow throw an `ArithmeticException`.
  */
abstract class Uint256Module {

  /** Opaque type wrapping a non-negative `BigInteger` in [0, 2^256 − 1]. */
  type Uint256 <: BigInteger

  // ---------------------------------------------------------------------------
  // Constants
  // ---------------------------------------------------------------------------

  /** The additive identity, 0. */
  val ZERO: Uint256

  /** The multiplicative identity, 1. */
  val ONE: Uint256

  /** 2^256 used internally for modular reduction. */
  private[data] val MODULUS: BigInteger = BigInteger.TWO.pow(256)

  /** The maximum representable value, 2^256 − 1. */
  val MaxValue: Uint256

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  @inline
  private[data] def cast(x: BigInteger): Uint256

  /** Reduce an arbitrary `BigInteger` into [0, 2^256 − 1]. */
  private def modReduce(x: BigInteger): Uint256 = {
    val r = x.mod(MODULUS)
    cast(r)
  }

  /** Validate that `x` is in range; return a descriptive `Left` otherwise. */
  private[data] def checkInRange(x: BigInteger): Either[String, Uint256] =
    Either.cond(
      x.signum >= 0 && x.compareTo(MODULUS) < 0,
      cast(x),
      s"Out-of-bounds Uint256: $x",
    )

  // ---------------------------------------------------------------------------
  // Construction helpers
  // ---------------------------------------------------------------------------

  /** Build a `Uint256` from a `BigInteger`, returning `Left` on out-of-range. */
  final def fromBigInteger(x: BigInteger): Either[String, Uint256] =
    checkInRange(x)

  /** Like `fromBigInteger` but throws on error. */
  @throws[IllegalArgumentException]
  final def assertFromBigInteger(x: BigInteger): Uint256 =
    assertRight(fromBigInteger(x))

  /** Build a `Uint256` from a `Long`, returning `Left` if negative. */
  final def fromLong(x: Long): Either[String, Uint256] =
    fromBigInteger(BigInteger.valueOf(x))

  /** Like `fromLong` but throws on error. */
  @throws[IllegalArgumentException]
  final def assertFromLong(x: Long): Uint256 =
    assertRight(fromLong(x))

  /** Parse a non-negative decimal string into a `Uint256`. */
  final def fromString(s: String): Either[String, Uint256] =
    try {
      val bi = new BigInteger(s)
      checkInRange(bi)
    } catch {
      case _: NumberFormatException =>
        Left(s"""Could not read Uint256 string "$s"""")
    }

  /** Like `fromString` but throws on error. */
  @throws[IllegalArgumentException]
  final def assertFromString(s: String): Uint256 =
    assertRight(fromString(s))

  /** Canonical decimal string representation. */
  final def toString(x: Uint256): String = x.toString

  /** Hex string (no `0x` prefix) representation. */
  final def toHexString(x: Uint256): String = x.toString(16)

  // ---------------------------------------------------------------------------
  // Byte-array conversion (fixed 32 bytes, big-endian, unsigned)
  // ---------------------------------------------------------------------------

  /** Number of bytes of the canonical binary encoding. */
  final val ByteLength: Int = 32

  /** Serialise a `Uint256` to exactly 32 big-endian unsigned bytes, left-padded with zeros. */
  final def toByteArray(x: Uint256): Array[Byte] = {
    val arr = (x: BigInteger).toByteArray
    val result = new Array[Byte](ByteLength)
    // BigInteger.toByteArray may add a leading 0x00 sign byte when the highest bit is set.
    val len = math.min(arr.length, ByteLength)
    System.arraycopy(arr, arr.length - len, result, ByteLength - len, len)
    result
  }

  /** Deserialise exactly 32 big-endian unsigned bytes into a `Uint256`. */
  final def fromByteArray(bytes: Array[Byte]): Either[String, Uint256] =
    Either.cond(
      bytes.length == ByteLength,
      cast(new BigInteger(1, bytes)),
      s"Uint256 must be exactly $ByteLength bytes, got ${bytes.length}",
    )

  /** Like `fromByteArray` but throws on error. */
  @throws[IllegalArgumentException]
  final def assertFromByteArray(bytes: Array[Byte]): Uint256 =
    assertRight(fromByteArray(bytes))

  /** Serialise to a protobuf `ByteString` (exactly 32 bytes, big-endian, unsigned). */
  final def toByteString(x: Uint256): ByteString =
    ByteString.copyFrom(toByteArray(x))

  /** Deserialise from a protobuf `ByteString` (exactly 32 bytes, big-endian, unsigned). */
  final def fromByteString(bytes: ByteString): Either[String, Uint256] =
    fromByteArray(bytes.toByteArray)

  /** Like `fromByteString` but throws on error. */
  @throws[IllegalArgumentException]
  final def assertFromByteString(bytes: ByteString): Uint256 =
    assertRight(fromByteString(bytes))

  /** Serialise to the LF `Bytes` wrapper (exactly 32 bytes, big-endian, unsigned). */
  final def toBytes(x: Uint256): Bytes =
    Bytes.fromByteString(toByteString(x))

  /** Deserialise from the LF `Bytes` wrapper (exactly 32 bytes, big-endian, unsigned). */
  final def fromBytes(bytes: Bytes): Either[String, Uint256] =
    fromByteString(bytes.toByteString)

  /** Like `fromBytes` but throws on error. */
  @throws[IllegalArgumentException]
  final def assertFromBytes(bytes: Bytes): Uint256 =
    assertRight(fromBytes(bytes))

  // ---------------------------------------------------------------------------
  // Comparison
  // ---------------------------------------------------------------------------

  /** Returns -1, 0, or 1 as `x` is less than, equal to, or greater than `y`. */
  final def compareTo(x: Uint256, y: Uint256): Int =
    (x: BigInteger).compareTo(y)

  // ---------------------------------------------------------------------------
  // Unsafe (modular 2^256) arithmetic – EVM semantics
  // ---------------------------------------------------------------------------

  /** Modular addition: (x + y) mod 2^256. */
  final def add(x: Uint256, y: Uint256): Uint256 =
    modReduce((x: BigInteger).add(y))

  /** Modular subtraction: (x − y) mod 2^256 (wraps to large values). */
  final def sub(x: Uint256, y: Uint256): Uint256 =
    modReduce((x: BigInteger).subtract(y))

  /** Modular multiplication: (x × y) mod 2^256. */
  final def mul(x: Uint256, y: Uint256): Uint256 =
    modReduce((x: BigInteger).multiply(y))

  /** EVM-style unsigned division: x / y, yielding ZERO when y == 0. */
  final def div(x: Uint256, y: Uint256): Uint256 =
    if ((y: BigInteger).signum == 0) ZERO
    else cast((x: BigInteger).divide(y))

  /** EVM-style modulo: x mod y, yielding ZERO when y == 0. */
  final def mod(x: Uint256, y: Uint256): Uint256 =
    if ((y: BigInteger).signum == 0) ZERO
    else cast((x: BigInteger).mod(y))

  /** Modular exponentiation: (x ^ y) mod 2^256. Uses `BigInteger.modPow` for efficiency.
    */
  final def exp(x: Uint256, y: Uint256): Uint256 =
    cast((x: BigInteger).modPow(y, MODULUS))

  // ---------------------------------------------------------------------------
  // EVM ternary modular arithmetic
  // ---------------------------------------------------------------------------

  /** EVM ADDMOD: (x + y) % n, yielding ZERO when n == 0. The intermediate sum is not reduced mod
    * 2^256 (can exceed 256 bits).
    */
  final def addMod(x: Uint256, y: Uint256, n: Uint256): Uint256 =
    if ((n: BigInteger).signum == 0) ZERO
    else cast((x: BigInteger).add(y).mod(n))

  /** EVM MULMOD: (x * y) % n, yielding ZERO when n == 0. The intermediate product is not reduced
    * mod 2^256 (can exceed 256 bits).
    */
  final def mulMod(x: Uint256, y: Uint256, n: Uint256): Uint256 =
    if ((n: BigInteger).signum == 0) ZERO
    else cast((x: BigInteger).multiply(y).mod(n))

  // ---------------------------------------------------------------------------
  // EVM signed (two's complement) helpers
  // ---------------------------------------------------------------------------

  /** 2^255 – the boundary between positive and negative in two's complement. */
  private val SIGN_BIT: BigInteger = BigInteger.TWO.pow(255)

  /** Interpret an unsigned Uint256 as a signed two's complement integer. Values in [0, 2^255 − 1]
    * are non-negative; [2^255, 2^256 − 1] are negative.
    */
  private def toSigned(x: BigInteger): BigInteger =
    if (x.compareTo(SIGN_BIT) >= 0) x.subtract(MODULUS) else x

  /** Convert a signed BigInteger (in [-2^255, 2^255 − 1]) back to unsigned Uint256. */
  private def fromSigned(x: BigInteger): Uint256 =
    if (x.signum < 0) cast(x.add(MODULUS)) else cast(x)

  // ---------------------------------------------------------------------------
  // EVM signed arithmetic
  // ---------------------------------------------------------------------------

  /** EVM SDIV: signed division. If y == 0, yields ZERO. Special case: (-2^255) / (-1) = -2^255
    * (overflow wraps in two's complement).
    */
  final def sDiv(x: Uint256, y: Uint256): Uint256 = {
    if ((y: BigInteger).signum == 0) return ZERO
    val sx = toSigned(x)
    val sy = toSigned(y)
    val r = sx.divide(sy)
    fromSigned(r)
  }

  /** EVM SMOD: signed modulo with sign of dividend. If y == 0, yields ZERO. Result sign matches x:
    * sign(x) * (|x| mod |y|).
    */
  final def sMod(x: Uint256, y: Uint256): Uint256 = {
    if ((y: BigInteger).signum == 0) return ZERO
    val sx = toSigned(x)
    val sy = toSigned(y)
    val ax = sx.abs
    val ay = sy.abs
    val r = ax.mod(ay)
    if (sx.signum < 0) fromSigned(r.negate) else fromSigned(r)
  }
  // ---------------------------------------------------------------------------
  // EVM signed comparisons
  // ---------------------------------------------------------------------------

  /** EVM SLT: signed less-than. Interprets both operands as two's complement. */
  final def slt(x: Uint256, y: Uint256): Boolean =
    toSigned(x).compareTo(toSigned(y)) < 0

  /** EVM SGT: signed greater-than. Interprets both operands as two's complement. */
  final def sgt(x: Uint256, y: Uint256): Boolean =
    toSigned(x).compareTo(toSigned(y)) > 0

  // ---------------------------------------------------------------------------
  // EVM signed shift
  // ---------------------------------------------------------------------------

  /** EVM SAR: arithmetic (signed) shift right by `n` bits. If the value is negative (bit 255 set),
    * shifted-in bits are 1. If n >= 256, yields ZERO for non-negative, MAX_UINT256 for negative.
    */
  final def sar(x: Uint256, n: Int): Uint256 = {
    val sx = toSigned(x)
    if (n >= 256) {
      if (sx.signum >= 0) ZERO else MaxValue
    } else {
      fromSigned(sx.shiftRight(n))
    }
  }

  // ---------------------------------------------------------------------------
  // EVM byte/sign operations
  // ---------------------------------------------------------------------------

  /** EVM BYTE: extract the i-th byte from a 256-bit word (big-endian indexing). i == 0 is the most
    * significant byte, i == 31 is the least significant. Returns 0 if i >= 32.
    */
  final def byteAt(x: Uint256, i: Int): Uint256 = {
    if (i < 0 || i >= 32) return ZERO
    val shift = (31 - i) * 8
    cast((x: BigInteger).shiftRight(shift).and(BigInteger.valueOf(0xff)))
  }

  /** EVM SIGNEXTEND: sign-extend x from byte position b (0 = least significant). If b >= 31,
    * returns x unchanged. Otherwise, extends the sign from bit (b * 8 + 7) to fill the upper bits.
    */
  final def signExtend(x: Uint256, b: Int): Uint256 = {
    if (b >= 31) return x
    if (b < 0) return ZERO
    val bitPos = b * 8 + 7
    val testBit = (x: BigInteger).testBit(bitPos)
    if (testBit) {
      // Sign bit is 1: set all bits above bitPos to 1 (mask with upper 1s)
      val mask = MODULUS.subtract(BigInteger.ONE).shiftLeft(bitPos + 1).mod(MODULUS)
      cast((x: BigInteger).or(mask))
    } else {
      // Sign bit is 0: clear all bits above bitPos
      val mask = BigInteger.ONE.shiftLeft(bitPos + 1).subtract(BigInteger.ONE)
      cast((x: BigInteger).and(mask))
    }
  }

  // ---------------------------------------------------------------------------
  // Bitwise operations (all results are in [0, 2^256 − 1] by construction)
  // ---------------------------------------------------------------------------

  /** Bitwise AND. */
  final def and(x: Uint256, y: Uint256): Uint256 =
    cast((x: BigInteger).and(y))

  /** Bitwise OR. */
  final def or(x: Uint256, y: Uint256): Uint256 =
    cast((x: BigInteger).or(y))

  /** Bitwise XOR. */
  final def xor(x: Uint256, y: Uint256): Uint256 =
    cast((x: BigInteger).xor(y))

  /** Bitwise NOT, masked to 256 bits: ~x & (2^256 − 1). */
  final def not(x: Uint256): Uint256 =
    cast((x: BigInteger).xor(MaxValue))

  /** Shift left by `n` bits, mod 2^256. If n >= 256, yields ZERO. */
  final def shl(x: Uint256, n: Int): Uint256 =
    if (n >= 256) ZERO
    else modReduce((x: BigInteger).shiftLeft(n))

  /** Logical (unsigned) shift right by `n` bits. If n >= 256, yields ZERO. */
  final def shr(x: Uint256, n: Int): Uint256 =
    if (n >= 256) ZERO
    else cast((x: BigInteger).shiftRight(n))
}

object Uint256Module {

  implicit final class `Uint256 methods`(private val self: Uint256) extends AnyVal {
    def toDecimalString: String = Uint256.toString(self)
    def toHexString: String = Uint256.toHexString(self)
    def toByteArray: Array[Byte] = Uint256.toByteArray(self)
    def toByteString: ByteString = Uint256.toByteString(self)
    def toBytes: Bytes = Uint256.toBytes(self)
  }

}
