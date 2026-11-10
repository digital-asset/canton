// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import org.scalatest
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import java.math.BigInteger
import scala.collection.immutable.ListMap

class Uint256Spec
    extends scalatest.wordspec.AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks {

  import Uint256Spec.evmOps

  // ---------------------------------------------------------------------------
  // Constants
  // ---------------------------------------------------------------------------

  "constants" should {
    "ZERO is 0" in {
      Uint256.ZERO shouldBe BigInteger.ZERO
    }
    "ONE is 1" in {
      Uint256.ONE shouldBe BigInteger.ONE
    }
    "MAX_UINT256 is 2^256 - 1" in {
      Uint256.MaxValue shouldBe BigInteger.TWO.pow(256).subtract(BigInteger.ONE)
    }
  }

  // ---------------------------------------------------------------------------
  // Construction / parsing
  // ---------------------------------------------------------------------------

  "fromBigInteger" should {
    "accept 0" in {
      Uint256.fromBigInteger(BigInteger.ZERO) shouldBe Right(Uint256.ZERO)
    }
    "accept MAX" in {
      Uint256.fromBigInteger(BigInteger.TWO.pow(256).subtract(BigInteger.ONE)) shouldBe Right(
        Uint256.MaxValue
      )
    }
    "reject negative" in {
      Uint256.fromBigInteger(BigInteger.ONE.negate) shouldBe a[Left[?, ?]]
    }
    "reject 2^256" in {
      Uint256.fromBigInteger(BigInteger.TWO.pow(256)) shouldBe a[Left[?, ?]]
    }
  }

  "fromLong" should {
    "accept 0" in {
      Uint256.fromLong(0L) shouldBe Right(Uint256.ZERO)
    }
    "reject negative" in {
      Uint256.fromLong(-1L) shouldBe a[Left[?, ?]]
    }
  }

  "fromString" should {
    "round-trip MAX" in {
      val s = BigInteger.TWO.pow(256).subtract(BigInteger.ONE).toString
      Uint256.fromString(s) shouldBe Right(Uint256.MaxValue)
    }
    "reject invalid" in {
      Uint256.fromString("not-a-number") shouldBe a[Left[?, ?]]
    }
    "reject negative string" in {
      Uint256.fromString("-1") shouldBe a[Left[?, ?]]
    }
  }

  // ---------------------------------------------------------------------------
  // Comparison
  // ---------------------------------------------------------------------------

  "compareTo" should {
    "ZERO < ONE" in {
      Uint256.compareTo(Uint256.ZERO, Uint256.ONE) shouldBe -1
    }
    "MAX > ZERO" in {
      Uint256.compareTo(Uint256.MaxValue, Uint256.ZERO) shouldBe 1
    }
    "reflexive" in {
      Uint256.compareTo(Uint256.MaxValue, Uint256.MaxValue) shouldBe 0
    }
  }

  // ---------------------------------------------------------------------------
  // Unsafe (modular) arithmetic
  // ---------------------------------------------------------------------------

  "add (unsafe/modular)" should {
    "0 + 0 == 0" in {
      Uint256.add(Uint256.ZERO, Uint256.ZERO) shouldBe Uint256.ZERO
    }
    "1 + 1 == 2" in {
      Uint256.add(Uint256.ONE, Uint256.ONE) shouldBe Uint256.assertFromLong(2L)
    }
    "MAX + 1 wraps to 0" in {
      Uint256.add(Uint256.MaxValue, Uint256.ONE) shouldBe Uint256.ZERO
    }
    "MAX + MAX wraps to MAX - 1" in {
      Uint256.add(Uint256.MaxValue, Uint256.MaxValue) shouldBe Uint256.sub(
        Uint256.MaxValue,
        Uint256.ONE,
      )
      Uint256.add(Uint256.MaxValue, Uint256.MaxValue) shouldBe Uint256.sub(
        Uint256.MaxValue,
        Uint256.ONE,
      )
    }
  }

  "sub (unsafe/modular)" should {
    "0 - 0 == 0" in {
      Uint256.sub(Uint256.ZERO, Uint256.ZERO) shouldBe Uint256.ZERO
    }
    "1 - 1 == 0" in {
      Uint256.sub(Uint256.ONE, Uint256.ONE) shouldBe Uint256.ZERO
    }
    "0 - 1 wraps to MAX" in {
      Uint256.sub(Uint256.ZERO, Uint256.ONE) shouldBe Uint256.MaxValue
    }
    "0 - MAX wraps to 1" in {
      Uint256.sub(Uint256.ZERO, Uint256.MaxValue) shouldBe Uint256.ONE
    }
  }

  "mul (unsafe/modular)" should {
    "0 * MAX == 0" in {
      Uint256.mul(Uint256.ZERO, Uint256.MaxValue) shouldBe Uint256.ZERO
    }
    "1 * MAX == MAX" in {
      Uint256.mul(Uint256.ONE, Uint256.MaxValue) shouldBe Uint256.MaxValue
    }
    "2 * MAX wraps to MAX - 1" in {
      // 2 * (2^256 -1) = 2^257 - 2 ≡ 2^256 - 2 mod 2^256 = MAX - 1
      Uint256.mul(Uint256.assertFromLong(2L), Uint256.MaxValue) shouldBe Uint256.sub(
        Uint256.MaxValue,
        Uint256.ONE,
      )
    }
  }

  "div (unsafe/EVM)" should {
    "anything / 0 == 0 (EVM rule)" in {
      Uint256.div(Uint256.MaxValue, Uint256.ZERO) shouldBe Uint256.ZERO
      Uint256.div(Uint256.ONE, Uint256.ZERO) shouldBe Uint256.ZERO
    }
    "0 / anything == 0" in {
      Uint256.div(Uint256.ZERO, Uint256.MaxValue) shouldBe Uint256.ZERO
    }
    "MAX / 1 == MAX" in {
      Uint256.div(Uint256.MaxValue, Uint256.ONE) shouldBe Uint256.MaxValue
    }
    "MAX / MAX == 1" in {
      Uint256.div(Uint256.MaxValue, Uint256.MaxValue) shouldBe Uint256.ONE
    }
  }

  "mod (unsafe/EVM)" should {
    "anything mod 0 == 0 (EVM rule)" in {
      Uint256.mod(Uint256.MaxValue, Uint256.ZERO) shouldBe Uint256.ZERO
    }
    "0 mod anything == 0" in {
      Uint256.mod(Uint256.ZERO, Uint256.MaxValue) shouldBe Uint256.ZERO
    }
    "MAX mod MAX == 0" in {
      Uint256.mod(Uint256.MaxValue, Uint256.MaxValue) shouldBe Uint256.ZERO
    }
    "3 mod 2 == 1" in {
      Uint256.mod(Uint256.assertFromLong(3L), Uint256.assertFromLong(2L)) shouldBe Uint256.ONE
    }
  }

  "exp (unsafe/modular)" should {
    "0 ^ 0 == 1 (convention)" in {
      Uint256.exp(Uint256.ZERO, Uint256.ZERO) shouldBe Uint256.ONE
    }
    "0 ^ 1 == 0" in {
      Uint256.exp(Uint256.ZERO, Uint256.ONE) shouldBe Uint256.ZERO
    }
    "MAX ^ 1 == MAX" in {
      Uint256.exp(Uint256.MaxValue, Uint256.ONE) shouldBe Uint256.MaxValue
    }
    "2 ^ 256 wraps to 0" in {
      Uint256.exp(Uint256.assertFromLong(2L), Uint256.assertFromLong(256L)) shouldBe Uint256.ZERO
    }
    "2 ^ 255 does not overflow" in {
      val half = Uint256.assertFromBigInteger(BigInteger.TWO.pow(255))
      Uint256.exp(Uint256.assertFromLong(2L), Uint256.assertFromLong(255L)) shouldBe half
    }
  }

  // ---------------------------------------------------------------------------
  // Bitwise
  // ---------------------------------------------------------------------------

  "and" should {
    "x & 0 == 0" in {
      Uint256.and(Uint256.MaxValue, Uint256.ZERO) shouldBe Uint256.ZERO
    }
    "x & MAX == x" in {
      Uint256.and(Uint256.MaxValue, Uint256.MaxValue) shouldBe Uint256.MaxValue
    }
  }

  "or" should {
    "x | 0 == x" in {
      Uint256.or(Uint256.MaxValue, Uint256.ZERO) shouldBe Uint256.MaxValue
    }
    "0 | 0 == 0" in {
      Uint256.or(Uint256.ZERO, Uint256.ZERO) shouldBe Uint256.ZERO
    }
  }

  "xor" should {
    "x ^ x == 0" in {
      Uint256.xor(Uint256.MaxValue, Uint256.MaxValue) shouldBe Uint256.ZERO
    }
    "x ^ 0 == x" in {
      Uint256.xor(Uint256.MaxValue, Uint256.ZERO) shouldBe Uint256.MaxValue
    }
  }

  "not (bitwise)" should {
    "~0 == MAX" in {
      Uint256.not(Uint256.ZERO) shouldBe Uint256.MaxValue
    }
    "~MAX == 0" in {
      Uint256.not(Uint256.MaxValue) shouldBe Uint256.ZERO
    }
    "~~x == x" in {
      Uint256.not(Uint256.not(Uint256.ONE)) shouldBe Uint256.ONE
    }
  }

  "shl" should {
    "1 << 0 == 1" in {
      Uint256.shl(Uint256.ONE, 0) shouldBe Uint256.ONE
    }
    "1 << 255 == 2^255" in {
      Uint256.shl(Uint256.ONE, 255) shouldBe Uint256.assertFromBigInteger(BigInteger.TWO.pow(255))
    }
    "1 << 256 == 0 (full shift)" in {
      Uint256.shl(Uint256.ONE, 256) shouldBe Uint256.ZERO
    }
    "MAX << 1 wraps (loses top bit)" in {
      Uint256.shl(Uint256.MaxValue, 1) shouldBe Uint256.sub(
        Uint256.MaxValue,
        Uint256.ONE,
      ) // MAX*2 mod 2^256 = MAX-1
    }
  }

  "shr" should {
    "1 >> 1 == 0" in {
      Uint256.shr(Uint256.ONE, 1) shouldBe Uint256.ZERO
    }
    "MAX >> 255 == 1" in {
      Uint256.shr(Uint256.MaxValue, 255) shouldBe Uint256.ONE
    }
    "MAX >> 256 == 0 (full shift)" in {
      Uint256.shr(Uint256.MaxValue, 256) shouldBe Uint256.ZERO
    }
    "0 >> anything == 0" in {
      Uint256.shr(Uint256.ZERO, 100) shouldBe Uint256.ZERO
    }
  }

  // ---------------------------------------------------------------------------
  // toString / toHexString
  // ---------------------------------------------------------------------------

  "toString" should {
    "render ZERO" in {
      Uint256.toString(Uint256.ZERO) shouldBe "0"
    }
    "render MAX" in {
      Uint256.toString(Uint256.MaxValue) shouldBe BigInteger.TWO
        .pow(256)
        .subtract(BigInteger.ONE)
        .toString
    }
  }

  "toHexString" should {
    "render ZERO" in {
      Uint256.toHexString(Uint256.ZERO) shouldBe "0"
    }
    "render MAX as all f's" in {
      Uint256.toHexString(Uint256.MaxValue) shouldBe ("f" * 64)
    }
  }

  // ---------------------------------------------------------------------------
  // toByteArray / fromByteArray
  // ---------------------------------------------------------------------------

  "toByteArray" should {
    def padded(tail: Byte*): Array[Byte] = Array.fill(32 - tail.length)(0.toByte) ++ tail

    "encode ZERO as 32 zero bytes" in {
      Uint256.toByteArray(Uint256.ZERO) shouldBe Array.fill(32)(0.toByte)
    }
    "encode ONE as 31 zero bytes then 0x01" in {
      Uint256.toByteArray(Uint256.ONE) shouldBe padded(1.toByte)
    }
    "encode 255 left-padded" in {
      Uint256.toByteArray(Uint256.assertFromLong(255L)) shouldBe padded(0xff.toByte)
    }
    "encode 256 left-padded" in {
      Uint256.toByteArray(Uint256.assertFromLong(256L)) shouldBe padded(1.toByte, 0.toByte)
    }
    "encode MAX as 32 bytes all 0xFF" in {
      Uint256.toByteArray(Uint256.MaxValue) shouldBe Array.fill(32)(0xff.toByte)
    }
    "drop the BigInteger sign byte" in {
      // 2^255 has the high bit set; BigInteger.toByteArray would add a sign byte
      val pow255 = Uint256.assertFromBigInteger(BigInteger.TWO.pow(255))
      val bytes = Uint256.toByteArray(pow255)
      bytes.length shouldBe 32
      bytes(0) shouldBe 0x80.toByte
    }
  }

  "fromByteArray" should {
    "decode 32 zero bytes as ZERO" in {
      Uint256.fromByteArray(Array.fill(32)(0.toByte)) shouldBe Right(Uint256.ZERO)
    }
    "decode 31 zero bytes then 0x01 as ONE" in {
      Uint256.fromByteArray(Array.fill(31)(0.toByte) :+ 1.toByte) shouldBe Right(Uint256.ONE)
    }
    "decode 32 x 0xFF as MAX" in {
      Uint256.fromByteArray(Array.fill(32)(0xff.toByte)) shouldBe Right(Uint256.MaxValue)
    }
    "reject any length other than 32" in {
      for (n <- Seq(0, 1, 31, 33))
        Uint256.fromByteArray(Array.fill(n)(0.toByte)) shouldBe a[Left[?, ?]]
    }
    "round-trip ZERO" in {
      Uint256.fromByteArray(Uint256.toByteArray(Uint256.ZERO)) shouldBe Right(Uint256.ZERO)
    }
    "round-trip ONE" in {
      Uint256.fromByteArray(Uint256.toByteArray(Uint256.ONE)) shouldBe Right(Uint256.ONE)
    }
    "round-trip MAX" in {
      Uint256.fromByteArray(Uint256.toByteArray(Uint256.MaxValue)) shouldBe Right(Uint256.MaxValue)
    }
    "round-trip 2^128" in {
      val v = Uint256.assertFromBigInteger(BigInteger.TWO.pow(128))
      Uint256.fromByteArray(Uint256.toByteArray(v)) shouldBe Right(v)
    }
  }

  // ---------------------------------------------------------------------------
  // toByteString / fromByteString  (protobuf ByteString)
  // ---------------------------------------------------------------------------

  "toByteString" should {
    "encode ZERO as 32-byte ByteString" in {
      Uint256.toByteString(Uint256.ZERO).size shouldBe 32
    }
    "encode MAX as 32-byte ByteString" in {
      Uint256.toByteString(Uint256.MaxValue).size shouldBe 32
    }
  }

  "fromByteString" should {
    "reject empty ByteString" in {
      Uint256.fromByteString(com.google.protobuf.ByteString.EMPTY) shouldBe a[Left[?, ?]]
    }
    "round-trip MAX via ByteString" in {
      Uint256.fromByteString(Uint256.toByteString(Uint256.MaxValue)) shouldBe Right(
        Uint256.MaxValue
      )
    }
    "round-trip 2^255 via ByteString" in {
      val v = Uint256.assertFromBigInteger(BigInteger.TWO.pow(255))
      Uint256.fromByteString(Uint256.toByteString(v)) shouldBe Right(v)
    }
  }

  // ---------------------------------------------------------------------------
  // toBytes / fromBytes  (daml-lf Bytes)
  // ---------------------------------------------------------------------------

  "toBytes" should {
    "encode ZERO as 32-byte Bytes" in {
      Uint256.toBytes(Uint256.ZERO).length shouldBe 32
    }
    "encode MAX as 32-byte Bytes" in {
      Uint256.toBytes(Uint256.MaxValue).length shouldBe 32
    }
  }

  "fromBytes" should {
    "reject empty Bytes" in {
      Uint256.fromBytes(Bytes.Empty) shouldBe a[Left[?, ?]]
    }
    "round-trip ONE via LfBytes" in {
      Uint256.fromBytes(Uint256.toBytes(Uint256.ONE)) shouldBe Right(Uint256.ONE)
    }
    "round-trip MAX via LfBytes" in {
      Uint256.fromBytes(Uint256.toBytes(Uint256.MaxValue)) shouldBe Right(Uint256.MaxValue)
    }
    "round-trip 42 via LfBytes" in {
      val v = Uint256.assertFromLong(42L)
      Uint256.fromBytes(Uint256.toBytes(v)) shouldBe Right(v)
    }
  }

  // ---------------------------------------------------------------------------
  // EVM ternary modular arithmetic
  // ---------------------------------------------------------------------------

  "addMod" should {
    "compute (x + y) % n" in {
      Uint256.addMod(
        Uint256.assertFromLong(10),
        Uint256.assertFromLong(10),
        Uint256.assertFromLong(3L),
      ) shouldBe Uint256.assertFromLong(20 % 3)
    }
    "return ZERO when n == 0" in {
      Uint256.addMod(
        Uint256.assertFromLong(10),
        Uint256.assertFromLong(10),
        Uint256.ZERO,
      ) shouldBe Uint256.ZERO
    }
    "handle intermediate sum exceeding 2^256" in {
      Uint256.addMod(
        Uint256.MaxValue,
        Uint256.ONE,
        Uint256.assertFromLong(2L),
      ) shouldBe Uint256.ZERO
    }
  }

  "mulMod" should {
    "compute (x * y) % n" in {
      Uint256.mulMod(
        Uint256.assertFromLong(10),
        Uint256.assertFromLong(10),
        Uint256.assertFromLong(3L),
      ) shouldBe Uint256.assertFromLong(100 % 3)
    }
    "return ZERO when n == 0" in {
      Uint256.mulMod(
        Uint256.assertFromLong(10),
        Uint256.assertFromLong(10),
        Uint256.ZERO,
      ) shouldBe Uint256.ZERO
    }
    "handle intermediate product exceeding 2^256" in {
      Uint256.mulMod(
        Uint256.MaxValue,
        Uint256.assertFromLong(2L),
        Uint256.MaxValue,
      ) shouldBe Uint256.ZERO
    }
  }

  // ---------------------------------------------------------------------------
  // EVM signed arithmetic
  // ---------------------------------------------------------------------------

  "sDiv" should {
    "divide positive by positive" in {
      Uint256.sDiv(Uint256.assertFromLong(10), Uint256.assertFromLong(3L)) shouldBe Uint256
        .assertFromLong(3)
    }
    "return ZERO on division by zero" in {
      Uint256.sDiv(Uint256.assertFromLong(10), Uint256.ZERO) shouldBe Uint256.ZERO
    }
    "handle negative / positive: -1 / 1 = -1" in {
      // -1 in two's complement is MAX_UINT256
      Uint256.sDiv(Uint256.MaxValue, Uint256.ONE) shouldBe Uint256.MaxValue
    }
    "handle negative / negative: -1 / -1 = 1" in {
      Uint256.sDiv(Uint256.MaxValue, Uint256.MaxValue) shouldBe Uint256.ONE
    }
  }

  "sMod" should {
    "compute positive mod positive" in {
      Uint256.sMod(Uint256.assertFromLong(10), Uint256.assertFromLong(3L)) shouldBe Uint256.ONE
    }
    "return ZERO on mod by zero" in {
      Uint256.sMod(Uint256.assertFromLong(10), Uint256.ZERO) shouldBe Uint256.ZERO
    }
  }

  // ---------------------------------------------------------------------------
  // EVM signed comparisons
  // ---------------------------------------------------------------------------

  "slt" should {
    "compare positive < positive" in {
      Uint256.slt(Uint256.ONE, Uint256.assertFromLong(2L)) shouldBe true
      Uint256.slt(Uint256.assertFromLong(2L), Uint256.ONE) shouldBe false
    }
    "negative < positive" in {
      Uint256.slt(Uint256.MaxValue, Uint256.ONE) shouldBe true // -1 < 1
    }
    "positive not < negative" in {
      Uint256.slt(Uint256.ONE, Uint256.MaxValue) shouldBe false // 1 not < -1
    }
  }

  "sgt" should {
    "compare positive > negative" in {
      Uint256.sgt(Uint256.ONE, Uint256.MaxValue) shouldBe true // 1 > -1
    }
    "negative not > positive" in {
      Uint256.sgt(Uint256.MaxValue, Uint256.ONE) shouldBe false // -1 not > 1
    }
  }

  // ---------------------------------------------------------------------------
  // EVM SAR
  // ---------------------------------------------------------------------------

  "sar" should {
    "shift positive right like logical shift" in {
      Uint256.sar(Uint256.assertFromLong(2L), 1) shouldBe Uint256.ONE
    }
    "shift negative right fills with 1s" in {
      Uint256.sar(Uint256.MaxValue, 1) shouldBe Uint256.MaxValue // -1 >> 1 = -1
    }
    "shift >= 256 on positive yields ZERO" in {
      Uint256.sar(Uint256.ONE, 256) shouldBe Uint256.ZERO
    }
    "shift >= 256 on negative yields MAX" in {
      Uint256.sar(Uint256.MaxValue, 256) shouldBe Uint256.MaxValue
    }
  }

  // ---------------------------------------------------------------------------
  // EVM BYTE
  // ---------------------------------------------------------------------------

  "byteAt" should {
    "extract least significant byte" in {
      Uint256.byteAt(Uint256.assertFromLong(0xff), 31) shouldBe Uint256.assertFromLong(0xff)
    }
    "extract most significant byte of small value" in {
      Uint256.byteAt(Uint256.assertFromLong(0xff), 0) shouldBe Uint256.ZERO
    }
    "return ZERO for out of range" in {
      Uint256.byteAt(Uint256.assertFromLong(0xff), 32) shouldBe Uint256.ZERO
    }
  }

  // ---------------------------------------------------------------------------
  // EVM SIGNEXTEND
  // ---------------------------------------------------------------------------

  "signExtend" should {
    "not change when extending from byte 31" in {
      Uint256.signExtend(Uint256.assertFromLong(0xff), 31) shouldBe Uint256.assertFromLong(0xff)
    }
    "extend positive byte 0 value unchanged" in {
      Uint256.signExtend(Uint256.assertFromLong(0x7f), 0) shouldBe Uint256.assertFromLong(0x7f)
    }
    "extend negative byte 0 value with upper 1s" in {
      val result = Uint256.signExtend(Uint256.assertFromLong(0x80), 0)
      // 0x80 sign-extended from byte 0 => all upper bits set
      Uint256.slt(result, Uint256.ZERO) shouldBe true
    }
  }
  // ---------------------------------------------------------------------------
  // EVM test vectors
  // ---------------------------------------------------------------------------
  // Hand-picked edge cases and regular cases in `evm/evm_vectors.jsonl`, checked against a real
  // EVM with the Foundry checker next to them (see the README there). They use EVM argument
  // order, which `evmOps` maps to the argument order of the model. The Speedy builtins are
  // checked against the same vectors in `EVMBuiltinSpec`.
  private val vectorsByOpcode = EvmVectors.loadByOpcode()
  "evm_vectors.jsonl" should {
    "only contain known opcodes" in {
      (vectorsByOpcode.keySet -- evmOps.keySet) shouldBe empty
    }
  }
  evmOps.foreach { case (opcode, op) =>
    s"$opcode" should {
      "match the EVM test vectors" in {
        val vectors = vectorsByOpcode.getOrElse(opcode, Seq.empty)
        vectors should not be empty
        forEvery(Table("vector", vectors*)) { vector =>
          val args = vector.args
          args should have length op.arity.toLong
          op.run(args) shouldBe vector.expected
        }
      }
    }
  }
}
object Uint256Spec {

  /** How to call the model for an EVM opcode, returning the result as an unsigned number.
    *
    * Shift amounts and byte positions are `Int` in the model. Arguments that do not fit an `Int`
    * are clamped, which keeps them out of range of every operation (the builtins do the same).
    */
  final case class Op(arity: Int, run: Seq[BigInteger] => BigInteger)
  private def u(x: BigInteger): Uint256 = Uint256.assertFromBigInteger(x)
  private def toInt(x: BigInteger): Int =
    if (x.bitLength < 32) x.intValue else Int.MaxValue
  private def bool(b: Boolean): BigInteger = if (b) BigInteger.ONE else BigInteger.ZERO
  private def unary(f: Uint256 => Uint256): Op =
    Op(1, args => f(u(args(0))))
  private def binary(f: (Uint256, Uint256) => Uint256): Op =
    Op(2, args => f(u(args(0)), u(args(1))))
  private def ternary(f: (Uint256, Uint256, Uint256) => Uint256): Op =
    Op(3, args => f(u(args(0)), u(args(1)), u(args(2))))
  val evmOps: ListMap[String, Op] = ListMap(
    "NOT_UINT256" -> unary(Uint256.not),
    "ADD_UINT256" -> binary(Uint256.add),
    "SUB_UINT256" -> binary(Uint256.sub),
    "MUL_UINT256" -> binary(Uint256.mul),
    "DIV_UINT256" -> binary(Uint256.div),
    "SDIV_UINT256" -> binary(Uint256.sDiv),
    "MOD_UINT256" -> binary(Uint256.mod),
    "SMOD_UINT256" -> binary(Uint256.sMod),
    "EXP_UINT256" -> binary(Uint256.exp),
    // EVM (index, value) maps to the model (value, index).
    "SIGNEXTEND_UINT256" -> Op(2, args => Uint256.signExtend(u(args(1)), toInt(args(0)))),
    "AND_UINT256" -> binary(Uint256.and),
    "OR_UINT256" -> binary(Uint256.or),
    "XOR_UINT256" -> binary(Uint256.xor),
    "BYTE_UINT256" -> Op(2, args => Uint256.byteAt(u(args(1)), toInt(args(0)))),
    // EVM (shift, value) maps to the model (value, shift).
    "SHL_UINT256" -> Op(2, args => Uint256.shl(u(args(1)), toInt(args(0)))),
    "SHR_UINT256" -> Op(2, args => Uint256.shr(u(args(1)), toInt(args(0)))),
    "SAR_UINT256" -> Op(2, args => Uint256.sar(u(args(1)), toInt(args(0)))),
    "SLT_UINT256" -> Op(2, args => bool(Uint256.slt(u(args(0)), u(args(1))))),
    "SGT_UINT256" -> Op(2, args => bool(Uint256.sgt(u(args(0)), u(args(1))))),
    "ADDMOD_UINT256" -> ternary(Uint256.addMod),
    "MULMOD_UINT256" -> ternary(Uint256.mulMod),
    "GT_UINT256" -> Op(2, args => bool(Uint256.compareTo(u(args(0)), u(args(1))) > 0)),
    "ISZERO_UINT256" -> Op(1, args => bool(Uint256.compareTo(u(args(0)), Uint256.ZERO) == 0)),
    "LT_UINT256" -> Op(2, args => bool(Uint256.compareTo(u(args(0)), u(args(1))) < 0)),
    "EQ_UINT256" -> Op(2, args => bool(Uint256.compareTo(u(args(0)), u(args(1))) == 0)),
  )
}
