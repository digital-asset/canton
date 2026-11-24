// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.data.*
import com.digitalasset.daml.lf.speedy.SValue.{SValue as _, *}
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class EVMBuiltinSpec
    extends AnyFreeSpec
    with Matchers
    with TableDrivenPropertyChecks
    with SuppressingLogging {

  val helpers = new SBuiltinTestHelpers
  import helpers.{parserParameters as _, *}

  implicit val parserParameters: ParserParameters[this.type] = ParserParameters.default

  "Uint256 operations" - {

    def u(v: Long): Uint256.Uint256 = Uint256.assertFromLong(v)

    val MaxHex = "ff" * 32
    val Max = Uint256.MaxValue

    val Zero = "0x0"
    val One = "0x1"
    val Two = "0x2"
    val Three = "0x3"
    val Ten = "0xa"
    val HexFF = "0xff"
    val HexMax = "0x" + MaxHex

    "Unsafe (modular) arithmetic" - {

      "ADD_UINT256" - {
        "computes modular addition" in {
          eval(e"ADD_UINT256 $Zero $Zero") shouldBe Right(SUint256(u(0)))
          eval(e"ADD_UINT256 $One $Two") shouldBe Right(SUint256(u(3)))
          eval(e"ADD_UINT256 $HexFF $One") shouldBe Right(SUint256(u(256)))
        }

        "wraps on overflow instead of throwing" in {
          eval(e"ADD_UINT256 $HexMax $One") shouldBe Right(SUint256(Uint256.ZERO))
          eval(e"ADD_UINT256 $HexMax $HexMax") shouldBe Right(
            SUint256(Uint256.sub(Uint256.MaxValue, Uint256.ONE))
          )
        }
      }

      "SUB_UINT256" - {
        "computes modular subtraction" in {
          eval(e"SUB_UINT256 $Three $Two") shouldBe Right(SUint256(u(1)))
          eval(e"SUB_UINT256 $HexFF $HexFF") shouldBe Right(SUint256(Uint256.ZERO))
        }

        "wraps on underflow" in {
          eval(e"SUB_UINT256 $Zero $One") shouldBe Right(SUint256(Max))
        }
      }

      "MUL_UINT256" - {
        "computes modular multiplication" in {
          eval(e"MUL_UINT256 $Three $Two") shouldBe Right(SUint256(u(6)))
          eval(e"MUL_UINT256 $HexFF $HexFF") shouldBe Right(SUint256(u(255 * 255)))
        }

        "wraps on overflow" in {
          eval(e"MUL_UINT256 $HexMax $Two") shouldBe Right(
            SUint256(Uint256.sub(Max, Uint256.ONE))
          )
        }
      }

      "DIV_UINT256" - {
        "computes integer division" in {
          eval(e"DIV_UINT256 $Ten $Three") shouldBe Right(SUint256(u(3)))
          eval(e"DIV_UINT256 $HexFF $One") shouldBe Right(SUint256(u(255)))
        }

        "returns ZERO on division by zero (EVM semantics)" in {
          eval(e"DIV_UINT256 $Ten $Zero") shouldBe Right(SUint256(Uint256.ZERO))
          eval(e"DIV_UINT256 $HexMax $Zero") shouldBe Right(SUint256(Uint256.ZERO))
        }
      }

      "MOD_UINT256" - {
        "computes modular remainder" in {
          eval(e"MOD_UINT256 $Ten $Three") shouldBe Right(SUint256(u(1)))
          eval(e"MOD_UINT256 $HexFF $Ten") shouldBe Right(SUint256(u(255 % 10)))
        }

        "returns ZERO on mod by zero (EVM semantics)" in {
          eval(e"MOD_UINT256 $Ten $Zero") shouldBe Right(SUint256(Uint256.ZERO))
        }
      }

      "EXP_UINT256" - {
        "computes modular exponentiation" in {
          eval(e"EXP_UINT256 $Two $Ten") shouldBe Right(SUint256(u(1024)))
          eval(e"EXP_UINT256 $Three $Three") shouldBe Right(SUint256(u(27)))
          eval(e"EXP_UINT256 $HexFF $Zero") shouldBe Right(SUint256(u(1)))
          eval(e"EXP_UINT256 $Zero $Zero") shouldBe Right(SUint256(u(1)))
        }

        "wraps on overflow" in {
          eval(e"EXP_UINT256 $Two 0x100") shouldBe Right(SUint256(Uint256.ZERO))
        }
      }
    }

    "Bitwise operations" - {

      "AND_UINT256" in {
        eval(e"AND_UINT256 $HexFF 0x0f") shouldBe Right(SUint256(u(0x0f)))
        eval(e"AND_UINT256 $HexMax $Zero") shouldBe Right(SUint256(Uint256.ZERO))
        eval(e"AND_UINT256 $HexMax $HexMax") shouldBe Right(SUint256(Max))
      }

      "OR_UINT256" in {
        eval(e"OR_UINT256 0xf0 0x0f") shouldBe Right(SUint256(u(0xff)))
        eval(e"OR_UINT256 $HexMax $Zero") shouldBe Right(SUint256(Max))
        eval(e"OR_UINT256 $Zero $Zero") shouldBe Right(SUint256(Uint256.ZERO))
      }

      "XOR_UINT256" in {
        eval(e"XOR_UINT256 $HexFF $HexFF") shouldBe Right(SUint256(Uint256.ZERO))
        eval(e"XOR_UINT256 $HexFF $Zero") shouldBe Right(SUint256(u(0xff)))
        eval(e"XOR_UINT256 0xf0 0x0f") shouldBe Right(SUint256(u(0xff)))
      }

      "NOT_UINT256" in {
        eval(e"NOT_UINT256 $Zero") shouldBe Right(SUint256(Max))
        eval(e"NOT_UINT256 $HexMax") shouldBe Right(SUint256(Uint256.ZERO))
      }

      "SHL_UINT256" in {
        eval(e"SHL_UINT256 $One $One") shouldBe Right(SUint256(u(2)))
        eval(e"SHL_UINT256 $One 0x8") shouldBe Right(SUint256(u(256)))
        eval(e"SHL_UINT256 $One 0x100") shouldBe Right(SUint256(Uint256.ZERO))
        eval(e"SHL_UINT256 $HexMax 0x100") shouldBe Right(SUint256(Uint256.ZERO))
      }

      "SHR_UINT256" in {
        eval(e"SHR_UINT256 0x100 $One") shouldBe Right(SUint256(u(128)))
        eval(e"SHR_UINT256 $HexFF $One") shouldBe Right(SUint256(u(127)))
        eval(e"SHR_UINT256 $HexMax 0x100") shouldBe Right(SUint256(Uint256.ZERO))
      }
    }

    "Conversions" - {

      "UINT256_TO_TEXT" in {
        eval(e"UINT256_TO_TEXT $Zero") shouldBe Right(SText("0x0"))
        eval(e"UINT256_TO_TEXT $One") shouldBe Right(SText("0x1"))
        eval(e"UINT256_TO_TEXT $HexFF") shouldBe Right(SText("0xff"))
        eval(e"UINT256_TO_TEXT $HexMax") shouldBe Right(SText("0x" + "f" * 64))
      }

      "TEXT_TO_UINT256" - {
        "succeeds for valid decimal strings" in {
          eval(e"""TEXT_TO_UINT256 "0" """) shouldBe Right(SOptional(Some(SUint256(Uint256.ZERO))))
          eval(e"""TEXT_TO_UINT256 "255" """) shouldBe Right(SOptional(Some(SUint256(u(255)))))
          eval(e"""TEXT_TO_UINT256 "1000" """) shouldBe Right(SOptional(Some(SUint256(u(1000)))))
        }

        "returns None for invalid strings" in {
          eval(e"""TEXT_TO_UINT256 "not_a_number" """) shouldBe Right(SOptional(None))
          eval(e"""TEXT_TO_UINT256 "-1" """) shouldBe Right(SOptional(None))
          eval(e"""TEXT_TO_UINT256 "ff" """) shouldBe Right(SOptional(None))
        }
      }

      "INT64_TO_UINT256" - {
        "succeeds for non-negative Int64" in {
          eval(e"INT64_TO_UINT256 0") shouldBe Right(SUint256(Uint256.ZERO))
          eval(e"INT64_TO_UINT256 1") shouldBe Right(SUint256(u(1)))
          eval(e"INT64_TO_UINT256 ${Long.MaxValue}") shouldBe Right(
            SUint256(u(Long.MaxValue))
          )
        }

        "throws for negative Int64" in {
          eval(e"INT64_TO_UINT256 -1") shouldBe a[Left[?, ?]]
          eval(e"INT64_TO_UINT256 ${Long.MinValue}") shouldBe a[Left[?, ?]]
        }
      }

      "UINT256_TO_INT64" - {
        "succeeds for values that fit in Int64" in {
          eval(e"UINT256_TO_INT64 $Zero") shouldBe Right(SInt64(0))
          eval(e"UINT256_TO_INT64 $One") shouldBe Right(SInt64(1))
          eval(e"UINT256_TO_INT64 $HexFF") shouldBe Right(SInt64(255))
        }

        "throws for values too large for Int64" in {
          eval(e"UINT256_TO_INT64 $HexMax") shouldBe a[Left[?, ?]]
        }
      }
    }

    "Comparisons (generic)" - {

      "LESS @Uint256" in {
        eval(e"LESS @Uint256 $Zero $One") shouldBe Right(SBool(true))
        eval(e"LESS @Uint256 $One $Zero") shouldBe Right(SBool(false))
        eval(e"LESS @Uint256 $One $One") shouldBe Right(SBool(false))
        eval(e"LESS @Uint256 $Zero $HexMax") shouldBe Right(SBool(true))
      }

      "LESS_EQ @Uint256" in {
        eval(e"LESS_EQ @Uint256 $Zero $One") shouldBe Right(SBool(true))
        eval(e"LESS_EQ @Uint256 $One $One") shouldBe Right(SBool(true))
        eval(e"LESS_EQ @Uint256 $One $Zero") shouldBe Right(SBool(false))
      }

      "GREATER @Uint256" in {
        eval(e"GREATER @Uint256 $One $Zero") shouldBe Right(SBool(true))
        eval(e"GREATER @Uint256 $Zero $One") shouldBe Right(SBool(false))
        eval(e"GREATER @Uint256 $One $One") shouldBe Right(SBool(false))
      }

      "GREATER_EQ @Uint256" in {
        eval(e"GREATER_EQ @Uint256 $One $Zero") shouldBe Right(SBool(true))
        eval(e"GREATER_EQ @Uint256 $One $One") shouldBe Right(SBool(true))
        eval(e"GREATER_EQ @Uint256 $Zero $One") shouldBe Right(SBool(false))
      }

      "EQUAL @Uint256" in {
        eval(e"EQUAL @Uint256 $Zero $Zero") shouldBe Right(SBool(true))
        eval(e"EQUAL @Uint256 $One $One") shouldBe Right(SBool(true))
        eval(e"EQUAL @Uint256 $Zero $One") shouldBe Right(SBool(false))
        eval(e"EQUAL @Uint256 $HexMax $HexMax") shouldBe Right(SBool(true))
      }
    }

    "EVM-specific operations" - {

      // -1 in two's complement = MAX_UINT256
      val Neg1 = HexMax

      "ADDMOD_UINT256" - {
        "computes (x + y) % n" in {
          eval(e"ADDMOD_UINT256 $Ten $Ten $Three") shouldBe Right(SUint256(u(20 % 3)))
          eval(e"ADDMOD_UINT256 $HexFF $One 0x100") shouldBe Right(SUint256(Uint256.ZERO))
        }
        "returns ZERO when n == 0" in {
          eval(e"ADDMOD_UINT256 $Ten $Ten $Zero") shouldBe Right(SUint256(Uint256.ZERO))
        }
        "intermediate sum can exceed 2^256" in {
          // MAX + 1 mod 2 = 0 (intermediate is 2^256 which exceeds 256 bits)
          eval(e"ADDMOD_UINT256 $HexMax $One $Two") shouldBe Right(SUint256(Uint256.ZERO))
        }
      }

      "MULMOD_UINT256" - {
        "computes (x * y) % n" in {
          eval(e"MULMOD_UINT256 $Ten $Ten $Three") shouldBe Right(SUint256(u(100 % 3)))
        }
        "returns ZERO when n == 0" in {
          eval(e"MULMOD_UINT256 $Ten $Ten $Zero") shouldBe Right(SUint256(Uint256.ZERO))
        }
        "intermediate product can exceed 2^256" in {
          // MAX * 2 mod MAX = 0
          eval(e"MULMOD_UINT256 $HexMax $Two $HexMax") shouldBe Right(SUint256(Uint256.ZERO))
        }
      }

      "SDIV_UINT256" - {
        "positive / positive" in {
          eval(e"SDIV_UINT256 $Ten $Three") shouldBe Right(SUint256(u(3)))
        }
        "negative / positive: -1 / 1 = -1 (MAX)" in {
          eval(e"SDIV_UINT256 $Neg1 $One") shouldBe Right(SUint256(Max))
        }
        "returns ZERO on division by zero" in {
          eval(e"SDIV_UINT256 $Ten $Zero") shouldBe Right(SUint256(Uint256.ZERO))
        }
      }

      "SMOD_UINT256" - {
        "positive % positive" in {
          eval(e"SMOD_UINT256 $Ten $Three") shouldBe Right(SUint256(u(1)))
        }
        "returns ZERO on mod by zero" in {
          eval(e"SMOD_UINT256 $Ten $Zero") shouldBe Right(SUint256(Uint256.ZERO))
        }
      }

      "SAR_UINT256" - {
        "positive value: same as SHR" in {
          eval(e"SAR_UINT256 $Two $One") shouldBe Right(SUint256(u(1)))
        }
        "negative value: fills with 1s" in {
          // -1 >> 1 = -1 (all bits set)
          eval(e"SAR_UINT256 $Neg1 $One") shouldBe Right(SUint256(Max))
        }
        "shift >= 256 on negative yields MAX" in {
          eval(e"SAR_UINT256 $Neg1 0x100") shouldBe Right(SUint256(Max))
        }
        "shift >= 256 on positive yields ZERO" in {
          eval(e"SAR_UINT256 $One 0x100") shouldBe Right(SUint256(Uint256.ZERO))
        }
      }

      "BYTE_UINT256" - {
        "extract least significant byte" in {
          eval(e"BYTE_UINT256 0x1f $HexFF") shouldBe Right(SUint256(u(0xff)))
        }
        "extract most significant byte" in {
          eval(e"BYTE_UINT256 $Zero $HexFF") shouldBe Right(SUint256(Uint256.ZERO))
        }
        "out of range returns ZERO" in {
          eval(e"BYTE_UINT256 0x20 $HexFF") shouldBe Right(SUint256(Uint256.ZERO))
        }
      }

      "SIGNEXTEND_UINT256" - {
        "extend from byte 0: 0x7f stays positive" in {
          eval(e"SIGNEXTEND_UINT256 $Zero 0x7f") shouldBe Right(SUint256(u(0x7f)))
        }
        "extend from byte 0: 0x80 becomes negative (all upper bits set)" in {
          // signExtend(0x80, 0) sets bits 8..255 to 1
          val result = eval(e"SIGNEXTEND_UINT256 $Zero 0x80")
          result shouldBe a[Right[?, ?]]
          // Check that SLT confirms it's negative
          eval(e"SLT_UINT256 $Neg1 $Zero") shouldBe Right(SBool(true))
        }
        "extend from byte 31 returns x unchanged" in {
          eval(e"SIGNEXTEND_UINT256 0x1f $HexFF") shouldBe Right(SUint256(u(0xff)))
        }
      }
    }

    // Hand-picked and edge-case vectors in `evm/evm_vectors.jsonl`, checked against a real EVM.
    // They use EVM argument order, which `evmOps` maps to the argument order of the builtins.
    // The Uint256 model is checked against the same vectors in `Uint256Spec`.
    "EVM test vectors" - {

      def hex(x: java.math.BigInteger): String = "0x" + x.toString(16)

      // Textual builtin application for an opcode, with its arity.
      final case class EvmOp(arity: Int, expr: Seq[String] => String, isBool: Boolean = false)

      def prefix(builtin: String, arity: Int): EvmOp =
        EvmOp(arity, args => (builtin +: args).mkString(" "))
      // EVM (shift, value) maps to the builtin (value, shift).
      def shift(builtin: String): EvmOp =
        EvmOp(2, args => s"$builtin ${args(1)} ${args(0)}")
      def predicate(builtin: String, arity: Int): EvmOp =
        EvmOp(arity, args => (builtin +: args).mkString(" "), isBool = true)

      val evmOps: Map[String, EvmOp] = Map(
        "NOT_UINT256" -> prefix("NOT_UINT256", 1),
        "ADD_UINT256" -> prefix("ADD_UINT256", 2),
        "SUB_UINT256" -> prefix("SUB_UINT256", 2),
        "MUL_UINT256" -> prefix("MUL_UINT256", 2),
        "DIV_UINT256" -> prefix("DIV_UINT256", 2),
        "SDIV_UINT256" -> prefix("SDIV_UINT256", 2),
        "MOD_UINT256" -> prefix("MOD_UINT256", 2),
        "SMOD_UINT256" -> prefix("SMOD_UINT256", 2),
        "EXP_UINT256" -> prefix("EXP_UINT256", 2),
        "SIGNEXTEND_UINT256" -> prefix("SIGNEXTEND_UINT256", 2),
        "AND_UINT256" -> prefix("AND_UINT256", 2),
        "OR_UINT256" -> prefix("OR_UINT256", 2),
        "XOR_UINT256" -> prefix("XOR_UINT256", 2),
        "BYTE_UINT256" -> prefix("BYTE_UINT256", 2),
        "SHL_UINT256" -> shift("SHL_UINT256"),
        "SHR_UINT256" -> shift("SHR_UINT256"),
        "SAR_UINT256" -> shift("SAR_UINT256"),
        "ADDMOD_UINT256" -> prefix("ADDMOD_UINT256", 3),
        "MULMOD_UINT256" -> prefix("MULMOD_UINT256", 3),
        "SLT_UINT256" -> predicate("SLT_UINT256", 2),
        "SGT_UINT256" -> predicate("SGT_UINT256", 2),
        // Equality and order use the polymorphic predicates instantiated at Uint256.
        "LT_UINT256" -> predicate("LESS @Uint256", 2),
        "GT_UINT256" -> predicate("GREATER @Uint256", 2),
        "EQ_UINT256" -> predicate("EQUAL @Uint256", 2),
        "ISZERO_UINT256" -> EvmOp(
          1,
          args => s"EQUAL @Uint256 ${args(0)} 0x0",
          isBool = true,
        ),
      )

      val vectorsByOpcode = EvmVectors.loadByOpcode()

      "evm_vectors.jsonl only contains known opcodes" in {
        (vectorsByOpcode.keySet -- evmOps.keySet) shouldBe empty
      }

      evmOps.foreach { case (opcode, op) =>
        opcode - {
          "matches the EVM test vectors" in {
            val vectors = vectorsByOpcode.getOrElse(opcode, Seq.empty)
            vectors should not be empty
            forEvery(Table("vector", vectors*)) { vector =>
              val args = vector.args
              withClue(vector.toString + ": ") {
                args should have length op.arity.toLong
                val result = eval(e"${op.expr(args.map(hex))}")
                val expected =
                  if (op.isBool) SBool(vector.expected != java.math.BigInteger.ZERO)
                  else SUint256(Uint256.assertFromBigInteger(vector.expected))
                result shouldBe Right(expected)
              }
            }
          }
        }
      }
    }
  }
}
