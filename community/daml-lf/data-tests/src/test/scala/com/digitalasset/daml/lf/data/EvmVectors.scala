// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import io.circe.Decoder
import io.circe.parser.decode

import java.math.BigInteger
import scala.io.{Codec, Source}
import scala.util.Using

/** Loader for the EVM test vectors in `evm/evm_vectors.jsonl`.
  *
  * The vectors are hand-picked edge cases and regular cases, checked against a real EVM with the
  * Foundry checker next to them (see the README in `src/test/resources/evm`). Arguments are in EVM
  * order. Shared by the Uint256 model tests and the Speedy builtin tests.
  */
object EvmVectors {

  private val VectorsResource = "evm/evm_vectors.jsonl"

  /** One line of the vector file. Arguments are in EVM order. */
  final case class TestVector(
      opcode: String,
      a: String,
      b: Option[String],
      c: Option[String],
      expected: String,
      note: Option[String],
  )

  private implicit val testVectorDecoder: Decoder[TestVector] =
    Decoder.forProduct6("opcode", "a", "b", "c", "expected", "note")(TestVector.apply)

  final case class EvmCase(line: Int, raw: TestVector) {
    private def rawArgs: Seq[String] = raw.a +: (raw.b.toList ++ raw.c.toList)

    def opcode: String = raw.opcode
    def args: Seq[BigInteger] = rawArgs.map(parseNumber)
    def expected: BigInteger = parseNumber(raw.expected)

    override def toString: String =
      s"line $line: ${raw.opcode}(${rawArgs.mkString(", ")}) = ${raw.expected}" +
        raw.note.fold("")(n => s" ($n)")
  }

  /** Parses a decimal or 0x-prefixed hex string. */
  def parseNumber(s: String): BigInteger =
    if (s.startsWith("0x")) new BigInteger(s.drop(2), 16) else new BigInteger(s)

  /** All vectors, grouped by opcode. */
  def loadByOpcode(): Map[String, Seq[EvmCase]] = load().groupBy(_.opcode)

  def load(): Seq[EvmCase] = {
    val stream = Option(getClass.getClassLoader.getResourceAsStream(VectorsResource))
      .getOrElse(throw new IllegalStateException(s"Missing test resource: $VectorsResource"))
    Using.resource(Source.fromInputStream(stream)(Codec.UTF8)) { source =>
      source
        .getLines()
        .zipWithIndex
        .map { case (line, index) =>
          decode[TestVector](line) match {
            case Right(raw) => EvmCase(index + 1, raw)
            case Left(error) =>
              throw new IllegalArgumentException(
                s"Failed to parse $VectorsResource line ${index + 1}: $line",
                error,
              )
          }
        }
        .toList
    }
  }
}
