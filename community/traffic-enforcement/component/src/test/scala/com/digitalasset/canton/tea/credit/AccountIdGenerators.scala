// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.credit

import org.scalacheck.{Gen, Shrink}

/** Generators for TEA account ids as specified by `AccountId` in `Canton.Traffic.Credit`: at most
  * 255 characters (code points), consisting of one or more non-empty `/`-separated segments over
  * `[A-Za-z0-9@^$.!`\-#+'~_|:() ]`.
  *
  * Each invalid generator violates exactly one rule of the spec.
  */
object AccountIdGenerators {

  val MaxLength: Int = 255

  val SegmentChars: Seq[Char] =
    ('A' to 'Z') ++ ('a' to 'z') ++ ('0' to '9') ++ "@^$.!`-#+'~_|:() "

  val segmentChar: Gen[Char] = Gen.oneOf(SegmentChars)

  /** Any code point allowed in Daml `Text`: Unicode scalar values (no surrogates) except NUL. */
  val codePoint: Gen[Int] = Gen.oneOf(Gen.choose(1, 0xd7ff), Gen.choose(0xe000, 0x10ffff))

  /** A single code point that is neither a segment character nor `/`. */
  val forbiddenChar: Gen[String] = Gen.oneOf(
    // U+10000 is the first code point outside the BMP
    Gen.oneOf(",", "\\", "\"", "\t", "\n", "é", new String(Character.toChars(0x10000))),
    codePoint
      .suchThat(cp => cp != '/' && !SegmentChars.exists(_.toInt == cp))
      .map(Character.toString),
  )

  private def segment(maxLength: Int): Gen[String] =
    Gen.choose(1, maxLength).flatMap(Gen.stringOfN(_, segmentChar))

  /** A valid account id of 1 to `maxLength` code points. */
  def validAccountIdUpTo(maxLength: Int): Gen[String] = {
    // k segments need at least 2k - 1 characters
    val maxSegments = (maxLength + 1) / 2
    for {
      segmentCount <- Gen.frequency(
        9 -> Gen.choose(1, math.min(8, maxSegments)),
        1 -> Gen.choose(1, maxSegments),
      )
      // Reserve space for separators so the total cannot exceed maxLength.
      maxSegmentLength = (maxLength - (segmentCount - 1)) / segmentCount
      segments <- Gen.listOfN(segmentCount, segment(maxSegmentLength))
    } yield segments.mkString("/")
  }

  /** A valid account id of exactly `length` code points: one of at most `length` code points whose
    * last segment is padded with segment characters.
    */
  def validAccountIdOfLength(length: Int): Gen[String] =
    for {
      id <- validAccountIdUpTo(length)
      padding <- Gen.stringOfN(length - id.length, segmentChar)
    } yield id + padding

  /** A valid account id of 1 to 255 code points, often exactly 255. */
  val validAccountId: Gen[String] =
    Gen.frequency(1 -> validAccountIdOfLength(MaxLength), 9 -> validAccountIdUpTo(MaxLength))

  /** Inserts `insert` into `s` at a random position. */
  private def insertAnywhere(s: String, insert: String): Gen[String] =
    Gen.choose(0, s.length).map(i => s.take(i) + insert + s.drop(i))

  /** A valid account id of at most 255 code points with one forbidden character inserted. */
  val accountIdWithForbiddenChar: Gen[String] =
    for {
      valid <- validAccountIdUpTo(MaxLength - 1)
      forbidden <- forbiddenChar
      invalid <- insertAnywhere(valid, forbidden)
    } yield invalid

  /** An account id of at most 255 code points with an empty segment. */
  val accountIdWithEmptySegment: Gen[String] =
    Gen.oneOf(
      Gen.const(""),
      Gen.const("/"),
      validAccountIdUpTo(MaxLength - 1).map("/" + _),
      validAccountIdUpTo(MaxLength - 1).map(_ + "/"),
      validAccountIdUpTo(MaxLength - 2).flatMap(insertAnywhere(_, "//")),
    )

  /** An account id of 256 to 300 code points that is otherwise valid, often exactly 256. */
  val tooLongAccountId: Gen[String] =
    Gen.frequency(
      1 -> validAccountIdOfLength(MaxLength + 1),
      3 -> Gen.choose(MaxLength + 2, MaxLength + 45).flatMap(validAccountIdOfLength),
    )

  /** Text that is mostly but not always a valid account id. */
  val anyAccountId: Gen[String] = Gen.oneOf(
    validAccountId,
    accountIdWithForbiddenChar,
    accountIdWithEmptySegment,
    tooLongAccountId,
    Gen
      .listOf(
        Gen.frequency(4 -> segmentChar.map(_.toInt), 1 -> Gen.const('/'.toInt), 1 -> codePoint)
      )
      .map(cps => new String(cps.toArray, 0, cps.size)),
  )

  /** Shrinks text by removing code points only. The default `String` shrinker also shrinks
    * characters towards NUL, which is not allowed in Daml `Text`.
    */
  val shrinkByRemovingCodePoints: Shrink[String] = {
    implicit val noCodePointShrink: Shrink[Int] = Shrink.shrinkAny
    Shrink { s =>
      Shrink
        .shrinkContainer[List, Int]
        .shrink(s.codePoints().toArray.toList)
        .map(cps => new String(cps.toArray, 0, cps.size))
    }
  }
}
