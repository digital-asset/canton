// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.digitalasset.canton.ledger.api.validation.ResourceAnnotationValidator.*
import org.scalacheck.Gen
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ResourceAnnotationValidationsSpec
    extends AnyFlatSpec
    with Matchers
    with EitherValues
    with ScalaCheckPropertyChecks {

  private def testedValidationAllowEmptyValues(v: Map[String, String]) =
    validateAnnotationsFromApiRequest(v, allowEmptyValues = true)
  private def testedValidationDisallowEmptyValues(v: Map[String, String]) =
    validateAnnotationsFromApiRequest(v, allowEmptyValues = false)

  it should "validate annotation values" in {
    testedValidationDisallowEmptyValues(Map("a" -> "")).left.value shouldBe a[
      EmptyAnnotationsValueError
    ]
  }

  it should "validate annotations' total size - single key, large value" in {
    val largeString = "a" * 256 * 1024
    val notSoLargeString = "a" * ((256 * 1024) - 1)
    testedValidationAllowEmptyValues(
      Map("a" -> largeString)
    ).left.value shouldBe AnnotationsSizeExceededError
    testedValidationAllowEmptyValues(Map("a" -> notSoLargeString)).value shouldBe ()
  }

  it should "count keys with empty values towards size limit" in {
    val notSoLargeString = "a" * ((256 * 1024) - 1)
    testedValidationAllowEmptyValues(
      Map("a" -> notSoLargeString)
    ).value shouldBe ()
    testedValidationAllowEmptyValues(
      Map("a" -> notSoLargeString, "b" -> "")
    ).left.value shouldBe AnnotationsSizeExceededError
    testedValidationAllowEmptyValues(
      Map("a" -> notSoLargeString, "deleteMe" -> "")
    ).left.value shouldBe AnnotationsSizeExceededError
    testedValidationAllowEmptyValues(
      Map("a" -> notSoLargeString, "deleteM" -> "e")
    ).left.value shouldBe AnnotationsSizeExceededError
  }

  it should "validate annotations' total size - multiple keys" in {
    val sixteenLetters = "abcdefghijklmnop"
    val value = "a" * 1022
    val mapWithManyKeys: Map[String, String] = (for {
      l1 <- sixteenLetters
      l2 <- sixteenLetters
    } yield {
      val key = s"$l1$l2"
      key -> value
    }).toMap
    val mapWithManyKeys2 = mapWithManyKeys.updated(key = "a", value = "b")
    testedValidationAllowEmptyValues(mapWithManyKeys).value shouldBe ()
    testedValidationAllowEmptyValues(
      mapWithManyKeys2
    ).left.value shouldBe AnnotationsSizeExceededError
  }

  behavior of "annotation key syntax validation"

  // Reference model of the key syntax, written without regular expressions so that it can serve as
  // an independent oracle for the regex-based implementation:
  //   key    := name | prefix '/' name
  //   name   := 1..63 chars of [a-zA-Z0-9._-], starting and ending with [a-zA-Z0-9]
  //   prefix := 1..253 chars, one or more dot-separated labels
  //   label  := 1+ chars of [a-zA-Z0-9-], starting and ending with [a-zA-Z0-9]
  private object KeySyntaxModel {
    private def isAsciiAlnum(c: Char): Boolean =
      (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')

    private def startsAndEndsWithAlnum(s: String): Boolean =
      s.nonEmpty && isAsciiAlnum(s.head) && isAsciiAlnum(s.last)

    def isValidName(s: String): Boolean =
      s.length <= 63 && startsAndEndsWithAlnum(s) &&
        s.forall(c => isAsciiAlnum(c) || c == '.' || c == '-' || c == '_')

    private def isValidLabel(s: String): Boolean =
      startsAndEndsWithAlnum(s) && s.forall(c => isAsciiAlnum(c) || c == '-')

    def isValidPrefix(s: String): Boolean =
      s.length <= 253 && s.split("\\.", -1).forall(isValidLabel)

    def isValidKey(key: String): Boolean =
      key.split("/", -1) match {
        case Array(name) => isValidName(name)
        case Array(prefix, name) => isValidPrefix(prefix) && isValidName(name)
        case _ => false
      }
  }

  private def isAccepted(key: String): Boolean =
    testedValidationAllowEmptyValues(Map(key -> "value")).isRight

  // Checks that both the reference model and the validator classify the key as expected, and that
  // rejected keys are reported with an InvalidAnnotationsKeyError
  private def checkKeyClassification(key: String, valid: Boolean): Unit = {
    val shownKey = if (key.length > 80) s"${key.take(77)}... (${key.length} chars)" else key
    withClue(s"key '$shownKey':") {
      KeySyntaxModel.isValidKey(key) shouldBe valid
      isAccepted(key) shouldBe valid
      if (!valid)
        testedValidationAllowEmptyValues(Map(key -> "value")).left.value shouldBe a[
          InvalidAnnotationsKeyError
        ]
    }
  }

  private val alnumGen: Gen[Char] = Gen.oneOf(('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9'))

  // A segment of length 1..maxLen that starts and ends with an alphanumeric character
  private def segmentGen(maxLen: Int, innerCharGen: Gen[Char]): Gen[String] =
    for {
      len <- Gen.choose(1, maxLen)
      first <- alnumGen
      rest <-
        if (len == 1) Gen.const("")
        else
          for {
            middle <- Gen.listOfN(len - 2, innerCharGen)
            last <- alnumGen
          } yield middle.mkString + last
    } yield s"$first$rest"

  private val validNameGen: Gen[String] =
    segmentGen(
      63,
      Gen.frequency(5 -> alnumGen, 1 -> Gen.const('.'), 1 -> Gen.const('-'), 1 -> Gen.const('_')),
    )

  // Up to 5 labels of up to 49 chars each, so the prefix never exceeds 253 chars
  private val validPrefixGen: Gen[String] =
    for {
      n <- Gen.choose(1, 5)
      labels <- Gen.listOfN(n, segmentGen(49, Gen.frequency(5 -> alnumGen, 1 -> Gen.const('-'))))
    } yield labels.mkString(".")

  private val validKeyGen: Gen[String] =
    Gen.oneOf(validNameGen, Gen.zip(validPrefixGen, validNameGen).map { case (p, n) => s"$p/$n" })

  // Biased towards the characters that are significant to the grammar, plus a few that are never
  // allowed (including non-ASCII letters and digits)
  private val keyCharGen: Gen[Char] = Gen.frequency(
    8 -> alnumGen,
    2 -> Gen.const('.'),
    2 -> Gen.const('-'),
    1 -> Gen.const('_'),
    1 -> Gen.const('/'),
    1 -> Gen.oneOf('!', ' ', '\u00e9', '\u0663'),
  )

  private val randomKeyGen: Gen[String] =
    Gen.choose(0, 80).flatMap(n => Gen.listOfN(n, keyCharGen).map(_.mkString))

  // Valid keys with a single character replaced or inserted, producing near-misses
  private val mutatedValidKeyGen: Gen[String] =
    for {
      key <- validKeyGen
      pos <- Gen.choose(0, key.length)
      c <- keyCharGen
      replace <- Gen.oneOf(true, false)
    } yield key.patch(pos, c.toString, if (replace) 1 else 0)

  it should "accept all keys that conform to the key syntax" in {
    forAll(validKeyGen, minSuccessful(1000)) { key =>
      checkKeyClassification(key, valid = true)
    }
  }

  // Also checks that rejected keys are reported with an InvalidAnnotationsKeyError
  it should "agree with the reference model on arbitrary and near-miss keys" in {
    forAll(Gen.oneOf(randomKeyGen, mutatedValidKeyGen), minSuccessful(5000)) { key =>
      checkKeyClassification(key, valid = KeySyntaxModel.isValidKey(key))
    }
  }

  it should "enforce the key syntax at the boundaries" in {
    val cases = Table(
      ("key", "valid"),
      // typical name segments
      ("a", true),
      ("Z", true),
      ("7", true),
      ("abcdefghijklmnopqrstuvwxyz.-_ABCDEFGHIJKLMNOPQRSTUVWXYZ", true),
      ("some-key.like_this", true),
      // typical prefix segments
      ("aaa/a", true),
      ("AAA/a", true),
      ("aaa-bbb.ccc-ddd/a", true),
      ("00.11.AA-bBb.ccc2/a", true),
      (".user.management.daml/foo_", false),
      // name segment length
      ("a" * 63, true),
      ("a" * 64, false),
      ("a" + "-" * 61 + "a", true),
      // name segment start and end characters
      ("-a", false),
      ("a-", false),
      ("_a", false),
      ("a_", false),
      (".a", false),
      ("a.", false),
      ("a._-a", true),
      ("a..a", true),
      // characters outside the allowed set
      ("a!a", false),
      ("a a", false),
      ("&", false),
      ("%", false),
      ("\u00e9", false),
      ("\u0663", false),
      ("", false),
      // prefix segment length
      (s"${"a" * 253}/a", true),
      (s"${"a" * 254}/a", false),
      (s"${("a" * 62 + ".") * 3}${"a" * 64}/a", true),
      // prefix segment structure
      ("a.b.c/a", true),
      ("a-b/a", true),
      ("a--b/a", true),
      ("a..b/a", false),
      (".a/a", false),
      ("a./a", false),
      ("-a/a", false),
      ("a-/a", false),
      ("a.-b/a", false),
      ("a_b/a", false),
      // slashes
      ("/a", false),
      ("a/", false),
      ("a//a", false),
      ("a/b/c", false),
    )
    forAll(cases)(checkKeyClassification)
  }

  // Maximum-length inputs that are valid, or only become invalid at the very end. Inputs of this
  // shape caused excessive regex backtracking with earlier versions of the key patterns, and are
  // too specific to be produced reliably by the generators above.
  it should "classify pathological maximum-length keys correctly" in {
    val maxLabel = "a" * 62 + "."
    val cases = Table(
      ("description", "key", "valid"),
      // prefix segment: a single 253-char label
      ("253-char single-label prefix", s"${"a" * 253}/a", true),
      ("252-char label ending in '-'", s"${"a" * 252}-/a", false),
      ("252-char label ending in '.'", s"${"a" * 252}./a", false),
      ("252-char label ending in '!'", s"${"a" * 252}!/a", false),
      ("long label with '_' in the middle", s"${"a" * 126}_${"a" * 126}/a", false),
      // prefix segment: many short labels or many hyphens
      ("126 hyphen-separated runs", s"${"a-" * 126}a/a", true),
      ("126 hyphen-separated runs ending in '!'", s"${"a-" * 126}!/a", false),
      ("127 one-char labels", s"${"a." * 126}a/a", true),
      ("126 one-char labels ending in '!'", s"${"a." * 126}!/a", false),
      ("126 one-char labels ending in '-'", s"${"a." * 126}-/a", false),
      ("hyphen run before the final dot", s"${"a" + "-" * 250}a./a", false),
      // prefix segment: several 62-char labels
      ("five long labels", s"${maxLabel * 4}a/a", true),
      ("four long labels then '!'", s"${maxLabel * 4}!/a", false),
      ("four long labels then '-'", s"${maxLabel * 4}-/a", false),
      ("four long labels with a trailing dot", s"${maxLabel * 4}/a", false),
      // name segment: 63 chars
      ("62 'a' then '-'", "a" * 62 + "-", false),
      ("62 'a' then '!'", "a" * 62 + "!", false),
      ("alternating separators", "a" + "._-" * 20 + "a", true),
      ("alternating separators ending in '!'", "a" + "._-" * 20 + "!", false),
      ("alternating separators ending in '_'", "a._" * 21, false),
      ("62 separators then 'a'", "-" * 62 + "a", false),
      ("'a' then 61 separators then 'a'", "a" + "._-" * 20 + ".a", true),
      // maximum prefix and name together
      ("maximum prefix and name", s"${maxLabel * 4}a/${"a" + "._-" * 20 + ".a"}", true),
      (
        "maximum prefix and name with the name ending in '-'",
        s"${maxLabel * 4}a/${"a" * 62}-",
        false,
      ),
      // far beyond the length limits
      ("10000-char name", "a" * 10000, false),
      ("10000-char prefix", s"${"a" * 10000}/a", false),
      ("10000-char hyphenated prefix", s"${"a-" * 5000}a/a", false),
    )
    forAll(cases) { (description, key, valid) =>
      withClue(s"$description:") {
        checkKeyClassification(key, valid)
      }
    }
  }

}
