// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea

import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{Assertion, EitherValues}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class AccountIdTest
    extends AnyWordSpec
    with Matchers
    with EitherValues
    with ScalaCheckDrivenPropertyChecks {

  private val formatError = "expected non-empty segments separated by '/'"
  private val lengthError = "maximum length of 255"

  private val segmentChars: Seq[Char] =
    ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9') ++ "@^$.!`-#+'~_|:() "

  private val segmentGen: Gen[String] =
    Gen.choose(1, 20).flatMap(Gen.listOfN(_, Gen.oneOf(segmentChars))).map(_.mkString)

  private val validIdGen: Gen[String] =
    Gen.choose(1, 5).flatMap(Gen.listOfN(_, segmentGen)).map(_.mkString("/"))

  // A '/' would split a segment into two valid ones, so we leave it out.
  private val disallowedCharGen: Gen[Char] =
    Gen.choose(Char.MinValue, Char.MaxValue).suchThat(c => c != '/' && !segmentChars.contains(c))

  private def assertValid(id: String): Assertion = {
    AccountId.create(id).value.unwrap shouldBe id
    AccountId.fromProtoPrimitive(id).value.unwrap shouldBe id
  }

  private def assertInvalid(id: String, expectedError: String): Assertion = {
    AccountId.create(id).left.value should include(expectedError)
    AccountId.fromProtoPrimitive(id).left.value.message should include(expectedError)
    an[IllegalArgumentException] should be thrownBy AccountId.tryCreate(id)
  }

  "AccountId" should {

    "accept non-empty segments of allowed characters" in {
      Seq(
        "alice",
        "alice::1220" + "ab" * 32,
        "acme/billing team",
        "a@^$.!`-#+'~_|:() Z9",
        "a" * 255,
      ).foreach(assertValid)
    }

    "reject empty segments and other characters" in {
      Seq("", "/a", "a/", "a//b", "a\\b", "a/b,c", "a\tb", "a\nb", "é", "𐀀")
        .foreach(assertInvalid(_, formatError))
    }

    "reject ids over 255 characters" in {
      assertInvalid("a" * 256, lengthError)
    }

    "accept any id made of allowed segments" in {
      forAll(validIdGen)(assertValid)
    }

    "reject an otherwise valid id with one other character inserted" in {
      val invalidIdGen = for {
        id <- validIdGen
        c <- disallowedCharGen
        pos <- Gen.choose(0, id.length)
      } yield id.patch(pos, c.toString, 0)

      forAll(invalidIdGen)(assertInvalid(_, formatError))
    }
  }
}
