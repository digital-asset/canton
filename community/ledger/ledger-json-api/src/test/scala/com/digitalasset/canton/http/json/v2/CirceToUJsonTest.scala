// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.typesafe.scalalogging.Logger
import io.circe.Json
import org.scalacheck.{Gen, Shrink}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import ujson.StringRenderer
import ujson.circe.CirceJson

import scala.concurrent.ExecutionContext

class CirceToUJsonTest extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  private val genScalar: Gen[Json] = Gen.oneOf(
    Gen.const(Json.Null),
    Gen.oneOf(true, false).map(Json.fromBoolean),
    Gen.choose(-1000, 1000).map(Json.fromInt),
    Gen.choose(-1.0e6, 1.0e6).map(Json.fromDoubleOrNull),
    Gen.alphaNumStr.map(Json.fromString),
  )

  private def genJson(depth: Int): Gen[Json] =
    if (depth <= 0) genScalar
    else
      Gen.frequency(
        3 -> genScalar,
        1 -> Gen.choose(0, 4).flatMap(n => Gen.listOfN(n, genJson(depth - 1)).map(Json.fromValues)),
        1 -> genObject(depth),
      )

  private def genObject(depth: Int): Gen[Json] =
    for {
      n <- Gen.choose(0, 4)
      keys <- Gen.listOfN(n, Gen.alphaNumStr.suchThat(_.nonEmpty)).map(_.distinct)
      values <- Gen.listOfN(keys.size, genJson(depth - 1))
    } yield Json.fromFields(keys.zip(values))

  /** Total nesting depth of `levels`: levels-1 arrays around a scalar leaf. */
  private def nestedArray(levels: Int): Json =
    (1 until levels).foldLeft(Json.fromInt(0))((acc, _) => Json.arr(acc))

  "CirceToUjson" should {
    "match the legacy render/re-parse for bounded JSON" in {

      def classicReparse(js: Json): ujson.Value =
        ujson.read(CirceJson.transform(js, StringRenderer()).toString)

      forAll(genJson(8)) { js =>
        CirceToUJson.transform(js, maxDepth = 1000) shouldBe classicReparse(js)
      }
    }

    "accept input at the limit and reject input one level deeper" in {
      implicit val noShrinkInt: Shrink[Int] = Shrink.shrinkAny
      forAll(Gen.choose(1, 50)) { limit =>
        noException should be thrownBy CirceToUJson.transform(nestedArray(limit), maxDepth = limit)
        a[CirceToUJson.MaxNestingExceededException] should be thrownBy
          CirceToUJson.transform(nestedArray(limit + 1), maxDepth = limit)
      }
    }

    "reject pathologically deep input cleanly without StackOverflowError" in {
      val js = nestedArray(CirceToUJson.DefaultMaxDepth + 100)
      val ex = intercept[CirceToUJson.MaxNestingExceededException] {
        CirceToUJson.transform(js)
      }
      ex.limit shouldBe CirceToUJson.DefaultMaxDepth
    }

    "be enforced by ProtocolConverters.fromCirce" in {
      implicit val ec: ExecutionContext = DirectExecutionContext(Logger(getClass))
      val converters =
        new ProtocolConverters(new MockSchemaProcessor(), new MockTranscodePackageIdResolver())
      noException should be thrownBy converters.fromCirce(nestedArray(CirceToUJson.DefaultMaxDepth))
      a[CirceToUJson.MaxNestingExceededException] should be thrownBy
        converters.fromCirce(nestedArray(CirceToUJson.DefaultMaxDepth + 1))
    }
  }
}
