// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.http.json.v2.{
  CirceToUJson,
  MockSchemaProcessor,
  MockTranscodePackageIdResolver,
  ProtocolConverters,
}
import com.typesafe.scalalogging.Logger
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContext

/** Layer 1 availability bug. A deeply nested document does NOT overflow at the parsers (circe parse
  * and `ujson.read` are stack-safe). The previous bridge overflowed in the recursive render step
  * `CirceJson.transform(js, StringRenderer())` used by `ProtocolConverters.fromCirce`. With the
  * depth-aware [[CirceToUJson]] bridge in place, deeply nested JSON is rejected cleanly with
  * [[CirceToUJson.MaxNestingExceededException]] instead of throwing `StackOverflowError`.
  */
class JsonParsingDepthTest extends AnyWordSpecLike with Matchers {

  private val depth = 100000

  private def arr(levels: Int): String = ("[" * levels) + ("]" * levels)

  private implicit val ec: ExecutionContext = DirectExecutionContext(Logger(getClass))
  private val converters =
    new ProtocolConverters(new MockSchemaProcessor(), new MockTranscodePackageIdResolver())

  "deeply nested JSON" should {
    "be parsed by circe without overflow (parse #1 is stack-safe)" in {
      io.circe.parser.parse(arr(depth)).isRight shouldBe true
    }

    "reject deeply nested JSON cleanly instead of overflowing the stack" in {
      val js = io.circe.parser.parse(arr(depth)).fold(throw _, identity)
      try {
        a[CirceToUJson.MaxNestingExceededException] should be thrownBy (converters.fromCirce(
          js
        ): Unit)
      } catch {
        case _: StackOverflowError =>
          fail("fromCirce overflowed the stack on deeply nested JSON")
      }
    }
  }
}
