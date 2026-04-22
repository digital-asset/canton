// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ResultSpec extends AnyWordSpec with Matchers {

  "Result.consume" should {
    "use the caller-provided external call response" in {
      val result =
        ResultNeedExternalCall[String](
          extensionId = "ext",
          functionId = "fun",
          configHash = "0a0b",
          input = "c0ff",
          resume = {
            case Right(output) => ResultDone(output)
            case Left(error) => ResultDone(s"unexpected-error:${error.message}")
          },
        )

      result.consume(externalCalls = { case ("ext", "fun", "0a0b", "c0ff") => "dead" }) shouldBe
        Right("dead")
    }

    "surface an error when no external call response is available" in {
      val result =
        ResultNeedExternalCall[String](
          extensionId = "ext",
          functionId = "fun",
          configHash = "0a0b",
          input = "c0ff",
          resume = {
            case Right(output) => ResultDone(s"unexpected-output:$output")
            case Left(error) => ResultDone(s"error:${error.statusCode}:${error.message}")
          },
        )

      result.consume() shouldBe
        Right("error:503:External call not available: extensionId=ext, functionId=fun")
    }
  }
}
