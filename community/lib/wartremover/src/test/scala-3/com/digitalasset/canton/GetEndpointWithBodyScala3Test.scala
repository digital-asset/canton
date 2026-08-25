// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.wartremover.test.WartTestTraverser
import sttp.tapir.*

/** Minimal Scala 3 only checks for [[GetEndpointWithBody]]. The shared test suite lives in
  * `src/test/scala`; this one keeps the simplest possible endpoint shapes, which makes it easier to
  * debug the Scala 3 (quotes reflection) implementation of the wart.
  */
class GetEndpointWithBodyScala3Test extends AnyWordSpec with Matchers {

  "GetEndpointWithBody (Scala 3)" should {

    "detect the simplest GET with a body" in {
      val result = WartTestTraverser(GetEndpointWithBody) {
        val _ = endpoint.get.in(stringBody)
        ()
      }
      result.errors.length shouldBe 1
    }

    "allow the simplest GET without a body" in {
      val result = WartTestTraverser(GetEndpointWithBody) {
        val _ = endpoint.get.in("resource")
        ()
      }
      result.errors shouldBe empty
    }
  }
}

