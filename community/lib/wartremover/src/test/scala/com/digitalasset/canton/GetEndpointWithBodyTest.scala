// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.wartremover.test.WartTestTraverser
import sttp.tapir.*
import sttp.tapir.json.circe.*

class GetEndpointWithBodyTest extends AnyWordSpec with Matchers {

  private def assertErrors(result: WartTestTraverser.Result, expectedErrors: Int): Assertion = {
    result.errors.length shouldBe expectedErrors
    result.errors.foreach {
      _ should include(GetEndpointWithBody.message)
    }
    succeed
  }

  "GetEndpointWithBody" should {

    "detect a request body added after .get" in {
      val result = WartTestTraverser(GetEndpointWithBody) {
        val _ = endpoint.get
          .in("resource")
          .in(stringBody)
        ()
      }
      assertErrors(result, 1)
    }

    "detect a request body added before .get" in {
      val result = WartTestTraverser(GetEndpointWithBody) {
        val _ = endpoint
          .in("resource")
          .in(stringBody)
          .get
        ()
      }
      assertErrors(result, 1)
    }

    "detect a jsonBody request body added before .get" in {
      val result = WartTestTraverser(GetEndpointWithBody) {
        val _ = endpoint
          .in("resource")
          .in(jsonBody[String])
          .get
        ()
      }
      assertErrors(result, 1)
    }

    "detect a request body when the chain continues after .get" in {
      val result = WartTestTraverser(GetEndpointWithBody) {
        val _ = endpoint
          .in("resource")
          .in(stringBody)
          .get
          .out(stringBody)
          .description("some description")
        ()
      }
      assertErrors(result, 1)
    }

    "allow a request body on POST" in {
      val result = WartTestTraverser(GetEndpointWithBody) {
        val _ = endpoint.post
          .in("resource")
          .in(stringBody)
          .out(stringBody)
        ()
      }
      assertErrors(result, 0)
    }

    "allow GET with query and path inputs" in {
      val result = WartTestTraverser(GetEndpointWithBody) {
        val _ = endpoint.get
          .in("resource")
          .in(path[String]("id"))
          .in(query[Option[String]]("filter"))
          .out(stringBody)
        ()
      }
      assertErrors(result, 0)
    }

    "allow GET with a response body" in {
      val result = WartTestTraverser(GetEndpointWithBody) {
        val _ = endpoint.get
          .in("resource")
          .out(stringBody)
        ()
      }
      assertErrors(result, 0)
    }

    "not report suppressed definitions" in {
      val result = WartTestTraverser(GetEndpointWithBody) {
        @SuppressWarnings(Array("com.digitalasset.canton.GetEndpointWithBody"))
        val suppressed = endpoint
          .in("resource")
          .in(stringBody)
          .get
        suppressed.info
        ()
      }
      assertErrors(result, 0)
    }
  }
}
