// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/** Test that ResultNeedExternalFetch trampolines correctly through map/flatMap/consume. */
class ResultNeedExternalFetchTest extends AnyWordSpec with Matchers {

  private val descriptor = ExternalFetchDescriptor(
    endpoints = Seq("oracle.example.com:9999"),
    payload = "get_price".getBytes,
    signerKeys = Seq("key1".getBytes),
    maxBytes = 1024,
    timeoutMs = 5000,
    nonce = Array.fill(32)(0.toByte),
  )

  private val response = ExternalFetchResponse(
    body = "42.50".getBytes,
    signature = "sig".getBytes,
    signerKey = "key1".getBytes,
    fetchedAt = 1000000L,
  )

  "ResultNeedExternalFetch" should {

    "map over the resumed value" in {
      val result: Result[String] = ResultNeedExternalFetch(
        descriptor,
        (_: ExternalFetchResponse) => ResultDone("hello"),
      )

      val mapped: Result[Int] = result.map(_.length)

      mapped match {
        case ResultNeedExternalFetch(_, resume) =>
          resume(response) shouldBe ResultDone(5)
        case other =>
          fail(s"Expected ResultNeedExternalFetch, got $other")
      }
    }

    "flatMap over the resumed value" in {
      val result: Result[String] = ResultNeedExternalFetch(
        descriptor,
        (_: ExternalFetchResponse) => ResultDone("hello"),
      )

      val flatMapped: Result[Int] = result.flatMap(s => ResultDone(s.length))

      flatMapped match {
        case ResultNeedExternalFetch(_, resume) =>
          resume(response) shouldBe ResultDone(5)
        case other =>
          fail(s"Expected ResultNeedExternalFetch, got $other")
      }
    }

    "chain through flatMap" in {
      val result: Result[Int] = ResultNeedExternalFetch(
        descriptor,
        (resp: ExternalFetchResponse) => ResultDone(new String(resp.body)),
      ).flatMap(s => ResultDone(s.length))

      result match {
        case ResultNeedExternalFetch(desc, resume) =>
          desc.endpoints shouldBe Seq("oracle.example.com:9999")
          resume(response) shouldBe ResultDone(5)
        case other =>
          fail(s"Expected ResultNeedExternalFetch, got $other")
      }
    }

    "preserve descriptor through map" in {
      val result = ResultNeedExternalFetch(
        descriptor,
        (_: ExternalFetchResponse) => ResultDone(42),
      ).map(_ + 1)

      result match {
        case ResultNeedExternalFetch(desc, _) =>
          desc.endpoints shouldBe Seq("oracle.example.com:9999")
          desc.maxBytes shouldBe 1024
          desc.timeoutMs shouldBe 5000
          desc.nonce.length shouldBe 32
        case other =>
          fail(s"Expected ResultNeedExternalFetch, got $other")
      }
    }

    "consume resolves with provided external fetch handler" in {
      val result: Result[String] = ResultNeedExternalFetch(
        descriptor,
        (resp: ExternalFetchResponse) => ResultDone(new String(resp.body)),
      )

      val consumed = result.consume(
        externalFetches = { case d if d.endpoints.contains("oracle.example.com:9999") => response },
      )

      consumed shouldBe Right("42.50")
    }

    "consume fails when no handler matches" in {
      val result: Result[String] = ResultNeedExternalFetch(
        descriptor,
        (resp: ExternalFetchResponse) => ResultDone(new String(resp.body)),
      )

      an[IllegalStateException] should be thrownBy result.consume()
    }

    "resume propagates errors from the continuation" in {
      val result: Result[String] = ResultNeedExternalFetch(
        descriptor,
        (_: ExternalFetchResponse) =>
          ResultDone("error: boom"),
      )

      result match {
        case ResultNeedExternalFetch(_, resume) =>
          resume(response) match {
            case ResultDone(v) => v should startWith("error:")
            case other => fail(s"Expected ResultDone with error, got $other")
          }
        case other =>
          fail(s"Expected ResultNeedExternalFetch, got $other")
      }
    }
  }
}
