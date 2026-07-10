// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.BaseTest
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.transaction.ExternalCallResult
import org.scalatest.wordspec.AnyWordSpec

class ExternalCallReplayDataTest extends AnyWordSpec with BaseTest {

  private def result(output: String, functionId: String = "function"): ExternalCallResult =
    ExternalCallResult(
      extensionId = "extension",
      functionId = functionId,
      config = Bytes.fromStringUtf8("config"),
      input = Bytes.fromStringUtf8("input"),
      output = Bytes.fromStringUtf8(output),
    )

  "ExternalCallReplayData.fromResults" should {

    "index a single output per key" in {
      val data = ExternalCallReplayData.fromResults(Seq(result("output"))).value
      data.outputFor(ExternalCallKey.fromResult(result("output"))) shouldBe
        Some(Bytes.fromStringUtf8("output"))
    }

    "collapse identical results recorded in several occurrences" in {
      val data =
        ExternalCallReplayData.fromResults(Seq(result("output"), result("output"))).value
      data.outputsByKey should have size 1
    }

    "keep distinct keys apart" in {
      val data = ExternalCallReplayData
        .fromResults(Seq(result("out-a"), result("out-b", functionId = "other")))
        .value
      data.outputsByKey should have size 2
    }

    "return empty replay data for no results" in {
      ExternalCallReplayData.fromResults(Seq.empty).value shouldBe ExternalCallReplayData.empty
    }

    "reject a key recorded with conflicting outputs without leaking the payloads" in {
      val error =
        ExternalCallReplayData
          .fromResults(Seq(result("output-one"), result("output-two")))
          .left
          .value
      error shouldBe conflictMessageFor("function")
      // Payloads would surface as hex renderings of the bytes, never as the raw strings.
      error should not include Bytes.fromStringUtf8("output-one").toHexString
      error should not include Bytes.fromStringUtf8("output-two").toHexString
      error should not include Bytes.fromStringUtf8("config").toHexString
      error should not include Bytes.fromStringUtf8("input").toHexString
    }
  }

  "ExternalCallReplayData.merge" should {

    "combine subview data with own results" in {
      val subview = ExternalCallReplayData.fromResults(Seq(result("out-a"))).value
      val merged = ExternalCallReplayData
        .merge(Seq(subview), Seq(result("out-b", functionId = "other")))
        .value
      merged.outputsByKey should have size 2
    }

    "collapse identical entries across subviews and own results" in {
      val subview = ExternalCallReplayData.fromResults(Seq(result("output"))).value
      ExternalCallReplayData
        .merge(Seq(subview, subview), Seq(result("output")))
        .value shouldBe subview
    }

    "reject conflicting outputs across subviews" in {
      val subviewA = ExternalCallReplayData.fromResults(Seq(result("output-one"))).value
      val subviewB = ExternalCallReplayData.fromResults(Seq(result("output-two"))).value
      ExternalCallReplayData
        .merge(Seq(subviewA, subviewB), Seq.empty)
        .left
        .value shouldBe conflictMessageFor("function")
    }
  }

  private def conflictMessageFor(functionId: String): String =
    "externalCallResults records conflicting outputs for the same external call: " +
      s"""ExternalCallKey(extension id = "extension", function id = "$functionId", """ +
      """config bytes = "6 bytes", input bytes = "5 bytes") with outputs [10 bytes, 10 bytes]"""
}
