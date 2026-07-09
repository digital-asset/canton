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
        ExternalCallReplayData.fromResults(Seq(result("output-one"), result("output-two"))).left.value
      error should startWith(
        "externalCallResults records conflicting outputs for the same external call:"
      )
      error should not include "output-one"
      error should not include "output-two"
      error should not include Bytes.fromStringUtf8("config").toHexString
      error should not include Bytes.fromStringUtf8("input").toHexString
    }
  }
}
