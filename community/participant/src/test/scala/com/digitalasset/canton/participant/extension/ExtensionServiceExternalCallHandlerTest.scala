// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.platform.execution.ExternalCallMode
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

class ExtensionServiceExternalCallHandlerTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext {

  private def emptyManager: ExtensionServiceManager =
    ExtensionServiceManager.empty(loggerFactory, ProcessingTimeout())

  "ExtensionServiceExternalCallHandler.create" should {

    "use provided manager when manager is provided" in {
      val manager = emptyManager
      val handler = ExtensionServiceExternalCallHandler.create(Some(manager))

      try {
        loggerFactory.assertLogs(
          {
            val result = handler
              .handleExternalCall(
                "unknown-ext",
                "test-func",
                "00000000",
                "deadbeef",
                ExternalCallMode.Submission,
              )
              .failOnShutdown
              .futureValue
            result.left.value.message shouldBe
              "ExtensionCallError(status code = 404, " +
              "message = \"Extension 'unknown-ext' not configured\", retryable = false)"
          },
          _.warningMessage shouldBe
            "External call to extension 'unknown-ext' (function 'test-func') failed: " +
            "ExtensionCallError(status code = 404, message = \"Extension 'unknown-ext' not configured\", retryable = false)",
        )
      } finally manager.close()
    }

    "return Unsupported when manager is None" in {
      val handler = ExtensionServiceExternalCallHandler.create(None)

      val result = handler
        .handleExternalCall(
          "any-ext",
          "any-func",
          "00000000",
          "deadbeef",
          ExternalCallMode.Submission,
        )
        .failOnShutdown
        .futureValue

      result.left.value.message should include("External calls not supported")
    }
  }

  "ExtensionServiceExternalCallHandler" should {

    "forward the full error to the engine for the submitting client" in {
      val manager = emptyManager
      val handler = new ExtensionServiceExternalCallHandler(manager)

      try {
        loggerFactory.assertLogs(
          {
            val error = handler
              .handleExternalCall(
                "unknown-ext",
                "test-func",
                "00000000",
                "deadbeef",
                ExternalCallMode.Submission,
              )
              .failOnShutdown
              .futureValue
              .left
              .value
            error.message shouldBe
              "ExtensionCallError(status code = 404, " +
              "message = \"Extension 'unknown-ext' not configured\", retryable = false)"
          },
          _.warningMessage shouldBe
            "External call to extension 'unknown-ext' (function 'test-func') failed: " +
            "ExtensionCallError(status code = 404, message = \"Extension 'unknown-ext' not configured\", retryable = false)",
        )
      } finally manager.close()
    }

  }

  "ExtensionCallError" should {
    // The full-error log interpolates `$error`, so its rendering (via PrettyPrinting) must cover
    // every field -- in particular the optional request id, which the 404 cases above omit.
    "render all fields, including the request id, via pretty-printing" in {
      ExtensionCallError(
        statusCode = 503,
        message = "boom",
        requestId = Some("req-1"),
        retryable = true,
      ).toString shouldBe
        "ExtensionCallError(status code = 503, message = \"boom\", request id = 'req-1', retryable = true)"
    }
  }
}
