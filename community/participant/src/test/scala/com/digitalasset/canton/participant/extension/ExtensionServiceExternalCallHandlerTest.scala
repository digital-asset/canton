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
            result.left.value.message should include("status 404")
          },
          _.warningMessage shouldBe
            "External call to extension 'unknown-ext' (function 'test-func') failed: " +
            "status=404, retryable=false, message=Extension 'unknown-ext' not configured",
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

    "map manager errors to sanitized engine external-call errors" in {
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
            error.message should include("status 404")
            error.message should not include "unknown-ext"
            error.message should not include "not configured"
            error.message should not include "Available extensions"
          },
          _.warningMessage shouldBe
            "External call to extension 'unknown-ext' (function 'test-func') failed: " +
            "status=404, retryable=false, message=Extension 'unknown-ext' not configured",
        )
      } finally manager.close()
    }

  }
}
