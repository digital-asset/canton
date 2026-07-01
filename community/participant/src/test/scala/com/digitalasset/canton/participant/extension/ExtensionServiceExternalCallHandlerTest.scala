// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.platform.execution.ExternalCallMode
import com.digitalasset.canton.util.Thereafter.syntax.*
import org.scalatest.wordspec.AsyncWordSpec

class ExtensionServiceExternalCallHandlerTest extends AsyncWordSpec with BaseTest {

  private def emptyManager: ExtensionServiceManager =
    ExtensionServiceManager.empty(loggerFactory, ProcessingTimeout())

  "ExtensionServiceExternalCallHandler.create" should {

    "use provided manager when manager is provided" in {
      val manager = emptyManager
      val handler = ExtensionServiceExternalCallHandler.create(Some(manager))

      handler
        .handleExternalCall(
          "unknown-ext",
          "test-func",
          "00000000",
          "deadbeef",
          ExternalCallMode.Submission,
        )
        .failOnShutdown
        .map { result =>
          result.left.value.message should include("status 404")
        }
        .thereafter(_ => manager.close())
    }

    "return Unsupported when manager is None" in {
      val handler = ExtensionServiceExternalCallHandler.create(None)

      handler
        .handleExternalCall(
          "any-ext",
          "any-func",
          "00000000",
          "deadbeef",
          ExternalCallMode.Submission,
        )
        .failOnShutdown
        .map { result =>
          result.left.value.message should include(
            "External calls not supported"
          )
        }
    }
  }

  "ExtensionServiceExternalCallHandler" should {

    "map manager errors to sanitized engine external-call errors" in {
      val manager = emptyManager
      val handler = new ExtensionServiceExternalCallHandler(manager)

      handler
        .handleExternalCall(
          "unknown-ext",
          "test-func",
          "00000000",
          "deadbeef",
          ExternalCallMode.Submission,
        )
        .failOnShutdown
        .map { result =>
          val error = result.left.value
          error.message should include("status 404")
          error.message should not include "unknown-ext"
          error.message should not include "not configured"
          error.message should not include "Available extensions"
        }
        .thereafter(_ => manager.close())
    }

  }
}
