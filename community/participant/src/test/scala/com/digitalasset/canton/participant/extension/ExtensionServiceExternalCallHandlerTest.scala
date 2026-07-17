// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.platform.execution.ExternalCallMode
import com.digitalasset.canton.tracing.TraceContext
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
              "message = \"Extension 'unknown-ext' not configured\", retryable = false, trace id = tid:)"
          },
          _.warningMessage shouldBe
            "External call to extension 'unknown-ext' (function 'test-func') failed: " +
            "ExtensionCallError(status code = 404, message = \"Extension 'unknown-ext' not configured\", retryable = false, trace id = tid:)",
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

    "forward the full client-actionable error to the engine for the submitting client" in {
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
              "message = \"Extension 'unknown-ext' not configured\", retryable = false, trace id = tid:)"
          },
          _.warningMessage shouldBe
            "External call to extension 'unknown-ext' (function 'test-func') failed: " +
            "ExtensionCallError(status code = 404, message = \"Extension 'unknown-ext' not configured\", retryable = false, trace id = tid:)",
        )
      } finally manager.close()
    }

  }

  "ExtensionServiceExternalCallHandler" when {
    "the error is not actionable for the client" should {
      "reduce it to the generic client message" in {
        val manager: ExtensionServiceManager =
          new ExtensionServiceManager(Map.empty, loggerFactory, ProcessingTimeout()) {
            override def handleExternalCall(
                extensionId: String,
                functionId: String,
                configHash: String,
                input: String,
                mode: ExternalCallMode,
            )(implicit
                tc: TraceContext
            ): FutureUnlessShutdown[Either[ExtensionCallError, String]] =
              FutureUnlessShutdown.pure(
                Left(
                  ExtensionCallError(
                    statusCode = 503,
                    message = "Connection failed",
                    externalCallId = Some("req-1"),
                    retryable = true,
                    clientActionable = false,
                  )(tc)
                )
              )
          }
        val handler = new ExtensionServiceExternalCallHandler(manager)

        try {
          val error = handler
            .handleExternalCall(
              "some-ext",
              "some-func",
              "00000000",
              "deadbeef",
              ExternalCallMode.Submission,
            )
            .failOnShutdown
            .futureValue
            .left
            .value
          error.message shouldBe
            "External call failed (retryable = true, external call id = 'req-1')"
        } finally manager.close()
      }
    }
  }

  "ExtensionServiceExternalCallHandler.genericClientMessage" should {
    "expose only the retry status and the tracing identifiers" in {
      TraceContext.withNewTraceContext("test") { tc =>
        ExtensionServiceExternalCallHandler.genericClientMessage(
          ExtensionCallError(
            statusCode = 503,
            message = "Connection failed",
            externalCallId = Some("req-1"),
            retryable = true,
            clientActionable = false,
          )(tc)
        ) shouldBe "External call failed (retryable = true, external call id = 'req-1'," +
          s" trace id = '${tc.traceId.value}')"
      }
    }

    "omit the identifiers when absent" in {
      ExtensionServiceExternalCallHandler.genericClientMessage(
        ExtensionCallError(
          statusCode = 500,
          message = "Unexpected error",
          externalCallId = None,
          retryable = false,
          clientActionable = false,
        )
      ) shouldBe "External call failed (retryable = false)"
    }
  }

  "ExtensionCallError" should {
    // The full-error log interpolates `$error`, so its rendering (via PrettyPrinting) must cover
    // every client-relevant field -- in particular the optional external call id, which the 404
    // cases above omit, and the trace id, which always renders (bare tid: when empty).
    // `clientActionable` is deliberately not rendered: it is control metadata, and the rendering
    // doubles as the client payload for actionable errors.
    "render the client-relevant fields, including the tracing identifiers, via pretty-printing" in {
      TraceContext.withNewTraceContext("test") { tc =>
        ExtensionCallError(
          statusCode = 503,
          message = "boom",
          externalCallId = Some("req-1"),
          retryable = true,
          clientActionable = false,
        )(tc).toString shouldBe
          "ExtensionCallError(status code = 503, message = \"boom\", external call id = 'req-1'," +
          s" retryable = true, trace id = tid:${tc.traceId.value})"
      }
    }

    "render an absent external call id as omitted and an empty trace context as a bare tid" in {
      ExtensionCallError(
        statusCode = 503,
        message = "boom",
        externalCallId = None,
        retryable = true,
        clientActionable = false,
      )(TraceContext.empty).toString shouldBe
        "ExtensionCallError(status code = 503, message = \"boom\", retryable = true, trace id = tid:)"
    }
  }
}
