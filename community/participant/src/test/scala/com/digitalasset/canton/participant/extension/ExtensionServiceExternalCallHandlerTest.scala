// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port}
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.participant.config.{EngineExtensionsConfig, ExtensionServiceConfig}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.engine.{ExternalCallError => LfExternalCallError}
import org.scalatest.wordspec.AsyncWordSpec

class ExtensionServiceExternalCallHandlerTest extends AsyncWordSpec with BaseTest {

  implicit val tc: TraceContext = TraceContext.empty

  private def makeEchoManager(extensions: Map[String, ExtensionServiceConfig]): ExtensionServiceManager =
    new ExtensionServiceManager(
      extensions,
      EngineExtensionsConfig(echoMode = true, validateExtensionsOnStartup = false),
      loggerFactory,
    )

  private def makeConfig(name: String, port: Int = 8080): ExtensionServiceConfig =
    ExtensionServiceConfig(
      name = name,
      host = "localhost",
      port = Port.tryCreate(port),
      useTls = false,
      requestTimeout = NonNegativeFiniteDuration.ofSeconds(10),
      maxRetries = NonNegativeInt.tryCreate(2),
    )

  "ExtensionServiceExternalCallHandler.create" should {

    "return handler when manager is provided" in {
      val manager = makeEchoManager(Map("test-ext" -> makeConfig("test-ext")))
      val handler = ExtensionServiceExternalCallHandler.create(Some(manager))

      handler shouldBe a[ExtensionServiceExternalCallHandler]
    }

    "return notSupported when manager is None" in {
      val handler = ExtensionServiceExternalCallHandler.create(None)

      handler should not be a[ExtensionServiceExternalCallHandler]

      handler
        .handleExternalCall("any-ext", "any-func", "00000000", "deadbeef", "submission", "test-command-id")
        .failOnShutdown
        .map { result =>
          result.isLeft shouldBe true
        }
    }
  }

  "ExtensionServiceExternalCallHandler" should {

    "delegate to extension service manager successfully" in {
      val manager = makeEchoManager(Map("echo-ext" -> makeConfig("echo-ext")))
      val handler = new ExtensionServiceExternalCallHandler(manager)

      val inputHex = "deadbeef"
      handler
        .handleExternalCall("echo-ext", "echo", "00000000", inputHex, "submission", "test-command-id")
        .failOnShutdown
        .map { result =>
          result shouldBe Right(inputHex)
        }
    }

    "convert ExtensionCallError to ExternalCallError for unknown extension" in {
      val manager = makeEchoManager(Map("test-ext" -> makeConfig("test-ext")))
      val handler = new ExtensionServiceExternalCallHandler(manager)

      handler
        .handleExternalCall("unknown-ext", "test-func", "00000000", "deadbeef", "submission", "test-command-id")
        .failOnShutdown
        .map { result =>
          result.isLeft shouldBe true
          val error = result.swap.getOrElse(fail("Expected Left"))
          error.statusCode shouldBe 404
          error.message should include("unknown-ext")
          error.message should include("not configured")
          error.requestId shouldBe None
        }
    }

    "map errors from empty manager" in {
      val manager = ExtensionServiceManager.empty(loggerFactory)
      val handler = new ExtensionServiceExternalCallHandler(manager)

      handler
        .handleExternalCall("any-ext", "any-func", "00000000", "deadbeef", "validation", "test-command-id")
        .failOnShutdown
        .map { result =>
          result.isLeft shouldBe true
          val error = result.swap.getOrElse(fail("Expected Left"))
          error.statusCode shouldBe 404
          error.requestId shouldBe None
        }
    }

    "work with different execution modes" in {
      val manager = makeEchoManager(Map("test-ext" -> makeConfig("test-ext")))
      val handler = new ExtensionServiceExternalCallHandler(manager)

      val inputHex = "cafebabe"

      for {
        submissionResult <- handler.handleExternalCall("test-ext", "func", "00000000", inputHex, "submission", "cmd-submit").failOnShutdown
        validationResult <- handler.handleExternalCall("test-ext", "func", "00000000", inputHex, "validation", "cmd-validate").failOnShutdown
      } yield {
        submissionResult shouldBe Right(inputHex)
        validationResult shouldBe Right(inputHex)
      }
    }
  }
}
