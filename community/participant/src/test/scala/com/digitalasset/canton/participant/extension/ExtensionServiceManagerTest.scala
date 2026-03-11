// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port}
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.participant.config.{EngineExtensionsConfig, ExtensionServiceConfig}
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class ExtensionServiceManagerTest extends AsyncWordSpec with BaseTest {

  implicit val tc: TraceContext = TraceContext.empty

  private def makeConfig(name: String, port: Int = 8080): ExtensionServiceConfig =
    ExtensionServiceConfig(
      name = name,
      host = "localhost",
      port = Port.tryCreate(port),
      useTls = false,
      requestTimeout = NonNegativeFiniteDuration.ofSeconds(10),
      maxRetries = NonNegativeInt.tryCreate(2),
    )

  private val echoEngineConfig = EngineExtensionsConfig(
    echoMode = true,
    validateExtensionsOnStartup = true,
  )

  "ExtensionServiceManager" should {

    "handle external call with unknown extension" in {
      val manager = new ExtensionServiceManager(
        Map("test-ext" -> makeConfig("test-ext")),
        EngineExtensionsConfig.default,
        loggerFactory,
      )

      manager
        .handleExternalCall("unknown-ext", "test-func", "00000000", "deadbeef", "submission")
        .failOnShutdown
        .map { result =>
          result.isLeft shouldBe true
          val error = result.swap.getOrElse(fail("Expected Left"))
          error.statusCode shouldBe 404
          error.message should include("unknown-ext")
          error.message should include("not configured")
        }
    }

    "route external call to correct client in echo mode" in {
      val manager = new ExtensionServiceManager(
        Map("echo-ext" -> makeConfig("echo-ext")),
        echoEngineConfig,
        loggerFactory,
      )

      val inputHex = "deadbeef"
      manager
        .handleExternalCall("echo-ext", "echo", "00000000", inputHex, "submission")
        .failOnShutdown
        .map { result =>
          result shouldBe Right(inputHex)
        }
    }

    "report hasExtensions correctly" in {
      val emptyManager = ExtensionServiceManager.empty(loggerFactory)
      val managerWithExtensions = new ExtensionServiceManager(
        Map("test-ext" -> makeConfig("test-ext")),
        EngineExtensionsConfig.default,
        loggerFactory,
      )

      emptyManager.hasExtensions shouldBe false
      managerWithExtensions.hasExtensions shouldBe true
    }

    "return correct extensionIds" in {
      val manager = new ExtensionServiceManager(
        Map("ext1" -> makeConfig("ext1", 8080), "ext2" -> makeConfig("ext2", 8081)),
        EngineExtensionsConfig.default,
        loggerFactory,
      )

      manager.extensionIds shouldBe Set("ext1", "ext2")
    }

    "create empty manager successfully" in {
      val emptyManager = ExtensionServiceManager.empty(loggerFactory)

      emptyManager.hasExtensions shouldBe false
      emptyManager.extensionIds shouldBe Set.empty

      emptyManager
        .handleExternalCall("any-ext", "any-func", "00000000", "deadbeef", "submission")
        .failOnShutdown
        .map { result =>
          result.isLeft shouldBe true
          val error = result.swap.getOrElse(fail("Expected Left"))
          error.statusCode shouldBe 404
        }
    }

    "use echo mode when enabled" in {
      val manager = new ExtensionServiceManager(
        Map("echo-ext" -> makeConfig("echo-ext")),
        echoEngineConfig,
        loggerFactory,
      )

      val testInputs = Seq("deadbeef", "cafebabe", "")

      Future.traverse(testInputs) { input =>
        manager
          .handleExternalCall("echo-ext", "any-function", "00000000", input, "submission")
          .failOnShutdown
          .map { result =>
            result shouldBe Right(input)
          }
      }.map(_ => succeed)
    }

    "validate extensions on startup when enabled" in {
      val manager = new ExtensionServiceManager(
        Map("test-ext" -> makeConfig("test-ext")),
        echoEngineConfig,
        loggerFactory,
      )

      manager.validateAllExtensions().failOnShutdown.map { results =>
        results.keySet should contain("test-ext")
        results("test-ext") shouldBe ExtensionValidationResult.Valid
      }
    }

    "skip validation when disabled" in {
      val config = EngineExtensionsConfig(
        echoMode = true,
        validateExtensionsOnStartup = false,
      )
      val manager = new ExtensionServiceManager(
        Map("test-ext" -> makeConfig("test-ext")),
        config,
        loggerFactory,
      )

      manager.validateAllExtensions().failOnShutdown.map { results =>
        results shouldBe empty
      }
    }

    "handle multiple extensions correctly" in {
      val manager = new ExtensionServiceManager(
        Map("echo1" -> makeConfig("echo1", 8080), "echo2" -> makeConfig("echo2", 8081)),
        echoEngineConfig,
        loggerFactory,
      )

      for {
        result1 <- manager.handleExternalCall("echo1", "func", "00000000", "input1", "submission").failOnShutdown
        result2 <- manager.handleExternalCall("echo2", "func", "00000000", "input2", "submission").failOnShutdown
      } yield {
        result1 shouldBe Right("input1")
        result2 shouldBe Right("input2")
      }
    }

    "properly close without errors" in {
      val manager = new ExtensionServiceManager(
        Map("test-ext" -> makeConfig("test-ext")),
        EngineExtensionsConfig.default,
        loggerFactory,
      )

      noException should be thrownBy manager.close()
    }
  }
}
