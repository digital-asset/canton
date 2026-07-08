// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.ExternalCallKey
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.validation.ExternalCallValidator
import com.digitalasset.canton.platform.execution.ExternalCallMode
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.daml.lf.data.Bytes
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.AtomicReference

class ExtensionServiceExternalCallValidatorTest extends AsyncWordSpec with BaseTest {

  private val externalCallKey =
    ExternalCallKey(
      extensionId = "extension-id",
      functionId = "function-id",
      config = "00010203",
      input = "deadbeef",
    )

  private val recordedOutput = Bytes.assertFromString("01020304")

  private def emptyManager: ExtensionServiceManager =
    ExtensionServiceManager.empty(loggerFactory, ProcessingTimeout())

  private final class StubExtensionServiceManager(
      result: Either[ExtensionCallError, String]
  ) extends ExtensionServiceManager(
        Map.empty,
        loggerFactory,
        ProcessingTimeout(),
      ) {

    private val observedCall =
      new AtomicReference[Option[(String, String, String, String, ExternalCallMode)]](None)

    def observed: Option[(String, String, String, String, ExternalCallMode)] =
      observedCall.get()

    override def handleExternalCall(
        extensionId: String,
        functionId: String,
        configHash: String,
        input: String,
        mode: ExternalCallMode,
    )(implicit tc: TraceContext): FutureUnlessShutdown[Either[ExtensionCallError, String]] = {
      observedCall.set(Some((extensionId, functionId, configHash, input, mode)))
      FutureUnlessShutdown.pure(result)
    }
  }

  "ExtensionServiceExternalCallValidator.create" should {

    "return UnableToValidate when the extension service is not configured" in {
      val validator = ExtensionServiceExternalCallValidator.create(None)

      validator
        .validate(externalCallKey, recordedOutput)
        .failOnShutdown
        .map { result =>
          result shouldBe ExternalCallValidator.UnableToValidate(
            "extension service is not configured"
          )
        }
    }

    "return UnableToValidate for an unconfigured extension" in {
      val manager = emptyManager
      val validator = ExtensionServiceExternalCallValidator.create(Some(manager))

      loggerFactory
        .assertLogs(
          validator
            .validate(externalCallKey, recordedOutput)
            .failOnShutdown
            .map { result =>
              result shouldBe ExternalCallValidator.UnableToValidate(
                "external-call validation failed with status 404"
              )
            },
          _.warningMessage shouldBe
            "External call to extension 'extension-id' (function 'function-id') failed: " +
            "status=404, retryable=false, message=Extension 'extension-id' not configured",
        )
        .thereafter(_ => manager.close())
    }
  }

  "ExtensionServiceExternalCallValidator" should {

    "sanitize manager errors" in {
      val manager =
        new StubExtensionServiceManager(
          Left(
            ExtensionCallError(
              statusCode = 503,
              message = "internal detail",
              requestId = Some("request-1"),
              retryable = true,
            )
          )
        )
      val validator = new ExtensionServiceExternalCallValidator(manager)

      validator
        .validate(externalCallKey, recordedOutput)
        .failOnShutdown
        .map { result =>
          result match {
            case ExternalCallValidator.UnableToValidate(reason) =>
              reason shouldBe
                "external-call validation failed with status 503, requestId=request-1"
              reason should not include externalCallKey.extensionId
              reason should not include "internal detail"

            case other => fail(s"Expected UnableToValidate, got $other")
          }
        }
        .thereafter(_ => manager.close())
    }

    "return UnableToValidate when the manager returns non-canonical output" in {
      val manager = new StubExtensionServiceManager(Right("not-hex"))
      val validator = new ExtensionServiceExternalCallValidator(manager)

      validator
        .validate(externalCallKey, recordedOutput)
        .failOnShutdown
        .map { result =>
          result shouldBe ExternalCallValidator.UnableToValidate(
            "external-call validation returned non-canonical output"
          )
        }
        .thereafter(_ => manager.close())
    }

    "return Matched when computed output matches recorded output" in {
      val manager = new StubExtensionServiceManager(Right(recordedOutput.toHexString))
      val validator = new ExtensionServiceExternalCallValidator(manager)

      validator
        .validate(externalCallKey, recordedOutput)
        .failOnShutdown
        .map { result =>
          result shouldBe ExternalCallValidator.Matched
          manager.observed shouldBe Some(
            (
              externalCallKey.extensionId,
              externalCallKey.functionId,
              externalCallKey.config,
              externalCallKey.input,
              ExternalCallMode.Validation,
            )
          )
        }
        .thereafter(_ => manager.close())
    }

    "return Mismatched when computed output differs from recorded output" in {
      val computedOutput = Bytes.assertFromString("05060708")
      val manager = new StubExtensionServiceManager(Right(computedOutput.toHexString))
      val validator = new ExtensionServiceExternalCallValidator(manager)

      validator
        .validate(externalCallKey, recordedOutput)
        .failOnShutdown
        .map { result =>
          result shouldBe ExternalCallValidator.Mismatched(
            computedOutput = computedOutput,
            recordedOutput = recordedOutput,
          )
        }
        .thereafter(_ => manager.close())
    }
  }
}
