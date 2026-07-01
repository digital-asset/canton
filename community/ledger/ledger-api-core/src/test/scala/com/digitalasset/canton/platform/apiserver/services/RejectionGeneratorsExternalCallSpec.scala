// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.digitalasset.base.error.ErrorResource
import com.digitalasset.base.error.utils.ErrorDetails
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors
import com.digitalasset.canton.logging.{ErrorLoggingContext, NoLogging}
import com.digitalasset.daml.lf.engine.Error as LfError
import com.digitalasset.daml.lf.interpretation.Error as LfInterpretationError
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RejectionGeneratorsExternalCallSpec extends AnyFlatSpec with Matchers with OptionValues {

  private implicit val loggingContext: ErrorLoggingContext = NoLogging

  behavior of "RejectionGenerators external-call interpretation errors"

  it should "emit preparation-failed metadata" in {
    assertExternalCallMetadata(
      LfInterpretationError.ExternalCall.PreparationFailed(
        extensionId = "test-extension",
        functionId = "test-function",
        message = "bad config",
      ),
      CommandExecutionErrors.Interpreter.ExternalCallError.PreparationFailed.id,
      "bad config",
    )
  }

  it should "emit execution-failed metadata" in {
    assertExternalCallMetadata(
      LfInterpretationError.ExternalCall.ExecutionFailed(
        extensionId = "test-extension",
        functionId = "test-function",
        error = LfInterpretationError.ExternalCall.ExecutionFailed.CallFailed("call failed"),
      ),
      CommandExecutionErrors.Interpreter.ExternalCallError.ExecutionFailed.id,
      "call failed",
    )
  }

  it should "emit invalid-output metadata" in {
    assertExternalCallMetadata(
      LfInterpretationError.ExternalCall.ExecutionFailed(
        extensionId = "test-extension",
        functionId = "test-function",
        error = LfInterpretationError.ExternalCall.ExecutionFailed.InvalidOutput("bad output"),
      ),
      CommandExecutionErrors.Interpreter.ExternalCallError.InvalidOutput.id,
      "bad output",
    )
  }

  private def assertExternalCallMetadata(
      externalCallError: LfInterpretationError.ExternalCall.Error,
      expectedErrorCodeId: String,
      expectedMessage: String,
  ): Unit = {
    val grpcError =
      RejectionGenerators
        .commandExecutorError(
          ErrorCause.DamlLf(
            LfError.Interpretation(
              LfError.Interpretation.DamlException(
                LfInterpretationError.ExternalCall(externalCallError)
              ),
              None,
            )
          )
        )
        .asGrpcError

    val details = ErrorDetails.from(grpcError)
    details.collectFirst { case ErrorDetails.ErrorInfoDetail(errorCodeId, _) =>
      errorCodeId
    }.value shouldBe expectedErrorCodeId

    details.collect { case ErrorDetails.ResourceInfoDetail(name, typ) =>
      typ -> name
    } shouldBe Seq(
      ErrorResource.ExternalCallExtensionId.asString -> "test-extension",
      ErrorResource.ExternalCallFunctionId.asString -> "test-function",
      ErrorResource.ExceptionText.asString -> expectedMessage,
    )
  }
}
