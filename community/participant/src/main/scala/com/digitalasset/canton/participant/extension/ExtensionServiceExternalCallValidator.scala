// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.data.ExternalCallKey
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.validation.ExternalCallValidator
import com.digitalasset.canton.platform.execution.ExternalCallMode
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Bytes

import scala.concurrent.ExecutionContext

class ExtensionServiceExternalCallValidator(
    extensionServiceManager: ExtensionServiceManager
)(implicit ec: ExecutionContext)
    extends ExternalCallValidator {

  override def validate(
      key: ExternalCallKey,
      recordedOutput: Bytes,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ExternalCallValidator.Result] =
    extensionServiceManager
      .handleExternalCall(
        extensionId = key.extensionId,
        functionId = key.functionId,
        configHash = key.config,
        input = key.input,
        mode = ExternalCallMode.Validation,
      )
      .map {
        case Left(error) =>
          val requestId = error.requestId.fold("")(id => s", requestId=$id")
          ExternalCallValidator.UnableToValidate(
            s"external-call validation failed with status ${error.statusCode}$requestId"
          )

        case Right(output) =>
          Bytes.fromString(output) match {
            case Left(_) =>
              ExternalCallValidator.UnableToValidate(
                "external-call validation returned non-canonical output"
              )
            case Right(computedOutput) if computedOutput == recordedOutput =>
              ExternalCallValidator.Matched
            case Right(computedOutput) =>
              ExternalCallValidator.Mismatched(
                computedOutput = computedOutput,
                recordedOutput = recordedOutput,
              )
          }
      }
}

object ExtensionServiceExternalCallValidator {
  def create(
      extensionServiceManagerO: Option[ExtensionServiceManager]
  )(implicit ec: ExecutionContext): ExternalCallValidator =
    extensionServiceManagerO match {
      case Some(manager) => new ExtensionServiceExternalCallValidator(manager)
      case None => unconfigured
    }

  private val unconfigured: ExternalCallValidator =
    new ExternalCallValidator {
      override def validate(
          key: ExternalCallKey,
          recordedOutput: Bytes,
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[ExternalCallValidator.Result] =
        FutureUnlessShutdown.pure(
          ExternalCallValidator.UnableToValidate("extension service is not configured")
        )
    }
}
