// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.platform.execution.ExternalCallMode
import com.digitalasset.canton.tracing.TraceContext

/** Error information from external call failures. */
final case class ExtensionCallError(
    statusCode: Int,
    message: String,
    requestId: Option[String],
    retryable: Boolean,
)

/** Result of validating an extension service at startup. */
sealed trait ExtensionValidationResult extends Product with Serializable

object ExtensionValidationResult {
  case object Valid extends ExtensionValidationResult

  final case class Invalid(errors: Seq[String]) extends ExtensionValidationResult
}

/** Trait for extension service clients.
  *
  * Each configured extension service has its own client that handles HTTP communication with
  * connection pooling managed at the participant level.
  */
trait ExtensionServiceClient {

  /** The extension identifier (from Canton config) */
  def extensionId: String

  /** Make an external call to the extension service.
    *
    * @param functionId
    *   Function identifier within the extension
    * @param configHash
    *   Configuration hash (hex) for version validation
    * @param input
    *   Input data (hex)
    * @param mode
    *   Execution mode
    * @return
    *   Either an error or the response body (hex)
    */
  def call(
      functionId: String,
      configHash: String,
      input: String,
      mode: ExternalCallMode,
  )(implicit tc: TraceContext): FutureUnlessShutdown[Either[ExtensionCallError, String]]
}
