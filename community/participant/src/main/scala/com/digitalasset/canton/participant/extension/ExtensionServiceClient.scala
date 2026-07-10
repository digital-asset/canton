// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.platform.execution.ExternalCallMode
import com.digitalasset.canton.tracing.TraceContext

/** Error information from external call failures.
  *
  * `message` must stay client-safe: fixed descriptions, identifiers, and non-sensitive operational
  * limits only -- never extension-service response bodies or credentials. The submission path
  * forwards the whole error to the submitting ledger API client in the command rejection, so
  * anything placed here becomes client-visible. (The Phase-3 validation path reduces the error to
  * status code and request id before it reaches confirmation responses.)
  */
final case class ExtensionCallError(
    statusCode: Int,
    message: String,
    requestId: Option[String],
    retryable: Boolean,
) extends PrettyPrinting {
  override protected def pretty: Pretty[ExtensionCallError] = prettyOfClass(
    param("status code", _.statusCode),
    param("message", _.message.doubleQuoted),
    paramIfDefined("request id", _.requestId.map(_.singleQuoted)),
    param("retryable", _.retryable),
  )
}

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
