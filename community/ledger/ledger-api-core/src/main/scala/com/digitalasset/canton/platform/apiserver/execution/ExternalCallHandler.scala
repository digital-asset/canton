// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.engine.ExternalCallError

/** Handler for external calls made during Daml contract execution.
  * External calls are deterministic HTTP calls to extension services
  * that are recorded in the transaction for replay during validation.
  */
trait ExternalCallHandler {
  def handleExternalCall(
      extensionId: String,
      functionId: String,
      configHash: String,
      input: String,
      mode: String,
      commandId: String,
  )(implicit tc: TraceContext): FutureUnlessShutdown[Either[ExternalCallError, String]]
}

object ExternalCallHandler {
  val notSupported: ExternalCallHandler = new ExternalCallHandler {
    override def handleExternalCall(
        extensionId: String,
        functionId: String,
        configHash: String,
        input: String,
        mode: String,
        commandId: String,
    )(implicit tc: TraceContext): FutureUnlessShutdown[Either[ExternalCallError, String]] =
      FutureUnlessShutdown.pure(Left(ExternalCallError(501, "External calls not supported", None)))
  }
}
