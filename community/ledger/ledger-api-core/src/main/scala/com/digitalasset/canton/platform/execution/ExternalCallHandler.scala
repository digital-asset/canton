// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.execution

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.engine.ResultNeedExternalCall

/** Handler for external calls made during Daml contract execution. External calls are deterministic
  * extension-service calls that are recorded in the transaction for replay during validation.
  */
trait ExternalCallHandler {
  def handleExternalCall(
      extensionId: String,
      functionId: String,
      configHash: String,
      input: String,
      mode: ExternalCallMode,
  )(implicit tc: TraceContext): FutureUnlessShutdown[Either[ResultNeedExternalCall.Error, String]]
}

object ExternalCallHandler {
  val Unsupported: ExternalCallHandler = new ExternalCallHandler {
    override def handleExternalCall(
        extensionId: String,
        functionId: String,
        configHash: String,
        input: String,
        mode: ExternalCallMode,
    )(implicit
        tc: TraceContext
    ): FutureUnlessShutdown[Either[ResultNeedExternalCall.Error, String]] =
      FutureUnlessShutdown.pure(Left(ResultNeedExternalCall.Error("External calls not supported")))
  }
}
