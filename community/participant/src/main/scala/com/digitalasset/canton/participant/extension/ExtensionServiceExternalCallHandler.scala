// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.platform.execution.{ExternalCallHandler, ExternalCallMode}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.engine.ResultNeedExternalCall

import scala.concurrent.ExecutionContext

/** ExternalCallHandler implementation that delegates to ExtensionServiceManager.
  *
  * This bridges the platform ExternalCallHandler interface with the participant's
  * ExtensionServiceManager for handling external calls during command submission.
  */
class ExtensionServiceExternalCallHandler(
    extensionServiceManager: ExtensionServiceManager
)(implicit ec: ExecutionContext)
    extends ExternalCallHandler {

  override def handleExternalCall(
      extensionId: String,
      functionId: String,
      configHash: String,
      input: String,
      mode: ExternalCallMode,
  )(implicit tc: TraceContext): FutureUnlessShutdown[Either[ResultNeedExternalCall.Error, String]] =
    extensionServiceManager
      .handleExternalCall(
        extensionId = extensionId,
        functionId = functionId,
        configHash = configHash,
        input = input,
        mode = mode,
      )
      .map(_.left.map { extensionError =>
        // Surfaces to the submitting ledger API client in the command rejection, wrapped by the
        // engine's "External call execution failed (extensionId=..., functionId=...)" rendering,
        // and is not distributed to other nodes on this path -- so the whole error is forwarded.
        // The message-content limits documented on ExtensionCallError keep it client-safe.
        ResultNeedExternalCall.Error(extensionError.toString)
      })
}

object ExtensionServiceExternalCallHandler {

  /** Create an ExternalCallHandler from an optional ExtensionServiceManager.
    *
    * @param extensionServiceManagerO
    *   Optional ExtensionServiceManager
    * @return
    *   ExternalCallHandler that delegates to the manager, or Unsupported if None
    */
  def create(
      extensionServiceManagerO: Option[ExtensionServiceManager]
  )(implicit ec: ExecutionContext): ExternalCallHandler =
    extensionServiceManagerO match {
      case Some(manager) => new ExtensionServiceExternalCallHandler(manager)
      case None => ExternalCallHandler.Unsupported
    }
}
