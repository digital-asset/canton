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
        // and is not distributed to other nodes on this path. Errors carrying actionable
        // feedback are forwarded whole; communication problems between the participant and the
        // extension service surface only generically, since the client cannot act on their
        // details. The full error is always logged by ExtensionServiceManager.
        val clientMessage =
          if (extensionError.clientActionable) extensionError.toString
          else ExtensionServiceExternalCallHandler.genericClientMessage(extensionError)
        ResultNeedExternalCall.Error(clientMessage)
      })
}

object ExtensionServiceExternalCallHandler {

  /** The client-facing rendering for errors that are not actionable for the client: only the retry
    * status and the request id (for correlation with the participant's log) are exposed.
    */
  private[extension] def genericClientMessage(error: ExtensionCallError): String = {
    val requestId = error.requestId.fold("")(id => s", request id = '$id'")
    s"External call failed (retryable = ${error.retryable}$requestId)"
  }

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
