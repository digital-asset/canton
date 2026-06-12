// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
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
        extensionId,
        functionId,
        configHash,
        input,
        mode,
      )
      .map(_.left.map { extensionError =>
        ResultNeedExternalCall.Error(sanitizedEngineError(extensionError))
      })

  private def sanitizedEngineError(extensionError: ExtensionCallError): String = {
    val requestId = extensionError.requestId.fold("")(requestId => s", requestId=$requestId")
    s"External call failed with status ${extensionError.statusCode}$requestId"
  }
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
