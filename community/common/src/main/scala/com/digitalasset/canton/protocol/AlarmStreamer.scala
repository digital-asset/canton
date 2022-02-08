// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

/** [[AlarmStreamer]] is responsible for streaming alarms, which are events that signal abnormal system operation as described in:
  * https://engineering.da-int.net/docs/platform-architecture-handbook/arch/ledger-sync/api.html?highlight=alarm#alarms-stream
  */
trait AlarmStreamer {

  /** Stream alarm event signaling that the referred error happened
    * @param throwable The Throwable that caused the abnormal behavior
    */
  def alarm(throwable: Throwable)(implicit traceContext: TraceContext): Future[Unit]

  def alarm(message: String)(implicit traceContext: TraceContext): Future[Unit]
}
