// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

/** [[AlarmStreamer]] is responsible for streaming alarms, which are events that signal abnormal system operation as described in:
  * https://engineering.da-int.net/docs/platform-architecture-handbook/arch/ledger-sync/api.html?highlight=alarm#alarms-stream
  */
trait AlarmStreamer {
  // TODO(i8744): remove this class

  def alarm(message: String)(implicit traceContext: TraceContext): Future[Unit]

  def alarm(cantonError: CantonError): Future[Unit]
}
