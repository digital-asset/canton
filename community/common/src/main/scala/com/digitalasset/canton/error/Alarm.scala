// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.daml.error.ErrorCategory.MaliciousOrFaultyBehaviour
import com.daml.error.{ErrorClass, ErrorCode}
import com.digitalasset.canton.logging.ErrorLoggingContext

abstract class Alarm(override val cause: String, override val throwableO: Option[Throwable] = None)(
    implicit override val code: AlarmErrorCode
) extends BaseCantonError {

  /** Report the alarm to the logger. */
  def report()(implicit loggingContext: ErrorLoggingContext): Unit = logWithContext()
}

abstract class AlarmErrorCode(id: String)(implicit parent: ErrorClass)
    extends ErrorCode(id, MaliciousOrFaultyBehaviour) {
  implicit override val code: AlarmErrorCode = this
}
