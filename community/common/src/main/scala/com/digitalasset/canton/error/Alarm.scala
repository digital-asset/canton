// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.daml.error.ErrorCategory.MaliciousOrFaultyBehaviour
import com.daml.error.{ErrorClass, ErrorCode}
import com.digitalasset.canton.logging.ErrorLoggingContext

trait BaseAlarm extends BaseCantonError {
  override def code: AlarmErrorCode

  /** Report the alarm to the logger. */
  def report()(implicit loggingContext: ErrorLoggingContext): Unit = logWithContext()

  /** Reports the alarm to the logger.
    * @return this alarm
    */
  def reported()(implicit loggingContext: ErrorLoggingContext): this.type = {
    report()
    this
  }
}

abstract class Alarm(override val cause: String, override val throwableO: Option[Throwable] = None)(
    implicit override val code: AlarmErrorCode
) extends BaseAlarm

abstract class AlarmErrorCode(id: String)(implicit parent: ErrorClass)
    extends ErrorCode(id, MaliciousOrFaultyBehaviour) {
  implicit override val code: AlarmErrorCode = this
}
