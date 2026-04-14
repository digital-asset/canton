// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

import scala.collection.mutable.Buffer
import com.digitalasset.canton.logging.LoggerNameFromClass
import com.digitalasset.daml.lf.data.Ref.Location

class RecordingMachineLogger(underlying: MachineLogger) extends MachineLogger {
  private val messages: Buffer[String] = Buffer.empty

  override def trace(message: String, location: Option[Location])(
    implicit loggerName: LoggerNameFromClass
  ): Unit = {
    llTrace(message)
    underlying.trace(message, location)
  }

  override def warn(message: String, location: Option[Location])(
    implicit loggerName: LoggerNameFromClass
  ): Unit = underlying.warn(message, location)

  def llTrace(message: String): Unit = {
    messages += message
  }

  def tracePartialFunction[X, Y](message: String, pf: PartialFunction[X, Y]): PartialFunction[X, Y] = {
    case x if { llTrace(message); pf.isDefinedAt(x) } => pf(x)
  }

  def recordedMessages: Seq[String] = messages.toSeq
}
