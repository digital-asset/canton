// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import com.digitalasset.canton.logging.SuppressingLogger
import com.digitalasset.canton.tracing.TraceContext

trait SuppressingLogging extends NamedLogging {
  val loggerFactory: SuppressingLogger = SuppressingLogger(getClass)
  implicit def traceContext: TraceContext = TraceContext.empty
}
