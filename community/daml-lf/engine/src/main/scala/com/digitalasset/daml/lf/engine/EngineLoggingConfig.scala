// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import org.slf4j.event.Level

/** Control logging of Daml `debug` statements
  *
  * @param enabled
  *   set to true to enable
  * @param logLevel
  *   the log level to use
  * @param matching
  *   if non-empty, then only output lines that match one of the given string. If the supporting *
  *   and ? wildcard characters are used, then the string must match, otherwise, the string must be
  *   included in the log message.
  */
final case class EngineLoggingConfig(
    enabled: Boolean = true,
    logLevel: Level = Level.DEBUG,
    matching: Seq[String] = Seq.empty,
)
