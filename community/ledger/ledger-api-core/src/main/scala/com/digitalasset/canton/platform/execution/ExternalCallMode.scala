// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.execution

/** Execution modes for external calls. */
sealed abstract class ExternalCallMode(val wireValue: String) extends Product with Serializable

object ExternalCallMode {
  case object Submission extends ExternalCallMode("submission")
  case object Validation extends ExternalCallMode("validation")
}
