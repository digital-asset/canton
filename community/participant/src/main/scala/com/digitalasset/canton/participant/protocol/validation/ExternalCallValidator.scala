// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.data.ExternalCallKey
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Bytes

trait ExternalCallValidator {
  def validate(
      key: ExternalCallKey,
      recordedOutput: Bytes,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ExternalCallValidator.Result]
}

object ExternalCallValidator {
  sealed trait Result extends Product with Serializable
  case object Matched extends Result
  final case class Mismatched(computedOutput: Bytes, recordedOutput: Bytes) extends Result
  final case class UnableToValidate(reason: String) extends Result
}
