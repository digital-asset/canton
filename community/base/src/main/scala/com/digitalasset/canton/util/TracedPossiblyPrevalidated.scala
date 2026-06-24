// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.tracing.{TraceContext, Traced}

/** Tag an object to have already passed some validation steps
  *
  * We use this specifically to optimistically validate signatures against older topology snapshots.
  * If they pass validation (signature is valid and key is valid), we can then skip the more
  * expensive validation steps and only validate the correct key usage.
  */
final case class TracedPossiblyPrevalidated[E](value: E, prevalidated: Boolean)(implicit
    val traceContext: TraceContext
) {
  def map[F](fn: E => F): TracedPossiblyPrevalidated[F] =
    TracedPossiblyPrevalidated(fn(value), prevalidated)

  def tracedValue: Traced[E] = Traced(value)(traceContext)

  def withTraceContext[F](fn: TraceContext => E => F): F = fn(traceContext)(value)

}

object TracedPossiblyPrevalidated {
  def notValidated[T](value: T)(implicit
      traceContext: TraceContext
  ): TracedPossiblyPrevalidated[T] =
    TracedPossiblyPrevalidated(value, prevalidated = false)
}
