// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import cats.Functor
import com.digitalasset.nonempty.NonEmpty

/** Wrapper for items that have a related list of [[TraceContext]]s.
  *
  * Intended for where items with possibly different [[TraceContext]]s are accumulated into batches
  * or conflated where we want to keep the relation with the original [[TraceContext]]s so that the
  * [[TraceContext]]s can later be combined into a single [[TraceContext]], e.g., using
  * [[TraceContext.ofBatch]].
  */
final case class TracedMany[+A](value: A)(val traceContexts: NonEmpty[Seq[TraceContext]]) {
  def map[B](f: A => B): TracedMany[B] = TracedMany(f(value))(traceContexts)

  def traverse[F[_], B](f: A => F[B])(implicit F: Functor[F]): F[TracedMany[B]] =
    F.map(f(value))(TracedMany(_)(traceContexts))

  def accumulate[B](f: A => B)(implicit traceContext: TraceContext): TracedMany[B] =
    TracedMany(f(value))(traceContext +-: traceContexts)

  def accumulateTraced[B](f: A => Traced[B]): TracedMany[B] = {
    val tracedB = f(value)
    TracedMany(tracedB.value)(tracedB.traceContext +-: traceContexts)
  }
}

object TracedMany {
  def from[A](x: A)(implicit traceContext: TraceContext): TracedMany[A] =
    TracedMany(x)(NonEmpty(Seq, traceContext))

  def fromTraced[A](traced: Traced[A]): TracedMany[A] =
    TracedMany(traced.value)(NonEmpty(Seq, traced.traceContext))
}
