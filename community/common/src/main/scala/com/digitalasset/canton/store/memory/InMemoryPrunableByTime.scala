// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.memory

import java.util.concurrent.atomic.AtomicReference

import cats.data.EitherT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.pruning.{PruningPhase, PruningStatus}
import com.digitalasset.canton.store.PrunableByTime
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.OptionUtil

import scala.concurrent.Future

/** Mixin for a in-memory store that provides a thread-safe storage slot for the latest point in time when
  * pruning has started or finished.
  *
  * The pruning method of the store must use [[advancePruningTimestamp]] to signal the start end completion
  * of each pruning.
  */
trait InMemoryPrunableByTime[E] extends PrunableByTime[E] { this: NamedLogging =>

  protected[this] val pruningStatusF: AtomicReference[Option[PruningStatus]] =
    new AtomicReference[Option[PruningStatus]](None)

  override def pruningStatus(implicit
      traceContext: TraceContext
  ): EitherT[Future, E, Option[PruningStatus]] =
    EitherT.pure[Future, E] {
      pruningStatusF.get
    }

  protected[canton] def advancePruningTimestamp(phase: PruningPhase, timestamp: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): EitherT[Future, E, Unit] = EitherT.pure[Future, E] {
    val previousO =
      pruningStatusF.getAndAccumulate(
        Some(PruningStatus(phase, timestamp)),
        OptionUtil.mergeWith(_, _)(Ordering[PruningStatus].max),
      )
    if (logger.underlying.isDebugEnabled && phase == PruningPhase.Started) {
      previousO match {
        case Some(previous) if previous.timestamp > timestamp =>
          logger.debug(
            s"Pruning at $timestamp started after another later pruning at ${previous.timestamp}."
          )
        case _ =>
      }
    }
  }
}
