// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.syntax.option.*
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.commitment.Timepoint
import com.digitalasset.canton.participant.store.AcsCommitmentSenderWatermarkStore
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference

class InMemoryAcsCommitmentSenderWatermarkStore(
    override protected val loggerFactory: NamedLoggerFactory
) extends AcsCommitmentSenderWatermarkStore
    with NamedLogging {

  private val watermark: AtomicReference[Option[Timepoint]] = new AtomicReference(None)

  override def lookupWatermark()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Timepoint]] =
    FutureUnlessShutdown.pure(watermark.get())

  override def increaseWatermark(
      timepoint: Timepoint
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.pure {
    watermark.updateAndGet {
      case None => timepoint.some
      case existing @ Some(existingTp) =>
        if (existingTp.offset <= timepoint.offset) timepoint.some
        else existing
    }.discard
  }
}
