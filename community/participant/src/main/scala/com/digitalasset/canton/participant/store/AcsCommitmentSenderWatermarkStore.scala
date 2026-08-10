// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.participant.commitment.Timepoint
import com.digitalasset.canton.tracing.TraceContext

trait AcsCommitmentSenderWatermarkStore { this: NamedLogging =>
  def lookupWatermark()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Timepoint]]

  def increaseWatermark(timepoint: Timepoint)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]
}
