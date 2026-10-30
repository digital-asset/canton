// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.ingestion

import com.daml.ledger.api.v2.offset_checkpoint.OffsetCheckpoint
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.tea.projection.ProjectionEvent
import com.digitalasset.canton.tracing.{TraceContext, Traced}

private[ingestion] object OffsetCheckpoints {

  /** Turns a Ledger API offset checkpoint into a projection event without deltas, so that the
    * projection records progress even when the stream carries nothing to ingest. Otherwise, a
    * (re)connection would resume from an old offset, which may have been pruned since.
    *
    * @return
    *   None if the checkpoint carries no record time
    */
  def toProjectionEvent(checkpoint: OffsetCheckpoint)(implicit
      traceContext: TraceContext
  ): Option[Traced[ProjectionEvent]] =
    checkpoint.synchronizerTimes
      .flatMap(_.recordTime)
      .flatMap(CantonTimestamp.fromProtoTimestamp(_).toOption)
      .maxOption
      .map(ts => Traced(ProjectionEvent.Checkpoint(checkpoint.offset, ts)))
}
