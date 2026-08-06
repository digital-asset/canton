// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.participant.commitment.BaseDigestProcessor.{
  CheckpointToBeWritten,
  DigestAccumulator_Input,
}
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

/** A [[DigestAccumulator]] processes
  * [[com.digitalasset.canton.participant.commitment.BaseDigestProcessor.Classification]]s or
  * [[com.digitalasset.canton.participant.commitment.BaseDigestProcessor.CheckpointFence]]s and
  * emits whenever a checkpoint has been written.
  *
  * Whenever
  * [[com.digitalasset.canton.participant.commitment.BaseDigestProcessor.CheckpointToBeWritten]] is
  * emitted, the digest accumulator implementation MUST guarantee that all changes to digests up to
  * [[com.digitalasset.canton.participant.commitment.BaseDigestProcessor.CheckpointToBeWritten.offsetInclusive]]
  * have been persisted.
  */
trait DigestAccumulator {
  def flow()(implicit
      traceContext: TraceContext
  ): Flow[DigestAccumulator_Input, CheckpointToBeWritten, NotUsed]

}
