// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.commitment.BaseDigestProcessor.CheckpointWritten
import com.digitalasset.canton.participant.store.AcsDigestStore.CheckpointType.ReconciliationIntervalBoundary
import com.digitalasset.canton.participant.store.AcsDigestStore.DigestJournal
import com.digitalasset.canton.participant.store.{AcsCommitmentPeriodStore, AcsDigestStore}
import com.digitalasset.canton.protocol.messages.Digest
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

class AcsCommitmentPeriodWriter(
    acsDigestStore: AcsDigestStore,
    acsCommitmentPeriodStore: AcsCommitmentPeriodStore,
    override protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  /** Handles a
    * [[com.digitalasset.canton.participant.commitment.BaseDigestProcessor.CheckpointWritten]] event
    * by reading all participant digests active at a reconciliation tick, converting valid digests
    * into outstanding commitment match periods, and advancing the insertion watermark.
    *
    * Ignores tombstone digests (`digestO == None`) and non-reconciliation checkpoint types.
    * Participant digest updates are processed in paginated batches using
    * [[com.digitalasset.canton.participant.store.AcsDigestStore.DigestJournal#processSnapshotInBatchesE]]
    * to limit memory usage.
    *
    * @param checkpointWritten
    *   The checkpoint event emitted when a fence has been written to storage.
    * @param pageSize
    *   The maximum number of participant digest updates to load and process per batch.
    * @return
    *   A [[com.digitalasset.canton.lifecycle.FutureUnlessShutdown]] completing when all outstanding
    *   periods for the tick have been marked and the insertion watermark has been updated.
    */
  def writeOutstandingAtTick(
      checkpointWritten: CheckpointWritten,
      pageSize: PositiveInt,
  )(implicit traceContext: TraceContext, ec: ExecutionContext): FutureUnlessShutdown[Unit] =
    checkpointWritten match {
      case CheckpointWritten(
            atInclusive,
            reconciliationOffsetTick,
            ReconciliationIntervalBoundary,
          ) =>
        logger.debug(
          s"Writing outstanding commitment match periods for reconciliation tick $atInclusive ..."
        )

        for {
          _ <- DigestJournal.processSnapshotInBatchesE(journal = acsDigestStore.participant)(
            startAtInclusive = reconciliationOffsetTick,
            pageSize = pageSize.unwrap,
          ) { acsDigestUpdates =>
            val outstandingPeriods = acsDigestUpdates.flatMap { acsDigestUpdate =>
              acsDigestUpdate.digestUpdate.digestO.map { rawDigest =>
                val hashedDigest = Digest.hashDigest(rawDigest).getCryptographicEvidence
                AcsCommitmentPeriodStore.CommitmentMatchPeriod.outstanding(
                  participant = acsDigestUpdate.digestUpdate.key,
                  fromExclusive = acsDigestUpdate.digestUpdate.timestamp.immediatePredecessor,
                  toInclusive = atInclusive,
                  hashedDigest = hashedDigest,
                )
              }
            }

            if (outstandingPeriods.nonEmpty) {
              acsCommitmentPeriodStore.markOutstanding(outstandingPeriods)
            } else {
              FutureUnlessShutdown.unit
            }
          }
          _ <- acsCommitmentPeriodStore.increaseInsertionWatermark(
            atInclusive,
            affirmationOnly = false,
          )
          _ = logger.debug(
            s"Outstanding commitment match periods for reconciliation tick $atInclusive has been written."
          )
        } yield ()
      case _ => FutureUnlessShutdown.unit
    }

}
