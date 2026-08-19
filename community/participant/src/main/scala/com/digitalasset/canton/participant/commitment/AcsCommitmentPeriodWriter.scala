// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.commitment.BaseDigestProcessor.CheckpointToBeWritten
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
    * by reading all participant digests active at a reconciliation tick and converting valid
    * digests into outstanding commitment match periods.
    *
    * Ignores tombstone digests
    * ([[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigest.digestO]] == None) and
    * non-reconciliation checkpoint types. Participant digest updates are processed in paginated
    * batches using
    * [[com.digitalasset.canton.participant.store.AcsDigestStore.DigestJournal.processSnapshotInBatchesE]]
    * to limit memory usage.
    *
    * @param checkpointToBeWritten
    *   The checkpoint event emitted when a fence is about to be written to storage.
    * @param reconciliationWatermark
    *   The watermark up to where reconciliation checkpoints have been previously persisted. This is
    *   used to cap the
    *   [[com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.CommitmentMatchPeriod.fromExclusive]]
    *   of digests that have not changed since.
    * @param pageSize
    *   The maximum number of participant digest updates to load and process per batch.
    * @return
    *   A [[com.digitalasset.canton.lifecycle.FutureUnlessShutdown]] completing when all outstanding
    *   periods for the tick have been marked. Returns true if the checkpoint is a reconciliation
    *   tick after the `reconciliationWatermark`.
    */
  def writeOutstandingAtTick(
      checkpointToBeWritten: CheckpointToBeWritten,
      reconciliationWatermark: CantonTimestamp,
      pageSize: PositiveInt,
  )(implicit traceContext: TraceContext, ec: ExecutionContext): FutureUnlessShutdown[Boolean] = {
    val CheckpointToBeWritten(atInclusive, reconciliationOffsetTick, checkpointType) =
      checkpointToBeWritten
    if (atInclusive <= reconciliationWatermark) {
      logger.debug(
        s"Skip writing outstanding commitment match periods for checkpoint $atInclusive, which is before or at the reconciliation watermark $reconciliationWatermark."
      )
      FutureUnlessShutdown.pure(false)
    } else
      checkpointType match {
        case ReconciliationIntervalBoundary =>
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
                    fromExclusive = acsDigestUpdate.digestUpdate.timestamp.immediatePredecessor
                      .max(reconciliationWatermark),
                    toInclusive = atInclusive,
                    hashedDigest = hashedDigest,
                  )
                }
              }

              acsCommitmentPeriodStore.markOutstanding(outstandingPeriods)
            }
            _ = logger.debug(
              s"Outstanding commitment match periods for reconciliation tick $atInclusive has been written."
            )
          } yield true
        case _ => FutureUnlessShutdown.pure(false)
      }
  }
}
