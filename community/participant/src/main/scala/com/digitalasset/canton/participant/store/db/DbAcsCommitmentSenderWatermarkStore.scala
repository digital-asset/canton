// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.commitment.Timepoint
import com.digitalasset.canton.participant.store.AcsCommitmentSenderWatermarkStore
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.tracing.TraceContext

class DbAcsCommitmentSenderWatermarkStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    indexedSynchronizer: IndexedSynchronizer,
) extends AcsCommitmentSenderWatermarkStore
    with DbStore {
  import storage.api.*

  override def lookupWatermark()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Timepoint]] = {
    val query =
      sql"select watermark_offset, watermark_timestamp from par_acs_commitment_sender_watermark where synchronizer_idx = $indexedSynchronizer"
        .as[Timepoint]
        .headOption

    storage.query(query, functionFullName)
  }

  override def increaseWatermark(
      tp: Timepoint
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val upsert = storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into par_acs_commitment_sender_watermark as w (synchronizer_idx, watermark_offset, watermark_timestamp) values ($indexedSynchronizer, ${tp.offset}, ${tp.recordTime})
              on conflict (synchronizer_idx) do
                update set watermark_offset = excluded.watermark_offset, watermark_timestamp = excluded.watermark_timestamp
                where w.watermark_offset <= excluded.watermark_offset"""
      case _: DbStorage.Profile.H2 =>
        sqlu"""merge into par_acs_commitment_sender_watermark w using (values($indexedSynchronizer, ${tp.offset}, ${tp.recordTime})) s(synchronizer_idx, watermark_offset, watermark_timestamp)
               on w.synchronizer_idx = s.synchronizer_idx
              when matched and w.watermark_offset <= s.watermark_offset then update set watermark_offset = s.watermark_offset, watermark_timestamp = s.watermark_timestamp
              when not matched then insert values (s.synchronizer_idx, s.watermark_offset, s.watermark_timestamp)"""
    }

    storage.update_(upsert, functionFullName)
  }
}
