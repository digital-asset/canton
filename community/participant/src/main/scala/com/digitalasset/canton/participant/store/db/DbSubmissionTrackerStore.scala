// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.SubmissionTrackerStore
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.db.DbPrunableByTimeSynchronizer
import com.digitalasset.canton.store.{
  IndexedPhysicalSynchronizer,
  IndexedString,
  PrunableByTimeParameters,
}
import com.digitalasset.canton.tracing.TraceContext
import slick.jdbc.SetParameter
import slick.jdbc.canton.ActionBasedSQLInterpolation.Implicits.actionBasedSQLInterpolationCanton

import scala.concurrent.ExecutionContext

class DbSubmissionTrackerStore(
    override protected val storage: DbStorage,
    override val indexedSynchronizer: IndexedPhysicalSynchronizer,
    batchingParametersConfig: PrunableByTimeParameters,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends SubmissionTrackerStore
    with DbPrunableByTimeSynchronizer[IndexedPhysicalSynchronizer]
    with DbStore {

  override protected[this] implicit def setParameterIndexedSynchronizer
      : SetParameter[IndexedPhysicalSynchronizer] = IndexedString.setParameterIndexedString
  override protected[this] def partitionColumn: String = "physical_synchronizer_idx"

  override protected def batchingParameters: Option[PrunableByTimeParameters] = Some(
    batchingParametersConfig
  )

  override def registerFreshRequest(
      rootHash: RootHash,
      requestId: RequestId,
      maxSequencingTime: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] = {
    // TODO(i13492): use batching to optimize processing

    val dbRequestId = requestId.unwrap

    val insertQuery =
      sqlu"""insert into par_fresh_submitted_transaction(
                 physical_synchronizer_idx,
                 root_hash_hex,
                 request_id,
                 max_sequencing_time)
             values ($indexedSynchronizer, $rootHash, $dbRequestId, $maxSequencingTime)
             on conflict do nothing"""

    val selectQuery =
      sql"""select count(*)
              from par_fresh_submitted_transaction
              where physical_synchronizer_idx=$indexedSynchronizer and root_hash_hex=$rootHash and request_id=$dbRequestId"""
        .as[Int]
        .headOption

    val f = for {
      _nbRows <- storage.update(insertQuery, "check freshness of submitted transaction")
      count <- storage.query(selectQuery, "lookup submitted transaction")
    } yield count.getOrElse(0) > 0

    f
  }

  override protected[this] def pruning_status_table: String =
    "par_fresh_submitted_transaction_pruning"

  override protected[canton] def doPrune(
      beforeAndIncluding: CantonTimestamp,
      lastPruning: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] = {
    val deleteQuery =
      sqlu"""delete from par_fresh_submitted_transaction
             where physical_synchronizer_idx = $indexedSynchronizer and max_sequencing_time <= $beforeAndIncluding"""

    storage.queryAndUpdate(deleteQuery, "prune par_fresh_submitted_transaction")
  }

  override def purge()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    storage.update_(
      sqlu"""delete from par_fresh_submitted_transaction
             where physical_synchronizer_idx = $indexedSynchronizer""",
      "purge par_fresh_submitted_transaction",
    )

  override def deleteDataChunk(chunkSize: PositiveInt)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] = for {
    // We materialize the last chunk key rather than using a subquery within the delete statement, as the update must
    // be idempotent.
    lastKeyInChunk <- storage.query(
      sql"""
        select root_hash_hex from (
          select root_hash_hex from par_fresh_submitted_transaction where physical_synchronizer_idx = $indexedSynchronizer
          order by root_hash_hex ASC #${storage.limit(chunkSize.value)}
        ) as chunk
        order by root_hash_hex DESC #${storage.limit(1)}
        """.as[RootHash].headOption,
      functionFullName,
    )
    deleted <-
      lastKeyInChunk match {
        case Some(rootHash) =>
          storage.update(
            sql"""delete from par_fresh_submitted_transaction
                    where physical_synchronizer_idx = $indexedSynchronizer
                    and root_hash_hex <= $rootHash
                    """.asUpdate,
            functionFullName,
          )
        case None => FutureUnlessShutdown.pure(0)
      }
  } yield {
    logger.info(
      if (deleted > 0)
        s"Deleted chunk of $deleted from submission tracker store for ${indexedSynchronizer.psid}."
      else s"No chunk to delete from submission tracker store for ${indexedSynchronizer.psid}."
    )
    deleted > 0
  }

  override def size(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Int] = {
    val selectQuery =
      sql"""select count(*)
          from par_fresh_submitted_transaction
          where physical_synchronizer_idx = $indexedSynchronizer
          """
        .as[Int]
        .headOption

    storage.query(selectQuery, "count number of entries").map(_.getOrElse(0))
  }

  override def deleteSince(
      including: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val deleteQuery =
      sqlu"""delete from par_fresh_submitted_transaction
         where physical_synchronizer_idx = $indexedSynchronizer and request_id >= $including"""

    storage.update_(deleteQuery, "cleanup par_fresh_submitted_transaction")
  }
}
