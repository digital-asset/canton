// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.{CantonTimestamp, ContractReassignment}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.ActiveContractStore.ChangeType
import com.digitalasset.canton.participant.store.PartyReplicationIndexingStore
import com.digitalasset.canton.participant.store.PartyReplicationIndexingStore.{
  ActivationChange,
  ActivationChangeBatchEntry,
  ContractActivationChangeBatch,
  Watermark,
}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{ReassignmentCounter, checked}

import scala.concurrent.ExecutionContext

class DbPartyReplicationIndexingStore(
    override protected val storage: DbStorage,
    indexedSynchronizer: IndexedSynchronizer,
    override protected val pauseIndexingDuringOnPR: Boolean,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends DbStore
    with PartyReplicationIndexingStore {
  import storage.api.*
  import storage.converters.*
  import DbStorage.Implicits.BuilderChain.*

  override def addImportedContractActivations(
      watermark: Watermark,
      contractActivationsToIndex: NonEmpty[Seq[ContractReassignment]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val watermarked = contractActivationsToIndex.zipWithIndex.map {
      case (ContractReassignment(contractInstance, _, _, reassignmentCounter), index) =>
        (
          Watermark(
            watermark.timestamp,
            watermark.counter + checked(NonNegativeLong.tryCreate(index.toLong)),
          ),
          contractInstance.contractId,
          reassignmentCounter,
        )
    }
    val insertQuery =
      s"""insert into par_party_replication_indexing(synchronizer_idx, ts, change_counter, contract_id, change, reassignment_counter)
         values (?, ?, ?, ?, CAST(? as change_type), ?)
         on conflict do nothing"""
    val insertAll =
      DbStorage.bulkOperation_(insertQuery, watermarked, storage.profile) { pp => element =>
        val (watermark, contractId, reassignmentCounter) = element
        pp >> indexedSynchronizer
        pp >> watermark
        pp >> contractId
        pp >> ChangeType.Activation
        pp >> reassignmentCounter
      }
    storage.queryAndUpdate(insertAll, functionFullName)
  }

  override def addContractActivationChanges(
      timestamp: CantonTimestamp,
      changes: Seq[(LfContractId, (ChangeType, ReassignmentCounter))],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = if (pauseIndexingDuringOnPR)
    FutureUnlessShutdown.unit
  else {
    val watermarked = changes.zipWithIndex.map {
      case ((cid, (change, reassignmentCounter)), index) =>
        (
          Watermark(timestamp, checked(NonNegativeLong.tryCreate(index.toLong))),
          cid,
          change,
          reassignmentCounter,
        )
    }
    val insertQuery =
      s"""insert into par_party_replication_indexing(synchronizer_idx, ts, change_counter, contract_id, change, reassignment_counter)
         values (?, ?, ?, ?, CAST(? as change_type), ?)
         on conflict do nothing"""
    val insertAll =
      DbStorage.bulkOperation_(insertQuery, watermarked, storage.profile) { pp => element =>
        val (watermark, contractId, change, reassignmentCounter) = element
        pp >> indexedSynchronizer
        pp >> watermark
        pp >> contractId
        pp >> change
        pp >> reassignmentCounter
      }
    storage.queryAndUpdate(insertAll, functionFullName)
  }

  override def consumeNextActivationChangesBatch(maxBatchSize: PositiveInt)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[ContractActivationChangeBatch]] = for {
    previousWatermarkO <- storage.query(
      sql"""
            select ts, change_counter
            from par_party_replication_indexing_watermarks
            where synchronizer_idx = $indexedSynchronizer""".as[Watermark].headOption,
      functionFullName + " look up indexing watermark",
    )
    activationChanges <- storage
      .query(
        (sql"""
        select ts, change_counter, contract_id, change, reassignment_counter
        from par_party_replication_indexing
        where synchronizer_idx = $indexedSynchronizer""" ++ previousWatermarkO.fold(sql"") {
          case Watermark(ts, counter) => sql""" and (ts, change_counter) > ($ts, $counter)"""
        } ++ sql"""
        order by synchronizer_idx, ts, change_counter
        #${storage.limit(maxBatchSize.unwrap)}
        """).as[
          (
              Watermark,
              LfContractId,
              ChangeType,
              ReassignmentCounter,
          )
        ],
        functionFullName,
      )
      .map(_.map { case (watermark, cid, change, reassignmentCounter) =>
        ActivationChangeBatchEntry(watermark, cid, change, reassignmentCounter)
      })
    nextBatchO <- NonEmpty
      .from(activationChanges)
      .fold(FutureUnlessShutdown.pure(Option.empty[ContractActivationChangeBatch])) {
        activationChangesNE =>
          val trimmedActivationChangesNE = trimActivationChangesBatch(activationChangesNE)
          val batchWatermark = trimmedActivationChangesNE.last1 match {
            case ActivationChangeBatchEntry(watermark, _, _, _) => watermark
          }
          val Watermark(ts, counter) = batchWatermark
          val upsertWatermark = storage.profile match {
            case _: DbStorage.Profile.Postgres =>
              sqlu"""
            insert into par_party_replication_indexing_watermarks(synchronizer_idx, ts, change_counter)
            values ($indexedSynchronizer, $ts, $counter)
            on conflict (synchronizer_idx) do update set ts = $ts, change_counter = $counter
            """
            case _: DbStorage.Profile.H2 =>
              sqlu"""
            merge into par_party_replication_indexing_watermarks using dual
              on (synchronizer_idx = $indexedSynchronizer)
              when matched then
                update set ts = $ts, change_counter = $counter
              when not matched then
                insert (synchronizer_idx, ts, change_counter)
                values ($indexedSynchronizer, $ts, $counter)
            """
          }
          storage.update_(upsertWatermark, functionFullName + " upsert indexing watermark").map {
            _ =>
              Some(
                ContractActivationChangeBatch(
                  trimmedActivationChangesNE.map {
                    case ActivationChangeBatchEntry(_, contractId, change, reassignmentCounter) =>
                      contractId -> (change, reassignmentCounter)
                  },
                  batchWatermark,
                )
              )
          }
      }
  } yield nextBatchO

  override def markContractActivationChangesAsIndexed(watermark: Watermark)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val Watermark(timestamp, counter) = watermark
    val upsertWatermark = storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        sqlu"""
            insert into par_party_replication_indexed_watermarks(synchronizer_idx, ts, change_counter)
            values ($indexedSynchronizer, $timestamp, $counter)
            on conflict (synchronizer_idx) do update set ts = $timestamp, change_counter = $counter
            """
      case _: DbStorage.Profile.H2 =>
        sqlu"""
            merge into par_party_replication_indexed_watermarks using dual
              on (synchronizer_idx = $indexedSynchronizer)
              when matched then
                update set ts = $timestamp, change_counter = $counter
              when not matched then
                insert (synchronizer_idx, ts, change_counter)
                values ($indexedSynchronizer, $timestamp, $counter)
            """
    }
    storage.update_(upsertWatermark, functionFullName)
  }

  override def purgeContractActivationChanges()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val delete =
      sqlu"delete from par_party_replication_indexing where synchronizer_idx = $indexedSynchronizer"
    storage.update_(delete, functionFullName)
  }

  override def deleteSince(deleteStartingAtInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = for {
    _ <- storage
      .update(
        sqlu"""delete from par_party_replication_indexing
             where synchronizer_idx = $indexedSynchronizer and ts >= $deleteStartingAtInclusive""",
        functionFullName,
      )
      .map(count =>
        logger.debug(
          s"DeleteSince on $deleteStartingAtInclusive removed at least $count ACS entries"
        )
      )
    maxWatermarkO <- storage.query(
      sql"""
            select ts, change_counter
            from par_party_replication_indexing
            where synchronizer_idx = $indexedSynchronizer
            order by ts desc, change_counter desc #${storage.limit(1)}
            """.as[Watermark].headOption,
      functionFullName + " look up indexing watermark",
    )
    // Delete or update watermarks if necessary
    _ <- maxWatermarkO.fold {
      storage
        .update_(
          sqlu"""delete from par_party_replication_indexing_watermarks where synchronizer_idx = $indexedSynchronizer""",
          functionFullName + " delete indexing watermark",
        )
        .flatMap(_ =>
          storage.update_(
            sqlu"""delete from par_party_replication_indexed_watermarks where synchronizer_idx = $indexedSynchronizer""",
            functionFullName + " delete indexed watermark",
          )
        )
    } { case Watermark(ts, counter) =>
      storage
        .update_(
          sqlu"""update par_party_replication_indexing_watermarks
                 set ts=$ts, change_counter=$counter
                 where synchronizer_idx = $indexedSynchronizer and (ts, change_counter) > ($ts, $counter)""",
          functionFullName + " update indexing watermark",
        )
        .flatMap(_ =>
          storage.update_(
            sqlu"""update par_party_replication_indexed_watermarks
                   set ts=$ts, change_counter=$counter
                   where synchronizer_idx = $indexedSynchronizer and (ts, change_counter) > ($ts, $counter)""",
            functionFullName + " update indexed watermark",
          )
        )
    }
  } yield ()

  protected[store] override def listContractActivationChanges()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[ActivationChange]] = storage
    .query(
      sql"""
        select ts, change_counter, contract_id, change,
          coalesce((select (indexing_watermark.ts, indexing_watermark.change_counter) >= (activation_change.ts, activation_change.change_counter)
           from par_party_replication_indexing_watermarks indexing_watermark
           where indexing_watermark.synchronizer_idx = $indexedSynchronizer), false) as is_indexing,
          coalesce((select (indexed_watermark.ts, indexed_watermark.change_counter) >= (activation_change.ts, activation_change.change_counter)
           from par_party_replication_indexed_watermarks indexed_watermark
           where indexed_watermark.synchronizer_idx = $indexedSynchronizer), false) as is_indexed,
          reassignment_counter
        from par_party_replication_indexing activation_change
        where synchronizer_idx = $indexedSynchronizer
        order by synchronizer_idx, ts, change_counter
        """
        .as[(Watermark, LfContractId, ChangeType, Boolean, Boolean, ReassignmentCounter)]
        .map(_.map { case (watermark, cid, change, isIndexing, isIndexed, reassignmentCounter) =>
          ActivationChange(watermark, cid, change, isIndexing, isIndexed, reassignmentCounter)
        }),
      functionFullName,
    )
}
