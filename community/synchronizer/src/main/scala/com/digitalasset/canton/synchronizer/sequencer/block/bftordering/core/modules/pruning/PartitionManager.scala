// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning

import cats.data.OptionT
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, DbStore, Storage}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatch
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting
import slick.jdbc.GetResult

import scala.concurrent.ExecutionContext

object PartitionManager {
  trait PartitionCreator[E <: Env[E]] extends FlagCloseable {
    def createPartitionsIfNeeded(epochNumber: EpochNumber)(implicit
        traceContext: TraceContext
    ): E#FutureUnlessShutdownT[Int]
  }

  trait PartitionPruner[E <: Env[E]] extends FlagCloseable {
    def prune(epochNumberInclusive: EpochNumber)(implicit
        traceContext: TraceContext
    ): E#FutureUnlessShutdownT[String]
  }

  def create(storage: Storage, timeouts: ProcessingTimeout, loggerFactory: NamedLoggerFactory)(
      implicit ec: ExecutionContext
  ): PekkoFutureUnlessShutdown[Option[(PartitionCreator[PekkoEnv], PartitionPruner[PekkoEnv])]] =
    PekkoFutureUnlessShutdown(
      functionFullName,
      () =>
        (for {
          dbStorage <- OptionT.fromOption[FutureUnlessShutdown](
            Some(storage)
              .collect { case dbStorage: DbStorage => (dbStorage, dbStorage.profile) }
              .collect { case (dbStorage, _: DbStorage.Profile.Postgres) => dbStorage }
          )
          initStore = new PruningManagerInitStore(dbStorage, timeouts, loggerFactory)
          ((minPartitionNumbers, maxPartitionNumbers), partitionSize) <-
            TraceContext.withNewTraceContext("initialize partition management")(
              implicit traceContext =>
                OptionT.liftF(for {
                  maps <- initStore.queryLowestAndHighestExistingPartitionNumberPerTableName
                  partitionSize <- initStore.latestPartitionSizeEntry
                } yield (maps, partitionSize))
            )
        } yield (
          new PartitionManager.PartitionCreatorImpl(
            dbStorage,
            timeouts,
            loggerFactory,
            maxPartitionNumbers,
            partitionSize,
          ),
          new PartitionManager.PartitionPrunerImpl(
            dbStorage,
            timeouts,
            loggerFactory,
            minPartitionNumbers,
            partitionSize,
          ),
        )).value,
    )

  private[pruning] val epochPartitionedTables =
    Seq(
      "ord_metadata_output_epochs",
      "ord_metadata_output_blocks",
      "ord_leader_selection_state",
      "ord_epochs",
      "ord_pbft_messages_completed",
      "ord_pbft_messages_in_progress",
    )
  private[pruning] val batchesTable = "ord_availability_batch"

  private[pruning] final case class Partition(
      partitionName: String,
      tableName: String,
      from: Long,
      to: Long,
  )
  private[pruning] final case class HighestCreatedPartitionNumbers(
      partitionNumber: Long,
      batchTablePartitionNumber: Long,
  )
  private[pruning] final case class PartitionSizeEntry(
      epochNumber: EpochNumber,
      partitionNumber: Long,
      partitionSize: Int,
  )

  @VisibleForTesting
  private[pruning] def newPartitions(
      partitionSize: Int,
      newEpochNumber: EpochNumber,
      maxEpochPartitionNumbers: HighestCreatedPartitionNumbers,
  ): (Seq[Partition], HighestCreatedPartitionNumbers) = {
    def partition(tableName: String, partitionNumber: Long) = {
      val partitionName = s"${tableName}_p$partitionNumber"
      val newPartitionFrom = partitionNumber * partitionSize
      val newPartitionTo = (partitionNumber + 1) * partitionSize
      Partition(partitionName, tableName, newPartitionFrom, newPartitionTo)
    }

    // we create partitions one step (partition size) ahead
    val nextEpochPartitionNumber = (newEpochNumber + partitionSize) / partitionSize
    // we have to support storing batches of epoch number twice the batch validity ahead, so
    // the step will be the max between that and the partition size
    val nextBatchesTableEpochPartitionNumber = {
      val step = Math.max(2 * OrderingRequestBatch.BatchValidityDurationEpochs, partitionSize)
      (newEpochNumber + step) / partitionSize
    }

    val partitions = (for {
      partitionNumber <- (maxEpochPartitionNumbers.partitionNumber + 1) to nextEpochPartitionNumber
      tableName <- epochPartitionedTables
    } yield partition(tableName, partitionNumber))

    val batchesPartitions = (for {
      partitionNumber <-
        (maxEpochPartitionNumbers.batchTablePartitionNumber + 1) to nextBatchesTableEpochPartitionNumber
    } yield partition(batchesTable, partitionNumber))

    (
      (partitions ++ batchesPartitions).sortBy(_.from),
      HighestCreatedPartitionNumbers(
        Math.max(maxEpochPartitionNumbers.partitionNumber, nextEpochPartitionNumber),
        Math.max(
          maxEpochPartitionNumbers.batchTablePartitionNumber,
          nextBatchesTableEpochPartitionNumber,
        ),
      ),
    )
  }

  private class PartitionCreatorImpl(
      val storage: DbStorage,
      val timeouts: ProcessingTimeout,
      val loggerFactory: NamedLoggerFactory,
      partitionNumberMap: Map[String, Long] = Map.empty,
      partitionSizeEntry: PartitionSizeEntry,
  )(implicit ec: ExecutionContext)
      extends PartitionCreator[PekkoEnv]
      with DbStore {

    import storage.api.*

    private val partitionSize = partitionSizeEntry.partitionSize

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var highestEpochPartitionNumbers = HighestCreatedPartitionNumbers(
      (partitionNumberMap - batchesTable).values.minOption.getOrElse(-1),
      partitionNumberMap.getOrElse(batchesTable, -1),
    )

    override def createPartitionsIfNeeded(newEpochNumber: EpochNumber)(implicit
        traceContext: TraceContext
    ): PekkoFutureUnlessShutdown[Int] = {
      val (partitions, newMaxEpochPartitionNumber) =
        newPartitions(partitionSize, newEpochNumber, highestEpochPartitionNumbers)
      PekkoFutureUnlessShutdown(
        "createPartitionsIfNeeded",
        () =>
          if (partitions.nonEmpty) {
            logger.info(
              s"About to create ${partitions.size} for epoch partition number ${highestEpochPartitionNumbers.partitionNumber} at epoch $newEpochNumber"
            )
            storage
              .update_(
                DBIO
                  .sequence(
                    partitions.map {
                      case Partition(partitionName, tableName, newPartitionFrom, newPartitionTo) =>
                        sqlu"""create table if not exists #$partitionName partition of #$tableName for values from (#$newPartitionFrom) to (#$newPartitionTo);"""
                    }
                  )
                  .transactionally,
                functionFullName,
              )
              .map { _ =>
                highestEpochPartitionNumbers = newMaxEpochPartitionNumber
                partitions.size
              }
          } else FutureUnlessShutdown.pure(0),
      )
    }
  }

  private[pruning] final case class HighestPrunedPartitionNumbers(
      partitionNumber: Long,
      batchTablePartitionNumber: Long,
  )

  @VisibleForTesting
  private[pruning] def partitionsToPrune(
      partitionSize: Int,
      epochNumber: EpochNumber,
      previous: HighestPrunedPartitionNumbers,
  ): (Seq[String], HighestPrunedPartitionNumbers) = {
    def computeNumberOfEpochPartitionToPrune(epochNumberToPrune: EpochNumber) = {
      val numberOfPartitionIncludingEpoch = epochNumberToPrune / partitionSize
      val lastEpochNumberInPartition = (numberOfPartitionIncludingEpoch + 1) * partitionSize - 1
      // if epoch is the last one in partition, we prune from that partition, otherwise we prune from the previous
      if (epochNumberToPrune == lastEpochNumberInPartition) numberOfPartitionIncludingEpoch
      else numberOfPartitionIncludingEpoch - 1
    }

    val epochPartitionNumberToPrune = computeNumberOfEpochPartitionToPrune(epochNumber)
    val epochPartitionNumberToPruneForBatchesTable = computeNumberOfEpochPartitionToPrune(
      EpochNumber(epochNumber - OrderingRequestBatch.BatchValidityDurationEpochs)
    )

    val partitionNames = (for {
      partitionNumber <- (previous.partitionNumber + 1 to epochPartitionNumberToPrune)
      tableName <- epochPartitionedTables
    } yield s"${tableName}_p$partitionNumber")

    val batchesTablePartitionNames = (for {
      partitionNumber <-
        (previous.batchTablePartitionNumber + 1) to epochPartitionNumberToPruneForBatchesTable
    } yield s"${batchesTable}_p$partitionNumber")

    val next = HighestPrunedPartitionNumbers(
      Math.max(previous.partitionNumber, epochPartitionNumberToPrune),
      Math.max(previous.batchTablePartitionNumber, epochPartitionNumberToPruneForBatchesTable),
    )
    (partitionNames ++ batchesTablePartitionNames, next)
  }

  private class PartitionPrunerImpl(
      val storage: DbStorage,
      val timeouts: ProcessingTimeout,
      val loggerFactory: NamedLoggerFactory,
      partitionNumberMap: Map[String, Long] = Map.empty,
      partitionSizeEntry: PartitionSizeEntry,
  )(implicit ec: ExecutionContext)
      extends PartitionPruner[PekkoEnv]
      with DbStore {

    import storage.api.*

    private val partitionSize = partitionSizeEntry.partitionSize

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var latestPrunedPartitionNumbers = HighestPrunedPartitionNumbers(
      (partitionNumberMap - batchesTable).values.minOption.getOrElse(0L) - 1L,
      partitionNumberMap.getOrElse(batchesTable, 0L) - 1L,
    )

    override def prune(epochNumberInclusive: EpochNumber)(implicit
        traceContext: TraceContext
    ): PekkoFutureUnlessShutdown[String] = {
      val (partitionNames, prunedPartitionNumbers) =
        partitionsToPrune(partitionSize, epochNumberInclusive, latestPrunedPartitionNumbers)

      PekkoFutureUnlessShutdown(
        "prune",
        () =>
          if (partitionNames.nonEmpty) {
            logger.info(
              s"About to prune ${partitionNames.size} partitions at epoch $epochNumberInclusive"
            )
            storage
              .update_(
                DBIO
                  .sequence(
                    partitionNames
                      .map(partitionName => sqlu"""drop table if exists #$partitionName;""")
                  )
                  .transactionally,
                functionFullName,
              )
              .map { _ =>
                latestPrunedPartitionNumbers = prunedPartitionNumbers
                s"Pruned ${partitionNames.size} partitions at epoch $epochNumberInclusive"
              }
          } else
            FutureUnlessShutdown.pure(
              s"Pruned no partitions at epoch $epochNumberInclusive"
            ),
      )
    }
  }

  @VisibleForTesting
  private[pruning] class PruningManagerInitStore(
      val storage: DbStorage,
      val timeouts: ProcessingTimeout,
      val loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext)
      extends DbStore {
    import storage.api.*

    private implicit val getResultTableNameAndPartitionNumbers: GetResult[(String, Long, Long)] =
      GetResult(r => (r.nextString(), r.nextLong(), r.nextLong()))
    private implicit val getResultPartitionSizeHistory: GetResult[PartitionSizeEntry] =
      GetResult(r => PartitionSizeEntry(EpochNumber(r.nextLong()), r.nextLong(), r.nextInt()))

    def latestPartitionSizeEntry(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[PartitionSizeEntry] = {
      val query =
        sql"""select epoch_number, partition_number, partition_size from ord_partition_size_history
           order by epoch_number desc limit 1""".as[PartitionSizeEntry]
      storage
        .query(query, functionFullName)
        .map(_.headOption.getOrElse(sys.error("Can't find partition size data")))
    }

    def queryLowestAndHighestExistingPartitionNumberPerTableName(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[(Map[String, Long], Map[String, Long])] = {
      def lowestPartitionNumbersQuery(tableNames: Seq[String]) = {
        val tablesSql = tableNames
          .map(tableName => s"('$tableName'::regclass)")
          .mkString(",\n    ")
        sql"""
          #${s"""
          with target_tables(parent_table) as (
            values
              $tablesSql
          ),
          partitions as (
            select
              parent.oid::regclass as parent_table,
              child.relname as partition_name,
              substring(child.relname from '_p([0-9]+)$$')::bigint as partition_number
            from target_tables t
            join pg_class parent on parent.oid = t.parent_table
            join pg_inherits i on i.inhparent = parent.oid
            join pg_class child on child.oid = i.inhrelid
          )
          select
            parent_table::text,
            min(partition_number) as lowest_partition_number,
            max(partition_number) as highest_partition_number
          from partitions
          group by parent_table
          order by parent_table
          """}
        """.as[(String, Long, Long)]
      }
      storage
        .query(
          lowestPartitionNumbersQuery(batchesTable +: epochPartitionedTables),
          functionFullName,
        )
        .map { rows =>
          val minPartitionsMap: Map[String, Long] =
            rows.map { case (tableName, minPartition, _) => tableName -> minPartition }.toMap
          val maxPartitionsMap: Map[String, Long] =
            rows.map { case (tableName, _, maxPartition) => tableName -> maxPartition }.toMap
          (minPartitionsMap, maxPartitionsMap)
        }
    }
  }

}
