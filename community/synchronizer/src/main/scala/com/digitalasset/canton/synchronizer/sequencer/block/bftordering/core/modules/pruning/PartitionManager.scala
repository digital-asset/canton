// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning

import cats.data.OptionT
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, DbStore, Storage}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore.OutputBlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.db.DbOutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatch
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{FailureMode, SimpleExecutionQueue}
import com.google.common.annotations.VisibleForTesting
import slick.jdbc.GetResult
import slick.jdbc.PostgresProfile.api.SimpleDBIO

import scala.concurrent.ExecutionContext

object PartitionManager {
  trait PartitionCreator[E <: Env[E]] extends FlagCloseable {
    def createPartitionsIfNeeded(epochNumber: EpochNumber)(implicit
        traceContext: TraceContext
    ): E#FutureUnlessShutdownT[Int]
  }

  trait PartitionPruner[E <: Env[E]] extends FlagCloseable {
    def prune(epochNumberInclusive: EpochNumber, latestCompletedEpochNumber: EpochNumber)(implicit
        traceContext: TraceContext
    ): E#FutureUnlessShutdownT[String]
  }

  def create(
      storage: Storage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      onboardedSequencerEpochNumberO: Option[EpochNumber],
  )(implicit
      ec: ExecutionContext
  ): PekkoFutureUnlessShutdown[Option[(PartitionCreator[PekkoEnv], PartitionPruner[PekkoEnv])]] =
    TraceContext.withNewTraceContext("initialize partition management")(implicit traceContext =>
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
            partitionSize <- OptionT.liftF(initStore.latestPartitionSizeEntry)
            (partitionCreator, partitionPruner) <- OptionT.liftF {
              def partitionCreator(maxPartitionNumbers: Map[String, Long]) =
                new PartitionManager.PartitionCreatorImpl(
                  dbStorage,
                  timeouts,
                  loggerFactory,
                  maxPartitionNumbers,
                  partitionSize,
                )
              def partitionPruner(minPartitionNumbers: Map[String, Long]) =
                new PartitionManager.PartitionPrunerImpl(
                  dbStorage,
                  timeouts,
                  loggerFactory,
                  minPartitionNumbers,
                  partitionSize,
                )
              onboardedSequencerEpochNumberO match {
                case Some(epochNumber) =>
                  // if we are initializing from a newly onboarded node, we could be starting from any arbitrary epochNumber,
                  // in that case, we want to drop the default partitions created by the migrations, and manually create
                  // the partitions we would need to support this epoch number
                  for {
                    _ <- initStore.cleanUpDefaultPartitions()
                    // we give an uninitialized maxPartitionNumbers map to the partition creator so that it tracks the right
                    // partition numbers when we call createPartitionsIfNeeded in the next line
                    creator = partitionCreator(Map())
                    _ <- creator.createPartitionsIfNeeded(epochNumber).futureUnlessShutdown()
                    // after the correct partitions have been created we can initialize the partition pruner with the right minPartitionNumbers
                    (minPartitionNumbers, _) <-
                      initStore.queryLowestAndHighestExistingPartitionNumberPerTableName
                  } yield (creator, partitionPruner(minPartitionNumbers))
                case None =>
                  for {
                    (minPartitionNumbers, maxPartitionNumbers) <-
                      initStore.queryLowestAndHighestExistingPartitionNumberPerTableName
                  } yield (
                    partitionCreator(maxPartitionNumbers),
                    partitionPruner(minPartitionNumbers),
                  )
              }
            }
          } yield (partitionCreator, partitionPruner)).value,
        orderingStage = Some(functionFullName),
      )
    )

  private[pruning] val outputBlocksTable = "ord_metadata_output_blocks"
  private[pruning] val consensusInProgressTable = "ord_pbft_messages_in_progress"
  private[pruning] val epochPartitionedTables =
    Seq(
      "ord_metadata_output_epochs",
      outputBlocksTable,
      "ord_leader_selection_state",
      "ord_epochs",
      "ord_pbft_messages_completed",
      consensusInProgressTable,
    )
  private[pruning] val batchesTable = "ord_availability_batch"

  private[pruning] final case class Partition(
      partitionName: String,
      tableName: String,
      from: Long,
      to: Long,
  )
  private sealed trait OutputBlockPartitionExclusionConstraint {
    def blockMetadata: OutputBlockMetadata
    def partitionNumber: Long
  }
  private final case class LowerBoundExclusionConstraint(
      blockMetadata: OutputBlockMetadata,
      partitionNumber: Long,
  ) extends OutputBlockPartitionExclusionConstraint
  private final case class UpperBoundExclusionConstraint(
      blockMetadata: OutputBlockMetadata,
      partitionNumber: Long,
  ) extends OutputBlockPartitionExclusionConstraint

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

    val currentPartitionNumber = newEpochNumber / partitionSize
    // we create partitions one step (partition size) ahead
    val nextEpochPartitionNumber = currentPartitionNumber + 1
    // we have to support storing batches of epoch number twice the batch validity ahead, so
    // the step will be the max between that and the partition size
    val nextBatchesTableEpochPartitionNumber = {
      val step = Math.max(2 * OrderingRequestBatch.BatchValidityDurationEpochs, partitionSize)
      (newEpochNumber + step) / partitionSize
    }

    val partitions = {
      // if the max partition number we keep track of is -1, it means this is an uninitialized value, which happens
      // with newly onboarded nodes. in that case, we create the current partition and the next.
      // Otherwise, we create the next one, if it hasn't been created yet.
      val initial =
        if (maxEpochPartitionNumbers.partitionNumber == -1) currentPartitionNumber
        else maxEpochPartitionNumbers.partitionNumber + 1
      (for {
        partitionNumber <- initial to nextEpochPartitionNumber
        tableName <- epochPartitionedTables
      } yield partition(tableName, partitionNumber))
    }

    val batchesPartitions = {
      // if the max partition number we keep track of is -1, it means this is an uninitialized value, which happens
      // with newly onboarded nodes. in that case, we create the partitions covering the validity window of batches,
      // including in the past. Otherwise, we only create partitions ahead.
      val initial =
        if (maxEpochPartitionNumbers.batchTablePartitionNumber == -1)
          (newEpochNumber - OrderingRequestBatch.BatchValidityDurationEpochs) / partitionSize
        else maxEpochPartitionNumbers.batchTablePartitionNumber + 1
      (for {
        partitionNumber <-
          initial to nextBatchesTableEpochPartitionNumber
      } yield partition(batchesTable, partitionNumber))
    }

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

    val vacuumQueue = new SimpleExecutionQueue(
      name = s"partition-creator-vacuum",
      futureSupervisor = FutureSupervisor.Noop,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
      logTaskTiming = true,
      failureMode = FailureMode.ContinueAfterFailure,
    )

    override def createPartitionsIfNeeded(newEpochNumber: EpochNumber)(implicit
        traceContext: TraceContext
    ): PekkoFutureUnlessShutdown[Int] = {
      val (partitions, newMaxEpochPartitionNumber) =
        newPartitions(partitionSize, newEpochNumber, highestEpochPartitionNumbers)

      def createPartitionDbIo(partition: Partition) = partition match {
        case Partition(partitionName, tableName, newPartitionFrom, newPartitionTo) =>
          for {
            _ <-
              sqlu"""create table if not exists #$partitionName partition of #$tableName for values from (#$newPartitionFrom) to (#$newPartitionTo);"""
            _ <-
              sqlu"""alter table #$partitionName set (
                    -- avoid that inserts trigger vacuuming in a hot partition
                    autovacuum_vacuum_insert_threshold = 1000000000,
                    autovacuum_vacuum_insert_scale_factor = 0,
                    -- avoid that a few deletes trigger vacuuming (which is only useful really to the batches table)
                    autovacuum_vacuum_threshold = 10000,
                    autovacuum_vacuum_scale_factor = 0.01,
                    -- if autovacuum gets triggered at some point, spread out the work a bit
                    autovacuum_vacuum_cost_limit = 500,
                    autovacuum_vacuum_cost_delay = 10
                  )"""
          } yield ()
      }

      PekkoFutureUnlessShutdown(
        "createPartitionsIfNeeded",
        () =>
          for {
            partitionsCreated <-
              if (partitions.nonEmpty) {
                logger.info(
                  s"About to create ${partitions.size} partitions for epoch partition number ${highestEpochPartitionNumbers.partitionNumber} at epoch $newEpochNumber"
                )
                storage
                  .update_(
                    DBIO.sequence(partitions.map(createPartitionDbIo)).transactionally,
                    functionFullName,
                  )
                  .map { _ =>
                    highestEpochPartitionNumbers = newMaxEpochPartitionNumber
                    partitions.size
                  }
              } else FutureUnlessShutdown.pure(0)
            _ <- maybeCreateExclusionConstraint(newEpochNumber)
            _ = maybeKickOffManualVacuuming(newEpochNumber)
          } yield partitionsCreated,
        orderingStage = Some(functionFullName),
      )
    }

    private val outputStore = new DbOutputMetadataStore(storage, timeouts, loggerFactory)

    // Adding exclusion constraints to the output blocks table will help with the queries by block number
    // and by bft timestamp that do not include epoch number in the where clause. In those cases
    // all partitions would have to be scanned but by having the constraints, the query planner is able
    // to exclude block numbers and bft timestamps outside of the constraints definitions.
    // We add both lower bound and upper bound constraints on the first and last blocks from each partition.
    // Postgres doc: https://www.postgresql.org/docs/current/ddl-partitioning.html#DDL-PARTITIONING-CONSTRAINT-EXCLUSION
    private def maybeCreateExclusionConstraint(newEpochNumber: EpochNumber)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Unit] =
      for {
        exclusionConstraint <- computeExclusionConstraint(newEpochNumber).futureUnlessShutdown()
        _ <- exclusionConstraint
          .map { constraint =>
            val partitionName = s"${outputBlocksTable}_p${constraint.partitionNumber}"
            val blockMetadata = constraint.blockMetadata
            val (constraintName, ineq, name) = constraint match {
              case _: LowerBoundExclusionConstraint => (s"${partitionName}_lb", ">=", "lower")
              case _: UpperBoundExclusionConstraint => (s"${partitionName}_ub", "<=", "upper")
            }
            logger.info(
              s"About to add $name bound exclusion constraint to partition ${constraint.partitionNumber} of output blocks table for blocks $ineq ${blockMetadata.blockNumber}"
            )
            val update = for {
              exists <-
                sql"""select exists (select 1 from pg_constraint where conname = '#$constraintName' and conrelid = '#$partitionName'::regclass)"""
                  .as[Boolean]
              _ <-
                if (!exists.forall(identity)) sqlu"""alter table #$partitionName
                              add constraint #$constraintName
                              check (bft_ts #$ineq #${blockMetadata.blockBftTime.toMicros} and block_number #$ineq #${blockMetadata.blockNumber})
                        """
                else sqlu""
            } yield ()
            storage.queryAndUpdate(update, functionFullName).map(_ => ())
          }
          .getOrElse(FutureUnlessShutdown.unit)
      } yield ()

    // if this is the last epoch in the partition, it means we are not supposed to ever add more data to this partition,
    // i.e. it goes from being a hot to a cold partition. that is a good time to kick off manual vacuum operation,
    // in a sequential manner, asynchronously, and using setting values that will make this a gentle, spread out, operation,
    // that will affect all rows of the cold partition, such that it shouldn't affect performance of other db operations
    // happening (especially inserts) and if autovacuum ever gets triggered again in the future for these partitions,
    // it will mostly skip doing any work here.
    private def maybeKickOffManualVacuuming(
        newEpochNumber: EpochNumber
    )(implicit traceContext: TraceContext): Unit = {
      val currentEpochPartitionNumber = newEpochNumber / partitionSize
      val lastEpochNumber = EpochNumber((currentEpochPartitionNumber + 1) * partitionSize - 1)
      if (newEpochNumber == lastEpochNumber) {
        val partitionNames = (batchesTable +: epochPartitionedTables).map(tableName =>
          s"${tableName}_p$currentEpochPartitionNumber"
        )
        val _ = partitionNames.map(partitionName =>
          vacuumQueue.executeUS(
            manualVacuumPartition(partitionName),
            s"manual vacuum $partitionName",
          )
        )
      }
    }
    private def manualVacuumPartition(
        partitionName: String
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
      logger.info(s"Starting manual vacuum on recently completed partition $partitionName")
      val manualVacuumAction = SimpleDBIO[Unit] { session =>
        val conn = session.connection
        val originalAutoCommit = conn.getAutoCommit
        val stmt = conn.createStatement()
        try {
          // VACUUM cannot execute inside a transaction block.
          // slick normally executes statements with autocommit disabled, meaning they
          // run inside a transaction. Temporarily enabling autocommit causes each
          // statement below to execute independently, allowing the VACUUM command to
          // run successfully.
          conn.setAutoCommit(true)
          // spread out the vacuuming work a bit
          stmt.execute("set vacuum_cost_limit = 500").discard
          stmt.execute("set vacuum_cost_delay = 10").discard
          // immediately freeze all rows, including very recent ones
          stmt.execute("set vacuum_freeze_min_age = 0").discard
          stmt.execute("set vacuum_freeze_table_age = 0").discard
          stmt.execute(s"vacuum (freeze, analyze) $partitionName").discard
          ()
        } finally {
          try {
            // restore the session settings before returning the connection to the pool so future users of this connection see the defaults
            stmt.execute("reset vacuum_cost_limit").discard
            stmt.execute("reset vacuum_cost_delay").discard
            stmt.execute("reset vacuum_freeze_min_age").discard
            stmt.execute("reset vacuum_freeze_table_age").discard
          } finally {
            try {
              stmt.close()
            } finally {
              // restore the original autocommit mode before releasing the connection back to slick
              conn.setAutoCommit(originalAutoCommit)
            }
          }
        }
      }
      storage
        .queryAndUpdate(manualVacuumAction, functionFullName)
        .map(_ => logger.info(s"Completed manual vacuum of partition $partitionName"))
    }

    private def computeExclusionConstraint(newEpochNumber: EpochNumber)(implicit
        traceContext: TraceContext
    ): PekkoFutureUnlessShutdown[Option[OutputBlockPartitionExclusionConstraint]] = {
      val currentEpochPartitionNumber = newEpochNumber / partitionSize
      val firstEpochNumber = EpochNumber(currentEpochPartitionNumber * partitionSize)
      val lastEpochNumber = EpochNumber(firstEpochNumber + partitionSize - 1)

      if (newEpochNumber == firstEpochNumber) {
        outputStore
          .getFirstBlockInEpoch(newEpochNumber)
          .map(_.map(block => LowerBoundExclusionConstraint(block, currentEpochPartitionNumber)))
      } else if (newEpochNumber == lastEpochNumber) {
        outputStore
          .getLastBlockInEpoch(newEpochNumber)
          .map(_.map(block => UpperBoundExclusionConstraint(block, currentEpochPartitionNumber)))
      } else PekkoFutureUnlessShutdown.pure(None)
    }

    override def onClosed(): Unit = LifeCycle.close(vacuumQueue)(logger)

  }

  private[pruning] final case class HighestPrunedPartitionNumbers(
      partitionNumber: Long,
      batchTablePartitionNumber: Long,
      consensusInProgressTablePartitionNumber: Long,
  )

  private val epochPartitionedTablesForPruning =
    epochPartitionedTables.filterNot(_ == consensusInProgressTable)

  @VisibleForTesting
  private[pruning] def partitionsToPrune(
      partitionSize: Int,
      pruneAtInclusiveEpochNumber: EpochNumber,
      latestCompletedEpochNumber: EpochNumber,
      previous: HighestPrunedPartitionNumbers,
  ): (Seq[String], HighestPrunedPartitionNumbers) = {
    def computeNumberOfEpochPartitionToPrune(epochNumberToPrune: EpochNumber) = {
      val numberOfPartitionIncludingEpoch = epochNumberToPrune / partitionSize
      val lastEpochNumberInPartition = (numberOfPartitionIncludingEpoch + 1) * partitionSize - 1
      // if epoch is the last one in partition, we prune from that partition, otherwise we prune from the previous
      if (epochNumberToPrune == lastEpochNumberInPartition) numberOfPartitionIncludingEpoch
      else numberOfPartitionIncludingEpoch - 1
    }

    val epochPartitionNumberToPrune = computeNumberOfEpochPartitionToPrune(
      pruneAtInclusiveEpochNumber
    )
    val epochPartitionNumberToPruneForBatchesTable = computeNumberOfEpochPartitionToPrune(
      EpochNumber(pruneAtInclusiveEpochNumber - OrderingRequestBatch.BatchValidityDurationEpochs)
    )
    val epochPartitionNumberToPruneForConsensusInProgressTable =
      computeNumberOfEpochPartitionToPrune(latestCompletedEpochNumber)

    val partitionNames = (for {
      partitionNumber <- (previous.partitionNumber + 1 to epochPartitionNumberToPrune)
      tableName <- epochPartitionedTablesForPruning
    } yield s"${tableName}_p$partitionNumber")

    val batchesTablePartitionNames = (for {
      partitionNumber <-
        (previous.batchTablePartitionNumber + 1) to epochPartitionNumberToPruneForBatchesTable
    } yield s"${batchesTable}_p$partitionNumber")

    val consensusInProgressTablePartitionNames = (for {
      partitionNumber <-
        (previous.consensusInProgressTablePartitionNumber + 1) to epochPartitionNumberToPruneForConsensusInProgressTable
    } yield s"${consensusInProgressTable}_p$partitionNumber")

    val next = HighestPrunedPartitionNumbers(
      Math.max(previous.partitionNumber, epochPartitionNumberToPrune),
      Math.max(previous.batchTablePartitionNumber, epochPartitionNumberToPruneForBatchesTable),
      Math.max(
        previous.consensusInProgressTablePartitionNumber,
        epochPartitionNumberToPruneForConsensusInProgressTable,
      ),
    )
    (partitionNames ++ batchesTablePartitionNames ++ consensusInProgressTablePartitionNames, next)
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
      partitionNumberMap.getOrElse(consensusInProgressTable, 0L) - 1L,
    )

    override def prune(epochNumberInclusive: EpochNumber, latestCompletedEpochNumber: EpochNumber)(
        implicit traceContext: TraceContext
    ): PekkoFutureUnlessShutdown[String] = {
      val (partitionNames, prunedPartitionNumbers) =
        partitionsToPrune(
          partitionSize,
          epochNumberInclusive,
          latestCompletedEpochNumber,
          latestPrunedPartitionNumbers,
        )

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
        orderingStage = Some(functionFullName),
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

    def cleanUpDefaultPartitions()(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Unit] = {
      val tables = batchesTable +: epochPartitionedTables
      val partitionsToDelete = for {
        table <- tables
        partitions <- Seq(s"${table}_p0", s"${table}_p1")
      } yield partitions
      storage
        .update_(
          DBIO
            .sequence(
              partitionsToDelete.map(partitionName => sqlu"drop table if exists #$partitionName;")
            )
            .transactionally,
          functionFullName,
        )
    }
  }

}
