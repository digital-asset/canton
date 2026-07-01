// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning

import com.digitalasset.canton.config.{DbConfig, DbParametersConfig, PartitionConfig}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import com.digitalasset.canton.store.db.PostgresTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore.OutputBlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.db.DbOutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.PartitionManager.{
  HighestCreatedPartitionNumbers,
  HighestPrunedPartitionNumbers,
  Partition,
  PruningManagerInitStore,
  newPartitions,
  partitionsToPrune,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import org.scalatest.wordspec.AsyncWordSpec
import slick.jdbc.GetResult

class PartitionManagerTest extends AsyncWordSpec with BftSequencerBaseTest {
  "PartitionManager" should {
    val partitionSize = 100
    "create partitions one step ahead" in {
      // for batches table we create partitions 1000 epochs ahead
      inside(newPartitions(partitionSize, EpochNumber(0), HighestCreatedPartitionNumbers(2, 2))) {
        case (partitions, HighestCreatedPartitionNumbers(2, 10)) =>
          partitions should have size 8
          forAll(partitions)(p => p.tableName shouldBe PartitionManager.batchesTable)
      }
      inside(
        newPartitions(partitionSize, EpochNumber(199), HighestCreatedPartitionNumbers(2, 10))
      ) { case (partitions, HighestCreatedPartitionNumbers(2, 11)) =>
        partitions should have size 1
        forAll(partitions)(p => p.tableName shouldBe PartitionManager.batchesTable)
      }
      // for other tables we create partitions one step ahead
      inside(
        newPartitions(partitionSize, EpochNumber(200), HighestCreatedPartitionNumbers(2, 12))
      ) {
        case (
              Partition(partitionName, tableName, 300, 400) +: _otherPartitions,
              HighestCreatedPartitionNumbers(3, 12),
            ) =>
          partitionName shouldBe s"${tableName}_p3"
      }
    }

    "prune partitions below current epoch number or block number" in {
      // nothing to prune because already pruned
      partitionsToPrune(
        partitionSize,
        EpochNumber(0),
        EpochNumber(1000),
        HighestPrunedPartitionNumbers(2, -1, 9),
      ) shouldBe (Seq.empty, HighestPrunedPartitionNumbers(2, -1, 9))

      // nothing to prune because already pruned previous partition 2 (from 200 to 299)
      partitionsToPrune(
        partitionSize,
        EpochNumber(398),
        EpochNumber(1000),
        HighestPrunedPartitionNumbers(2, -1, 9),
      ) shouldBe (Seq.empty, HighestPrunedPartitionNumbers(2, -1, 9))
      // because it is the last epochNumber in the partition it will prune partition 3 (from 300 to 399)
      inside(
        partitionsToPrune(
          partitionSize,
          EpochNumber(399),
          EpochNumber(1000),
          HighestPrunedPartitionNumbers(2, -1, 9),
        )
      ) { case (partitions, HighestPrunedPartitionNumbers(3, -1, 9)) =>
        partitions should not contain s"${PartitionManager.batchesTable}_p3"
        forAll(partitions)(p => p should endWith("_p3"))
      }
      // same result as above since haven't yet pruned partition 3 and that is the previous one
      inside(
        partitionsToPrune(
          partitionSize,
          EpochNumber(400),
          EpochNumber(1100),
          HighestPrunedPartitionNumbers(2, -1, 10),
        )
      ) { case (partitions, HighestPrunedPartitionNumbers(3, -1, 10)) =>
        partitions should not contain s"${PartitionManager.batchesTable}_p3"
        forAll(partitions)(p => p should endWith("_p3"))
      }

      // batches table pruning takes place 500 epochs earlier
      inside(
        partitionsToPrune(
          partitionSize,
          EpochNumber(600),
          EpochNumber(1200),
          HighestPrunedPartitionNumbers(5, -1, 11),
        )
      ) { case (partitions, HighestPrunedPartitionNumbers(5, 0, 11)) =>
        partitions should contain only (s"${PartitionManager.batchesTable}_p0")
      }

      // in progress table is pruned based on latest epoch completed
      inside(
        partitionsToPrune(
          partitionSize,
          EpochNumber(600),
          EpochNumber(1099),
          HighestPrunedPartitionNumbers(5, 0, 9),
        )
      ) { case (partitions, HighestPrunedPartitionNumbers(5, 0, 10)) =>
        partitions should contain only (s"${PartitionManager.consensusInProgressTable}_p10")
      }
    }
  }
}

class PartitionManagerDatabaseTest
    extends AsyncWordSpec
    with BftSequencerBaseTest
    with PostgresTest {

  def create() = PartitionManager.create(storage, timeouts, loggerFactory)

  override def cleanDb(storage: DbStorage)(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.unit

  override def mkDbConfig(basicConfig: DbBasicConfig): DbConfig.Postgres = {
    val defaultDbConfig = super.mkDbConfig(basicConfig)
    defaultDbConfig.copy(parameters =
      DbParametersConfig(partitions = PartitionConfig(initialBftOrdererTablesPartitionSize = 100))
    )
  }

  def howManyPartitionsAreSearched(blockNumber: BlockNumber): FutureUnlessShutdown[Int] = {
    import storage.api.*
    val explain =
      sql"""explain (format json) select * from #${PartitionManager.outputBlocksTable} where block_number = $blockNumber order by block_number limit 1 """
        .as[String](GetResult(_.nextString()))
        .map(
          _.headOption
            .getOrElse("")
            .split(s""""Relation Name": "${PartitionManager.outputBlocksTable}_p""")
            .length - 1
        )
    storage.query(explain, "explain")
  }

  def getPartitionSettings(partitionName: String) = {
    import storage.api.*
    storage.query(
      sql"""select c.reloptions from pg_class c where c.oid = '#$partitionName'::regclass;"""
        .as[String](GetResult(_.nextString()))
        .map(_.headOption.getOrElse("")),
      "get settings",
    )
  }

  def getVacuumStats(partitionName: String) = {
    import storage.api.*
    storage.query(
      sql"""select vacuum_count, autovacuum_count from pg_stat_user_tables where relid = '#$partitionName'::regclass;"""
        .as[(Long, Long)](
          GetResult(r => (r.nextLong(), r.nextLong()))
        )
        .map(_.headOption.getOrElse(fail())),
      "get manual vacuum stats",
    )
  }

  "PartitionManagerDatabase" should {
    lazy val createPartitionManagement = create().futureUnlessShutdown()

    "prune and creation of partitions does not error" in {
      val initStore = new PruningManagerInitStore(storage, timeouts, loggerFactory)
      def getMinMaxPartitionNumbers =
        initStore.queryLowestAndHighestExistingPartitionNumberPerTableName

      (for {
        case Some((creator, pruner)) <- createPartitionManagement

        partitionSizeEntry <- initStore.latestPartitionSizeEntry
        _ = partitionSizeEntry shouldBe PartitionManager.PartitionSizeEntry(EpochNumber(0), 0, 100)

        (minMap0, maxMap0) <- getMinMaxPartitionNumbers
        _ = forAll(minMap0.values)(_ shouldBe (0))
        _ = forAll(maxMap0.values)(_ shouldBe (1))

        msg <- pruner.prune(EpochNumber(98L), EpochNumber(98L)).futureUnlessShutdown()
        (minMap1, maxMap1) <- getMinMaxPartitionNumbers
        _ = msg shouldBe "Pruned no partitions at epoch 98"
        _ = forAll(minMap1.values)(_ shouldBe (0))
        _ = forAll(maxMap1.values)(_ shouldBe (1))

        msg2 <- pruner.prune(EpochNumber(99L), EpochNumber(99L)).futureUnlessShutdown()
        (minMap2, maxMap2) <- getMinMaxPartitionNumbers
        _ = msg2 shouldBe "Pruned 6 partitions at epoch 99"
        _ = {
          minMap2(PartitionManager.batchesTable) shouldBe (0)
          forAll((minMap2 - PartitionManager.batchesTable).values)(_ shouldBe (1))
        }
        _ = forAll(maxMap2.values)(_ shouldBe (1))

        _ <- creator
          .createPartitionsIfNeeded(EpochNumber(199))
          .futureUnlessShutdown()
        (minMap3, maxMap3) <- getMinMaxPartitionNumbers
        _ = minMap3 shouldBe (minMap2)
        _ = {
          maxMap3(PartitionManager.batchesTable) shouldBe (11)
          forAll((maxMap3 - PartitionManager.batchesTable).values)(_ shouldBe (2))
        }

        // checking that custom autovacuum settings are applied to a new partition
        opts <- getPartitionSettings(s"${PartitionManager.consensusInProgressTable}_p2")
        _ =
          opts should fullyMatch regex raw"\{autovacuum_vacuum_insert_threshold=\d+,autovacuum_vacuum_insert_scale_factor=\d+,autovacuum_vacuum_threshold=\d+,autovacuum_vacuum_scale_factor=0\.01,autovacuum_vacuum_cost_limit=\d+,autovacuum_vacuum_cost_delay=\d+\}".r

        _ <- creator
          .createPartitionsIfNeeded(EpochNumber(200))
          .futureUnlessShutdown()
        (minMap4, maxMap4) <- getMinMaxPartitionNumbers
        _ = minMap4 shouldBe (minMap2)
        _ = {
          maxMap4(PartitionManager.batchesTable) shouldBe (12)
          forAll((maxMap4 - PartitionManager.batchesTable).values)(_ shouldBe (3))
        }
      } yield succeed).onShutdown(fail())
    }

    "exclusion constraints make query scan fewer partitions" in {
      val outputStore = new DbOutputMetadataStore(storage, timeouts, loggerFactory)

      (for {
        case Some((creator, _)) <- createPartitionManagement

        // we create 100 partitions
        _ <- creator
          .createPartitionsIfNeeded(EpochNumber(9950))
          .futureUnlessShutdown()

        // a block search scans all of them
        result1 <- howManyPartitionsAreSearched(BlockNumber(49999))
        _ = result1 shouldBe 100

        // now we add one block to the beginning of each partition and
        // call createPartitionsIfNeeded with the block whose epoch is the first one in the partition,
        // which will cause the partition creator to add an exclusion constraint at the lower bound of each partition
        _ <- MonadUtil.sequentialTraverse(1 to 99) { i =>
          val epochNumber = EpochNumber((i * 100).toLong)
          val block = OutputBlockMetadata(
            epochNumber,
            BlockNumber(epochNumber * 10L),
            CantonTimestamp.fromProtoPrimitive(epochNumber * 10L).value,
          )
          for {
            _ <- outputStore.insertBlockIfMissing(block).futureUnlessShutdown()
            _ <- creator.createPartitionsIfNeeded(epochNumber).futureUnlessShutdown()
          } yield ()
        }

        // now searching for a block at the middle of all partitions will only scan half of them
        result2 <- howManyPartitionsAreSearched(BlockNumber(49999))
        _ = result2 shouldBe 50
        // searching for a recent block is still bad
        result3 <- howManyPartitionsAreSearched(BlockNumber(99999))
        _ = result3 shouldBe 100
        // searching for old blocks is very good
        result4 <- howManyPartitionsAreSearched(BlockNumber(100))
        _ = result4 shouldBe 1

        // now we add the upper bounds
        _ <- MonadUtil.sequentialTraverse(1 to 99) { i =>
          val epochNumber = EpochNumber(((i + 1) * 100 - 1).toLong)
          val block = OutputBlockMetadata(
            epochNumber,
            BlockNumber(epochNumber * 10L),
            CantonTimestamp.fromProtoPrimitive(epochNumber * 10L).value,
          )
          for {
            _ <- outputStore.insertBlockIfMissing(block).futureUnlessShutdown()
            _ <- creator.createPartitionsIfNeeded(epochNumber).futureUnlessShutdown()
          } yield ()
        }
        // now all searches should be fast
        result5 <- howManyPartitionsAreSearched(BlockNumber(49999))
        _ = result5 shouldBe 1
        result6 <- howManyPartitionsAreSearched(BlockNumber(99999))
        _ = result6 shouldBe 1
        result7 <- howManyPartitionsAreSearched(BlockNumber(100))
        _ = result7 shouldBe 1

      } yield succeed).onShutdown(fail())
    }

    "autovacuum" in {
      val partitionName = s"${PartitionManager.consensusInProgressTable}_p2"
      (for {
        case Some((creator, _)) <- createPartitionManagement

        // checking that custom autovacuum settings are applied to a new partition
        opts <- getPartitionSettings(partitionName)
        _ =
          opts should fullyMatch regex raw"\{autovacuum_vacuum_insert_threshold=\d+,autovacuum_vacuum_insert_scale_factor=\d+,autovacuum_vacuum_threshold=\d+,autovacuum_vacuum_scale_factor=0\.01,autovacuum_vacuum_cost_limit=\d+,autovacuum_vacuum_cost_delay=\d+\}".r

        _ = eventually() {
          // we should see that manual vacuum took place (twice instead of once because of how unit tests run everything twice)
          // and autovacuum does not run
          getVacuumStats(partitionName).onShutdown(fail()).futureValue shouldBe (2, 0)
        }
        _ = creator.close()
      } yield succeed).onShutdown(fail())
    }
  }

}
