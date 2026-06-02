// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning

import com.digitalasset.canton.config.{DbConfig, DbParametersConfig, PartitionConfig}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import com.digitalasset.canton.store.db.PostgresTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.PartitionManager.{
  HighestCreatedPartitionNumbers,
  HighestPrunedPartitionNumbers,
  Partition,
  PruningManagerInitStore,
  newPartitions,
  partitionsToPrune,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochNumber
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

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
        HighestPrunedPartitionNumbers(2, -1),
      ) shouldBe (Seq.empty, HighestPrunedPartitionNumbers(2, -1))

      // nothing to prune because already pruned previous partition 2 (from 200 to 299)
      partitionsToPrune(
        partitionSize,
        EpochNumber(398),
        HighestPrunedPartitionNumbers(2, -1),
      ) shouldBe (Seq.empty, HighestPrunedPartitionNumbers(2, -1))
      // because it is the last epochNumber in the partition it will prune partition 3 (from 300 to 399)
      inside(
        partitionsToPrune(partitionSize, EpochNumber(399), HighestPrunedPartitionNumbers(2, -1))
      ) { case (partitions, HighestPrunedPartitionNumbers(3, -1)) =>
        partitions should not contain s"${PartitionManager.batchesTable}_p3"
        forAll(partitions)(p => p should endWith("_p3"))
      }
      // same result as above since haven't yet pruned partition 3 and that is the previous one
      inside(
        partitionsToPrune(partitionSize, EpochNumber(400), HighestPrunedPartitionNumbers(2, -1))
      ) { case (partitions, HighestPrunedPartitionNumbers(3, -1)) =>
        partitions should not contain s"${PartitionManager.batchesTable}_p3"
        forAll(partitions)(p => p should endWith("_p3"))
      }

      // batches table pruning takes place 500 epochs earlier
      inside(
        partitionsToPrune(partitionSize, EpochNumber(600), HighestPrunedPartitionNumbers(5, -1))
      ) { case (partitions, HighestPrunedPartitionNumbers(5, 0)) =>
        partitions should contain only (s"${PartitionManager.batchesTable}_p0")
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

  "PartitionManagerDatabase" should {
    "prune and creation of partitions does not error" in {
      val initStore = new PruningManagerInitStore(storage, timeouts, loggerFactory)
      def getMinMaxPartitionNumbers =
        initStore.queryLowestAndHighestExistingPartitionNumberPerTableName

      (for {
        case Some((creator, pruner)) <- create().futureUnlessShutdown()

        partitionSizeEntry <- initStore.latestPartitionSizeEntry
        _ = partitionSizeEntry shouldBe PartitionManager.PartitionSizeEntry(EpochNumber(0), 0, 100)

        (minMap0, maxMap0) <- getMinMaxPartitionNumbers
        _ = forAll(minMap0.values)(_ shouldBe (0))
        _ = forAll(maxMap0.values)(_ shouldBe (1))

        msg <- pruner.prune(EpochNumber(98L)).futureUnlessShutdown()
        (minMap1, maxMap1) <- getMinMaxPartitionNumbers
        _ = msg shouldBe "Pruned no partitions at epoch 98"
        _ = forAll(minMap1.values)(_ shouldBe (0))
        _ = forAll(maxMap1.values)(_ shouldBe (1))

        msg2 <- pruner.prune(EpochNumber(99L)).futureUnlessShutdown()
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
  }

}
