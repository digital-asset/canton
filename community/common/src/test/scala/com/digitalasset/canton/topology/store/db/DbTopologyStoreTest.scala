// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.db

import com.digitalasset.canton.config.{BatchAggregatorConfig, TopologyConfig}
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerPredecessor}
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.topology.processing.{InitialTopologySnapshotValidator, SequencedTime}
import com.digitalasset.canton.topology.store.{StoredTopologyTransactions, TopologyStoreTest}
import com.digitalasset.canton.topology.transaction.TopologyMapping

trait DbTopologyStoreTest extends TopologyStoreTest with DbTopologyStoreHelper {
  this: DbTest =>

  "DbTopologyStore" should {
    behave like topologyStore(mkStore)

    "properly handle insertion order for large topology snapshots" in {
      val store = mkStore(testData.synchronizer1_p1p2_physicalSynchronizerId, "dbtest1")

      for {
        _ <- new InitialTopologySnapshotValidator(
          testData.factory.syncCryptoClient.crypto.pureCrypto,
          store,
          BatchAggregatorConfig.defaultsForTesting,
          TopologyConfig.forTesting.copy(validateInitialTopologySnapshot = true),
          Some(defaultStaticSynchronizerParameters),
          timeouts,
          futureSupervisor = futureSupervisor,
          loggerFactory,
        ).validateAndApplyInitialTopologySnapshot(largeTestSnapshot)
          .valueOrFail("topology bootstrap")

        maxTimestamp <- store
          .maxTimestamp(SequencedTime.MaxValue, includeRejected = true)
      } yield {
        val lastSequenced = largeTestSnapshot.result.last.sequenced
        val lastEffective = largeTestSnapshot.result.last.validFrom
        maxTimestamp shouldBe Some((lastSequenced, lastEffective))
      }
    }

    "copyFromPredecessorSynchronizerStore" should {
      "reject when source does not match configured predecessor" in {
        val sourcePsid = testData.synchronizer1_p1p2_physicalSynchronizerId
        val targetPsid = sourcePsid.incrementSerial
        val unrelatedLsid = testData.da_vp123_physicalSynchronizerId

        val sourceStore = mkStore(sourcePsid, "correct predecessor")
        val unrelatedStore = mkStore(unrelatedLsid, "predecessorMismatch")
        val targetStore = mkStoreWithPredecessor(
          targetPsid,
          "predecessorMismatch",
          predecessor = Some(
            SynchronizerPredecessor(
              psid = sourcePsid,
              upgradeTime = CantonTimestamp.Epoch,
              isLateUpgrade = false,
            )
          ),
        )

        for {
          _ <- targetStore.copyFromPredecessorSynchronizerStore(sourceStore)

          _ <- loggerFactory.assertLoggedWarningsAndErrorsSeq(
            targetStore.copyFromPredecessorSynchronizerStore(unrelatedStore).failed,
            _.loneElement.throwable.value.getMessage should include(
              "does not match the configured predecessor"
            ),
          )
        } yield succeed
      }
      "correctly work" in {
        val sourcePsid = testData.synchronizer1_p1p2_physicalSynchronizerId
        val sourceStore = mkStore(sourcePsid, "case12")
        val successor = sourcePsid.incrementSerial
        val targetStore = mkStoreWithPredecessor(
          successor,
          "case12",
          predecessor = Some(
            SynchronizerPredecessor(
              psid = sourcePsid,
              upgradeTime = CantonTimestamp.Epoch,
              isLateUpgrade = false,
            )
          ),
        )

        for {
          _ <- new InitialTopologySnapshotValidator(
            pureCrypto = testData.factory.syncCryptoClient.crypto.pureCrypto,
            store = sourceStore,
            topologyCacheAggregatorConfig = BatchAggregatorConfig.defaultsForTesting,
            topologyConfig = TopologyConfig.forTesting,
            staticSynchronizerParameters = Some(defaultStaticSynchronizerParameters),
            timeouts,
            futureSupervisor = futureSupervisor,
            loggerFactory = loggerFactory.appendUnnamedKey("TestName", "case12"),
            cleanupTopologySnapshot = true,
          ).validateAndApplyInitialTopologySnapshot(testData.bootstrapTransactions)
            .valueOrFail("topology bootstrap")

          targetDataBeforeCopy <- targetStore.dumpStoreContent()
          _ = targetDataBeforeCopy.result shouldBe empty

          _ <- targetStore.copyFromPredecessorSynchronizerStore(sourceStore)
          sourceData <- sourceStore.dumpStoreContent()
          targetData <- targetStore.dumpStoreContent()

          // verify that the copy is idempotent
          _ <- targetStore.copyFromPredecessorSynchronizerStore(sourceStore)
          targetData2 <- targetStore.dumpStoreContent()

        } yield {
          val actual = targetData.result
          val expected = sourceData.result.view
            .filter(_.rejectionReason.isEmpty)
            .filter((stored => !stored.transaction.isProposal || stored.validUntil.isEmpty))
            .toSeq

          actual should contain theSameElementsInOrderAs expected
          targetData2.result should contain theSameElementsInOrderAs expected
        }
      }

      "resume copyFromPredecessorSynchronizerStore from the last imported transaction" in {
        val sourcePsid = testData.synchronizer1_p1p2_physicalSynchronizerId
        val sourceStore =
          mkStore(sourcePsid, "dbtestResume")
        val successor = sourcePsid.incrementSerial
        val targetStore = mkStoreWithPredecessor(
          successor,
          "dbtestResume",
          predecessor = Some(
            SynchronizerPredecessor(
              psid = sourcePsid,
              upgradeTime = CantonTimestamp.Epoch,
              isLateUpgrade = false,
            )
          ),
        )

        val prefix = StoredTopologyTransactions(largeTestSnapshot.result.take(10))

        for {
          _ <- sourceStore.bulkInsert(largeTestSnapshot)
          // simulate a previous, partial copy by pre-inserting a prefix into the target
          _ <- targetStore.bulkInsert(prefix)

          _ <- targetStore.copyFromPredecessorSynchronizerStore(sourceStore)

          sourceData <- sourceStore.dumpStoreContent()
          targetData <- targetStore.dumpStoreContent()
        } yield {
          val expected = sourceData.result.view
            .filter(_.rejectionReason.isEmpty)
            .filter(stored => !stored.transaction.isProposal || stored.validUntil.isEmpty)
            .toSeq
          targetData.result should contain theSameElementsInOrderAs expected
        }

      }
    }

    "properly insert transactions in bulk" in {
      val store = mkStore(testData.synchronizer1_p1p2_physicalSynchronizerId, "dbtest1")
      for {
        _ <- store.bulkInsert(largeTestSnapshot)
        txs <- store.findPositiveTransactions(
          CantonTimestamp.MaxValue,
          asOfInclusive = true,
          isProposal = false,
          types = TopologyMapping.Code.all,
          filterUid = None,
          filterNamespace = None,
        )
      } yield {
        txs.result shouldBe largeTestSnapshot.result
      }
    }

  }
}

class TopologyStoreTestPostgres extends DbTopologyStoreTest with PostgresTest

class TopologyStoreTestH2 extends DbTopologyStoreTest with H2Test
