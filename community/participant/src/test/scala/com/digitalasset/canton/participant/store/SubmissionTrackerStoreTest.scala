// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, TestHash}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.UnlessShutdown.Outcome
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.store.PrunableByTimeTest
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.HasTestCloseContext
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait SubmissionTrackerStoreTest extends AsyncWordSpec with BaseTest with PrunableByTimeTest {
  private def mkHash(data: String): Hash = Hash
    .build(TestHash.testHashPurpose, HashAlgorithm.Sha256)
    .addWithoutLengthPrefix(data)
    .finish()

  private lazy val rootHashes =
    Seq("falafel", "hummus", "tahini", "fattoush", "tabbouleh").map(seed => RootHash(mkHash(seed)))
  private lazy val requestIds =
    (1 to rootHashes.size).map(i => RequestId(CantonTimestamp.Epoch.plusSeconds(i.toLong)))
  private lazy val maxSeqTimes =
    (1 to rootHashes.size).map(i => CantonTimestamp.Epoch.plusSeconds(i.toLong + 10))

  def submissionTrackerStore(mkStore: () => SubmissionTrackerStore): Unit = {
    behave like prunableByTime(_ => mkStore())

    "when registering" should {
      "confirm a first registration is fresh" in {
        val store = mkStore()

        for {
          result <- store.registerFreshRequest(rootHashes(0), requestIds(0), maxSeqTimes(0)).unwrap
        } yield {
          result shouldBe Outcome(true)
        }
      }

      "be idempotent" in {
        val store = mkStore()

        for {
          _ <- store.registerFreshRequest(rootHashes(0), requestIds(0), maxSeqTimes(0)).unwrap
          result <- store.registerFreshRequest(rootHashes(0), requestIds(0), maxSeqTimes(0)).unwrap
        } yield {
          result shouldBe Outcome(true)
        }
      }

      "signal a replay when registering a different request for an existing root hash" in {
        val store = mkStore()

        for {
          first <- store.registerFreshRequest(rootHashes(0), requestIds(0), maxSeqTimes(0)).unwrap
          second <- store.registerFreshRequest(rootHashes(0), requestIds(1), maxSeqTimes(1)).unwrap
        } yield {
          first shouldBe Outcome(true)
          second shouldBe Outcome(false)
        }
      }
    }

    "delete entries when pruning" in {
      val store = mkStore()
      implicit val closeContext = HasTestCloseContext.makeTestCloseContext(logger)

      val pruningTs = maxSeqTimes(1)
      val expectedCountAfterPrune = maxSeqTimes.count(_ > pruningTs)

      for {
        initialCount <- store.size.unwrap

        _ <- Future.sequence(rootHashes.indices.map { i =>
          store.registerFreshRequest(rootHashes(i), requestIds(i), maxSeqTimes(i)).unwrap
        })
        finalCount <- store.size.unwrap

        _ <- store.prune(pruningTs).failOnShutdown
        countAfterPrune <- store.size.unwrap

        _ <- store.purge().failOnShutdown
        countAfterPurge <- store.size.unwrap
      } yield {
        initialCount shouldBe Outcome(0)
        finalCount shouldBe Outcome(rootHashes.size)
        countAfterPrune shouldBe Outcome(expectedCountAfterPrune)
        countAfterPurge shouldBe Outcome(0)
      }
    }

    "delete entries when cleaning up" in {
      val store = mkStore()

      val cleanupTs = requestIds(3).unwrap
      val expectedCountAfterDelete = requestIds.count(_.unwrap < cleanupTs)

      for {
        initialCount <- store.size.failOnShutdown

        _ <- FutureUnlessShutdown
          .sequence(rootHashes.indices.map { i =>
            store.registerFreshRequest(rootHashes(i), requestIds(i), maxSeqTimes(i))
          })
          .failOnShutdown
        finalCount <- store.size.failOnShutdown

        _ <- store.deleteSince(cleanupTs).failOnShutdown
        countAfterDelete <- store.size.failOnShutdown
      } yield {
        initialCount shouldBe 0
        finalCount shouldBe rootHashes.size
        countAfterDelete shouldBe expectedCountAfterDelete
      }
    }

    "delete chunk of data" in {
      val store = mkStore()

      val indices = rootHashes.take(5).indices
      indices should have size 5

      for {
        _ <- MonadUtil
          .sequentialTraverse(rootHashes.indices) { i =>
            store.registerFreshRequest(rootHashes(i), requestIds(i), maxSeqTimes(i))
          }
          .failOnShutdown

        initialSize <- store.size.failOnShutdown
        _ = initialSize shouldBe 5

        data <- MonadUtil
          .sequentialTraverse((1 to 4)) { _ =>
            for {
              deleted <- store.deleteDataChunk(PositiveInt.two)
              newSize <- store.size
            } yield (deleted, newSize)
          }
          .failOnShutdown

      } yield {
        /*
          Purging is done 4 times
          Size should decrease by 2 initially, then 1
          The boolean indicates whether data was deleted
         */

        data shouldBe Seq((true, 3), (true, 1), (true, 0), (false, 0))
      }
    }
  }

}
