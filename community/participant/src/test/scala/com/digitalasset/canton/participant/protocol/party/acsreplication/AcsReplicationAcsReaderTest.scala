// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party.acsreplication

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.mutable

/** Ensure that reading active contracts returns properly sized batches and never deadlocks by
  * waiting indefinitely instead of sending available batches before the queue stops growing.
  */
class AcsReplicationAcsReaderTest extends AnyWordSpec with BaseTest {

  private def queue(size: Int): mutable.Queue[Int] = mutable.Queue.from(1 to size)

  private def read(
      tpBatchSize: Int,
      spBatchSize: Int,
      q: mutable.Queue[Int],
      donePushing: Boolean,
  ) =
    AcsReplicationAcsReader.readContracts(
      PositiveInt.tryCreate(tpBatchSize),
      PositiveInt.tryCreate(spBatchSize),
      q,
      donePushing,
    )

  private def enqueue(size: Int, q: mutable.Queue[Int]): Unit = {
    val last = q.last
    q.enqueueAll(1 + last to size + last)
  }

  private val (isNotDone, isDone) = (false, true)

  "AcsReplicationAcsReader.readContracts" should {
    "return batches when TP batch size > SP batch size" in {
      val q = queue(25)
      val (done12, batches12) = read(100, 10, q, donePushing = false)
      (done12, batches12) shouldBe (isNotDone, Seq(1 to 10, 11 to 20))

      // Don't return a "partial batch" if we still expect the queue to grow.
      val (doneNone, batchesNone) = read(80, 10, q, donePushing = false)
      (doneNone, batchesNone) shouldBe (isNotDone, Seq.empty)

      // More batches to read after enqueuing.
      enqueue(36, q)
      val (done3456, batches3456) = read(80, 10, q, donePushing = false)
      (done3456, batches3456) shouldBe (isNotDone, Seq(21 to 30, 31 to 40, 41 to 50, 51 to 60))

      // Once queue is known to stop growing, read partial batch.
      val (done7, batches7) = read(40, 10, q, donePushing = true)
      (done7, batches7) shouldBe (isDone, Seq(61 to 61))
    }

    "return at most single batch when SP size > TP batch size" in {
      val q = queue(25)
      // Returned batch is gated by lower TP batch size.
      val (done1, batch1) = read(10, 100, q, donePushing = false)
      (done1, batch1) shouldBe (isNotDone, Seq(1 to 10))

      // Don't return a "partial batch" if we still expect the queue to grow.
      val (doneNone, batchNone) = read(20, 100, q, donePushing = false)
      (doneNone, batchNone) shouldBe (isNotDone, Seq.empty)

      // More batches to read after enqueuing.
      enqueue(36, q)
      val (done2, batch2) = read(20, 100, q, donePushing = false)
      (done2, batch2) shouldBe (isNotDone, Seq(11 to 30))

      // Once queue is known to stop growing, properly read remaining batches one by one.
      val (done3, batch3) = read(20, 100, q, donePushing = true)
      (done3, batch3) shouldBe (isNotDone, Seq(31 to 50))
      val (done4, batch4) = read(20, 100, q, donePushing = true)
      (done4, batch4) shouldBe (isDone, Seq(51 to 61))

      // Nothing else to read after queue is fully drained.
      read(20, 100, q, donePushing = true) shouldBe (isDone, Seq.empty)
    }
  }
}
