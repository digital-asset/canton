// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.component

import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.unused

class StateServiceComponentTest extends AnyWordSpec with IndexComponentTest {
  private val nextRecordTime = new SingleStepIncreasingRecordTime

  "state service" should {
    "track ledger end with synchronizer indices after each transaction" in {
      val synch1Create1 =
        creates(nextRecordTime, payloadLength = 10, synchronizer = synchronizer1)(size = 1)
      val synch1Create2 =
        creates(nextRecordTime, payloadLength = 10, synchronizer = synchronizer1)(size = 1)
      val synch2Create =
        creates(nextRecordTime, payloadLength = 10, synchronizer = synchronizer2)(size = 1)
      val synch1Create3 =
        creates(nextRecordTime, payloadLength = 10, synchronizer = synchronizer1)(size = 1)

      restartServices()
      index.currentLedgerEnd() shouldBe None

      val offset1 = ingestUpdates(synch1Create1)
      val ledgerEnd1 = index.currentLedgerEnd().value
      ledgerEnd1.lastOffset shouldBe (offset1)
      ledgerEnd1.synchronizerIndices shouldBe (Map(
        synchronizer1 -> synch1Create1._1.synchronizerIndex
      ))

      val offset2 = ingestUpdates(synch1Create2)
      val ledgerEnd2 = index.currentLedgerEnd().value
      ledgerEnd2.lastOffset shouldBe (offset2)
      ledgerEnd2.synchronizerIndices shouldBe (Map(
        synchronizer1 -> synch1Create2._1.synchronizerIndex
      ))

      val offset3 = ingestUpdates(synch2Create)
      val ledgerEnd3 = index.currentLedgerEnd().value
      ledgerEnd3.lastOffset shouldBe (offset3)
      ledgerEnd3.synchronizerIndices shouldBe (Map(
        synchronizer1 -> synch1Create2._1.synchronizerIndex,
        synchronizer2 -> synch2Create._1.synchronizerIndex,
      ))

      val offset4 = ingestUpdates(synch1Create3)
      val ledgerEnd4 = index.currentLedgerEnd().value
      ledgerEnd4.lastOffset shouldBe (offset4)
      ledgerEnd4.synchronizerIndices shouldBe (Map(
        synchronizer1 -> synch1Create3._1.synchronizerIndex,
        synchronizer2 -> synch2Create._1.synchronizerIndex,
      ))
    }

    "convert record time to offset where there is a single transaction at given record time" in {
      val synch1Create1 =
        creates(nextRecordTime, payloadLength = 10, synchronizer = synchronizer1)(size = 1)
      val synch2Create =
        creates(nextRecordTime, payloadLength = 10, synchronizer = synchronizer2)(size = 1)
      val synch1Create2 =
        creates(nextRecordTime, payloadLength = 10, synchronizer = synchronizer1)(size = 1)
      val offset1 = ingestUpdates(synch1Create1)
      val offset2 = ingestUpdates(synch2Create)
      val offset3 = ingestUpdates(synch1Create2)

      index
        .highestOffsetBeforeOrFirstAt(synchronizer1, synch1Create1._1.recordTime)
        .futureValue should equal(Some(offset1))
      index
        .highestOffsetBeforeOrFirstAt(synchronizer2, synch1Create2._1.recordTime)
        .futureValue should equal(Some(offset2))
      index
        .highestOffsetBeforeOrFirstAt(synchronizer1, synch1Create2._1.recordTime)
        .futureValue should equal(Some(offset3))
    }

    "convert record time to offset should provide correct offset if both synchronizers sequenced an event at the same time" in {
      val sharedRecordTime = nextRecordTime()
      val synch1Create =
        creates(() => sharedRecordTime, payloadLength = 10, synchronizer = synchronizer1)(size = 1)
      val synch2Create =
        creates(() => sharedRecordTime, payloadLength = 10, synchronizer = synchronizer2)(size = 1)
      val offset1 = ingestUpdates(synch1Create)
      val offset2 = ingestUpdates(synch2Create)

      index.highestOffsetBeforeOrFirstAt(synchronizer1, sharedRecordTime).futureValue should equal(
        Some(offset1)
      )
      index.highestOffsetBeforeOrFirstAt(synchronizer2, sharedRecordTime).futureValue should equal(
        Some(offset2)
      )
    }

    "convert record time to offset should return the first offset when there are multiple events at the record time" in {
      val synch1Create =
        creates(nextRecordTime, payloadLength = 10, synchronizer = synchronizer1)(size = 1)
      val createTransactionOffset = ingestUpdates(synch1Create)
      val topoTransactionOffset =
        ingestTopologyEvents(parties = Set("alice"), recordTime = synch1Create._1.recordTime)

      topoTransactionOffset should be > (createTransactionOffset)
      index
        .highestOffsetBeforeOrFirstAt(synchronizer1, synch1Create._1.recordTime)
        .futureValue should equal(Some(createTransactionOffset))
    }

    "convert record time to offset properly when there are regular create and repair at the same record time" in {
      val realCreate =
        creates(nextRecordTime, payloadLength = 10, synchronizer = synchronizer1)(size = 1)
      val repairCreate1 =
        repairCreates(() => realCreate._1.recordTime, payloadLength = 10)(size = 1)

      val realOffset = ingestUpdates(realCreate)
      @unused
      val repairOffset = ingestUpdates(repairCreate1)

      val recordTimeInBetween = nextRecordTime()
      val realCreateAfter =
        creates(nextRecordTime, payloadLength = 10, synchronizer = synchronizer1)(size = 1)
      val realOffsetAfter = ingestUpdates(realCreateAfter)

      index
        .highestOffsetBeforeOrFirstAt(synchronizer1, realCreate._1.recordTime)
        .futureValue shouldEqual (Some(realOffset))
      index
        .highestOffsetBeforeOrFirstAt(synchronizer1, recordTimeInBetween)
        .futureValue shouldEqual (Some(repairOffset))
      index
        .highestOffsetBeforeOrFirstAt(synchronizer1, realCreateAfter._1.recordTime)
        .futureValue shouldEqual (Some(realOffsetAfter))
    }
  }
}
