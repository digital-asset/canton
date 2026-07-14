// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.component

import org.scalatest.wordspec.AnyWordSpec

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
  }
}
