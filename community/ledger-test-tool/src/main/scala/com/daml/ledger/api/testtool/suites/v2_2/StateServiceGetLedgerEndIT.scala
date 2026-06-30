// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_2

import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  Participant,
  Participants,
  SingleParty,
  allocate,
}
import com.daml.ledger.api.testtool.infrastructure.Assertions.assertDefined
import com.daml.ledger.api.testtool.infrastructure.Eventually.eventually
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.test.java.model.test.Dummy
import org.scalatest.Inside.inside

import scala.math.Ordered.orderingToOrdered

final class StateServiceGetLedgerEndIT extends LedgerTestSuite {

  test(
    "StateServiceGetLedgerEnd",
    "Get ledger end should return an offset and synchronizer record time",
    allocate(SingleParty).expectingMinimumNumberOfSynchronizers(2),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      ledgerEnd0 <- ledger.getLedgerEnd()
      (synchronizer1, synchronizer2) =
        inside(party.initialSynchronizers) { case Seq(synchronizer1, synchronizer2, _*) =>
          synchronizer1 -> synchronizer2
        }
      requestForSynchronizer1 = ledger
        .submitAndWaitForTransactionRequest(party, new Dummy(party).create.commands)
        .update(_.commands.synchronizerId := synchronizer1)
      tr1 <- ledger.submitAndWaitForTransaction(requestForSynchronizer1)
      ledgerEnd1 <- eventually("wait for contract db pruning finishes") { // After transaction is ingested, contract db pruning kicks in and increases offset twice
        ledger.getLedgerEnd().map { le =>
          assert(le.offset == tr1.transaction.value.offset + 2)
          le
        }
      }
      requestForSynchronizer2 = ledger
        .submitAndWaitForTransactionRequest(party, new Dummy(party).create.commands)
        .update(_.commands.synchronizerId := synchronizer2)
      tr2 <- ledger.submitAndWaitForTransaction(requestForSynchronizer2)
      ledgerEnd2 <- eventually("wait for contract db pruning finishes") { // After transaction is ingested, contract db pruning kicks in and increases offset twice
        ledger.getLedgerEnd().map { le =>
          assert(le.offset == ledgerEnd1.offset + 3)
          le
        }
      }
    } yield {
      val transaction1 = tr1.transaction.value
      assert(
        ledgerEnd1.offset == transaction1.offset + 2,
        s"ledger end offset after first transaction: expected ${transaction1.offset}+2 but got ${ledgerEnd1.offset}",
      )
      val synchronizer1RecordTimeAfterTx1 = assertDefined(
        ledgerEnd1.synchronizerTimes.find(_.synchronizerId == transaction1.synchronizerId),
        s"ledger end after first transaction must contain record time for the first synchronizer($synchronizer1), got ${ledgerEnd1.synchronizerTimes
            .map(_.synchronizerId)}",
      ).recordTime
      assert(
        synchronizer1RecordTimeAfterTx1 >= transaction1.recordTime,
        s"ledger end record time for synchronizer ${transaction1.synchronizerId} after first transaction: expected ${transaction1.recordTime} but got $synchronizer1RecordTimeAfterTx1",
      )

      val transaction2 = tr2.transaction.value
      assert(
        ledgerEnd2.offset == transaction2.offset + 2,
        s"ledger end offset after second transaction: expected ${transaction2.offset}+2 but got ${ledgerEnd2.offset}",
      )
      val synchronizer1RecordTimeAfterTx2 = assertDefined(
        ledgerEnd2.synchronizerTimes.find(_.synchronizerId == transaction1.synchronizerId),
        s"ledger end after second transaction must still contain record time for the first synchronizer(${transaction1.synchronizerId}), got ${ledgerEnd2.synchronizerTimes
            .map(_.synchronizerId)}",
      ).recordTime
      assert(
        synchronizer1RecordTimeAfterTx2 == synchronizer1RecordTimeAfterTx1,
        s"ledger end record time for synchronizer ${transaction1.synchronizerId} after second transaction (unchanged from first transaction): expected ${transaction1.recordTime} but got $synchronizer1RecordTimeAfterTx2",
      )
      val synchronizer2RecordTimeAfterTx2 = assertDefined(
        ledgerEnd2.synchronizerTimes.find(_.synchronizerId == transaction2.synchronizerId),
        s"ledger end after second transaction must contain record time for the second synchronizer (${transaction2.synchronizerId}), got ${ledgerEnd2.synchronizerTimes
            .map(_.synchronizerId)}",
      ).recordTime
      assert(
        synchronizer2RecordTimeAfterTx2 >= transaction2.recordTime,
        s"ledger end record time for synchronizer ${transaction2.synchronizerId} after second transaction: expected ${transaction2.recordTime} but got $synchronizer2RecordTimeAfterTx2",
      )
    }
  })
}
