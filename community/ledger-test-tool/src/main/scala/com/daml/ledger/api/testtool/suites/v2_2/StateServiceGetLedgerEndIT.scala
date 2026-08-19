// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_2

import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  Participant,
  Participants,
  SingleParty,
  allocate,
}
import com.daml.ledger.api.testtool.infrastructure.Assertions.{
  assertDefined,
  assertEquals,
  assertGrpcError,
}
import com.daml.ledger.api.testtool.infrastructure.Eventually.eventually
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.test.java.model.test.Dummy
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import org.scalatest.Inside.inside

import scala.math.Ordered.orderingToOrdered

final class StateServiceGetLedgerEndIT extends LedgerTestSuite {

  test(
    "StateServiceGetLedgerEnd",
    "Get ledger end should return an offset and synchronizer record time",
    allocate(SingleParty).expectingMinimumNumberOfSynchronizers(2),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val (synchronizer1, synchronizer2) =
      inside(party.initialSynchronizers) { case Seq(synchronizer1, synchronizer2, _*) =>
        synchronizer1 -> synchronizer2
      }
    for {
      ledgerEnd0 <- ledger.getLedgerEnd(Seq(synchronizer1, synchronizer2))
      requestForSynchronizer1 = ledger
        .submitAndWaitForTransactionRequest(party, new Dummy(party).create.commands)
        .update(_.commands.synchronizerId := synchronizer1)
      tr1 <- ledger.submitAndWaitForTransaction(requestForSynchronizer1)
      ledgerEnd1 <- eventually("wait for contract db pruning finishes") { // After transaction is ingested, contract db pruning kicks in and increases offset twice
        ledger.getLedgerEnd(Seq(synchronizer1, synchronizer2)).map { le =>
          assert(le.offset == tr1.transaction.value.offset + 2)
          le
        }
      }
      requestForSynchronizer2 = ledger
        .submitAndWaitForTransactionRequest(party, new Dummy(party).create.commands)
        .update(_.commands.synchronizerId := synchronizer2)
      tr2 <- ledger.submitAndWaitForTransaction(requestForSynchronizer2)
      ledgerEnd2 <- eventually("wait for contract db pruning finishes") { // After transaction is ingested, contract db pruning kicks in and increases offset twice
        ledger.getLedgerEnd(Seq(synchronizer1, synchronizer2)).map { le =>
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

  test(
    "StateServiceLedgerEndSynchronizerFilter",
    "Get ledger end should return synchronizer index for synchronizers selected by filter only",
    allocate(SingleParty).expectingMinimumNumberOfSynchronizers(2),
    runConcurrently = true,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val (synchronizer1, synchronizer2) =
      inside(party.initialSynchronizers) { case Seq(synchronizer1, synchronizer2, _*) =>
        synchronizer1 -> synchronizer2
      }
    for {
      ledgerEndBothSynchronizers <- ledger.getLedgerEnd(Seq(synchronizer1, synchronizer2))
      ledgerEndFirstSynchronizer <- ledger.getLedgerEnd(Seq(synchronizer1))
      ledgerEndSecondSynchronizer <- ledger.getLedgerEnd(Seq(synchronizer2))
      ledgerEndNoSynchronizer <- ledger.getLedgerEnd(Seq())
    } yield {
      assert(ledgerEndBothSynchronizers.synchronizerTimes.sizeIs == 2)
      assert(
        ledgerEndBothSynchronizers.synchronizerTimes.map(_.synchronizerId).contains(synchronizer1)
      )
      assert(
        ledgerEndBothSynchronizers.synchronizerTimes.map(_.synchronizerId).contains(synchronizer2)
      )
      assertEquals(
        ledgerEndFirstSynchronizer.synchronizerTimes.map(_.synchronizerId),
        List(synchronizer1),
      )
      assertEquals(
        ledgerEndSecondSynchronizer.synchronizerTimes.map(_.synchronizerId),
        List(synchronizer2),
      )
      assertEquals(ledgerEndNoSynchronizer.synchronizerTimes.map(_.synchronizerId), List())
    }
  })

  test(
    "StateServiceLedgerEndNonExistentSynchronizerFilter",
    "Get ledger end should return an error for filter containing only not connected synchronizers",
    allocate(SingleParty).expectingMinimumNumberOfSynchronizers(2),
    runConcurrently = true,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val (synchronizer1, synchronizer2) =
      inside(party.initialSynchronizers) { case Seq(synchronizer1, synchronizer2, _*) =>
        synchronizer1 -> synchronizer2
      }
    for {
      ledgerEndError <- ledger.getLedgerEnd(Seq("nx::synchronizer-1", "nx::synchronizer-2")).failed
    } yield {
      assertGrpcError(
        ledgerEndError,
        RequestValidationErrors.NoRecordTimeFoundForSynchronizerId.code,
        Some("No record time found for synchronizer ids: nx::synchronizer-1, nx::synchronizer-2"),
      )
    }
  })

  test(
    "StateServiceLedgerEndExistendAndNonExistentSynchronizerFilter",
    "Get ledger end return error for synchronizer index for filter containing both connected and not connected synchronizer",
    allocate(SingleParty).expectingMinimumNumberOfSynchronizers(2),
    runConcurrently = true,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val (synchronizer1, synchronizer2) =
      inside(party.initialSynchronizers) { case Seq(synchronizer1, synchronizer2, _*) =>
        synchronizer1 -> synchronizer2
      }
    for {
      ledgerEndError <- ledger
        .getLedgerEnd(Seq(synchronizer1, "nx::synchronizer-1", "nx::synchronizer-2"))
        .failed
    } yield {
      assertGrpcError(
        ledgerEndError,
        RequestValidationErrors.NoRecordTimeFoundForSynchronizerId.code,
        Some("No record time found for synchronizer ids: nx::synchronizer-1, nx::synchronizer-2"),
      )
    }
  })

  test(
    "StateServiceLedgerEndMalformedSynchronizerId",
    "Get ledger end should return an error when filtering with malformed synchronizer id",
    allocate(SingleParty).expectingMinimumNumberOfSynchronizers(2),
    runConcurrently = true,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val (synchronizer1, synchronizer2) =
      inside(party.initialSynchronizers) { case Seq(synchronizer1, synchronizer2, _*) =>
        synchronizer1 -> synchronizer2
      }
    for {
      ledgerEndError <- ledger.getLedgerEnd(Seq(synchronizer1, "sy*nchron::i//zer")).failed
    } yield {
      assertGrpcError(
        ledgerEndError,
        RequestValidationErrors.InvalidField.code,
        Some(
          "The submitted command has a field with invalid value: Invalid field synchronizer_id: Identifier decoding of `sy*nchron::i//zer` failed with: non expected character 0x2a in Daml-LF Party \"sy*nchron"
        ),
      )
    }
  })
}
