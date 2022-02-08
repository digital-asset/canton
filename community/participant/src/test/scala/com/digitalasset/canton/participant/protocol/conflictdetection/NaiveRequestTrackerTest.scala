// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.RequestCounter
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.store.memory.{InMemoryTransferStore, TransferCache}
import com.digitalasset.canton.participant.store.{
  ActiveContractStore,
  ContractKeyJournal,
  TransferStoreTest,
}
import com.digitalasset.canton.{BaseTest, HasExecutorService, SequencerCounter}
import org.scalatest.wordspec.AsyncWordSpec

class NaiveRequestTrackerTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutorService
    with ConflictDetectionHelpers
    with RequestTrackerTest {

  def mk(
      rc: RequestCounter,
      sc: SequencerCounter,
      ts: CantonTimestamp,
      acs: ActiveContractStore,
      ckj: ContractKeyJournal,
  ): NaiveRequestTracker = {
    val transferCache =
      new TransferCache(
        new InMemoryTransferStore(TransferStoreTest.targetDomain, loggerFactory),
        loggerFactory,
      )

    val conflictDetector =
      new ConflictDetector(
        acs,
        ckj,
        transferCache,
        loggerFactory,
        checkedInvariant = true,
        parallelExecutionContext,
        timeouts,
      )

    new NaiveRequestTracker(
      sc,
      ts,
      conflictDetector,
      ParticipantTestMetrics.domain.conflictDetection,
      timeouts,
      loggerFactory,
    )
  }

  "NaiveRequestTracker" should {
    behave like requestTracker(mk)
  }

  "requests are evicted when they are finalized" in {
    for {
      acs <- mkAcs()
      ckj <- mkCkj()
      rt = mk(0L, 0L, CantonTimestamp.MinValue, acs, ckj)
      (cdF, toF) <- enterCR(
        rt,
        0L,
        1L,
        CantonTimestamp.ofEpochMilli(1),
        CantonTimestamp.ofEpochMilli(10),
        ActivenessSet.empty,
      )
      _ = assert(rt.requestInFlight(0L), "Request present immediately after adding")
      _ = enterTick(rt, 0L, CantonTimestamp.Epoch)
      _ <- checkConflictResult(0L, cdF, ActivenessResult.success)
      _ = assert(rt.requestInFlight(0L), "Request present immediately after conflict detection")
      finalize0 <- enterTR(rt, 0L, 2L, CantonTimestamp.ofEpochMilli(2), CommitSet.empty, 1L, toF)
      _ = assert(rt.requestInFlight(0L), "Request present immediately after transaction result")
      _ = enterTick(rt, 3L, CantonTimestamp.ofEpochMilli(3))
      _ <- finalize0.map(result => assert(result == Right(())))
      _ = assert(!rt.requestInFlight(0L), "Request evicted immediately after finalization")
    } yield succeed
  }

  "requests are evicted when they time out" in {
    for {
      acs <- mkAcs()
      ckj <- mkCkj()
      rt = mk(0L, 0L, CantonTimestamp.Epoch, acs, ckj)
      (cdF, toF) <- enterCR(
        rt,
        0L,
        0L,
        CantonTimestamp.ofEpochMilli(1),
        CantonTimestamp.ofEpochMilli(10),
        ActivenessSet.empty,
      )
      _ <- checkConflictResult(0L, cdF, ActivenessResult.success)
      _ = enterTick(rt, 1L, CantonTimestamp.ofEpochMilli(10))
      _ <- toF.map(timeout => assert(timeout.timedOut))
      _ = assert(!rt.requestInFlight(0L), "Request evicted immediately after timeout")
    } yield succeed
  }
}
