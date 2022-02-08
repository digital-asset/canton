// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import cats.data.NonEmptyChain
import cats.syntax.foldable._
import com.digitalasset.canton._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.RequestCounter
import com.digitalasset.canton.participant.protocol.conflictdetection.ConflictDetector.LockedStates
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker._
import com.digitalasset.canton.participant.store.ActiveContractStore._
import com.digitalasset.canton.participant.store.{ActiveContractStore, ContractKeyJournal}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, LfContractId}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant
import scala.concurrent.Future
import scala.util.{Failure, Success}

trait RequestTrackerTest { this: AsyncWordSpec with BaseTest with ConflictDetectionHelpers =>
  import ConflictDetectionHelpers._

  val coid00: LfContractId = ExampleTransactionFactory.suffixedId(0, 0)
  val coid01: LfContractId = ExampleTransactionFactory.suffixedId(0, 1)
  val coid10: LfContractId = ExampleTransactionFactory.suffixedId(1, 0)
  val coid11: LfContractId = ExampleTransactionFactory.suffixedId(1, 1)

  def requestTracker(
      genMk: (
          RequestCounter,
          SequencerCounter,
          CantonTimestamp,
          ActiveContractStore,
          ContractKeyJournal,
      ) => RequestTracker
  ): Unit = {
    import com.digitalasset.canton.data.CantonTimestamp.ofEpochMilli

    def mk(
        rc: RequestCounter,
        sc: SequencerCounter,
        ts: CantonTimestamp,
        acs: ActiveContractStore = mkEmptyAcs(),
        ckj: ContractKeyJournal = mkEmptyCkj(),
    ): RequestTracker = genMk(rc, sc, ts, acs, ckj)

    "allow transaction result at the decision time" in {
      val ts = CantonTimestamp.Epoch
      val ts1 = ts.plusMillis(1)
      val rt = mk(0L, 0L, CantonTimestamp.MinValue)
      for {
        _ <- singleCRwithTR(
          rt,
          0,
          0,
          ts,
          ts1,
          ActivenessSet.empty,
          ActivenessResult.success,
          CommitSet.empty,
          1,
        )
      } yield succeed
    }

    "report timeouts" in {
      val sc = 0L
      val rc = 0L
      val ts = CantonTimestamp.Epoch
      val rt = mk(rc, sc, CantonTimestamp.MinValue)
      for {
        (conflictCheckFuture, timeoutFuture) <- enterCR(
          rt,
          rc,
          sc,
          ts,
          ts.plusMillis(1),
          ActivenessSet.empty,
        )
        _ <- checkConflictResult(rc, conflictCheckFuture, ActivenessResult.success)
        _ = enterTick(rt, sc + 1, ts.plusMillis(2))

        timeout <- timeoutFuture
        _ = assert(timeout == Timeout, "timeout signalled")

        resTR = rt.addResult(rc, sc + 2, ts.plusMillis(3), ts.plusMillis(3))
        _ = assert(resTR.isLeft, "submitting the result after the timeout fails")
      } yield succeed
    }

    "complain about too early decision time" in {
      val ts = ofEpochMilli(1)
      val rt = mk(0L, 0L, CantonTimestamp.Epoch)
      loggerFactory.assertInternalError[IllegalArgumentException](
        rt.addRequest(3L, 4L, ts, ts, CantonTimestamp.Epoch, ActivenessSet.empty),
        _.getMessage shouldBe "Request 3: Activeness check at 1970-01-01T00:00:00.001Z must be before the decision time at 1970-01-01T00:00:00Z.",
      ) // earlier decision time

      loggerFactory.assertInternalError[IllegalArgumentException](
        rt.addRequest(
          4L,
          5L,
          CantonTimestamp.MaxValue,
          CantonTimestamp.MaxValue,
          CantonTimestamp.MaxValue,
          ActivenessSet.empty,
        ),
        _.getMessage shouldBe "Request 4: Activeness check at 9999-12-31T23:59:59.999999Z must be before the decision time at 9999-12-31T23:59:59.999999Z.",
      ) // equal decision time
    }

    "complain about too early activeness check time" in {
      val ts = ofEpochMilli(1)
      val rt = mk(0L, 0L, CantonTimestamp.Epoch)
      loggerFactory.assertInternalError[IllegalArgumentException](
        rt.addRequest(3L, 4L, ts, ts.minusMillis(1), CantonTimestamp.MaxValue, ActivenessSet.empty),
        _.getMessage shouldBe "Request 3: Activeness time 1970-01-01T00:00:00Z must not be earlier than the request timestamp 1970-01-01T00:00:00.001Z.",
      )
    }

    "complain about too late activeness check time" in {
      val ts = ofEpochMilli(1)
      val rt = mk(0L, 0L, CantonTimestamp.Epoch)
      loggerFactory.assertInternalError[IllegalArgumentException](
        rt.addRequest(3L, 4L, ts, ts.minusMillis(10), ts.plusMillis(1), ActivenessSet.empty),
        _.getMessage shouldBe "Request 3: Activeness time 1969-12-31T23:59:59.991Z must not be earlier than the request timestamp 1970-01-01T00:00:00.001Z.",
      )
    }

    "complain about nonexistent requests for transaction results" in {
      val rt = mk(0L, 0L, CantonTimestamp.Epoch)
      val resTR = rt.addResult(0L, 5L, ofEpochMilli(1), ofEpochMilli(1))
      resTR shouldBe Left(RequestNotFound(0))
    }

    "complain if the transaction result is timestamped before the confirmation request" in {
      val rt = mk(0L, 0L, CantonTimestamp.Epoch)
      for {
        (cdF, toF) <- enterCR(rt, 1L, 1L, ofEpochMilli(2), ofEpochMilli(10), ActivenessSet.empty)
        resTR = rt.addResult(1L, 0L, ofEpochMilli(1), ofEpochMilli(3))
        _ = assert(
          resTR == Left(RequestNotFound(1L)),
          "complain that the request does not exist at the given time",
        )
        _ = enterTick(rt, 2L, ofEpochMilli(10)) // timeout everything
        _ <- checkConflictResult(1L, cdF, ActivenessResult.success)
        timeout <- toF
        _ = assert(timeout.timedOut, "timeout happened")
      } yield succeed
    }

    "complain about non-increasing timestamps" in {
      val rt1 = mk(0L, 0L, CantonTimestamp.Epoch)
      // initial timestamp must be smaller than ticks
      loggerFactory
        .assertInternalError[IllegalArgumentException](
          rt1.tick(0L, CantonTimestamp.Epoch),
          _.getMessage shouldBe "Timestamp 1970-01-01T00:00:00Z for sequence counter 0 is not after current time 1970-01-01T00:00:00Z.",
        )

      val rt2 = mk(0L, 0L, CantonTimestamp.Epoch)
      for {
        _ <- enterCR(rt2, 0L, 0L, ofEpochMilli(1), ofEpochMilli(3), ActivenessSet.empty)
        // timestamps must strictly increase with sequencer counters
        _ = loggerFactory
          .assertInternalError[IllegalArgumentException](
            rt2.tick(1L, ofEpochMilli(1)),
            _.getMessage shouldBe "Timestamp 1970-01-01T00:00:00.001Z for sequence counter 1 is not after current time 1970-01-01T00:00:00.001Z.",
          )
      } yield succeed
    }

    "process tasks in timestamp order" in {
      val rc = 0L
      val sc = 0L
      val ts = CantonTimestamp.assertFromInstant(Instant.parse("2000-01-01T00:00:00.00Z"))
      for {
        acs <- mkAcs()
        ckj <- mkCkj()
        rt = mk(rc, sc, ts.minusMillis(2), acs, ckj)

        rc0 = rc
        scCR0 = 1L
        scTR0 = 2L
        tsCR0 = ts
        tsCommit0 = ts.plusMillis(2)
        tsTimeout0 = ts.plusMillis(3)

        rc1 = rc + 1
        scCR1 = 4L
        scTR1 = 6L
        tsCR1 = ts.plusMillis(3)
        tsCommit1 = ts.plusMillis(5)
        tsTimeout1 = ts.plusMillis(6)
        actSet1 = mkActivenessSet(deact = Set(coid00), create = Set(coid10))

        (cdF1, toF1) <- enterCR(rt, rc1, scCR1, tsCR1, tsTimeout1, actSet1)
        (cdF0, toF0) <- enterCR(
          rt,
          rc0,
          scCR0,
          tsCR0,
          tsTimeout0,
          mkActivenessSet(create = Set(coid00, coid01)),
        )

        _ = assert(!toF0.isCompleted, "timeout for request 0 not completed")
        _ = enterTick(rt, 0L, ts.minusMillis(1))
        cd0 <- cdF0
        _ = assert(cd0 == ActivenessResult.success, "validation of request 0 succeeded")
        _ = assert(!cdF1.isCompleted, "request 1 is still pending")

        finalize0 <- enterTR(
          rt,
          rc0,
          scTR0,
          tsCommit0,
          mkCommitSet(create = Set(coid00, coid01)),
          0L,
          toF0,
        )
        _ <- checkFinalize(rc0, finalize0)

        _ <- checkContractState(acs, coid00, (Active, rc, tsCR0))("contract 00 created")
        _ <- checkContractState(acs, coid01, (Active, rc, tsCR0))("contract 01 created")

        _ = enterTick(rt, 3L, tsCommit0.addMicros(1))

        _ <- checkConflictResult(rc1, cdF1, ActivenessResult.success)

        finalize1 <- enterTR(
          rt,
          rc1,
          scTR1,
          tsCommit1,
          mkCommitSet(arch = Set(coid00), create = Set(coid10)),
          1L,
          toF1,
        )
        _ = enterTick(rt, 5L, tsCR1.plusMillis(1))
        _ = enterTick(rt, 7L, tsCommit1.plusMillis(5))
        _ <- checkFinalize(rc1, finalize1)

        _ <- checkSnapshot(acs, tsCR0.addMicros(1), Map(coid00 -> tsCR0, coid01 -> tsCR0))
        _ <- checkSnapshot(acs, tsCR1.addMicros(1), Map(coid01 -> tsCR0, coid10 -> tsCR1))
      } yield succeed
    }

    "transactions may have identical timestamps" in {
      val rc = 10L
      val sc = 10L
      val ts = ofEpochMilli(100)
      val timeout = ts.plusMillis(100)
      val actSet1 = mkActivenessSet(deact = Set(coid01, coid10))
      val toc0 = TimeOfChange(0L, CantonTimestamp.Epoch)
      for {
        acs <- mkAcs(
          (coid00, toc0, Active),
          (coid01, toc0, Active),
          (coid10, toc0, Active),
        )
        ckj <- mkCkj()
        rt = mk(rc, sc, CantonTimestamp.Epoch, acs, ckj)

        (cdF1, toF1) <- enterCR(rt, rc + 1, sc + 1, ts.plusMillis(1), timeout, actSet1)
        (cdF0, toF0) <- enterCR(
          rt,
          rc,
          sc,
          ts,
          ts.plusMillis(1),
          timeout,
          mkActivenessSet(deact = Set(coid00, coid10)),
        )
        _ <- checkConflictResult(rc, cdF0, ActivenessResult.success)
        _ <- checkConflictResult(rc, cdF1, mkActivenessResult(locked = Set(coid10)))
        finalize0 <- enterTR(
          rt,
          rc,
          sc + 3,
          ts.plusMillis(11),
          mkCommitSet(arch = Set(coid00)),
          0,
          toF0,
        )
        finalize1 <- enterTR(
          rt,
          rc + 1,
          sc + 2,
          ts.plusMillis(10),
          mkCommitSet(arch = Set(coid01)),
          1,
          toF1,
        )
        _ <- checkFinalize(rc + 1, finalize1)
        _ <- checkContractState(acs, coid01, (Archived, rc + 1, ts.plusMillis(1)))(
          s"contract $coid01 is archived"
        )
        _ <- checkFinalize(rc, finalize0)
        _ <- checkContractState(acs, coid00, (Archived, rc, ts))(s"contract $coid00 is archived")
      } yield succeed
    }

    "detect conflicts" in {
      val rc = 10L
      val sc = 10L
      val ts = CantonTimestamp.assertFromInstant(Instant.parse("2050-10-11T00:00:10.00Z"))
      val toc0 = TimeOfChange(0L, ts.minusMillis(10))
      val toc1 = TimeOfChange(1L, ts.minusMillis(5))
      val toc2 = TimeOfChange(2L, ts.minusMillis(1))
      for {
        acs <- mkAcs(
          (coid00, toc0, Active),
          (coid01, toc0, Active),
          (coid01, toc1, Archived),
          (coid10, toc2, Active),
          (coid11, toc2, Active),
        )
        ckj <- mkCkj()
        rt = mk(rc, sc, ts.addMicros(-1), acs, ckj)
        activenessSet0 = mkActivenessSet(deact = Set(coid00, coid11), useOnly = Set(coid10))
        (cdF0, toF0) <- enterCR(rt, rc, sc, ts, ts.plusMillis(100), activenessSet0)
        _ <- checkConflictResult(rc, cdF0, ActivenessResult.success)
        activenessSet1 = mkActivenessSet(deact = Set(coid00, coid10), useOnly = Set(coid11, coid01))
        (cdF1, toF1) <- enterCR(
          rt,
          rc + 1,
          sc + 1,
          ts.plusMillis(1),
          ts.plusMillis(100),
          activenessSet1,
        )
        activenessResult1 = mkActivenessResult(
          locked = Set(coid00, coid11),
          notActive = Map(coid01 -> Archived),
        )
        _ <- checkConflictResult(rc + 1, cdF1, activenessResult1, "00 and 11 are locked")
        finalize0 <- enterTR(
          rt,
          rc,
          sc + 2,
          ts.plusMillis(2),
          mkCommitSet(arch = Set(coid11)),
          0L,
          toF0,
        )
        _ <- checkFinalize(rc, finalize0)
        activenessSet2 = mkActivenessSet(deact = Set(coid00, coid11), useOnly = Set(coid01, coid10))
        (cdF2, toF2) <- enterCR(
          rt,
          rc + 2,
          sc + 3,
          ts.plusMillis(3),
          ts.plusMillis(100),
          activenessSet2,
        )
        activenessResult2 = mkActivenessResult(
          locked = Set(coid00, coid10),
          notActive = Map(coid01 -> Archived, coid11 -> Archived),
        )
        _ <- checkConflictResult(rc + 2, cdF2, activenessResult2, "contracts remain locked")

        _ = enterTick(rt, sc + 4, ts.plusMillis(100))
        timeout1 <- toF1
        _ = assert(timeout1.timedOut)
        timeout2 <- toF2
        _ = assert(timeout2.timedOut)
      } yield succeed
    }

    "complain about invalid commit sets due to archivals" in {
      val ts = ofEpochMilli(1)
      val toc0 = TimeOfChange(0L, CantonTimestamp.Epoch)
      for {
        acs <- mkAcs((coid00, toc0, Active), (coid01, toc0, Active))
        ckj <- mkCkj()
        rt = mk(1L, 1L, CantonTimestamp.Epoch, acs, ckj)
        activenessSet = mkActivenessSet(deact = Set(coid00, coid10), useOnly = Set(coid01))
        (cdF, toF) <- enterCR(rt, 1L, 1L, ts, ts.plusMillis(1), activenessSet)
        _ <- checkConflictResult(1L, cdF, mkActivenessResult(unknown = Set(coid10)))
        commitSet = mkCommitSet(arch = Set(coid00, coid11))
        resTR = rt.addResult(1L, 2L, ts.plusMillis(1), ts.plusMillis(1))
        _ = assert(
          resTR == Right(()),
          s"adding the transaction result's timestamp succeeds for request 1",
        )
        timeout <- toF
        _ = assert(!timeout.timedOut, s"timeout promise for request 1 is kept with NoTimeout")
        _ <- loggerFactory.suppressWarningsAndErrors {
          for {
            finalizationResult <- rt.addCommitSet(1L, Success(commitSet)).value.value.failed
          } yield {
            assert(
              finalizationResult == InvalidCommitSet(
                1L,
                commitSet,
                LockedStates(Set.empty, Seq(coid00, coid10), Seq.empty),
              ),
              "commit set archives non-locked contracts",
            )
          }
        }
      } yield succeed
    }

    "complain about invalid commit sets due to creates" in {
      val ts = ofEpochMilli(1)
      val rt = mk(1L, 1L, CantonTimestamp.Epoch)
      for {
        (cdF, toF) <- enterCR(
          rt,
          1L,
          1L,
          ts,
          ts.plusMillis(1),
          mkActivenessSet(create = Set(coid00, coid01)),
        )
        _ <- checkConflictResult(1L, cdF, ActivenessResult.success)
        commitSet = mkCommitSet(create = Set(coid00, coid11))
        resTR = rt.addResult(1L, 2L, ts.plusMillis(1), ts.plusMillis(1))
        _ = assert(
          resTR == Right(()),
          s"adding the transaction result's timestamp succeeds for request 1",
        )
        timeout <- toF
        _ = assert(!timeout.timedOut, s"timeout promise for request 1 is kept with NoTimeout")
        _ <- loggerFactory.suppressWarningsAndErrors {
          for {
            finalize <- rt.addCommitSet(1L, Success(commitSet)).value.value.failed
          } yield {
            assert(
              finalize ==
                InvalidCommitSet(
                  1L,
                  commitSet,
                  LockedStates(Set.empty, Seq(coid00, coid01), Seq.empty),
                ),
              "commit set creates non-locked contracts",
            )
          }
        }
      } yield succeed
    }

    "complain about too early commit time" in {
      val ts = ofEpochMilli(10)
      val rt = mk(0L, 0L, CantonTimestamp.Epoch)
      for {
        (cdF, toF) <- enterCR(rt, 0L, 0L, ts, ts.plusMillis(100), ActivenessSet.empty)
        _ <- checkConflictResult(0L, cdF, ActivenessResult.success)
        _ = loggerFactory.assertInternalError[IllegalArgumentException](
          rt.addResult(0L, 1L, ts.plusMillis(1), ts.addMicros(1)),
          _.getMessage shouldBe "Request 0: Commit time 1970-01-01T00:00:00.010001Z before result timestamp 1970-01-01T00:00:00.011Z",
        )
      } yield succeed
    }

    "complain about elapsed decision time" in {
      val rt = mk(0L, 0L, CantonTimestamp.MinValue)
      for {
        (cdF, toF) <- enterCR(
          rt,
          0L,
          0L,
          CantonTimestamp.Epoch,
          ofEpochMilli(10),
          ActivenessSet.empty,
        )
        _ <- checkConflictResult(0L, cdF, ActivenessResult.success)
        _ = loggerFactory.assertInternalError[IllegalArgumentException](
          rt.addResult(0L, 1L, ofEpochMilli(11), ofEpochMilli(11)),
          _.getMessage shouldBe "Request 0: Result timestamp 1970-01-01T00:00:00.011Z after the decision time 1970-01-01T00:00:00.010Z.",
        )
      } yield succeed
    }

    "a timeout unlocks the contract immediately before the next confirmation request" in {
      val rc = 10L
      val sc = 10L
      val ts = CantonTimestamp.assertFromInstant(Instant.parse("2010-10-10T12:00:00.00Z"))
      val tocN1 = TimeOfChange(rc - 1, ts.minusMillis(1))
      for {
        acs <- mkAcs((coid00, tocN1, Active), (coid01, tocN1, Active))
        ckj <- mkCkj()
        rt = mk(rc, sc, ts.minusMillis(1), acs, ckj)

        to1 = ts.plusMillis(2)
        (cdF0, toF0) <- enterCR(
          rt,
          rc + 1,
          sc,
          ts,
          to1,
          mkActivenessSet(deact = Set(coid00, coid01)),
        )
        _ <- checkConflictResult(rc + 1, cdF0, ActivenessResult.success)
        activenessSet1 = mkActivenessSet(deact = Set(coid00), useOnly = Set(coid01))
        (cdF1, toF1) <- enterCR(rt, rc + 2, sc + 1, to1, to1.plusMillis(1), activenessSet1)
        _ <- checkConflictResult(
          rc + 1,
          cdF1,
          ActivenessResult.success,
          "locks on 00 and 01 released before next confirmation request is processed",
        )
        timeout0 <- toF0
        _ = assert(timeout0 == Timeout, "original transaction has timed out")
        _ = enterTick(rt, sc + 2, ts.plusMillis(1000)) // time out everything
      } yield succeed
    }

    "contracts become active immediately before the commit time" in {
      val rc = 0L
      val sc = 0L
      val ts = CantonTimestamp.Epoch
      val rt = mk(rc, sc, CantonTimestamp.MinValue)
      for {
        (cdF0, toF0) <- enterCR(
          rt,
          rc,
          sc,
          ts,
          ts.plusMillis(3),
          mkActivenessSet(create = Set(coid00, coid01)),
        )
        _ <- checkConflictResult(rc, cdF0, ActivenessResult.success)
        finalize0 <- enterTR(
          rt,
          rc,
          sc + 1,
          ts.plusMillis(1),
          mkCommitSet(create = Set(coid00, coid01)),
          1L,
          toF0,
        )
        act1 = mkActivenessSet(deact = Set(coid00), useOnly = Set(coid01))
        (cdF1, _) <- enterCR(rt, rc + 1, sc + 2, ts.plusMillis(2), ts.plusMillis(4), act1)
        _ <- checkConflictResult(rc + 1, cdF1, ActivenessResult.success)
        finalizeResult0 <- finalize0
        _ = assert(finalizeResult0 == Right(()))
        _ = enterTick(rt, sc + 3, ts.plusMillis(500)) // time out everything
      } yield succeed
    }

    "addConfirmationRequest is idempotent" in {
      val rt = mk(0L, 0L, CantonTimestamp.MinValue)
      for {
        (cdF, toF) <- enterCR(rt, 0L, 1L, ofEpochMilli(1), ofEpochMilli(10), ActivenessSet.empty)
        (cdF2, toF2) <- enterCR(rt, 0L, 1L, ofEpochMilli(1), ofEpochMilli(10), ActivenessSet.empty)
        _ = assert(
          (cdF == cdF2) && (toF == toF2),
          "adding the same confirmation request twice yields the same futures",
        )
        ts5 = ofEpochMilli(100)
        resCR = rt.addRequest(0L, 5L, ts5, ts5, ofEpochMilli(1000), ActivenessSet.empty)
        _ = assert(resCR == Left(RequestAlreadyExists(0L, 1L, ofEpochMilli(1))))
        _ = enterTick(rt, 0, CantonTimestamp.Epoch)
        _ = enterTick(rt, 2, ofEpochMilli(10))
        timeout <- toF
        _ = assert(timeout.timedOut)
      } yield succeed
    }

    "addTransactionResult is idempotent" in {
      val rt = mk(0L, 0L, CantonTimestamp.MinValue)
      for {
        (cdF, toF) <- enterCR(
          rt,
          0L,
          0L,
          CantonTimestamp.Epoch,
          ofEpochMilli(10),
          ActivenessSet.empty,
        )
        _ <- checkConflictResult(0, cdF, ActivenessResult.success)
        resTR1 = rt.addResult(0L, 1L, ofEpochMilli(1), ofEpochMilli(3))
        resTR2 = rt.addResult(0L, 1L, ofEpochMilli(1), ofEpochMilli(3))
        _ = assert(resTR1 == Right(()), "first transaction result call succeeds")
        _ = assert(resTR2 == Right(()), "second transaction result call is swallowed")
        resTR3 = loggerFactory.suppressWarningsAndErrors(
          rt.addResult(0L, 2L, ofEpochMilli(2), ofEpochMilli(4))
        )
        _ = assert(
          resTR3 == Left(ResultAlreadyExists(0L)),
          "transaction result with different parameters fails",
        )
      } yield succeed
    }

    "addCommitSet is idempotent" in {
      val rt = mk(0L, 0L, CantonTimestamp.MinValue)
      for {
        (cdF, toF) <- enterCR(
          rt,
          0L,
          0L,
          CantonTimestamp.Epoch,
          ofEpochMilli(10),
          ActivenessSet.empty,
        )
        _ <- checkConflictResult(0, cdF, ActivenessResult.success)
        resTR1 = rt.addResult(0L, 1L, ofEpochMilli(1), ofEpochMilli(2))
        _ = assert(resTR1 == Right(()), "transaction result call succeeds")
        finalize1 = rt.addCommitSet(0L, Success(CommitSet.empty))
        _ = assert(finalize1.isRight, "first call to commit set succeeds")
        finalize2 = rt.addCommitSet(0L, Success(CommitSet.empty))
        finalize3 = loggerFactory.suppressWarningsAndErrors(
          rt.addCommitSet(0L, Success(mkCommitSet(arch = Set(coid00))))
        )
        _ = assert(
          finalize3 == Left(CommitSetAlreadyExists(0L)),
          "setting a different commit set fails",
        )
        _ = enterTick(rt, 2, ofEpochMilli(100))
        finalize1Result <- finalize1.value.value
        _ = assert(finalize1Result == Right(()), "request finalized")
        _ = assert(finalize2.value == finalize1.value, "same result returned")
      } yield succeed
    }

    "complain if the same request counter is used for different requests" in {
      val rt = mk(0L, 0L, CantonTimestamp.Epoch)
      for {
        _ <- enterCR(rt, 0L, 1L, ofEpochMilli(1), ofEpochMilli(10), ActivenessSet.empty)
      } yield assert(
        rt.addRequest(
          0L,
          1L,
          ofEpochMilli(1),
          ofEpochMilli(1),
          ofEpochMilli(11),
          ActivenessSet.empty,
        ) == Left(RequestAlreadyExists(0L, 1L, ofEpochMilli(1))),
        "request counter used twice",
      )
    }

    "complain about adding commit set before transaction result" in {
      val rt = mk(0L, 0L, CantonTimestamp.MinValue)
      for {
        (cdF, toF) <- enterCR(
          rt,
          0L,
          0L,
          CantonTimestamp.Epoch,
          CantonTimestamp.MaxValue,
          ActivenessSet.empty,
        )
        _ <- checkConflictResult(0L, cdF, ActivenessResult.success)
        resCS = rt.addCommitSet(0L, Success(CommitSet.empty))
        _ = assert(resCS == Left(ResultNotFound(0L)), "transaction result is missing")
      } yield succeed
    }

    "transaction results may arrive out of order" in {
      val rc = 0L
      val sc = 0L
      val ts = ofEpochMilli(1000)
      val tocN2 = TimeOfChange(rc - 2, ts.minusMillis(20))
      val tocN1 = TimeOfChange(rc - 1, ts.minusMillis(10))
      for {
        acs <- mkAcs((coid10, tocN2, Active), (coid00, tocN1, Active), (coid01, tocN1, Active))
        ckj <- mkCkj()
        rt = mk(rc, sc, CantonTimestamp.Epoch, acs, ckj)
        activenessSet0 = mkActivenessSet(deact = Set(coid00), useOnly = Set(coid01))
        (cdF0, toF0) <- enterCR(rt, rc, sc, ts, ts.plusMillis(100), activenessSet0)
        _ <- checkConflictResult(rc, cdF0, ActivenessResult.success)
        activenessSet1 = mkActivenessSet(
          deact = Set(coid01),
          useOnly = Set(coid10),
          create = Set(coid11),
        )
        (cdF1, toF1) <- enterCR(
          rt,
          rc + 1,
          sc + 1,
          ts.plusMillis(1),
          ts.plusMillis(100),
          activenessSet1,
        )
        _ <- checkConflictResult(rc + 1, cdF1, ActivenessResult.success)
        commitSet1 = mkCommitSet(arch = Set(coid01), create = Set(coid11))
        finalize1 <- enterTR(rt, rc + 1, sc + 2, ts.plusMillis(2), commitSet1, 0L, toF1)
        _ <- checkFinalize(rc + 1, finalize1)
        _ <- checkContractState(acs, coid01, (Archived, rc + 1, ts.plusMillis(1)))(
          s"contract $coid01 archived by second confirmation request"
        )
        _ <- checkContractState(acs, coid00, (Active, rc - 1, ts.minusMillis(10)))(
          s"contract $coid00 still active"
        )
        _ <- checkContractState(acs, coid11, (Active, rc + 1, ts.plusMillis(1)))(
          s"contract $coid11 created by second transaction"
        )
        finalize0 <- enterTR(
          rt,
          rc,
          sc + 3,
          ts.plusMillis(3),
          mkCommitSet(arch = Set(coid00)),
          2L,
          toF0,
        )
        _ = enterTick(rt, sc + 4, ts.plusMillis(5))
        _ <- checkFinalize(rc, finalize0)
        _ <- checkContractState(acs, coid00, (Archived, rc, ts))(
          s"contract $coid00 archived by first transaction"
        )
      } yield succeed
    }

    "conflict detection progresses even if a commit set is missing" in {
      val rc = 0L
      val sc = 0L
      val ts = ofEpochMilli(1000)
      val rt = mk(rc, sc, CantonTimestamp.Epoch)
      for {
        (cdF0, toF0) <- enterCR(rt, rc, sc, ts, ts.plusMillis(100), ActivenessSet.empty)
        _ <- checkConflictResult(rc, cdF0, ActivenessResult.success)
        (cdF1, toF1) <- enterCR(
          rt,
          rc + 1,
          sc + 1,
          ts.plusMillis(1),
          ts.plusMillis(100),
          ActivenessSet.empty,
        )
        _ <- checkConflictResult(rc + 1, cdF1, ActivenessResult.success)
        (cdF2, toF2) <- enterCR(
          rt,
          rc + 2,
          sc + 2,
          ts.plusMillis(2),
          ts.plusMillis(4),
          ActivenessSet.empty,
        )
        _ <- checkConflictResult(rc + 2, cdF2, ActivenessResult.success)
        resTR1 = rt.addResult(rc + 1, sc + 3, ts.plusMillis(4), ts.plusMillis(20))
        _ = assert(resTR1 == Right(()))
        timeout2 <- toF2
        _ = assert(timeout2.timedOut, "third request timed out")
        timeout1 <- toF1
        _ = assert(!timeout1.timedOut, "second request does not time out")
        finalize0 <- enterTR(rt, rc, sc + 4, ts.plusMillis(15), CommitSet.empty, 10L, toF0)
        _ = enterTick(
          rt,
          sc + 5,
          ts.plusMillis(22),
        ) // check that a missing commit set does not cause deadlock
        resCS = rt.addCommitSet(rc + 1, Success(CommitSet.empty))
        _ = assert(resCS.isRight, "adding the missing commit set succeeded")
        res2 <- resCS.value.value
        _ = assert(res2 == Right(()), "finalizing the second request succeeded")
        _ = enterTick(rt, sc + 6, ts.plusMillis(25))
        _ <- checkFinalize(rc, finalize0)
      } yield succeed
    }

    "detect duplicate concurrent creates" in {
      for {
        acs <- mkAcs()
        ckj <- mkCkj()
        rt = mk(1L, 1L, CantonTimestamp.Epoch, acs, ckj)

        activenessSet0 = mkActivenessSet(create = Set(coid00, coid01, coid11))
        (cdF0, toF0) <- enterCR(rt, 1L, 1L, ofEpochMilli(1), ofEpochMilli(100), activenessSet0)
        _ <- checkConflictResult(1L, cdF0, ActivenessResult.success)

        activenessSet1 = mkActivenessSet(create = Set(coid00, coid01, coid10))
        (cdF1, toF1) <- enterCR(rt, 2L, 2L, ofEpochMilli(2), ofEpochMilli(100), activenessSet1)
        _ <- checkConflictResult(2L, cdF1, mkActivenessResult(locked = Set(coid00, coid01)))

        finalize0 <- enterTR(
          rt,
          1L,
          3L,
          ofEpochMilli(3),
          mkCommitSet(create = Set(coid01, coid11)),
          0L,
          toF0,
        )
        _ <- checkFinalize(1L, finalize0)

        finalize1 <- enterTR(
          rt,
          2L,
          4L,
          ofEpochMilli(4),
          mkCommitSet(create = Set(coid10)),
          0L,
          toF1,
        )
        _ <- checkFinalize(2L, finalize1)

        _ <- checkContractState(acs, coid00, None)(s"contract $coid00's creation is rolled back")
        _ <- checkContractState(acs, coid01, (Active, 1L, ofEpochMilli(1)))(
          s"contract $coid01 is active"
        )
      } yield succeed
    }

    "rollback archival while contract is being created" in {
      val rc = 10L
      val sc = 10L
      val ts = ofEpochMilli(10)
      val timeout = ts.plusMillis(100)
      for {
        acs <- mkAcs()
        rt = mk(rc, sc, CantonTimestamp.Epoch, acs = acs)

        (cdF0, toF0) <- enterCR(
          rt,
          rc,
          sc,
          ts,
          timeout,
          mkActivenessSet(create = Set(coid00, coid01)),
        )
        _ <- checkConflictResult(rc, cdF0, ActivenessResult.success, "first request succeeds")
        (cdF1, toF1) <- enterCR(
          rt,
          rc + 1,
          sc + 1,
          ts.plusMillis(1),
          timeout,
          mkActivenessSet(deact = Set(coid00)),
        )
        _ <- checkConflictResult(rc + 1, cdF1, mkActivenessResult(locked = Set(coid00)))
        (cdF2, toF2) <- enterCR(
          rt,
          rc + 2,
          sc + 2,
          ts.plusMillis(2),
          timeout,
          mkActivenessSet(deact = Set(coid01)),
        )
        _ <- checkConflictResult(rc = 2, cdF2, mkActivenessResult(locked = Set(coid01)))
        finalize1 <- enterTR(rt, rc + 1, sc + 3, ts.plusMillis(5), CommitSet.empty, 0L, toF1)
        _ <- checkFinalize(rc + 1, finalize1)
        finalize0 <- enterTR(
          rt,
          rc,
          sc + 4,
          ts.plusMillis(6),
          mkCommitSet(create = Set(coid00, coid01)),
          0L,
          toF0,
        )
        _ <- checkFinalize(rc, finalize0)
        finalize2 <- enterTR(rt, rc + 2, sc + 5, ts.plusMillis(7), CommitSet.empty, 0L, toF2)
        _ <- checkFinalize(rc + 2, finalize2)
        _ <- List(coid00, coid01).traverse_ { coid =>
          checkContractState(acs, coid, (Active, rc, ts))(s"contract $coid was created")
        }
        activenessSet3 = mkActivenessSet(useOnly = Set(coid00, coid01))
        (cdF3, toF3) <- enterCR(rt, rc + 3, sc + 6, ts.plusMillis(10), timeout, activenessSet3)
        _ <- checkConflictResult(
          rc + 3,
          cdF3,
          ActivenessResult.success,
          "created contracts are active",
        )
        _ = enterTick(rt, sc + 7, timeout)
      } yield succeed
    }

    "halt upon a commit set failure" in {
      val rc = 100L
      val sc = 100L
      val ts = ofEpochMilli(10)
      val timeout = ts.plusMillis(100)
      val failure = new RuntimeException("Failing commit set")
      for {
        acs <- mkAcs()
        rt = mk(rc, sc, CantonTimestamp.Epoch, acs = acs)
        (cdF0, toF0) <- enterCR(
          rt,
          rc,
          sc,
          ts,
          timeout,
          mkActivenessSet(create = Set(coid00, coid01)),
        )
        (cdF1, toF1) <- enterCR(
          rt,
          rc + 1,
          sc + 1,
          ts.plusMillis(1),
          timeout,
          mkActivenessSet(create = Set(coid10)),
        )
        _ <- checkConflictResult(rc, cdF0, ActivenessResult.success, "first request succeeds")
        _ <- checkConflictResult(rc, cdF1, ActivenessResult.success, "second request succeeds")
        finalize0 = rt.addResult(rc, sc + 2, ts.plusMillis(2), ts.plusMillis(3))
        _ = finalize0 shouldBe Right(())
        to0 <- toF0
        _ = to0.timedOut shouldBe false
        (commit0, commitF1) <- loggerFactory.assertLogs(
          {
            val commitF0 =
              rt.addCommitSet(rc, Failure(failure))
                .valueOrFail("no commit set error expected for first request")
            rt.tick(sc + 3, ts.plusMillis(5)) // Trigger finalization of first request
            for {
              commit0 <- commitF0.value.failed
              finalize1 = rt.addResult(rc + 1, sc + 4, ts.plusMillis(6), ts.plusMillis(6))
              _ = finalize1 shouldBe Right(())
              to1 <- toF1
              _ = to1.timedOut shouldBe false
              commitF1 = valueOrFail(
                rt.addCommitSet(rc + 1, Success(mkCommitSet(create = Set(coid10))))
              )("no commit set error expected for second request")
              _ <- rt.taskScheduler.flush()
            } yield (commit0, commitF1)
          },
          _.errorMessage should include("A task failed with an exception."),
          _.errorMessage should include("A task failed with an exception."),
        )
        contracts <- acs.fetchStates(Seq(coid00, coid10))
      } yield {
        commit0 shouldBe failure
        commitF1.value.isCompleted shouldBe false
        contracts shouldBe Map.empty // No contracts are created
      }
    }
  }

  def enterTick(rt: RequestTracker, sc: SequencerCounter, ts: CantonTimestamp): Unit = {
    rt.tick(sc, ts)
  }

  def enterCR(
      rt: RequestTracker,
      rc: RequestCounter,
      sc: SequencerCounter,
      confirmationRequestTimestamp: CantonTimestamp,
      decisionTime: CantonTimestamp,
      activenessSet: ActivenessSet,
  ): Future[(Future[ActivenessResult], Future[TimeoutResult])] =
    enterCR(
      rt,
      rc,
      sc,
      confirmationRequestTimestamp,
      confirmationRequestTimestamp,
      decisionTime,
      activenessSet,
    )

  def enterCR(
      rt: RequestTracker,
      rc: RequestCounter,
      sc: SequencerCounter,
      confirmationRequestTimestamp: CantonTimestamp,
      activenessTimestamp: CantonTimestamp,
      decisionTime: CantonTimestamp,
      activenessSet: ActivenessSet,
  ): Future[(Future[ActivenessResult], Future[TimeoutResult])] = {
    val resCR = rt.addRequest(
      rc,
      sc,
      confirmationRequestTimestamp,
      activenessTimestamp,
      decisionTime,
      activenessSet,
    )
    assert(resCR.isRight, s"adding the confirmation request $rc succeeds")
    resCR.value.map { case RequestFutures(activenessResult, timeoutResult) =>
      (activenessResult, timeoutResult)
    }
  }

  def checkConflictResult(
      rc: RequestCounter,
      conflictResultFuture: Future[ActivenessResult],
      activenessResult: ActivenessResult,
      msg: String = "",
  ): Future[Assertion] =
    conflictResultFuture.map(conflictResult =>
      assert(
        conflictResult == activenessResult,
        s"validation result for request $rc is correct: $msg",
      )
    )

  def enterTR(
      rt: RequestTracker,
      rc: RequestCounter,
      sc: SequencerCounter,
      trTimestamp: CantonTimestamp,
      commitSet: CommitSet,
      commitDelay: Long,
      timeoutFuture: Future[TimeoutResult],
  ): Future[Future[Either[NonEmptyChain[RequestTrackerStoreError], Unit]]] = {
    val resTR = rt.addResult(rc, sc, trTimestamp, trTimestamp.plusMillis(commitDelay))
    assert(
      resTR == Right(()),
      s"adding the transaction result's timestamp succeeds for request $rc",
    )
    for {
      timeout <- timeoutFuture
      _ = assert(!timeout.timedOut, s"timeout promise for request $rc is kept with NoTimeout")
      resCS = rt.addCommitSet(rc, Success(commitSet))
    } yield resCS.value.value
  }

  def checkFinalize(
      rc: RequestCounter,
      finalizeFuture: Future[Either[NonEmptyChain[RequestTrackerStoreError], Unit]],
  ): Future[Assertion] =
    finalizeFuture.map(result => assert(result == Right(()), s"request $rc finalized"))

  def checkContractState(
      acs: ActiveContractStore,
      coid: LfContractId,
      cs: (Status, RequestCounter, CantonTimestamp),
  )(clue: String): Future[Assertion] =
    checkContractState(acs, coid, Some(ContractState(cs._1, cs._2, cs._3)))(clue)

  def checkContractState(
      acs: ActiveContractStore,
      coid: LfContractId,
      state: Option[ContractState],
  )(clue: String): Future[Assertion] =
    acs.fetchState(coid).map(result => assert(result == state, clue))

  def checkSnapshot(
      acs: ActiveContractStore,
      ts: CantonTimestamp,
      expected: Map[LfContractId, CantonTimestamp],
  ): Future[Assertion] =
    acs
      .snapshot(ts)
      .map(snapshot => assert(snapshot == Right(expected), s"ACS snapshot at time $ts correct"))

  def singleCRwithTR(
      rt: RequestTracker,
      rc: RequestCounter,
      sc: SequencerCounter,
      crTimestamp: CantonTimestamp,
      activenessSet: ActivenessSet,
      activenessResult: ActivenessResult,
      commitSet: CommitSet,
      commitDelay: Long,
  ): Future[Assertion] =
    singleCRwithTR(
      rt,
      rc,
      sc,
      crTimestamp,
      crTimestamp.plusMillis(2),
      activenessSet,
      activenessResult,
      commitSet,
      commitDelay,
    )

  def singleCRwithTR(
      rt: RequestTracker,
      rc: RequestCounter,
      sc: SequencerCounter,
      crTimestamp: CantonTimestamp,
      decisionTime: CantonTimestamp,
      activenessSet: ActivenessSet,
      activenessResult: ActivenessResult,
      commitSet: CommitSet,
      commitDelay: Long,
  ): Future[Assertion] = {
    for {
      (cdFuture, timeoutFuture) <- enterCR(rt, rc, sc, crTimestamp, decisionTime, activenessSet)
      _ <- checkConflictResult(rc, cdFuture, activenessResult)
      finalizeFuture <- enterTR(
        rt,
        rc,
        sc + 1,
        crTimestamp.plusMillis(1),
        commitSet,
        commitDelay,
        timeoutFuture,
      )
      _ = if (commitDelay > 0)
        enterTick(rt, sc + 2, crTimestamp.plusMillis(1 + commitDelay))
      _ <- checkFinalize(rc, finalizeFuture)
    } yield succeed
  }
}
