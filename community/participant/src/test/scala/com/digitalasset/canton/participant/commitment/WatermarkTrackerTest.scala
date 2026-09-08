// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.data.CantonTimestamp
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.Future

class WatermarkTrackerTest extends AsyncWordSpec with BaseTest {

  def mk(): WatermarkTracker[CantonTimestamp] =
    new WatermarkTracker[CantonTimestamp](
      CantonTimestamp.MinValue,
      loggerFactory,
      FutureSupervisor.Noop,
    )

  "highWatermark" should {
    "return the initial watermark on an empty tracker" in {
      val tracker = mk()
      tracker.highWatermark shouldBe CantonTimestamp.MinValue
    }
  }

  "runWithMark" should {
    "succeed on an empty tracker" in {
      val tracker = mk()
      val runMark = new AtomicReference[Option[CantonTimestamp]](None)
      for {
        _ <- tracker.runWithMark(
          CantonTimestamp.Epoch,
          mark => Future.successful(runMark.set(Some(mark))),
        )
      } yield runMark.get shouldBe Some(CantonTimestamp.Epoch)
    }

    "set the mark to the watermark if below" in {
      val tracker = mk()
      val runMark = new AtomicReference[Option[CantonTimestamp]](None)
      for {
        _ <- tracker.increaseWatermark(CantonTimestamp.Epoch)
        _ <- tracker.runWithMark(
          CantonTimestamp.MinValue,
          mark => Future.successful(runMark.set(Some(mark))),
        )
      } yield runMark.get shouldBe Some(CantonTimestamp.Epoch)
    }

    "support concurrent running for the same mark" in {
      val tracker = mk()
      val counter = new AtomicInteger()
      for {
        _ <- tracker.runWithMark(
          CantonTimestamp.Epoch,
          _ => {
            counter.incrementAndGet()
            tracker.runWithMark(
              CantonTimestamp.Epoch,
              _ => {
                counter.incrementAndGet()
                Future.unit
              },
            )
          },
        )
      } yield counter.get shouldBe 2
    }

    "support concurrent running for different marks" in {
      val tracker = mk()
      val counter = new AtomicInteger()
      for {
        _ <- tracker.runWithMark(
          CantonTimestamp.Epoch,
          _ => {
            counter.incrementAndGet()
            tracker.runWithMark(
              CantonTimestamp.Epoch.plusSeconds(1),
              _ => {
                counter.incrementAndGet()

                Future.unit
              },
            )
          },
        )
      } yield counter.get shouldBe 2
    }

    "support concurrent running for reversed marks" in {
      val tracker = mk()
      val counter = new AtomicInteger()
      for {
        _ <- tracker.runWithMark(
          CantonTimestamp.Epoch,
          _ => {
            counter.incrementAndGet()
            tracker.runWithMark(
              CantonTimestamp.Epoch.minusSeconds(1),
              _ => {
                counter.incrementAndGet()
                Future.unit
              },
            )
          },
        )
      } yield counter.get shouldBe 2
    }

    "record finishing if the task fails" in {
      val tracker = mk()
      val ex = new RuntimeException("RUN FAILURE")
      for {
        error <- tracker
          .runWithMark(CantonTimestamp.Epoch, _ => Future.failed(ex))
          .failed
        obs = tracker.increaseWatermark(CantonTimestamp.Epoch)
        _ <- obs // should have been completed immediately
      } yield {
        error shouldBe ex
      }
    }

    "interleave running and raising" in {
      val tracker = mk()
      for {
        _ <- tracker.runWithMark(
          CantonTimestamp.ofEpochSecond(2),
          _ => tracker.increaseWatermark(CantonTimestamp.Epoch),
        )
      } yield {
        tracker.highWatermark shouldBe CantonTimestamp.Epoch
      }
    }
  }

  "increaseWatermark" should {

    "block until running tasks are done" in {
      val tracker = mk()
      val ts = CantonTimestamp.Epoch
      tracker.registerBegin(ts)
      val obs1 = tracker.increaseWatermark(ts.plusSeconds(1))
      tracker.highWatermark shouldBe ts.plusSeconds(1)
      for {
        _ <- tracker.increaseWatermark(ts)
        _ = obs1.isCompleted shouldBe false
        _ = tracker.highWatermark shouldBe ts.plusSeconds(1)
        _ = tracker.registerEnd(ts)
        _ <- obs1
      } yield succeed
    }

    "unblock incrementally" in {
      val tracker = mk()
      val ts = CantonTimestamp.ofEpochSecond(10)
      tracker.registerBegin(ts) shouldBe ts
      tracker.registerBegin(ts) shouldBe ts
      tracker.registerBegin(ts.plusSeconds(2)) shouldBe ts.plusSeconds(2)
      val obs0 = tracker.increaseWatermark(ts.immediateSuccessor)
      tracker.highWatermark shouldBe ts.immediateSuccessor
      val obs1 = tracker.increaseWatermark(ts.plusSeconds(1))
      tracker.highWatermark shouldBe ts.plusSeconds(1)
      // We can increase several times to the same mark
      val obs1a = tracker.increaseWatermark(ts.plusSeconds(1))
      val obs2 = tracker.increaseWatermark(ts.plusSeconds(2).immediateSuccessor)
      tracker.highWatermark shouldBe ts.plusSeconds(2).immediateSuccessor
      obs0.isCompleted shouldBe false
      obs1.isCompleted shouldBe false
      obs1a.isCompleted shouldBe false
      tracker.registerEnd(ts)
      obs0.isCompleted shouldBe false
      obs1.isCompleted shouldBe false
      obs1a.isCompleted shouldBe false
      tracker.registerEnd(ts)
      for {
        _ <- obs0
        _ <- obs1
        _ <- obs1a
        _ = obs2.isCompleted shouldBe false
        _ = tracker.registerEnd(ts.plusSeconds(2))
        _ <- obs2
      } yield succeed
    }

    "run tasks with low marks at watermark" in {
      val tracker = mk()

      val ts = CantonTimestamp.ofEpochSecond(100)
      val ts1 = ts.plusSeconds(1)
      val ts2 = ts.plusSeconds(2)
      tracker.registerBegin(ts) shouldBe ts
      val obs1 = tracker.increaseWatermark(ts1)
      tracker.registerBegin(ts) shouldBe ts1
      tracker.registerEnd(ts)
      val obs2 = tracker.increaseWatermark(ts2)
      always() {
        obs2.isCompleted shouldBe false
      }
      tracker.registerEnd(ts1)
      for {
        _ <- obs1
        _ <- obs2
      } yield succeed
    }
  }
}
