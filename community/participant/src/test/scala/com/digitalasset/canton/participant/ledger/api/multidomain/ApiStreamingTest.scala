// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.multidomain

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{KillSwitches, ThrottleMode}
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.participant.ledger.api.multidomain.ApiStreaming.IndexRange
import com.digitalasset.canton.util.{AkkaUtil, DelayUtil}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.chaining.scalaUtilChainingOps

class ApiStreamingTest
    extends AsyncWordSpec
    with HasExecutionContext // TODO(#11002) revisit usage of AsyncTestSuite with HasExecutionContext
    with BeforeAndAfterAll
    with BaseTest {

  "asyncIndexPoller" should {

    "work correctly in the happy path" in {
      val poller = new PollerFixture(List(1, 2, 3, 3, 3, 3, 3, 7, 7, 8, 10, 10, 10, 11, 11, 11))
      ApiStreaming
        .asyncIndexPoller()(poller)
        .takeWhile(_ < 9)
        .runWith(Sink.collection)
        .map(_ shouldBe List(1, 2, 3, 7, 8))
    }

    "poll only if there is demand" in {
      val poller = new PollerFixture(List(1, 2, 3, 3, 3, 3, 3, 7, 7, 8, 10))
      ApiStreaming
        .asyncIndexPoller()(poller)
        .throttle(
          elements = 1,
          per = FiniteDuration(1000, "millis"),
          maximumBurst = 1,
          mode = ThrottleMode.Shaping,
        )
        .map { elem =>
          elem match {
            case 1 => poller.pollingSeq.get() shouldBe List(2, 3, 3, 3, 3, 3, 7, 7, 8, 10)
            case 2 => poller.pollingSeq.get() shouldBe List(3, 3, 3, 3, 3, 7, 7, 8, 10)
            case 3 => poller.pollingSeq.get() shouldBe List(3, 3, 3, 3, 7, 7, 8, 10)
            case 7 => poller.pollingSeq.get() shouldBe List(7, 8, 10)
            case 8 => poller.pollingSeq.get() shouldBe List(10)
            case _ => ()
          }
          elem
        }
        .takeWhile(_ < 9)
        .runWith(Sink.collection)
        .map(_ shouldBe List(1, 2, 3, 7, 8))
    }

    "poll without delay if no backpressure, and all elements are distinct" in {
      val poller = new PollerFixture(Range.Long.inclusive(1, 10, 1).toList)
      val start = System.nanoTime()
      ApiStreaming
        .asyncIndexPoller()(poller)
        .takeWhile(_ < 10)
        .runWith(Sink.collection)
        .map { result =>
          val end = System.nanoTime()
          (end - start) / 1000000 should be < 500L
          result shouldBe Range.Long.inclusive(1, 9, 1)
        }
    }

    "poll with expected delay sequence if no backpressure, and all elements are repeated" in {
      val poller = new PollerFixture(
        List(
          1, // delay: 0
          1, // delay: 25
          1, // delay: 50
          1, // delay: 100
          1, // delay: 200
          1, // delay: 400
          1, // delay: 800
          1, // delay: 1000
          1, // delay: 1000
          2,
        )
      )
      val start = System.nanoTime()
      ApiStreaming
        .asyncIndexPoller()(poller)
        .takeWhile(_ < 2)
        .runWith(Sink.collection)
        .map { result =>
          val end = System.nanoTime()
          (end - start) / 1000000 should be >= (25 + 50 + 100 + 200 + 400 + 800 + 1000 + 1000).toLong
          result shouldBe List(1)
        }
    }

    "interrupt gracefully an ongoing polling on stream termination" in {
      val poller = new PollerFixture(
        List(
          1, // delay: 0
          1, // delay: 25
          1, // delay: 50
          1, // delay: 100
          1, // delay: 200
          1, // delay: 400
          1, // delay: 800
          1, // delay: 1000
          1, // delay: 1000
          2,
        ) // 7575 millis if runs to the end
      )
      val (killSwitch, streamCompletedF) = ApiStreaming
        .asyncIndexPoller()(poller)
        .takeWhile(_ < 2)
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.collection)(Keep.both)
        .run()

      for {
        _ <- DelayUtil.delay(FiniteDuration(2000, "millis"))
        _ = {
          logger.debug("After 2 seconds")
          poller.pollingSeq.get() shouldBe List(1, 2)
          logger.debug("We are in the middle of waiting for the before last 1")
          logger.debug("As shutting the stream down")
          killSwitch.shutdown()
        }
        _ <- DelayUtil.delay(FiniteDuration(100, "millis"))
        _ = {
          logger.debug("After 100 millis")
          streamCompletedF.isCompleted shouldBe true
          logger.debug("The stream should be finished as well")
        }
        resultsFromStream <- streamCompletedF
        _ = {
          resultsFromStream shouldBe List(1)
        }
        _ <- DelayUtil.delay(FiniteDuration(1000, "millis"))
      } yield {
        logger.debug(
          "And after 1000 millis more (as the delay for the current polling future should be done already), we should observe no error."
        )
        poller.pollingSeq.get() shouldBe List(1, 2)
        logger.debug("And the polling function should not be triggered anymore.")
        succeed
      }
    }
  }

  "tailingInclusiveIndexRanges" should {

    "work correctly for past ranges" in {
      ApiStreaming
        .tailingInclusiveIndexRanges(IndexRange(10, 100))(Source(List(1000, 1100)))
        .runWith(Sink.collection)
        .map(_ shouldBe List(IndexRange(10, 100)))
    }

    "work correctly if end of range is in the future" in {
      ApiStreaming
        .tailingInclusiveIndexRanges(IndexRange(10, 1500))(
          Source(List(1000, 1100, 1200, 1300, 1700))
        )
        .runWith(Sink.collection)
        .map(
          _ shouldBe List(
            IndexRange(10, 1000),
            IndexRange(1001, 1100),
            IndexRange(1101, 1200),
            IndexRange(1201, 1300),
            IndexRange(1301, 1500),
          )
        )
    }

    "work correctly if start and end of range is in the future" in {
      ApiStreaming
        .tailingInclusiveIndexRanges(IndexRange(1250, 1500))(
          Source(List(1000, 1100, 1200, 1300, 1700))
        )
        .runWith(Sink.collection)
        .map(
          _ shouldBe List(
            IndexRange(1250, 1300),
            IndexRange(1301, 1500),
          )
        )
    }

    "work correctly on boundaries around end (exactly that)" in {
      ApiStreaming
        .tailingInclusiveIndexRanges(IndexRange(1250, 1500))(
          Source(List(1000, 1100, 1200, 1500, 1700))
        )
        .runWith(Sink.collection)
        .map(
          _ shouldBe List(
            IndexRange(1250, 1500)
          )
        )
    }

    "work correctly on boundaries around end (+1)" in {
      ApiStreaming
        .tailingInclusiveIndexRanges(IndexRange(1250, 1500))(
          Source(List(1000, 1100, 1200, 1501, 1700))
        )
        .runWith(Sink.collection)
        .map(
          _ shouldBe List(
            IndexRange(1250, 1500)
          )
        )
    }

    "work correctly on boundaries around end (-1)" in {
      ApiStreaming
        .tailingInclusiveIndexRanges(IndexRange(1250, 1500))(
          Source(List(1000, 1100, 1200, 1499, 1700))
        )
        .runWith(Sink.collection)
        .map(
          _ shouldBe List(
            IndexRange(1250, 1499),
            IndexRange(1500, 1500),
          )
        )
    }

    "work correctly on boundaries around start (exactly that)" in {
      ApiStreaming
        .tailingInclusiveIndexRanges(IndexRange(1250, 1500))(Source(List(1250, 1300, 1700)))
        .runWith(Sink.collection)
        .map(
          _ shouldBe List(
            IndexRange(1250, 1250),
            IndexRange(1251, 1300),
            IndexRange(1301, 1500),
          )
        )
    }

    "work correctly on boundaries around start (-1)" in {
      ApiStreaming
        .tailingInclusiveIndexRanges(IndexRange(1250, 1500))(Source(List(1249, 1300, 1700)))
        .runWith(Sink.collection)
        .map(
          _ shouldBe List(
            IndexRange(1250, 1300),
            IndexRange(1301, 1500),
          )
        )
    }

    "work correctly on boundaries around start (+1)" in {
      ApiStreaming
        .tailingInclusiveIndexRanges(IndexRange(1250, 1500))(Source(List(1251, 1300, 1700)))
        .runWith(Sink.collection)
        .map(
          _ shouldBe List(
            IndexRange(1250, 1251),
            IndexRange(1252, 1300),
            IndexRange(1301, 1500),
          )
        )
    }
  }

  "pagedRangeQueryStream" should {

    "work correctly for the happy path" in {
      val rangeQuery = new RangeQueryFixture(4)
      ApiStreaming
        .pagedRangeQueryStream(rangeQuery)(_.toLong)(IndexRange(10L, 20L))
        .runWith(Sink.collection)
        .map { result =>
          result shouldBe Range.inclusive(10, 20).toVector
          rangeQuery.populatedRanges.get() shouldBe Vector(
            IndexRange(10, 20),
            IndexRange(14, 20),
            IndexRange(18, 20),
          )
        }
    }

    "work correctly for the happy path if end falls on a full window" in {
      val rangeQuery = new RangeQueryFixture(4)
      ApiStreaming
        .pagedRangeQueryStream(rangeQuery)(_.toLong)(IndexRange(10L, 22L))
        .runWith(Sink.collection)
        .map { result =>
          result shouldBe Range.inclusive(10, 22).toVector
          rangeQuery.populatedRanges.get() shouldBe Vector(
            IndexRange(10, 22),
            IndexRange(14, 22),
            IndexRange(18, 22),
            IndexRange(22, 22),
          )
        }
    }

    "work correctly for the happy path if end falls almost on a full window (-1)" in {
      val rangeQuery = new RangeQueryFixture(4)
      ApiStreaming
        .pagedRangeQueryStream(rangeQuery)(_.toLong)(IndexRange(10L, 21L))
        .runWith(Sink.collection)
        .map { result =>
          result shouldBe Range.inclusive(10, 21).toVector
          rangeQuery.populatedRanges.get() shouldBe Vector(
            IndexRange(10, 21),
            IndexRange(14, 21),
            IndexRange(18, 21),
          )
        }
    }

    "work correctly for the happy path if end falls almost on a full window (+1)" in {
      val rangeQuery = new RangeQueryFixture(4)
      ApiStreaming
        .pagedRangeQueryStream(rangeQuery)(_.toLong)(IndexRange(10L, 23L))
        .runWith(Sink.collection)
        .map { result =>
          result shouldBe Range.inclusive(10, 23).toVector
          rangeQuery.populatedRanges.get() shouldBe Vector(
            IndexRange(10, 23),
            IndexRange(14, 23),
            IndexRange(18, 23),
            IndexRange(22, 23),
          )
        }
    }

    "work correctly for every third number if end falls on a full window" in {
      val rangeQuery = new RangeQueryFixture(4, _ % 3 == 0)
      ApiStreaming
        .pagedRangeQueryStream(rangeQuery)(_.toLong)(IndexRange(10L, 21L))
        .runWith(Sink.collection)
        .map { result =>
          result shouldBe Vector(12, 15, 18, 21)
          rangeQuery.populatedRanges.get() shouldBe Vector(
            IndexRange(10, 21)
          )
        }
    }

    "work correctly for every third number if end falls almost on a full window (-1)" in {
      val rangeQuery = new RangeQueryFixture(4, _ % 3 == 0)
      ApiStreaming
        .pagedRangeQueryStream(rangeQuery)(_.toLong)(IndexRange(10L, 20L))
        .runWith(Sink.collection)
        .map { result =>
          result shouldBe Vector(12, 15, 18)
          rangeQuery.populatedRanges.get() shouldBe Vector(
            IndexRange(10, 20),
            IndexRange(19, 20),
          )
        }
    }

    "work correctly for every third number if end falls almost on a full window (+1)" in {
      val rangeQuery = new RangeQueryFixture(4, _ % 3 == 0)
      ApiStreaming
        .pagedRangeQueryStream(rangeQuery)(_.toLong)(IndexRange(10L, 22L))
        .runWith(Sink.collection)
        .map { result =>
          result shouldBe Vector(12, 15, 18, 21)
          rangeQuery.populatedRanges.get() shouldBe Vector(
            IndexRange(10, 22),
            IndexRange(22, 22),
          )
        }
    }
  }

  "running asyncIndexPoller, tailingInclusiveIndexRanges and pagedRangeQueryStream in concert" should {

    "work for the happy path case" in {
      ApiStreaming
        .asyncIndexPoller()(
          new PollerFixture(
            List(40, 40, 40, 40, 50, 50, 50, 60, 70, 70, 70, 70, 110)
          )
        )
        .pipe(ApiStreaming.tailingInclusiveIndexRanges(IndexRange(55, 100)))
        .flatMapConcat(
          ApiStreaming.pagedRangeQueryStream(
            rangeQueryWithLimit = new RangeQueryFixture(
              limit = 3,
              filter = _ % 2 == 1,
              // Please not the delay specified here helps to visualize in logs expected behavior:
              // backpressure from this stage should stop polling.
              // Test were not implement to prove that directly due to the complexity it raises.
              takes = FiniteDuration(200, "millis"),
            )
          )(_.toLong)
        )
        .runWith(Sink.collection)
        .map(_ shouldBe Range.inclusive(55, 99, 2))
    }
  }

  private class RangeQueryFixture(
      limit: Int,
      filter: Int => Boolean = _ => true,
      takes: FiniteDuration = FiniteDuration(0, "millis"),
  ) extends (IndexRange => Future[Vector[Int]]) {
    val populatedRanges = new AtomicReference[Vector[IndexRange]](Vector.empty)
    def apply(indexRange: IndexRange): Future[Vector[Int]] =
      DelayUtil
        .delay(takes)
        .map(_ =>
          Range
            .inclusive(indexRange.fromInclusive.toInt, indexRange.toInclusive.toInt, 1)
            .filter(filter)
            .take(limit)
            .toVector
            .tap(results =>
              logger.debug(s"range query $indexRange with limit $limit, resulted in $results")
            )
            .tap(_ => populatedRanges.updateAndGet(acc => acc :+ indexRange))
        )
  }

  private class PollerFixture(sequence: List[Long]) extends (() => Future[Long]) {
    val pollingSeq = new AtomicReference(sequence)
    def apply(): Future[Long] =
      pollingSeq.getAndUpdate(_.drop(1)) match {
        case head :: tail =>
          logger.debug(s"polling $head")
          Future.successful(head)
        case Nil =>
          logger.debug(s"polling reached end")
          Future.failed(new IllegalStateException("no more elements"))
      }
  }

  private implicit val actorSystem: ActorSystem =
    AkkaUtil.createActorSystem(loggerFactory.threadName)(parallelExecutionContext)

  override def afterAll(): Unit = {
    Lifecycle.toCloseableActorSystem(actorSystem, logger, timeouts).close()
    super.afterAll()
  }
}
