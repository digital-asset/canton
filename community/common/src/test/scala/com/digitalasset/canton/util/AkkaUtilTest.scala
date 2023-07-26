// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.util.AkkaUtil.syntax.*
import com.digitalasset.canton.{BaseTest, DiscardOps}
import org.scalactic.source.Position

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future}

class AkkaUtilTest extends StreamSpec with BaseTest {

  implicit val executionContext: ExecutionContext = system.dispatcher

  private def abortOn(trigger: Int)(x: Int): FutureUnlessShutdown[Int] =
    FutureUnlessShutdown(Future {
      if (x == trigger) UnlessShutdown.AbortedDueToShutdown
      else UnlessShutdown.Outcome(x)
    })

  private def outcomes(length: Int, abortedFrom: Int): Seq[UnlessShutdown[Int]] =
    (1 until (abortedFrom min (length + 1))).map(UnlessShutdown.Outcome.apply) ++
      Seq.fill((length - abortedFrom + 1) max 0)(UnlessShutdown.AbortedDueToShutdown)

  "mapAsyncUS" when {
    "parallelism is 1" should {
      "run everything sequentially" in assertAllStagesStopped {
        val currentParallelism = new AtomicInteger(0)
        val maxParallelism = new AtomicInteger(0)

        val source = Source(1 to 10).mapAsyncUS(parallelism = 1) { elem =>
          FutureUnlessShutdown(Future {
            val nextCurrent = currentParallelism.addAndGet(1)
            maxParallelism.getAndUpdate(_ max nextCurrent)
            Thread.`yield`()
            Threading.sleep(10)
            currentParallelism.addAndGet(-1)
            UnlessShutdown.Outcome(elem)
          })
        }
        source.runWith(Sink.seq).futureValue(defaultPatience, Position.here) should ===(
          outcomes(10, 11)
        )

        maxParallelism.get shouldBe 1
        currentParallelism.get shouldBe 0
      }

      "emit only AbortedDueToShutdown after the first" in assertAllStagesStopped {
        val shutdownAt = 5
        val source = Source(1 to 10).mapAsyncUS(parallelism = 1)(abortOn(shutdownAt))
        source.runWith(Sink.seq).futureValue(defaultPatience, Position.here) should
          ===(outcomes(10, shutdownAt))
      }

      "stop evaluation upon the first AbortedDueToShutdown" in assertAllStagesStopped {
        val evaluationCount = new AtomicInteger(0)
        val shutdownAt = 5
        val source = Source(1 to 10).mapAsyncUS(parallelism = 1) { elem =>
          evaluationCount.addAndGet(1).discard[Int]
          abortOn(shutdownAt)(elem)
        }
        source.runWith(Sink.seq).futureValue(defaultPatience, Position.here) should
          ===(outcomes(10, shutdownAt))
        evaluationCount.get shouldBe shutdownAt
      }

      "drain the source" in assertAllStagesStopped {
        val evaluationCount = new AtomicInteger(0)
        val source = Source(1 to 10).map { elem =>
          evaluationCount.addAndGet(1).discard[Int]
          elem
        }
        val shutdownAt = 6
        val mapped = source.mapAsyncUS(parallelism = 1)(abortOn(shutdownAt))
        mapped.runWith(Sink.seq).futureValue(defaultPatience, Position.here) should
          ===(outcomes(10, shutdownAt))
        evaluationCount.get shouldBe 10
      }
    }

    "parallelism is greater than 1" should {
      "run several futures in parallel" in assertAllStagesStopped {
        val parallelism = 4
        require(parallelism > 1)
        val semaphores = Seq.fill(parallelism)(new Semaphore(1))
        semaphores.foreach(_.acquire())

        val currentParallelism = new AtomicInteger(0)
        val maxParallelism = new AtomicInteger(0)

        val source = Source(1 to 10 * parallelism).mapAsyncUS(parallelism) { elem =>
          FutureUnlessShutdown(Future {
            val nextCurrent = currentParallelism.addAndGet(1)
            maxParallelism.getAndUpdate(_ max nextCurrent)

            val index = elem % parallelism
            semaphores(index).release()
            semaphores((index + 1) % parallelism).acquire()
            Thread.`yield`()
            Threading.sleep(10)
            currentParallelism.addAndGet(-1)
            UnlessShutdown.Outcome(elem)
          })
        }
        source.runWith(Sink.seq).futureValue(defaultPatience, Position.here) should ===(
          (1 to 10 * parallelism).map(UnlessShutdown.Outcome.apply)
        )
        // The above synchronization allows for some futures finishing before others are started
        // but at least two must run in parallel.
        maxParallelism.get shouldBe <=(parallelism)
        maxParallelism.get shouldBe >=(2)
        currentParallelism.get shouldBe 0
      }

      "emit only AbortedDueToShutdown after the first" in assertAllStagesStopped {
        val shutdownAt = 4
        val source = Source(1 to 10).mapAsyncUS(parallelism = 3) { elem =>
          val outcome =
            if (elem == shutdownAt) UnlessShutdown.AbortedDueToShutdown
            else UnlessShutdown.Outcome(elem)
          FutureUnlessShutdown.lift(outcome)
        }
        source.runWith(Sink.seq).futureValue(defaultPatience, Position.here) should ===(
          (1 until shutdownAt).map(UnlessShutdown.Outcome.apply) ++
            Seq.fill(10 - shutdownAt + 1)(UnlessShutdown.AbortedDueToShutdown)
        )
      }

      "drain the source" in assertAllStagesStopped {
        val evaluationCount = new AtomicInteger(0)
        val source = Source(1 to 10).map { elem =>
          evaluationCount.addAndGet(1).discard[Int]
          elem
        }
        val shutdownAt = 6
        val mapped = source.mapAsyncUS(parallelism = 10)(abortOn(shutdownAt))
        mapped.runWith(Sink.seq).futureValue(defaultPatience, Position.here) should
          ===(outcomes(10, shutdownAt))
        evaluationCount.get shouldBe 10
      }
    }
  }

  "mapAsyncAndDrainUS" should {
    "stop upon the first AbortedDueToShutdown" in assertAllStagesStopped {
      val shutdownAt = 3
      val source = Source(1 to 10).mapAsyncAndDrainUS(parallelism = 3)(abortOn(shutdownAt))
      source.runWith(Sink.seq).futureValue(defaultPatience, Position.here) should
        ===(1 until shutdownAt)
    }

    "drain the source" in assertAllStagesStopped {
      val evaluationCount = new AtomicInteger(0)
      val source = Source(1 to 10).map { elem =>
        evaluationCount.addAndGet(1).discard[Int]
        elem
      }
      val shutdownAt = 5
      val mapped = source.mapAsyncAndDrainUS(parallelism = 1)(abortOn(shutdownAt))
      mapped.runWith(Sink.seq).futureValue(defaultPatience, Position.here) should
        ===(1 until shutdownAt)
      evaluationCount.get shouldBe 10
    }
  }
}
