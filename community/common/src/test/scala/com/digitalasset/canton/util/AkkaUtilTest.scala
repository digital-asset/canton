// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.{KillSwitch, OverflowStrategy}
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.util.AkkaUtil.syntax.*
import com.digitalasset.canton.{BaseTest, DiscardOps}

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}

class AkkaUtilTest extends StreamSpec with BaseTest {
  // Override the implicit from AkkaSpec so that we don't get ambiguous implicits
  override val patience: PatienceConfig = defaultPatience

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
        source.runWith(Sink.seq).futureValue should ===(
          outcomes(10, 11)
        )

        maxParallelism.get shouldBe 1
        currentParallelism.get shouldBe 0
      }

      "emit only AbortedDueToShutdown after the first" in assertAllStagesStopped {
        val shutdownAt = 5
        val source = Source(1 to 10).mapAsyncUS(parallelism = 1)(abortOn(shutdownAt))
        source.runWith(Sink.seq).futureValue should
          ===(outcomes(10, shutdownAt))
      }

      "stop evaluation upon the first AbortedDueToShutdown" in assertAllStagesStopped {
        val evaluationCount = new AtomicInteger(0)
        val shutdownAt = 5
        val source = Source(1 to 10).mapAsyncUS(parallelism = 1) { elem =>
          evaluationCount.addAndGet(1).discard[Int]
          abortOn(shutdownAt)(elem)
        }
        source.runWith(Sink.seq).futureValue should
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
        mapped.runWith(Sink.seq).futureValue should
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
        source.runWith(Sink.seq).futureValue should ===(
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
        source.runWith(Sink.seq).futureValue should ===(
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
        mapped.runWith(Sink.seq).futureValue should
          ===(outcomes(10, shutdownAt))
        evaluationCount.get shouldBe 10
      }
    }
  }

  "mapAsyncAndDrainUS" should {
    "stop upon the first AbortedDueToShutdown" in assertAllStagesStopped {
      val shutdownAt = 3
      val source = Source(1 to 10).mapAsyncAndDrainUS(parallelism = 3)(abortOn(shutdownAt))
      source.runWith(Sink.seq).futureValue should
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
      mapped.runWith(Sink.seq).futureValue should
        ===(1 until shutdownAt)
      evaluationCount.get shouldBe 10
    }
  }

  "restartSource" should {
    case class RetryCallArgs(
        lastState: Int,
        lastEmittedElement: Option[Int],
        lastFailure: Option[Throwable],
    )

    "restart upon normal completion" in assertAllStagesStopped {
      def mkSource(s: Int): Source[Int, NotUsed] = Source(s until s + 3)
      val lastStates = new AtomicReference[Seq[Int]](Seq.empty[Int])
      val policy = new AkkaUtil.RetrySourcePolicy[Int, Int, NotUsed] {
        override def shouldRetry(
            lastState: Int,
            mat: NotUsed,
            lastEmittedElement: Option[Int],
            lastFailure: Option[Throwable],
        ): Option[(FiniteDuration, Int)] = {
          lastStates.updateAndGet(states => states :+ lastState)
          Option.when(lastState < 10)((0.seconds, lastState + 3))
        }
      }

      val ((_killSwitch, doneF), retrievedElemsF) = AkkaUtil
        .restartSource("restart-upon-completion", 1, mkSource, policy)
        .toMat(Sink.seq)(Keep.both)
        .run()
      retrievedElemsF.futureValue shouldBe (1 to 12)

      doneF.futureValue
      lastStates.get() shouldBe Seq(1, 4, 7, 10)
    }

    "restart with a delay" in assertAllStagesStopped {
      def mkSource(s: Int): Source[Int, NotUsed] = Source(s until s + 3)
      val delay = 200.milliseconds
      val policy = new AkkaUtil.RetrySourcePolicy[Int, Int, NotUsed] {
        override def shouldRetry(
            lastState: Int,
            mat: NotUsed,
            lastEmittedElement: Option[Int],
            lastFailure: Option[Throwable],
        ): Option[(FiniteDuration, Int)] = {
          Option.when(lastEmittedElement.forall(_ < 10))((delay, lastState + 3))
        }
      }

      val stream = AkkaUtil
        .restartSource("restart-with-delay", 1, mkSource, policy)
        .toMat(Sink.seq)(Keep.both)

      val start = System.nanoTime()
      val ((_killSwitch, doneF), retrievedElemsF) = stream.run()
      retrievedElemsF.futureValue shouldBe (1 to 12)
      val stop = System.nanoTime()
      (stop - start) shouldBe >=(3 * delay.toNanos)
      doneF.futureValue
    }

    "deal with empty sources" in assertAllStagesStopped {
      val shouldRetryCalls = new AtomicReference[Seq[RetryCallArgs]](Seq.empty[RetryCallArgs])

      def mkSource(s: Int): Source[Int, NotUsed] =
        if (s > 3) Source(1 until 3) else Source.empty[Int]
      val policy = new AkkaUtil.RetrySourcePolicy[Int, Int, NotUsed] {
        override def shouldRetry(
            lastState: Int,
            mat: NotUsed,
            lastEmittedElement: Option[Int],
            lastFailure: Option[Throwable],
        ): Option[(FiniteDuration, Int)] = {
          shouldRetryCalls
            .updateAndGet(RetryCallArgs(lastState, lastEmittedElement, lastFailure) +: _)
            .discard
          Option.when(lastState < 5)((0.seconds, lastState + 1))
        }
      }
      val ((_killSwitch, doneF), retrievedElemsF) = AkkaUtil
        .restartSource("restart-with-delay", 1, mkSource, policy)
        .toMat(Sink.seq)(Keep.both)
        .run()
      retrievedElemsF.futureValue shouldBe Seq(1, 2, 1, 2)
      doneF.futureValue
      shouldRetryCalls.get().foreach {
        case RetryCallArgs(lastState, lastEmittedElement, lastFailure) =>
          lastFailure shouldBe None
          lastEmittedElement shouldBe Option.when(lastState > 3)(2)
      }
    }

    "propagate errors" in assertAllStagesStopped {
      case class StreamFailure(i: Int) extends Exception(i.toString)
      val shouldRetryCalls = new AtomicReference[Seq[RetryCallArgs]](Seq.empty[RetryCallArgs])

      def mkSource(s: Int): Source[Int, NotUsed] =
        if (s % 2 == 0) Source.failed[Int](StreamFailure(s)) else Source.single(10 + s)
      val policy = new AkkaUtil.RetrySourcePolicy[Int, Int, NotUsed] {
        override def shouldRetry(
            lastState: Int,
            mat: NotUsed,
            lastEmittedElement: Option[Int],
            lastFailure: Option[Throwable],
        ): Option[(FiniteDuration, Int)] = {
          shouldRetryCalls
            .updateAndGet(RetryCallArgs(lastState, lastEmittedElement, lastFailure) +: _)
            .discard
          Option.when(lastState < 5)((0.seconds, lastState + 1))
        }
      }
      val ((_killSwitch, doneF), retrievedElemsF) = AkkaUtil
        .restartSource("restart-propagate-error", 1, mkSource, policy)
        .toMat(Sink.seq)(Keep.both)
        .run()
      retrievedElemsF.futureValue shouldBe Seq(11, 13, 15)
      doneF.futureValue

      shouldRetryCalls.get().foreach {
        case RetryCallArgs(lastState, lastEmittedElement, lastFailure) =>
          lastFailure shouldBe Option.when(lastState % 2 == 0)(StreamFailure(lastState))
          lastEmittedElement shouldBe Option.when(lastState % 2 != 0)(10 + lastState)
      }
    }

    "stop upon pulling the kill switch" in assertAllStagesStopped {
      val pulledKillSwitchAt = new SingleUseCell[Int]
      val pullKillSwitch = new SingleUseCell[KillSwitch]

      def mkSource(s: Int): Source[Int, NotUsed] = Source.single(s)
      val policy = new AkkaUtil.RetrySourcePolicy[Int, Int, NotUsed] {
        override def shouldRetry(
            lastState: Int,
            mat: NotUsed,
            lastEmittedElement: Option[Int],
            lastFailure: Option[Throwable],
        ): Option[(FiniteDuration, Int)] = {
          pullKillSwitch.get.foreach { killSwitch =>
            if (lastState > 10) {
              pulledKillSwitchAt.putIfAbsent(lastState)
              killSwitch.shutdown()
            }
          }
          Some((1.millisecond, lastState + 1))
        }
      }
      val ((killSwitch, doneF), retrievedElemsF) = AkkaUtil
        .restartSource("restart-stop-on-kill", 1, mkSource, policy)
        .toMat(Sink.seq)(Keep.both)
        .run()
      pullKillSwitch.putIfAbsent(killSwitch)
      val retrievedElems = retrievedElemsF.futureValue
      val lastRetry = pulledKillSwitchAt.get.value
      lastRetry shouldBe >(10)
      retrievedElems shouldBe (1 to lastRetry)
      doneF.futureValue
    }

    "abort the delay when the KillSwitch is closed" in assertAllStagesStopped {
      def mkSource(s: Int): Source[Int, NotUsed] = Source.single(s)
      val pullKillSwitch = new SingleUseCell[KillSwitch]
      val longBackoff = 10.seconds

      val policy = new AkkaUtil.RetrySourcePolicy[Int, Int, NotUsed] {
        override def shouldRetry(
            lastState: Int,
            mat: NotUsed,
            lastEmittedElement: Option[Int],
            lastFailure: Option[Throwable],
        ): Option[(FiniteDuration, Int)] = {
          pullKillSwitch.get.foreach { killSwitch =>
            if (lastState > 10) killSwitch.shutdown()
          }
          val backoff =
            if (pullKillSwitch.isEmpty || lastState <= 10) 1.millisecond else longBackoff
          Some((backoff, lastState + 1))
        }
      }
      val graph = AkkaUtil
        .restartSource("restart-stop-immediately-on-kill-switch", 1, mkSource, policy)
        .toMat(Sink.seq)(Keep.both)
      val start = System.nanoTime()
      val ((killSwitch, doneF), retrievedElemsF) = graph.run()
      pullKillSwitch.putIfAbsent(killSwitch)
      doneF.futureValue
      val stop = System.nanoTime()
      (stop - start) shouldBe <(longBackoff.toNanos)
      retrievedElemsF.futureValue shouldBe (1 to 11)
    }

    "the completion future awaits the retry to finish" in assertAllStagesStopped {
      def mkSource(s: Int): Source[Int, NotUsed] = Source.single(s)

      val pullKillSwitch = new SingleUseCell[KillSwitch]
      val policyDelayPromise = Promise[Unit]()

      val policy = new AkkaUtil.RetrySourcePolicy[Int, Int, NotUsed] {
        override def shouldRetry(
            lastState: Int,
            mat: NotUsed,
            lastEmittedElement: Option[Int],
            lastFailure: Option[Throwable],
        ): Option[(FiniteDuration, Int)] = {
          pullKillSwitch.get.foreach { killSwitch =>
            if (lastState > 10) {
              killSwitch.shutdown()
              policyDelayPromise.future.futureValue
            }
          }
          Some((1.millisecond, lastState + 1))
        }
      }
      val ((killSwitch, doneF), retrievedElemsF) = AkkaUtil
        .restartSource("restart-synchronize-retry", 1, mkSource, policy)
        .toMat(Sink.seq)(Keep.both)
        .run()
      pullKillSwitch.putIfAbsent(killSwitch)
      retrievedElemsF.futureValue
      // The retry policy is still running as we haven't yet completed the promise,
      // so the completion future must not be completed yet
      always(durationOfSuccess = 1.second) {
        doneF.isCompleted shouldBe false
      }
      policyDelayPromise.success(())
      doneF.futureValue
    }

    "propagate the materialized value" in assertAllStagesStopped {
      val materializedValues = new AtomicReference[Seq[Int]](Seq.empty[Int])
      def mkSource(s: Int): Source[Int, Int] = Source.empty.mapMaterializedValue(_ => s + 10)
      val policy = new AkkaUtil.RetrySourcePolicy[Int, Int, Int] {
        override def shouldRetry(
            lastState: Int,
            mat: Int,
            lastEmittedElement: Option[Int],
            lastFailure: Option[Throwable],
        ): Option[(FiniteDuration, Int)] = {
          materializedValues.updateAndGet(_ :+ mat)
          Option.when(lastState < 10)((0.millisecond, lastState + 1))
        }
      }
      val ((_killSwitch, doneF), retrievedElemsF) = AkkaUtil
        .restartSource("restart-propagate-materialization", 1, mkSource, policy)
        .toMat(Sink.seq)(Keep.both)
        .run()
      retrievedElemsF.futureValue shouldBe Seq.empty
      materializedValues.get() shouldBe (11 to 20)
      doneF.futureValue
    }

    "log errors thrown during the retry step and complete the stream" in assertAllStagesStopped {
      def mkSource(s: Int): Source[Int, NotUsed] = Source.single(s)
      val exception = new Exception("Retry policy failure")
      val policy = new AkkaUtil.RetrySourcePolicy[Int, Int, NotUsed] {
        override def shouldRetry(
            lastState: Int,
            mat: NotUsed,
            lastEmittedElement: Option[Int],
            lastFailure: Option[Throwable],
        ): Option[(FiniteDuration, Int)] = {
          if (lastState > 3) throw exception
          Some((0.milliseconds, lastState + 1))
        }
      }
      val name = "restart-log-error"
      val graph = AkkaUtil
        .restartSource(name, 1, mkSource, policy)
        .toMat(Sink.seq)(Keep.both)
      val retrievedElems = loggerFactory.assertLogs(
        {
          val ((_killSwitch, doneF), retrievedElemsF) = graph.run()
          doneF.futureValue
          retrievedElemsF.futureValue
        },
        entry => {
          entry.errorMessage should include(
            s"The retry policy for RestartSource $name failed with an error. Stop retrying."
          )
          entry.throwable should contain(exception)
        },
        // The log line from the flush
        _.errorMessage should include(s"RestartSource $name at state 4 failed"),
      )
      retrievedElems shouldBe Seq(1, 2, 3, 4)
    }

    "can pull the kill switch after retries have stopped" in assertAllStagesStopped {
      def mkSource(s: Int): Source[Int, NotUsed] = Source.empty[Int]
      val policy = new AkkaUtil.RetrySourcePolicy[Int, Int, NotUsed] {
        override def shouldRetry(
            lastState: Int,
            mat: NotUsed,
            lastEmittedElement: Option[Int],
            lastFailure: Option[Throwable],
        ): Option[(FiniteDuration, Int)] = None
      }
      val ((killSwitch, doneF), retrievedElemsF) = AkkaUtil
        .restartSource("restart-kill-switch-after-complete", 1, mkSource, policy)
        .toMat(Sink.seq)(Keep.both)
        .run()
      retrievedElemsF.futureValue shouldBe Seq.empty
      doneF.futureValue
      killSwitch.shutdown()
    }
  }

  "withMaterializedValueMat" should {
    "pass the materialized value into the stream" in assertAllStagesStopped {
      val source = Source(1 to 10)
      val (mat, fut) = source
        .withMaterializedValueMat(new AtomicInteger(1))(Keep.right)
        .map { case (i, m) => m.addAndGet(i) }
        // Add a buffer so that the map function executes even though the resulting element doesn't end up in the sink
        .buffer(size = 2, OverflowStrategy.backpressure)
        // Stop the stream early to test cancellation support
        .take(5)
        .toMat(Sink.seq)(Keep.both)
        .run()
      fut.futureValue should ===(Seq(2, 4, 7, 11, 16))
      mat.get shouldBe 22
    }

    "create a new value upon each materialization" in assertAllStagesStopped {
      val stream = AkkaUtil
        .withMaterializedValueMat(new AtomicInteger(0))(Source(1 to 5))(Keep.right)
        .map { case (i, m) => m.addAndGet(i) }
        .toMat(Sink.seq)(Keep.both)

      val (mat1, seq1) = stream.run()
      val (mat2, seq2) = stream.run()

      // We get two different materialized atomic integers
      mat1 shouldNot be(mat2)

      seq1.futureValue should ===(Seq(1, 3, 6, 10, 15))
      seq2.futureValue should ===(Seq(1, 3, 6, 10, 15))
    }

    "propagate errors down" in assertAllStagesStopped {
      val ((source, mat), sink) = TestSource
        .probe[Int]
        .withMaterializedValueMat(new AtomicInteger(0))(Keep.both)
        .map { case (i, m) => m.addAndGet(i) }
        .buffer(2, OverflowStrategy.backpressure)
        .toMat(TestSink.probe[Int])(Keep.both)
        .run()

      sink.request(1)
      source.sendNext(1)
      sink.expectNext(1)
      source.sendNext(2)
      source.sendNext(3)
      val ex = new Exception("Source error")
      source.sendError(ex)
      sink.expectError() should ===(ex)

      mat.get() should ===(6)
    }

    "propagate errors up" in assertAllStagesStopped {
      val ((source, mat), sink) = TestSource
        .probe[Int]
        .withMaterializedValueMat(new AtomicInteger(0))(Keep.both)
        .map { case (i, m) => m.addAndGet(i) }
        .buffer(2, OverflowStrategy.backpressure)
        .toMat(TestSink.probe[Int])(Keep.both)
        .run()

      sink.request(1)
      source.sendNext(1)
      sink.expectNext(1)
      source.sendNext(2)
      source.sendNext(3)
      val ex = new Exception("Sink error")
      sink.cancel(ex)
      source.expectCancellationWithCause(ex)

      mat.get() should ===(6)
    }
  }

  "withUniqueKillSwitchMat" should {
    "make the kill switch available inside the stream" in assertAllStagesStopped {
      val (source, sink) = TestSource
        .probe[Int]
        .withUniqueKillSwitchMat()(Keep.left)
        .map { case (i, killSwitch) =>
          if (i > 0) killSwitch.shutdown()
          i
        }
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(4)
      source.expectRequest() shouldBe >=(3L)
      source.sendNext(0).sendNext(-1).sendNext(2)
      sink.expectNext(0).expectNext(-1).expectNext(2).expectComplete()
    }

    "make the same kill switch available in the materialization" in assertAllStagesStopped {
      val ((source, killSwitch), sink) = TestSource
        .probe[Int]
        .withUniqueKillSwitchMat()(Keep.both)
        .toMat(TestSink.probe)(Keep.both)
        .run()
      sink.request(3)
      source.sendNext(100)
      sink.expectNext(100 -> killSwitch)
      killSwitch.shutdown()
      source.expectCancellation()
      sink.expectComplete()
    }

    "propagate completions even without pulling the kill switch" in assertAllStagesStopped {
      val (source, sink) = TestSource
        .probe[Int]
        .withUniqueKillSwitchMat()(Keep.left)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(1)
      source.sendComplete()
      sink.expectComplete()
    }

    "propagate errors" in assertAllStagesStopped {
      val ex = new Exception("Kill Switch")
      val (source, sink) = TestSource
        .probe[Int]
        .withUniqueKillSwitchMat()(Keep.left)
        .map { case (i, killSwitch) =>
          killSwitch.abort(ex)
          i
        }
        .toMat(TestSink.probe)(Keep.both)
        .run()
      sink.request(1)
      source.sendNext(1)
      source.expectCancellationWithCause(ex)
      // Since the kill switch is pulled from within the stream's handler,
      // the OnNext message will arrive at the sink before the kill switches
      // OnError message goes through the flow that pulled the kill switch.
      // So given Akka's in-order deliver guarantees between actor pairs,
      // the OnError will arrive after the OnNext.
      sink.expectNext(1)
      sink.expectError(ex)
    }

  }
}
