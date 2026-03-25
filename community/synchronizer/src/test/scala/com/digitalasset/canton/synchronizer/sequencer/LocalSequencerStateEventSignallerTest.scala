// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, LifeCycle}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.synchronizer.sequencer.store.SequencerMemberId
import com.digitalasset.canton.topology.{Member, ParticipantId}
import com.digitalasset.canton.util.PekkoUtil
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.scaladsl.{Keep, Sink, SinkQueueWithCancel, Source}
import org.scalatest.wordspec.FixtureAsyncWordSpec
import org.scalatest.{Assertion, FutureOutcome}

import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.Future
import scala.concurrent.duration.*

class LocalSequencerStateEventSignallerTest
    extends FixtureAsyncWordSpec
    with BaseTest
    with HasExecutionContext {

  val alice: Member = ParticipantId("alice")
  val bob: Member = ParticipantId("bob")
  val aliceId = SequencerMemberId(0)
  val bobId = SequencerMemberId(1)

  class Env extends FlagCloseable with NamedLogging {
    override val timeouts = LocalSequencerStateEventSignallerTest.this.timeouts
    protected override val loggerFactory = LocalSequencerStateEventSignallerTest.this.loggerFactory
    implicit val actorSystem: ActorSystem =
      PekkoUtil.createActorSystem(loggerFactory.threadName)(parallelExecutionContext)

    val nextTimestampSecond = new AtomicLong(0L)
    def generateTimestamp(): CantonTimestamp =
      CantonTimestamp.ofEpochSecond(nextTimestampSecond.getAndIncrement())
    // generate a few upfront
    val ts0 = generateTimestamp()
    val ts1 = generateTimestamp()
    val ts2 = generateTimestamp()

    val signaller = new LocalSequencerStateEventSignaller(timeouts, loggerFactory)

    def expectSignal(queue: SinkQueueWithCancel[ReadSignal]): Future[Assertion] =
      queue.pull() map {
        case None => fail("queue completed without a signal")
        case Some(_) => succeed
      }

    def expectComplete(queue: SinkQueueWithCancel[ReadSignal]): Future[Assertion] =
      queue.pull() map {
        case None => succeed
        case Some(_) => fail("received item but expected complete")
      }

    override protected def onClosed(): Unit =
      LifeCycle.close(
        signaller,
        LifeCycle.toCloseableActorSystem(actorSystem, logger, timeouts),
      )(logger)
  }

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Env()
    complete {
      withFixture(test.toNoArgAsyncTest(env))
    } lastly {
      env.close()
    }
  }
  override type FixtureParam = Env

  "writer updates without subscribers don't block writer" in { env =>
    import env.*

    (0 until 3001).toList.foreach(_ =>
      signaller.notifyOfLocalWrite(WriteNotification.forMemberIds(NonEmpty(Set, aliceId)))
    )
    for {
      // none of the previously produced signals are retained for future subscriptions
      aliceSignals <- PekkoUtil.runSupervised(
        signaller
          .readSignalsForMember(alice, aliceId)
          .takeWithin(
            200.millis
          ) // crude wait to receive more signals if some were going to be produced
          .toMat(Sink.seq)(Keep.right),
        errorLogMessagePrefix = "writer updates",
      )
    } yield {
      aliceSignals shouldBe empty
    }
  }

  "a slow consumer doesn't block other consumers" in { env =>
    import env.*

    val numSignals = 20

    // run a producer that notifies both consumers 20 times at a certain rate
    val notifierF =
      PekkoUtil.runSupervised(
        Source
          .tick(0.seconds, 100.millis, ())
          .take(numSignals.toLong)
          .map(_ =>
            signaller.notifyOfLocalWrite(
              WriteNotification.forMemberIds(NonEmpty(Set, aliceId, bobId))
            )
          )
          .toMat(Sink.ignore)(Keep.right),
        errorLogMessagePrefix = "notifier",
      )

    val consumerKillSwitch = KillSwitches.shared("end-of-signal")
    // alice is a slow consumer
    val aliceF = PekkoUtil.runSupervised(
      signaller
        .readSignalsForMember(alice, aliceId)
        .throttle(1, 1.second)
        .viaMat(consumerKillSwitch.flow)(Keep.left)
        .toMat(Sink.seq)(Keep.right),
      errorLogMessagePrefix = "alice consumer",
    )

    // bob consumes and never backpressures
    val bobF =
      PekkoUtil.runSupervised(
        signaller
          .readSignalsForMember(bob, bobId)
          .viaMat(consumerKillSwitch.flow)(Keep.left)
          .toMat(Sink.seq)(Keep.right),
        errorLogMessagePrefix = "bob consumer",
      )

    for {
      // wait for the producer to terminate
      _ <- notifierF
      // terminate the consumer flows so we can collect the results
      _ = consumerKillSwitch.shutdown()
      // collect all signals that bob received
      bob <- bobF
      // collect all signals that alice received
      alice <- aliceF
    } yield {
      // bob should have received all signals, but because CI can be slow, we allow
      // for some leeway
      bob.size should be > (numSignals / 2)
      // alice should have received fewer signals than bob
      alice.size should be < bob.length
    }
  }
}
