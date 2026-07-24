// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.signalling

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, LifeCycle}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.topology.{Member, ParticipantId}
import com.digitalasset.canton.util.PekkoUtil
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.digitalasset.nonempty.NonEmpty
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.scalatest.FutureOutcome
import org.scalatest.wordspec.FixtureAsyncWordSpec

import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration.*

class LocalEventSignallerTest extends FixtureAsyncWordSpec with BaseTest with HasExecutionContext {

  val alice: Member = ParticipantId("alice")
  val bob: Member = ParticipantId("bob")
  val aliceId = 0
  val bobId = 1

  class Env extends FlagCloseable with NamedLogging {
    override val timeouts = LocalEventSignallerTest.this.timeouts
    protected override val loggerFactory = LocalEventSignallerTest.this.loggerFactory
    implicit val actorSystem: ActorSystem =
      PekkoUtil.createActorSystem(loggerFactory.threadName)(parallelExecutionContext)

    val nextTimestampSecond = new AtomicLong(0L)
    def generateTimestamp(): CantonTimestamp =
      CantonTimestamp.ofEpochSecond(nextTimestampSecond.getAndIncrement())
    // generate a few upfront
    val ts0 = generateTimestamp()
    val ts1 = generateTimestamp()
    val ts2 = generateTimestamp()

    val signaller = new LocalEventSignaller[Int, Int]("member", timeouts, loggerFactory)

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

    (0 until 3001).toList.foreach(i =>
      signaller.notify(Notification.Keys(NonEmpty(Set, aliceId)), i)
    )
    for {
      // none of the previously produced signals are retained for future subscriptions
      aliceSignals <- PekkoUtil.runSupervised(
        signaller
          .readSignals(aliceId, alice.toString)
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
          .map(_ => signaller.notify(Notification.Keys(NonEmpty(Set, aliceId, bobId)), 1))
          .toMat(Sink.ignore)(Keep.right),
        errorLogMessagePrefix = "notifier",
      )

    val consumerKillSwitch = KillSwitches.shared("end-of-signal")
    // alice is a slow consumer
    val aliceF = PekkoUtil.runSupervised(
      signaller
        .readSignals(aliceId, alice.toString)
        .throttle(1, 1.second)
        .viaMat(consumerKillSwitch.flow)(Keep.left)
        .toMat(Sink.seq)(Keep.right),
      errorLogMessagePrefix = "alice consumer",
    )

    // bob consumes and never backpressures
    val bobF =
      PekkoUtil.runSupervised(
        signaller
          .readSignals(bobId, bob.toString)
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
