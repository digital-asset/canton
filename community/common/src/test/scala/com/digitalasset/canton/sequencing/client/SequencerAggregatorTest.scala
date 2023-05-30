// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.SequencerAggregator.SequencerAggregatorError
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  ProtocolVersionChecksFixtureAnyWordSpec,
}
import com.google.protobuf.ByteString
import org.scalatest.Outcome
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.wordspec.FixtureAnyWordSpec

import scala.concurrent.{Future, Promise}

class SequencerAggregatorTest
    extends FixtureAnyWordSpec
    with BaseTest
    with ScalaFutures
    with ProtocolVersionChecksFixtureAnyWordSpec
    with HasExecutionContext {

  override type FixtureParam = SequencedEventTestFixture

  override def withFixture(test: OneArgTest): Outcome = {
    val env = new SequencedEventTestFixture(loggerFactory, testedProtocolVersion, timeouts)
    withFixture(test.toNoArgTest(env))
  }

  "Single sequencer aggregator" should {
    "pass-through the event" in { fixture =>
      import fixture.*
      val event = createEvent().futureValue

      val aggregator = mkAggregator()

      aggregator.eventQueue.size() shouldBe 0

      aggregator
        .combineAndMergeEvent(sequencerId, event)
        .futureValue shouldBe Right(sequencerId)

      aggregator.eventQueue.take() shouldBe event
    }

    "pass-through events in sequence" in { fixture =>
      import fixture.*
      val events = (1 to 100).map(s =>
        createEvent(timestamp = CantonTimestamp.Epoch.plusSeconds(s.toLong)).futureValue
      )

      val aggregator = mkAggregator()

      events.foreach { event =>
        aggregator
          .combineAndMergeEvent(sequencerId, event)
          .futureValue shouldBe Right(sequencerId)
        aggregator.eventQueue.take() shouldBe event
      }
    }

    "block on queue is full" in { fixture =>
      import fixture.*
      val events = (1 to 2).map(s =>
        createEvent(timestamp = CantonTimestamp.Epoch.plusSeconds(s.toLong)).futureValue
      )

      val aggregator = mkAggregator()

      aggregator.eventQueue.size() shouldBe 0

      events.foreach { event =>
        aggregator
          .combineAndMergeEvent(sequencerId, event)
          .futureValue shouldBe Right(sequencerId)
      }

      val blockingEvent = createEvent(timestamp = CantonTimestamp.Epoch.plusSeconds(3L)).futureValue

      val p = Promise[Future[Either[SequencerAggregatorError, SequencerId]]]()
      p.completeWith(
        Future(
          aggregator
            .combineAndMergeEvent(sequencerId, blockingEvent)
        )
      )
      always() {
        p.isCompleted shouldBe false
      }
      aggregator.eventQueue.take() shouldBe events(0)
      eventually() {
        p.isCompleted shouldBe true
      }
    }
  }

  "Sequencer aggregator with two expected sequencers" should {
    // TODO(#12373) Adapt PV when releasing BFT
    "pass-through the combined event only if both sequencers emitted it" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      fixture =>
        import fixture.*
        val event1 = createEvent().futureValue
        val event2 = createEvent().futureValue

        val aggregator = mkAggregator(
          expectedSequencers = NonEmpty.mk(Set, sequencerId, SecondSequencerId)
        )

        val combinedMessage = aggregator.combine(NonEmpty(Seq, event1, event2)).value

        aggregator.eventQueue.size() shouldBe 0

        val f1 = aggregator
          .combineAndMergeEvent(sequencerId, event1)

        f1.isCompleted shouldBe false
        aggregator.eventQueue.size() shouldBe 0

        val f2 = aggregator
          .combineAndMergeEvent(SecondSequencerId, event2)

        f1.isCompleted shouldBe true
        f2.isCompleted shouldBe true

        aggregator.eventQueue.size() shouldBe 1
        aggregator.eventQueue.take() shouldBe combinedMessage
        f1.futureValue shouldBe Right(SecondSequencerId)
        f2.futureValue shouldBe Right(SecondSequencerId)
    }

    "fail if events share timestamp but timestampOfSigningKey is different" in { fixture =>
      import fixture.*
      val event1 = createEvent(timestampOfSigningKey = Some(CantonTimestamp.Epoch)).futureValue
      val event2 = createEvent(timestampOfSigningKey = None).futureValue

      val aggregator = mkAggregator(
        expectedSequencers = NonEmpty.mk(Set, sequencerId, SecondSequencerId)
      )
      val f1 = aggregator
        .combineAndMergeEvent(sequencerId, event1)

      f1.isCompleted shouldBe false
      aggregator.eventQueue.size() shouldBe 0

      aggregator
        .combineAndMergeEvent(SecondSequencerId, event2)
        .futureValue shouldBe Left(
        SequencerAggregatorError.NotTheSameTimestampOfSigningKey(
          NonEmpty(Set, Some(CantonTimestamp.Epoch), None)
        )
      )
    }

    "fail if events share timestamp but content is different" in { fixture =>
      import fixture.*
      val event1 = createEvent().futureValue
      val event2 = createEvent(serializedOverride = Some(ByteString.EMPTY)).futureValue

      val aggregator = mkAggregator(
        expectedSequencers = NonEmpty.mk(Set, sequencerId, SecondSequencerId)
      )
      val f1 = aggregator
        .combineAndMergeEvent(sequencerId, event1)

      f1.isCompleted shouldBe false
      aggregator.eventQueue.size() shouldBe 0

      val hashes = NonEmpty(
        Set,
        hash(event1.signedEvent.content.toByteString),
        hash(event2.signedEvent.content.toByteString),
      )

      aggregator
        .combineAndMergeEvent(SecondSequencerId, event2)
        .futureValue shouldBe Left(SequencerAggregatorError.NotTheSameContentHash(hashes))
    }

    "emit events in order when all sequencers confirmed" in { fixture =>
      import fixture.*
      val events = (1 to 2).map(s =>
        createEvent(timestamp = CantonTimestamp.Epoch.plusSeconds(s.toLong)).futureValue
      )
      val aggregator = mkAggregator(
        expectedSequencers = NonEmpty.mk(Set, sequencerId, SecondSequencerId)
      )

      val futures = events.map { event =>
        val f = aggregator.combineAndMergeEvent(sequencerId, event)
        f.isCompleted shouldBe false
        f
      }

      aggregator
        .combineAndMergeEvent(SecondSequencerId, events(0))
        .futureValue shouldBe Right(SecondSequencerId)

      futures(0).futureValue shouldBe Right(SecondSequencerId)
      futures(1).isCompleted shouldBe false

      aggregator
        .combineAndMergeEvent(SecondSequencerId, events(1))
        .futureValue shouldBe Right(SecondSequencerId)

      futures(1).futureValue shouldBe Right(SecondSequencerId)
    }
  }

}
