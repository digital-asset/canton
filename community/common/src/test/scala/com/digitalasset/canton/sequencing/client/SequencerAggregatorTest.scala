// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.SequencerAggregator.SequencerAggregatorError
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
        .futureValue shouldBe Right(true)

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
          .futureValue shouldBe Right(true)
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
          .futureValue shouldBe Right(true)
      }

      val blockingEvent = createEvent(timestamp = CantonTimestamp.Epoch.plusSeconds(3L)).futureValue

      val p = Promise[Future[Either[SequencerAggregatorError, Boolean]]]()
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
          expectedSequencers = NonEmpty.mk(Set, sequencerId, SecondSequencerId),
          expectedSequencersSize = 2,
        )

        val combinedMessage = aggregator.combine(NonEmpty(Seq, event1, event2)).value

        aggregator.eventQueue.size() shouldBe 0

        val f1 = aggregator
          .combineAndMergeEvent(sequencerId, event1)

        f1.isCompleted shouldBe false
        aggregator.eventQueue.size() shouldBe 0

        val f2 = aggregator
          .combineAndMergeEvent(SecondSequencerId, event2)

        f2.futureValue.discard

        f1.isCompleted shouldBe true
        f2.isCompleted shouldBe true

        aggregator.eventQueue.size() shouldBe 1
        aggregator.eventQueue.take() shouldBe combinedMessage
        f1.futureValue shouldBe Right(false)
        f2.futureValue shouldBe Right(true)
    }

    "fail if events share timestamp but timestampOfSigningKey is different" in { fixture =>
      import fixture.*
      val event1 = createEvent(timestampOfSigningKey = Some(CantonTimestamp.Epoch)).futureValue
      val event2 = createEvent(timestampOfSigningKey = None).futureValue

      val aggregator = mkAggregator(
        expectedSequencers = NonEmpty.mk(Set, sequencerId, SecondSequencerId),
        expectedSequencersSize = 2,
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
        expectedSequencers = NonEmpty.mk(Set, sequencerId, SecondSequencerId),
        expectedSequencersSize = 2,
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

    "emit events in order when all sequencers confirmed" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      fixture =>
        import fixture.*
        val events = (1 to 2).map(s =>
          createEvent(timestamp = CantonTimestamp.Epoch.plusSeconds(s.toLong)).futureValue
        )
        val aggregator = mkAggregator(
          expectedSequencers = NonEmpty.mk(Set, sequencerId, SecondSequencerId),
          expectedSequencersSize = 2,
        )

        val futures = events.map { event =>
          val f = aggregator.combineAndMergeEvent(sequencerId, event)
          f.isCompleted shouldBe false
          f
        }

        aggregator
          .combineAndMergeEvent(SecondSequencerId, events(0))
          .futureValue shouldBe Right(true)

        futures(0).futureValue shouldBe Right(false)
        futures(1).isCompleted shouldBe false

        aggregator
          .combineAndMergeEvent(SecondSequencerId, events(1))
          .futureValue shouldBe Right(true)

        futures(1).futureValue shouldBe Right(false)
    }
  }

  "Sequencer aggregator with two out of 3 expected sequencers" should {
    // TODO(#12373) Adapt PV when releasing BFT
    "pass-through the combined event only if both sequencers emitted it" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      fixture =>
        import fixture.*
        val event1 = createEvent().futureValue
        val event2 = createEvent().futureValue
        val event3 = createEvent().futureValue

        val aggregator = mkAggregator(
          expectedSequencers = NonEmpty.mk(Set, sequencerId, SecondSequencerId, ThirdSequencerId),
          expectedSequencersSize = 2,
        )

        val combinedMessage = aggregator.combine(NonEmpty(Seq, event1, event2)).value

        aggregator.eventQueue.size() shouldBe 0

        val f1 = aggregator
          .combineAndMergeEvent(sequencerId, event1)

        f1.isCompleted shouldBe false
        aggregator.eventQueue.size() shouldBe 0

        val f2 = aggregator
          .combineAndMergeEvent(SecondSequencerId, event2)

        f2.futureValue.discard

        f1.isCompleted shouldBe true
        f2.isCompleted shouldBe true

        aggregator.eventQueue.size() shouldBe 1
        aggregator.eventQueue.take() shouldBe combinedMessage
        f1.futureValue shouldBe Right(false)
        f2.futureValue shouldBe Right(true)

        val f3 = aggregator
          .combineAndMergeEvent(ThirdSequencerId, event3) // late event
        f3.isCompleted shouldBe true // should be immediately resolved
        f3.futureValue shouldBe Right(false)
    }

    "recover after skipping an event" onlyRunWithOrGreaterThan ProtocolVersion.dev in { fixture =>
      import fixture.*

      val aliceEvents = (1 to 3).map(s =>
        createEvent(
          timestamp = CantonTimestamp.Epoch.plusSeconds(s.toLong),
          signatureOverride = Some(signatureAlice),
        ).futureValue
      )
      val bobEvents = (1 to 3).map(s =>
        createEvent(
          timestamp = CantonTimestamp.Epoch.plusSeconds(s.toLong),
          signatureOverride = Some(signatureBob),
        ).futureValue
      )
      val carlosEvents = (1 to 3).map(s =>
        createEvent(
          timestamp = CantonTimestamp.Epoch.plusSeconds(s.toLong),
          signatureOverride = Some(signatureCarlos),
        ).futureValue
      )

      val aggregator = mkAggregator(
        expectedSequencers = NonEmpty.mk(Set, sequencerId, SecondSequencerId, ThirdSequencerId),
        expectedSequencersSize = 2,
      )

      val combinedMessage = aggregator.combine(NonEmpty(Seq, aliceEvents(0), bobEvents(0))).value

      aggregator.eventQueue.size() shouldBe 0

      aggregator
        .combineAndMergeEvent(sequencerId, aliceEvents(0))
        .discard
      aggregator
        .combineAndMergeEvent(SecondSequencerId, bobEvents(0))
        .discard
      aggregator.eventQueue.size() shouldBe 1
      aggregator.eventQueue.take() shouldBe combinedMessage
      aggregator
        .combineAndMergeEvent(ThirdSequencerId, carlosEvents(0))
        .discard // late event

      val combinedMessage2 =
        aggregator.combine(NonEmpty(Seq, aliceEvents(1), carlosEvents(1))).value
      aggregator
        .combineAndMergeEvent(sequencerId, aliceEvents(1))
        .discard
      aggregator
        .combineAndMergeEvent(ThirdSequencerId, carlosEvents(1))
        .discard

      aggregator.eventQueue.size() shouldBe 1
      aggregator.eventQueue.take() shouldBe combinedMessage2
    }
  }

}
