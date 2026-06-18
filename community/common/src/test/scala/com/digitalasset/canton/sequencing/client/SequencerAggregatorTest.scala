// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.syntax.either.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.sequencing.SequencerAggregator.SequencerAggregatorError
import com.digitalasset.canton.sequencing.SequencerAggregator.SequencerAggregatorError.BogusEvent
import com.digitalasset.canton.sequencing.{
  SequencedSerializedEvent,
  SequencerAggregator,
  SequencerAggregatorTesting,
}
import com.digitalasset.canton.store.SequencedEventStore.SequencedEventWithTraceContext
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.util.{DelayUtil, MonadUtil, ResourceUtil}
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  TestPredicateFiltersFixtureAnyWordSpec,
}
import com.google.protobuf.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.wordspec.FixtureAnyWordSpec
import org.scalatest.{Assertion, Outcome}

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.tailrec
import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}
import scala.util.Random

class SequencerAggregatorTest
    extends FixtureAnyWordSpec
    with BaseTest
    with ScalaFutures
    with TestPredicateFiltersFixtureAnyWordSpec
    with HasExecutionContext {
  import SequencerAggregatorTest.*

  private val useNewAggregator = SequencerAggregatorTesting.useNewAggregatorForTests

  private lazy val rand: Random = {
    val seed = Random.nextLong()
    logger.debug(s"Seed for tests = $seed")
    new Random(seed)
  }

  override type FixtureParam = SequencedEventTestFixture

  override def withFixture(test: OneArgTest): Outcome =
    ResourceUtil.withResource(
      new SequencedEventTestFixture(
        loggerFactory,
        testedProtocolVersion,
        timeouts,
        futureSupervisor,
      )
    )(env => withFixture(test.toNoArgTest(env)))

  "Single sequencer aggregator" should {
    "pass-through the event" in { fixture =>
      import fixture.*
      val event = createEvent().futureValueUS

      val aggregator = mkAggregator(useNewAggregator = useNewAggregator)

      assertNoMessageDownstream(aggregator)

      aggregator
        .combineAndMergeEvent(sequencerAlice, event)
        .futureValueUS shouldBe Either.unit

      aggregator.eventQueue.take() shouldBe event
    }

    "pass-through events in sequence" in { fixture =>
      import fixture.*
      val events = (1 to 100).map(s =>
        createEvent(timestamp = CantonTimestamp.Epoch.plusSeconds(s.toLong)).futureValueUS
      )

      val aggregator = mkAggregator(useNewAggregator = useNewAggregator)

      events.foreach { event =>
        aggregator
          .combineAndMergeEvent(sequencerAlice, event)
          .futureValueUS shouldBe Either.unit
        aggregator.eventQueue.take() shouldBe event
      }
    }

    "block on queue is full" in { fixture =>
      import fixture.*
      val events = (1 to 2).map(s =>
        createEvent(timestamp = CantonTimestamp.Epoch.plusSeconds(s.toLong)).futureValueUS
      )

      val aggregator = mkAggregator(useNewAggregator = useNewAggregator)

      assertNoMessageDownstream(aggregator)

      events.foreach { event =>
        aggregator
          .combineAndMergeEvent(sequencerAlice, event)
          .futureValueUS shouldBe Either.unit
      }

      val blockingEvent =
        createEvent(timestamp = CantonTimestamp.Epoch.plusSeconds(3L)).futureValueUS

      val p = Promise[Future[Either[SequencerAggregatorError, Unit]]]()
      p.completeWith(
        Future(
          aggregator
            .combineAndMergeEvent(sequencerAlice, blockingEvent)
            .failOnShutdown
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

    "support reconfiguration to 2 out of 3" in { fixture =>
      import fixture.*

      val aggregator = mkAggregator(useNewAggregator = useNewAggregator)

      assertNoMessageDownstream(aggregator)

      aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(0))
        .futureValueUS shouldBe Either.unit

      assertDownstreamMessage(aggregator, aliceEvents(0))

      aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(1))
        .futureValueUS shouldBe Either.unit

      assertDownstreamMessage(aggregator, aliceEvents(1))

      aggregator.changeMessageAggregationConfig(config(2))

      val f1 = aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(2))

      f1.isCompleted shouldBe false

      val f2 = aggregator
        .combineAndMergeEvent(sequencerBob, bobEvents(2))
      f2.futureValueUS shouldBe Either.unit
      f1.futureValueUS shouldBe Either.unit

      aggregator.eventQueue.size() shouldBe 1
      assertCombinedDownstreamMessage(aggregator, aliceEvents(2), bobEvents(2))
    }

    "support reconfiguration to another sequencer" in { fixture =>
      import fixture.*

      val aggregator = mkAggregator(useNewAggregator = useNewAggregator, config(1))

      assertNoMessageDownstream(aggregator)

      aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(0))
        .futureValueUS shouldBe Either.unit

      assertDownstreamMessage(aggregator, aliceEvents(0))

      aggregator.changeMessageAggregationConfig(config(1))

      aggregator
        .combineAndMergeEvent(sequencerBob, bobEvents(0)) // arrived late event which we ignore
        .futureValueUS shouldBe Either.unit
      assertNoMessageDownstream(aggregator)

      aggregator
        .combineAndMergeEvent(sequencerBob, bobEvents(1))
        .futureValueUS shouldBe Either.unit
      assertDownstreamMessage(aggregator, bobEvents(1))
    }
  }

  "Sequencer aggregator with two expected sequencers" should {
    "pass-through the combined event only if both sequencers emitted it" in { fixture =>
      import fixture.*
      val event1 = createEvent().futureValueUS
      val event2 = createEvent().futureValueUS

      val aggregator =
        mkAggregator(useNewAggregator = useNewAggregator, config(sequencerTrustThreshold = 2))

      assertNoMessageDownstream(aggregator)

      val f1 = aggregator
        .combineAndMergeEvent(sequencerAlice, event1)

      f1.isCompleted shouldBe false
      assertNoMessageDownstream(aggregator)

      val f2 = aggregator
        .combineAndMergeEvent(sequencerBob, event2)

      f1.futureValueUS.discard
      f2.futureValueUS.discard

      f1.isCompleted shouldBe true
      f2.isCompleted shouldBe true

      assertCombinedDownstreamMessage(aggregator, event1, event2)
      f1.futureValueUS shouldBe Either.unit
      f2.futureValueUS shouldBe Either.unit
    }

    "fail if events share timestamp but content is different" in { fixture =>
      import fixture.*
      val event1 = createEvent().futureValueUS
      val event2 = createEvent(serializedOverride = Some(ByteString.EMPTY)).futureValueUS

      val aggregator =
        mkAggregator(useNewAggregator = useNewAggregator, config(sequencerTrustThreshold = 2))
      val f1 = aggregator
        .combineAndMergeEvent(sequencerAlice, event1)

      f1.isCompleted shouldBe false
      assertNoMessageDownstream(aggregator)

      if (useNewAggregator) {
        val expectedError =
          SequencerAggregatorError.ThresholdUnreachable(
            decided = 2,
            undecided = 0,
            largestGroup = 1,
            threshold = 2,
          )

        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          aggregator
            .combineAndMergeEvent(sequencerBob, event2)
            .futureValueUS shouldBe Left(expectedError),
          LogEntry.assertLogSeq(
            Seq(
              (
                _.warningMessage should include(expectedError.toString),
                "Threshold unreachable",
              )
            )
          ),
        )
      } else {
        val hashes = NonEmpty(
          Set,
          hash(event1.signedEvent.content.toByteString),
          hash(event2.signedEvent.content.toByteString),
        )

        val expectedError = SequencerAggregatorError.NotTheSameContentHash(hashes)
        aggregator
          .combineAndMergeEvent(sequencerBob, event2)
          .futureValueUS shouldBe Left(expectedError)
      }

    }

    "emit events in order when all sequencers confirmed" in { fixture =>
      import fixture.*

      val aggregator =
        mkAggregator(
          useNewAggregator = useNewAggregator,
          config(sequencerTrustThreshold = 2, overrideMaxNbOfContributionsO = Some(3)),
        )

      if (useNewAggregator) {
        val nbMessages = 10

        val events = (0 until nbMessages)
          .map(s =>
            createEvent(timestamp = CantonTimestamp.Epoch.plusSeconds(s.toLong)).futureValueUS
          )

        val ScenarioResults(Seq(resultsAlice, resultsBob, resultsCarlos), output) = runScenario(
          aggregator,
          Seq((sequencerAlice, events), (sequencerBob, events), (sequencerCarlos, events)),
        ): @unchecked

        forAll(Seq(resultsAlice, resultsBob, resultsCarlos))(
          _ shouldBe Seq.fill(nbMessages)(Either.unit)
        )
        output.size shouldBe nbMessages
        forAll(events.zip(output)) { case (event, output) =>
          output shouldBe combinedMessage(aggregator, event, event)
        }
      } else {
        val events = (1 to 2).map(s =>
          createEvent(timestamp = CantonTimestamp.Epoch.plusSeconds(s.toLong)).futureValueUS
        )

        val futures = events.map { event =>
          val f = aggregator.combineAndMergeEvent(sequencerAlice, event)
          f.isCompleted shouldBe false
          f
        }

        aggregator
          .combineAndMergeEvent(sequencerBob, events(0))
          .futureValueUS shouldBe Either.unit

        futures(0).futureValueUS shouldBe Either.unit
        futures(1).isCompleted shouldBe false

        aggregator
          .combineAndMergeEvent(sequencerBob, events(1))
          .futureValueUS shouldBe Either.unit

        futures(1).futureValueUS shouldBe Either.unit
      }
    }

    "ignore duplicate contributions from the same sequencer" in { fixture =>
      import fixture.*

      if (useNewAggregator) {
        val aggregator =
          mkAggregator(
            useNewAggregator = useNewAggregator,
            config(sequencerTrustThreshold = 2, overrideMaxNbOfContributionsO = Some(3)),
          )

        val event1 = createEvent().futureValueUS
        val event2 = createEvent(serializedOverride = Some(ByteString.EMPTY)).futureValueUS

        val alice1F = aggregator.combineAndMergeEvent(sequencerAlice, event1)
        // Duplicate submissions with the same event...
        val alice2F = aggregator.combineAndMergeEvent(sequencerAlice, event1)
        // ... or with a different event...
        val alice3F = aggregator.combineAndMergeEvent(sequencerAlice, event2)
        // are ignored
        eventuallyForever(durationOfSuccess = 1.second) {
          forAll(Seq(alice1F, alice2F, alice3F))(_.isCompleted shouldBe false)
        }

        assertNoMessageDownstream(aggregator)

        val bobF = aggregator.combineAndMergeEvent(sequencerBob, event1)

        // All submissions are successful, because Alice submitted a correct event before the bad event, which got ignored
        forAll(Seq(alice1F, alice2F, alice3F, bobF)) { fut =>
          fut.futureValueUS shouldBe Either.unit
        }

        assertCombinedDownstreamMessage(aggregator, event1, event1)
      } else succeed
    }

    "properly error in case of a reconfiguration that brings the threshold too low" in { fixture =>
      import fixture.*

      // This is the same situation as if the threshold had been set too low in the first place

      if (useNewAggregator) {
        val aggregator =
          mkAggregator(
            useNewAggregator = useNewAggregator,
            config(sequencerTrustThreshold = 2, overrideMaxNbOfContributionsO = Some(3)),
          )

        val event1 = createEvent().futureValueUS
        val event2 = createEvent(serializedOverride =
          Some(ByteString.copyFrom(rand.nextBytes(5))) // Random to ensure non-determinism
        ).futureValueUS

        val aliceF = aggregator.combineAndMergeEvent(sequencerAlice, event1)
        val bobF = aggregator.combineAndMergeEvent(sequencerBob, event2)
        eventuallyForever(durationOfSuccess = 1.second) {
          forAll(Seq(aliceF, bobF))(_.isCompleted shouldBe false)
        }

        aggregator.changeMessageAggregationConfig(
          config(sequencerTrustThreshold = 1, overrideMaxNbOfContributionsO = Some(3))
        )

        aliceF.futureValueUS match {
          case Right(()) =>
            // Alice won
            bobF.futureValueUS shouldBe Left(
              SequencerAggregatorError.BogusEvent(sequencerBob, event2)
            )
            assertCombinedDownstreamMessage(aggregator, event1)
          case _ =>
            // Bob won
            aliceF.futureValueUS shouldBe Left(
              SequencerAggregatorError.BogusEvent(sequencerAlice, event1)
            )
            bobF.futureValueUS shouldBe Either.unit
            assertCombinedDownstreamMessage(aggregator, event2)
        }
      } else succeed
    }

    "support reconfiguration to another sequencer" in { fixture =>
      import fixture.*

      val aggregator =
        mkAggregator(useNewAggregator = useNewAggregator, config(sequencerTrustThreshold = 2))

      assertNoMessageDownstream(aggregator)

      aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(0))
        .discard

      aggregator
        .combineAndMergeEvent(sequencerBob, bobEvents(0))
        .discard

      assertCombinedDownstreamMessage(aggregator, aliceEvents(0), bobEvents(0))

      aggregator.changeMessageAggregationConfig(config(1))

      aggregator
        .combineAndMergeEvent(sequencerCarlos, carlosEvents(1))
        .futureValueUS shouldBe Either.unit

      assertDownstreamMessage(aggregator, carlosEvents(1))
    }

    "emit AbortedDueToShutdown for a pending aggregation" in { fixture =>
      import fixture.*
      val aggregator =
        mkAggregator(useNewAggregator = useNewAggregator, config(sequencerTrustThreshold = 2))

      assertNoMessageDownstream(aggregator)

      val incompleteF = aggregator.combineAndMergeEvent(sequencerAlice, aliceEvents(0))

      eventuallyForever(timeUntilSuccess = 0.seconds, durationOfSuccess = 500.millis) {
        incompleteF.isCompleted shouldBe false
      }
      aggregator.close()
      eventually() {
        incompleteF.unwrap.futureValue shouldBe AbortedDueToShutdown
      }
    }

    "emit AbortedDueToShutdown when observing events after having been closed" in { fixture =>
      import fixture.*
      val aggregator =
        mkAggregator(useNewAggregator = useNewAggregator, config(sequencerTrustThreshold = 2))

      assertNoMessageDownstream(aggregator)

      val incompleteAliceF = aggregator.combineAndMergeEvent(sequencerAlice, aliceEvents(0))

      eventuallyForever(timeUntilSuccess = 0.seconds, durationOfSuccess = 500.millis) {
        incompleteAliceF.isCompleted shouldBe false
      }
      aggregator.close()

      eventually() {
        incompleteAliceF.unwrap.futureValue shouldBe AbortedDueToShutdown
      }

      val incompleteBobF = aggregator.combineAndMergeEvent(sequencerBob, bobEvents(0))
      eventually() {
        incompleteBobF.unwrap.futureValue shouldBe AbortedDueToShutdown
      }
    }

  }

  "Sequencer aggregator with two out of 3 expected sequencers" should {
    "pass-through the combined event only if both sequencers emitted it" in { fixture =>
      import fixture.*

      val aggregator =
        mkAggregator(useNewAggregator = useNewAggregator, config(sequencerTrustThreshold = 2))

      assertNoMessageDownstream(aggregator)

      val f1 = aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(0))

      f1.isCompleted shouldBe false
      assertNoMessageDownstream(aggregator)

      val f2 = aggregator
        .combineAndMergeEvent(sequencerBob, bobEvents(0))

      eventually() {
        f1.isCompleted shouldBe true
        f2.isCompleted shouldBe true
      }

      assertCombinedDownstreamMessage(
        aggregator,
        aliceEvents(0),
        bobEvents(0),
      )
      f1.futureValueUS shouldBe Either.unit
      f2.futureValueUS shouldBe Either.unit

      val f3 = aggregator
        .combineAndMergeEvent(sequencerCarlos, carlosEvents(0)) // late event
      f3.futureValueUS shouldBe Either.unit
    }

    "recover after skipping an event" in { fixture =>
      import fixture.*

      val aggregator =
        mkAggregator(useNewAggregator = useNewAggregator, config(sequencerTrustThreshold = 2))

      assertNoMessageDownstream(aggregator)

      aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(0))
        .discard
      aggregator
        .combineAndMergeEvent(sequencerBob, bobEvents(0))
        .discard

      assertCombinedDownstreamMessage(aggregator, aliceEvents(0), bobEvents(0))

      aggregator
        .combineAndMergeEvent(sequencerCarlos, carlosEvents(0))
        .discard // late event

      aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(1))
        .discard
      aggregator
        .combineAndMergeEvent(sequencerCarlos, carlosEvents(1))
        .discard

      assertCombinedDownstreamMessage(
        aggregator,
        aliceEvents(1),
        carlosEvents(1),
      )
    }

    "support reconfiguration to 1 out of 3" in { fixture =>
      import fixture.*

      val aggregator =
        mkAggregator(useNewAggregator = useNewAggregator, config(sequencerTrustThreshold = 2))

      assertNoMessageDownstream(aggregator)

      aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(0))
        .discard
      aggregator
        .combineAndMergeEvent(sequencerBob, bobEvents(0))
        .discard

      assertCombinedDownstreamMessage(
        aggregator,
        aliceEvents(0),
        bobEvents(0),
      )

      aggregator
        .combineAndMergeEvent(sequencerCarlos, carlosEvents(0))
        .discard // late event

      aggregator.changeMessageAggregationConfig(config(1))
      aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(1))
        .discard

      assertDownstreamMessage(aggregator, aliceEvents(1))
    }

    "support reconfiguration to 1 out of 3 while incomplete consensus" in { fixture =>
      import fixture.*

      val aggregator =
        mkAggregator(useNewAggregator = useNewAggregator, config(sequencerTrustThreshold = 2))
      assertNoMessageDownstream(aggregator)

      val f = aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(0))
      f.isCompleted shouldBe false

      assertNoMessageDownstream(aggregator)

      aggregator.changeMessageAggregationConfig(config(sequencerTrustThreshold = 1))

      // consensus requirement is changed which is enough to push the message out

      assertDownstreamMessage(aggregator, aliceEvents(0))
      f.futureValueUS shouldBe Either.unit
    }
  }

  "Sequencer aggregator with 3 out of 3 expected sequencers" should {
    "support reconfiguration to 1 out of 3 while overfulfilled consensus" in { fixture =>
      import fixture.*

      val aggregator =
        mkAggregator(useNewAggregator = useNewAggregator, config(sequencerTrustThreshold = 3))
      assertNoMessageDownstream(aggregator)

      val f1 = aggregator
        .combineAndMergeEvent(sequencerAlice, aliceEvents(0))
      f1.isCompleted shouldBe false

      val f2 = aggregator
        .combineAndMergeEvent(sequencerBob, bobEvents(0))
      f2.isCompleted shouldBe false

      assertNoMessageDownstream(aggregator)

      aggregator.changeMessageAggregationConfig(config(sequencerTrustThreshold = 1))

      // consensus requirement is changed which more than enough (2 are there, 1 is required) to push the message out
      // we do accumulate all signatures still as sequencers are still expected ones
      assertCombinedDownstreamMessage(aggregator, aliceEvents(0), bobEvents(0))
    }
  }

  "BFT aggregator" should {
    "return an error to the sequencer that provides a bad event" in { fixture =>
      import fixture.*

      if (useNewAggregator) {
        val aggregator =
          mkAggregator(useNewAggregator = true, config(2, overrideMaxNbOfContributionsO = Some(3)))

        assertNoMessageDownstream(aggregator)

        val badEvent = createEvent(
          timestamp = bobEvents(0).timestamp,
          signatureOverride = Some(bobEvents(0).signedEvent.signature),
          serializedOverride = Some(bobEvents(0).signedEvent.content.toByteString.substring(1)),
        ).failOnShutdown("create bad event").futureValue

        val aliceF = aggregator.combineAndMergeEvent(sequencerAlice, aliceEvents(0))
        aliceF.isCompleted shouldBe false

        val bobF = aggregator.combineAndMergeEvent(sequencerBob, badEvent)
        bobF.isCompleted shouldBe false

        val carlosF = aggregator.combineAndMergeEvent(sequencerCarlos, carlosEvents(0))

        // Alice and Carlos receive a positive result, Bob receives an error
        aliceF.futureValueUS shouldBe Either.unit
        bobF.futureValueUS shouldBe Left(BogusEvent(sequencerBob, badEvent))
        carlosF.futureValueUS shouldBe Either.unit

        assertCombinedDownstreamMessage(aggregator, aliceEvents(0), carlosEvents(0))
      } else succeed
    }

    "handle provided messages that have already been decided" in { fixture =>
      import fixture.*

      if (useNewAggregator) {
        val aggregator =
          mkAggregator(useNewAggregator = true, pastEventCacheSize = PositiveInt.two)

        assertNoMessageDownstream(aggregator)

        // Events are processed normally
        (0 to 2).foreach { index =>
          aggregator
            .combineAndMergeEvent(sequencerAlice, aliceEvents(index))
            .futureValueUS shouldBe Either.unit
          assertCombinedDownstreamMessage(aggregator, aliceEvents(index))
        }

        // Providing an old valid event succeeds...
        aggregator
          .combineAndMergeEvent(sequencerAlice, aliceEvents(2))
          .futureValueUS shouldBe Either.unit
        // ... and does not generate a new event downstream
        assertNoMessageDownstream(aggregator)

        // Providing an old valid event fails if it is no longer in the past event cache...
        aggregator
          .combineAndMergeEvent(sequencerAlice, aliceEvents(0))
          .futureValueUS shouldBe Left(
          SequencerAggregatorError.BogusEvent(sequencerAlice, aliceEvents(0))
        )
        // ... and does not generate a new event downstream
        assertNoMessageDownstream(aggregator)

        // Providing an old event with invalid hash...
        val eventWithBadHash = createEvent(
          timestamp = aliceEvents(2).timestamp,
          signatureOverride = Some(aliceEvents(2).signedEvent.signature),
          serializedOverride = Some(aliceEvents(2).signedEvent.content.toByteString.substring(1)),
        ).failOnShutdown("create bad event").futureValue

        // ... fails...
        aggregator
          .combineAndMergeEvent(sequencerAlice, eventWithBadHash)
          .futureValueUS shouldBe Left(
          SequencerAggregatorError.BogusEvent(sequencerAlice, eventWithBadHash)
        )
        // ... and does not generate a new event downstream
        assertNoMessageDownstream(aggregator)

        // Providing an old event with invalid timestamp...
        val eventWithBadTimestamp = createEvent(
          timestamp = aliceEvents(0).timestamp.plusMillis(666),
          signatureOverride = Some(aliceEvents(2).signedEvent.signature),
        ).failOnShutdown("create bad event").futureValue

        // ... fails...
        aggregator
          .combineAndMergeEvent(sequencerAlice, eventWithBadTimestamp)
          .futureValueUS shouldBe Left(
          SequencerAggregatorError.BogusEvent(sequencerAlice, eventWithBadTimestamp)
        )
        // ... and does not generate a new event downstream
        assertNoMessageDownstream(aggregator)

      } else succeed
    }

    "tolerate bogus messages" in { fixture =>
      import fixture.*

      /* Scenario: trust threshold = 2
       *
       * Alice:  E0_t0, E1_t1, E2_t2, E3_t3
       * Bob:    E0_t0, E1_t1, E2_t2, E3_t3
       * Carlos: E0_t0, X_t1,  E2_t2, E3_t3
       */

      if (useNewAggregator) {
        val nbEvents = 4
        val aggregator =
          mkAggregator(
            useNewAggregator = useNewAggregator,
            config(sequencerTrustThreshold = 2, overrideMaxNbOfContributionsO = Some(3)),
          )

        val events = mkEvents(fixture, nbEvents)

        val badEvent = createEvent(
          timestamp = events(1).timestamp,
          serializedOverride = Some(ByteString.EMPTY),
        ).futureValueUS
        val eventsCarlos = (events.take(1) :+ badEvent) ++ events.drop(2)

        val ScenarioResults(Seq(resultsAlice, resultsBob, resultsCarlos), output) = runScenario(
          aggregator,
          Seq((sequencerAlice, events), (sequencerBob, events), (sequencerCarlos, eventsCarlos)),
        ): @unchecked

        forAll(Seq(resultsAlice, resultsBob))(_ shouldBe Seq.fill(nbEvents)(Either.unit))

        resultsCarlos shouldBe Seq(
          Either.unit,
          Left(SequencerAggregatorError.BogusEvent(sequencerCarlos, eventsCarlos(1))),
          Either.unit,
          Either.unit,
        )

        output.size shouldBe nbEvents
        forAll(events.zip(output)) { case (event, output) =>
          output shouldBe combinedMessage(aggregator, event, event)
        }
      } else succeed
    }

    "tolerate skipped messages" in { fixture =>
      import fixture.*

      /* Scenario: trust threshold = 2
       *
       * Alice:  E0_t0, E1_t1, E2_t2, E3_t3
       * Bob:    E0_t0, E1_t1, E2_t2, E3_t3
       * Carlos: E0_t0,        X_t2,  E3_t3     (event at t2 is necessarily bad because last_timestamp is different)
       */

      if (useNewAggregator) {
        val nbEvents = 4
        val aggregator =
          mkAggregator(
            useNewAggregator = useNewAggregator,
            config(sequencerTrustThreshold = 2, overrideMaxNbOfContributionsO = Some(3)),
          )

        val events = mkEvents(fixture, nbEvents)

        val badEvent = createEvent(
          timestamp = events(2).timestamp,
          serializedOverride = Some(ByteString.EMPTY),
        ).futureValueUS
        val eventsCarlos = Seq(events(0), badEvent, events(3))

        val ScenarioResults(Seq(resultsAlice, resultsBob, resultsCarlos), output) = runScenario(
          aggregator,
          Seq((sequencerAlice, events), (sequencerBob, events), (sequencerCarlos, eventsCarlos)),
        ): @unchecked

        forAll(Seq(resultsAlice, resultsBob))(_ shouldBe Seq.fill(nbEvents)(Either.unit))

        resultsCarlos(0) shouldBe Either.unit
        // Rejected whether it comes up during processing at t1 or t2
        resultsCarlos(1) shouldBe Left(
          SequencerAggregatorError.BogusEvent(sequencerCarlos, eventsCarlos(1))
        )
        // resultsCarlos(2) does not really matter, because in practice a failure of the previous event would trigger
        // a restart of the subscription
        resultsCarlos(2) should (
          // Rejected if it comes up during processing at t2
          be(
            Left(SequencerAggregatorError.BogusEvent(sequencerCarlos, eventsCarlos(2)))
          ) or
            // Accepted if it comes up during processing at t3
            be(Either.unit)
        )

        output.size shouldBe nbEvents
        forAll(events.zip(output)) { case (event, output) =>
          output shouldBe combinedMessage(aggregator, event, event)
        }
      } else succeed
    }

    "tolerate inserted messages" in { fixture =>
      import fixture.*

      /* Scenario: trust threshold = 2
       *
       * Alice:  E0_t0, E1_t1,        E2_t2, E3_t3
       * Bob:    E0_t0, E1_t1,        E2_t2, E3_t3
       * Carlos: E0_t0, E1_t1, X_t1', X_t2,  E3_t3
       */

      if (useNewAggregator) {
        val nbEvents = 4
        val aggregator =
          mkAggregator(
            useNewAggregator = useNewAggregator,
            config(sequencerTrustThreshold = 2, overrideMaxNbOfContributionsO = Some(3)),
          )

        val events = mkEvents(fixture, nbEvents)

        val insertedEvent =
          createEvent(timestamp = events(1).timestamp.plusMillis(666)).futureValueUS
        val badEvent = createEvent(
          timestamp = events(2).timestamp,
          serializedOverride = Some(ByteString.EMPTY),
        ).futureValueUS
        val eventsCarlos = Seq(events(0), events(1), insertedEvent, badEvent, events(3))

        val ScenarioResults(Seq(resultsAlice, resultsBob, resultsCarlos), output) = runScenario(
          aggregator,
          Seq((sequencerAlice, events), (sequencerBob, events), (sequencerCarlos, eventsCarlos)),
        ): @unchecked

        forAll(Seq(resultsAlice, resultsBob))(_ shouldBe Seq.fill(nbEvents)(Either.unit))
        resultsCarlos shouldBe Seq(
          Either.unit,
          Either.unit,
          Left(
            SequencerAggregatorError.BogusEvent(sequencerCarlos, insertedEvent)
          ),
          Left(
            SequencerAggregatorError.BogusEvent(sequencerCarlos, badEvent)
          ),
          Either.unit,
        )

        output.size shouldBe nbEvents
        forAll(events.zip(output)) { case (event, output) =>
          output shouldBe combinedMessage(aggregator, event, event)
        }
      } else succeed
    }

    "tolerate a subscription going dry" in { fixture =>
      import fixture.*

      /* Scenario: trust threshold = 2
       *
       * Alice:  E0_t0, E1_t1, E2_t2, E3_t3
       * Bob:    E0_t0, E1_t1, E2_t2, E3_t3
       * Carlos: E0_t0
       */

      if (useNewAggregator) {
        val nbEvents = 4
        val aggregator =
          mkAggregator(
            useNewAggregator = useNewAggregator,
            config(sequencerTrustThreshold = 2, overrideMaxNbOfContributionsO = Some(3)),
          )

        val events = mkEvents(fixture, nbEvents)

        val eventsCarlos = events.take(1)

        val ScenarioResults(Seq(resultsAlice, resultsBob, resultsCarlos), output) = runScenario(
          aggregator,
          Seq((sequencerAlice, events), (sequencerBob, events), (sequencerCarlos, eventsCarlos)),
        ): @unchecked

        forAll(Seq(resultsAlice, resultsBob))(_ shouldBe Seq.fill(nbEvents)(Either.unit))
        resultsCarlos shouldBe Seq(Either.unit)

        output.size shouldBe nbEvents
        forAll(events.zip(output)) { case (event, output) =>
          output shouldBe combinedMessage(aggregator, event, event)
        }
      } else succeed
    }

    "detect when the threshold is not reachable" in { fixture =>
      import fixture.*

      /* Scenario: trust threshold = 3
       *
       * Alice:  E0_t0, E1_t1, E2_t2, E3_t3
       * Bob:    E0_t0, E1_t1, E2_t2, E3_t3
       * Carlos: E0_t0, E1_t1, E2_t2, E3'_t3
       * Dave:   E0_t0, E1_t1, E2_t2, E3'_t3
       */

      if (useNewAggregator) {
        val nbEvents = 4
        val aggregator =
          mkAggregator(
            useNewAggregator = useNewAggregator,
            config(sequencerTrustThreshold = 3, overrideMaxNbOfContributionsO = Some(4)),
          )

        val events1 = mkEvents(fixture, nbEvents)
        val otherEvent = createEvent(
          timestamp = events1(3).timestamp,
          serializedOverride = Some(ByteString.EMPTY),
        ).futureValueUS
        val events2 = events1.take(3) :+ otherEvent

        val expectedError = SequencerAggregatorError.ThresholdUnreachable(
          decided = 4,
          undecided = 0,
          largestGroup = 2,
          threshold = 3,
        )

        val ScenarioResults(Seq(resultsAlice, resultsBob, resultsCarlos, resultsDave), output) =
          loggerFactory.assertLoggedWarningsAndErrorsSeq(
            runScenario(
              aggregator,
              Seq(
                (sequencerAlice, events1),
                (sequencerBob, events1),
                (sequencerCarlos, events2),
                (sequencerDave, events2),
              ),
            ),
            LogEntry.assertLogSeq(
              Seq(
                (
                  _.warningMessage should include(expectedError.toString),
                  "Threshold unreachable",
                )
              )
            ),
          ): @unchecked

        forAll(Seq(resultsAlice, resultsBob, resultsCarlos, resultsDave)) { results =>
          results.take(3) shouldBe Seq.fill(3)(Either.unit)
          results(3) shouldBe Left(expectedError)
        }

        // The last event is never emitted
        output.size shouldBe nbEvents - 1
        forAll(events1.zip(output)) { case (event, output) =>
          output shouldBe combinedMessage(aggregator, event, event, event)
        }
      } else succeed
    }

    "properly error on a fork" in { fixture =>
      import fixture.*

      /* Scenario: trust threshold = 2
       *
       * Alice:  E0_t0, E1_t1, E2_t2, E3_t3
       * Bob:    E0_t0, E1_t1, E2_t2, E3_t3
       * Carlos: E0_t0, E1_t1, E2_t2, E3'_t3
       * Dave:   E0_t0, E1_t1, E2_t2, E3'_t3
       */

      if (useNewAggregator) {
        val nbEvents = 4
        val aggregator =
          mkAggregator(
            useNewAggregator = useNewAggregator,
            config(sequencerTrustThreshold = 2, overrideMaxNbOfContributionsO = Some(4)),
          )

        val events1 = mkEvents(fixture, nbEvents)
        val otherEvent = createEvent(
          timestamp = events1(3).timestamp,
          serializedOverride = Some(ByteString.EMPTY),
        ).futureValueUS
        val events2 = events1.take(3) :+ otherEvent

        val ScenarioResults(Seq(resultsAlice, resultsBob, resultsCarlos, resultsDave), output) =
          runScenario(
            aggregator,
            Seq(
              (sequencerAlice, events1),
              (sequencerBob, events1),
              (sequencerCarlos, events2),
              (sequencerDave, events2),
            ),
          ): @unchecked

        forAll(Seq(resultsAlice, resultsBob, resultsCarlos, resultsDave))(
          _.take(3) shouldBe Seq.fill(3)(Either.unit)
        )
        output.size shouldBe nbEvents

        // Results on the last event
        val Seq(a, b, c, d) =
          Seq(resultsAlice, resultsBob, resultsCarlos, resultsDave).map(_(3)): @unchecked

        a match {
          case Right(()) =>
            // Alice and Bob have won
            b shouldBe Either.unit
            c shouldBe Left(SequencerAggregatorError.BogusEvent(sequencerCarlos, events2(3)))
            d shouldBe Left(SequencerAggregatorError.BogusEvent(sequencerDave, events2(3)))

            // The sequence is decided by Alice and Bob
            forAll(events1.zip(output)) { case (event, output) =>
              output shouldBe combinedMessage(aggregator, event, event)
            }
          case _ =>
            // Carlos and Dave have won
            a shouldBe Left(SequencerAggregatorError.BogusEvent(sequencerAlice, events1(3)))
            b shouldBe Left(SequencerAggregatorError.BogusEvent(sequencerBob, events1(3)))
            c shouldBe Either.unit
            d shouldBe Either.unit

            // The sequence is decided by Carlos and Dave
            forAll(events2.zip(output)) { case (event, output) =>
              output shouldBe combinedMessage(aggregator, event, event)
            }
        }

      } else succeed
    }
  }

  private def assertDownstreamMessage(
      aggregator: SequencerAggregator,
      message: SequencedSerializedEvent,
  ): Assertion =
    clue("Expected a single downstream message") {
      aggregator.eventQueue.size() shouldBe 1
      aggregator.eventQueue.take() shouldBe message
    }

  private def assertCombinedDownstreamMessage(
      aggregator: SequencerAggregator,
      events: SequencedSerializedEvent*
  ): Assertion = clue("Expected a single combined downstream message from multiple sequencers") {
    aggregator.eventQueue.size() shouldBe 1
    aggregator.eventQueue.take() shouldBe combinedMessage(aggregator, events*)
  }

  private def assertNoMessageDownstream(aggregator: SequencerAggregator): Assertion =
    clue("Expected no downstream messages") {
      aggregator.eventQueue.size() shouldBe 0
    }

  private def combinedMessage(
      aggregator: SequencerAggregator,
      events: SequencedSerializedEvent*
  ): SequencedSerializedEvent =
    if (useNewAggregator) {
      // The aggregator combines the signatures in reverse order of appearance
      val combinedSignatures =
        NonEmptyUtil.fromUnsafe(events.reverse.flatMap(_.signedEvent.signatures))
      SequencedEventWithTraceContext(
        events.head.signedEvent.copy(signatures = combinedSignatures)
      )(traceContext)
    } else
      aggregator
        .combine(NonEmptyUtil.fromUnsafe(events.toList))
        .value

  private def mkEvents(fixture: FixtureParam, nb: Int) = {
    import fixture.*

    (0 until nb)
      .map(index =>
        createEvent(
          timestamp = ts(index),
          previousTimestamp = Option.when(index > 0)(ts(index - 1)),
        ).futureValueUS
      )
  }

  private def runScenario(
      aggregator: SequencerAggregator,
      sequencerIdAndEvents: Seq[(SequencerId, Seq[SequencedSerializedEvent])],
  ): ScenarioResults = {
    def runSubscription(
        sequencerId: SequencerId,
        events: Seq[SequencedSerializedEvent],
    ): FutureUnlessShutdown[Seq[Either[SequencerAggregatorError, Unit]]] =
      MonadUtil.sequentialTraverse(events) { event =>
        for {
          // Artificial delay
          // NOTE: The seed picked and printed above in the test might not help much for reproducibility because whether the
          // interleaving of events will happen is not fully determined by these delays - especially on CI where this
          // test competes with a myriad of other tests for resources vs. a reproduction attempt on an idle machine.
          _ <- FutureUnlessShutdown.outcomeF(DelayUtil.delay(rand.nextInt(200).millis))
          result <- aggregator.combineAndMergeEvent(sequencerId, event)
        } yield result
      }

    val testComplete = new AtomicBoolean()

    def readOutputs: Seq[SequencedSerializedEvent] = {
      val buf = Seq.newBuilder[SequencedSerializedEvent]

      @tailrec
      def go: Seq[SequencedSerializedEvent] =
        if (!testComplete.get) {
          Option(aggregator.eventQueue.poll(100, TimeUnit.MILLISECONDS)).foreach(buf.addOne)
          go
        } else buf.result()

      go
    }

    val outputF = Future(readOutputs)

    val results = FutureUnlessShutdown
      .sequence(sequencerIdAndEvents.map { case (sequencerId, events) =>
        runSubscription(sequencerId, events)
      })
      .futureValueUS

    testComplete.set(true)
    val output = outputF.futureValue

    ScenarioResults(results, output)
  }

}

object SequencerAggregatorTest {
  private final case class ScenarioResults(
      results: Seq[Seq[Either[SequencerAggregatorError, Unit]]],
      output: Seq[SequencedSerializedEvent],
  )
}
