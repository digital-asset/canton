// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  NamedLoggerFactory,
  NamedLogging,
  NamedLoggingContext,
}
import com.digitalasset.canton.sequencing.client.ResilientSequencerSubscription.LostSequencerSubscription
import com.digitalasset.canton.sequencing.client.TestSubscriptionError.{
  FatalExn,
  RetryableError,
  RetryableExn,
  UnretryableError,
}
import com.digitalasset.canton.sequencing.protocol.{Batch, Deliver, SignedContent}
import com.digitalasset.canton.sequencing.{OrdinarySerializedEvent, client}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.AkkaUtil.syntax.*
import com.digitalasset.canton.{BaseTest, SequencerCounter}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.{Deadline, DurationInt, FiniteDuration}

class ResilientSequencerSubscriberAkkaTest extends StreamSpec with BaseTest {
  import TestSequencerSubscriptionFactoryAkka.*

  // Override the implicit from AkkaSpec so that we don't get ambiguous implicits
  override val patience: PatienceConfig = defaultPatience

  // very short to speedup test
  private val InitialDelay: FiniteDuration = 1.millisecond
  private val MaxDelay: FiniteDuration =
    1025.millis // 1 + power of 2 because InitialDelay keeps being doubled

  private def retryDelay(maxDelay: FiniteDuration = MaxDelay) =
    SubscriptionRetryDelayRule(InitialDelay, maxDelay, maxDelay)

  private def domainId = DefaultTestIdentities.domainId

  private def createResilientSubscriber[E](
      subscriptionFactory: SequencerSubscriptionFactoryAkka[E],
      retryDelayRule: SubscriptionRetryDelayRule = retryDelay(),
  ): ResilientSequencerSubscriberAkka[E] = {
    new ResilientSequencerSubscriberAkka[E](
      domainId,
      retryDelayRule,
      subscriptionFactory,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    )
  }

  "ResilientSequencerSubscriberAkka" should {
    "not retry on an unrecoverable error" in assertAllStagesStopped {
      val factory = new TestSequencerSubscriptionFactoryAkka(loggerFactory)
      val subscriber = createResilientSubscriber(factory)
      factory.add(Error(UnretryableError))
      loggerFactory.assertLogs(
        subscriber
          .subscribeFrom(SequencerCounter.Genesis)
          .source
          .toMat(Sink.ignore)(Keep.right)
          .run()
          .futureValue,
        _.warningMessage should include(
          s"Closing resilient sequencer subscription due to error: $UnretryableError"
        ),
      )
    }

    "retry on recoverable errors" in assertAllStagesStopped {
      val factory = new TestSequencerSubscriptionFactoryAkka(loggerFactory)
      val subscriber = createResilientSubscriber(factory)
      factory.add(Error(RetryableError))
      factory.add(Error(RetryableError))
      factory.add(Error(UnretryableError))
      loggerFactory.assertLogs(
        subscriber
          .subscribeFrom(SequencerCounter.Genesis)
          .source
          .toMat(Sink.ignore)(Keep.right)
          .run()
          .futureValue,
        _.warningMessage should include(
          s"Closing resilient sequencer subscription due to error: $UnretryableError"
        ),
      )
    }

    "retry on exceptions until one is fatal" in {
      val factory = new client.TestSequencerSubscriptionFactoryAkka(loggerFactory)
      val subscriber = createResilientSubscriber(factory)
      factory.add(Failure(RetryableExn))
      factory.add(Failure(FatalExn))

      loggerFactory.assertLogs(
        subscriber
          .subscribeFrom(SequencerCounter.Genesis)
          .source
          .toMat(Sink.ignore)(Keep.right)
          .run()
          .futureValue,
        _.warningMessage should include(
          "The sequencer subscription encountered an exception and will be restarted"
        ),
        _.errorMessage should include("Closing resilient sequencer subscription due to exception"),
      )
    }

    "restart from last received counter" in {
      val factory = new client.TestSequencerSubscriptionFactoryAkka(loggerFactory)
      val subscriber = createResilientSubscriber(factory)
      factory.subscribe(start =>
        (start to (start + 10)).map(sc => Event(sc)) :+ Error(RetryableError)
      )
      factory.subscribe(start => (start to (start + 10)).map(sc => Event(sc)))

      val ((killSwitch, doneF), sink) =
        subscriber
          .subscribeFrom(SequencerCounter.Genesis)
          .source
          .toMat(TestSink.probe)(Keep.both)
          .run()
      sink.request(30)
      val expectedCounters =
        (SequencerCounter.Genesis to SequencerCounter(10)) ++
          (SequencerCounter(10) to SequencerCounter(20))
      expectedCounters.zipWithIndex.foreach { case (sc, i) =>
        clue(s"Output stream element $i") {
          sink.expectNext(Right(mkOrdinarySerializedEvent(sc)))
        }
      }
      killSwitch.shutdown()
      doneF.futureValue
    }

    "correctly indicates whether we've received items when calculating the next retry delay" in assertAllStagesStopped {
      val hasReceivedEventsCalls = new AtomicReference[Seq[Boolean]](Seq.empty)
      val captureHasEvent = new SubscriptionRetryDelayRule {
        override def nextDelay(
            previousDelay: FiniteDuration,
            hasReceivedEvent: Boolean,
        ): FiniteDuration = {
          hasReceivedEventsCalls.getAndUpdate(_ :+ hasReceivedEvent)
          1.milli
        }

        override val initialDelay: FiniteDuration = 1.milli
        override val warnDelayDuration: FiniteDuration = 100.millis
      }
      val factory = new client.TestSequencerSubscriptionFactoryAkka(loggerFactory)
      val subscriber = createResilientSubscriber(factory, captureHasEvent)

      // provide an event then close with a recoverable error
      factory.add(Event(SequencerCounter(1L)), Error(RetryableError))
      // don't provide an event and close immediately with a recoverable error
      factory.add(Error(RetryableError))
      // don't provide an event and close immediately
      factory.add(Complete)

      subscriber
        .subscribeFrom(SequencerCounter.Genesis)
        .source
        .toMat(Sink.ignore)(Keep.right)
        .run()
        .futureValue

      // An unretryable completions does not call the retry delay rule, so there should be only to calls recorded
      hasReceivedEventsCalls.get() shouldBe Seq(true, false)
    }

    "retry until closing if the sequencer is permanently unavailable" in {
      val maxDelay = 100.milliseconds

      val factory = new client.TestSequencerSubscriptionFactoryAkka(loggerFactory)
      val subscriber = createResilientSubscriber(factory, retryDelay(maxDelay))
      // Always close with RetryableError
      for (_ <- 1 to 100) {
        factory.add(Error(RetryableError))
      }

      val startTime = Deadline.now
      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          val (killSwitch, doneF) =
            subscriber
              .subscribeFrom(SequencerCounter.Genesis)
              .source
              .toMat(Sink.ignore)(Keep.left)
              .run()

          // we retry until we see a warning
          eventually() {
            loggerFactory.numberOfRecordedEntries should be > 0
          }

          // Check that it has hit MaxDelay. We can't really check an upper bound as it would make the test flaky
          -startTime.timeLeft should be >= maxDelay

          killSwitch.shutdown()
          doneF.futureValue
        },
        logEntries => {
          logEntries should not be empty
          forEvery(logEntries) {
            _.warningMessage should (include(s"Waiting $maxDelay before reconnecting") or include(
              LostSequencerSubscription.id
            ))
          }
        },
      )
    }
  }
}

class TestSequencerSubscriptionFactoryAkka(
    override protected val loggerFactory: NamedLoggerFactory
) extends SequencerSubscriptionFactoryAkka[TestSubscriptionError]
    with NamedLogging {
  import TestSequencerSubscriptionFactoryAkka.*

  private val sources = new AtomicReference[Seq[SequencerCounter => Seq[Element]]](Seq.empty)

  def add(next: Element*): Unit = subscribe(_ => next)

  def subscribe(subscribe: SequencerCounter => Seq[Element]): Unit =
    sources.getAndUpdate(_ :+ subscribe).discard

  override def create(startingCounter: SequencerCounter)(implicit
      loggingContext: NamedLoggingContext
  ): SequencerSubscriptionAkka[TestSubscriptionError] = {
    val srcs = sources.getAndUpdate(_.drop(1))
    val subscribe = srcs.headOption.getOrElse(
      throw new IllegalStateException(
        "Requesting more resubscriptions than provided by the test setup"
      )
    )

    val source = Source(subscribe(startingCounter))
      // Add a incomplete unproductive source at the end to prevent automatic completion signals
      .concat(Source.never[Element])
      .withUniqueKillSwitchMat()(Keep.right)
      .mapConcat { withKillSwitch =>
        noTracingLogger.debug(s"Processing element ${withKillSwitch.unwrap}")
        withKillSwitch.traverse {
          case Error(error) =>
            withKillSwitch.killSwitch.shutdown()
            Seq(Left(error))
          case Complete =>
            withKillSwitch.killSwitch.shutdown()
            Seq.empty
          case event: Event => Seq(Right(event.asOrdinarySerializedEvent))
          case Failure(ex) => throw ex
        }
      }
      .takeUntilThenDrain(_.isLeft)
      .map(_.unwrap)
      .watchTermination()(Keep.both)

    SequencerSubscriptionAkka[TestSubscriptionError](source)
  }

  override def retryPolicy: SubscriptionErrorRetryPolicyAkka[TestSubscriptionError] =
    new TestSubscriptionErrorRetryPolicyAkka
}

object TestSequencerSubscriptionFactoryAkka {
  sealed trait Element extends Product with Serializable

  final case class Error(error: TestSubscriptionError) extends Element
  final case class Failure(exception: Exception) extends Element
  case object Complete extends Element
  final case class Event(counter: SequencerCounter) extends Element {
    def asOrdinarySerializedEvent: OrdinarySerializedEvent = mkOrdinarySerializedEvent(counter)
  }

  def mkOrdinarySerializedEvent(counter: SequencerCounter): OrdinarySerializedEvent = {
    val sequencedEvent = Deliver.create(
      counter,
      CantonTimestamp.Epoch.addMicros(counter.unwrap),
      DefaultTestIdentities.domainId,
      None,
      Batch.empty(BaseTest.testedProtocolVersion),
      BaseTest.testedProtocolVersion,
    )
    val signedContent =
      SignedContent.tryCreate(
        sequencedEvent,
        NonEmpty(Seq, Signature.noSignature),
        None,
        SignedContent.protocolVersionRepresentativeFor(BaseTest.testedProtocolVersion),
      )
    OrdinarySequencedEvent(signedContent, None)(TraceContext.empty)
  }

  private class TestSubscriptionErrorRetryPolicyAkka
      extends SubscriptionErrorRetryPolicyAkka[TestSubscriptionError] {
    override def retryOnError(subscriptionError: TestSubscriptionError, receivedItems: Boolean)(
        implicit loggingContext: ErrorLoggingContext
    ): Boolean = {
      subscriptionError match {
        case RetryableError => true
        case UnretryableError => false
      }
    }

    override def retryOnException(ex: Throwable)(implicit
        loggingContext: ErrorLoggingContext
    ): Boolean = ex match {
      case RetryableExn => true
      case _ => false
    }
  }
}