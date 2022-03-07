// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import com.digitalasset.canton.config.{DefaultProcessingTimeouts, ProcessingTimeout}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.lifecycle.AsyncOrSyncCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.sequencing.client.ResilientSequencerSubscription.LostSequencerSubscription
import com.digitalasset.canton.sequencing.client.SubscriptionCloseReason.SubscriptionError
import com.digitalasset.canton.sequencing.client.TestSubscriptionError.{
  FatalExn,
  RetryableError,
  RetryableExn,
  UnretryableError,
}
import com.digitalasset.canton.sequencing.protocol.{ClosedEnvelope, SequencedEvent, SignedContent}
import com.digitalasset.canton.sequencing.{SequencerTestUtils, SerializedEventHandler}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter}
import org.scalatest.Assertion
import org.scalatest.wordspec.{AnyWordSpec, AsyncWordSpec}

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Future, Promise}

sealed trait TestSubscriptionError extends SubscriptionError
object TestSubscriptionError {
  case object RetryableError extends TestSubscriptionError
  case object UnretryableError extends TestSubscriptionError
  case object RetryableExn extends Exception
  case object FatalExn extends Exception

  val retryRule: CheckedSubscriptionErrorRetryPolicy[TestSubscriptionError] =
    new CheckedSubscriptionErrorRetryPolicy[TestSubscriptionError] {
      override protected def retryInternal(error: TestSubscriptionError, receivedItems: Boolean)(
          implicit traceContext: TraceContext
      ): Boolean = error match {
        case RetryableError => true
        case UnretryableError => false
      }

      override def retryOnException(exn: Throwable, Logger: TracedLogger)(implicit
          traceContext: TraceContext
      ): Boolean =
        exn match {
          case RetryableExn => true
          case _ => false
        }
    }
}

case class TestHandlerError(message: String)

@SuppressWarnings(
  Array("com.digitalasset.canton.DiscardedFuture")
) // TODO(#8448) Do not discard the futures
class ResilientSequencerSubscriptionTest
    extends AsyncWordSpec
    with BaseTest
    with ResilientSequencerSubscriptionTestUtils
    with HasExecutionContext {

  "ResilientSequencerSubscription" should {
    "not retry on an unrecoverable error" in {
      val testSubscriptions = SubscriptionTestFactory.mocked.addUnrecoverable()
      val subscription = createSubscription(testSubscriptions)

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          subscription.start

          for {
            reason <- subscription.closeReason
          } yield reason should be(UnretryableError)
        },
        _.map(
          _.warningMessage
        ) should contain only "Closing resilient sequencer subscription due to error: UnretryableError",
      )
    }

    "retry on recoverable errors" in {
      val testSubscriptions = SubscriptionTestFactory.mocked
        .addRecoverable()
        .addRecoverable()
        .addUnrecoverable()
      val subscription = createSubscription(testSubscriptions)

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          subscription.start

          for {
            closeReason <- subscription.closeReason
            _ = closeReason should be(UnretryableError)
          } yield testSubscriptions.allShouldHaveBeenUsed
        },
        _.map(
          _.warningMessage
        ) should contain only "Closing resilient sequencer subscription due to error: UnretryableError",
      )
    }

    "retry on exceptions until one is fatal" in {
      val testSubscriptions = SubscriptionTestFactory.mocked
      val subscription1F = testSubscriptions.addRunning()
      val subscription2F = testSubscriptions.addRunning()

      val subscription = createSubscription(testSubscriptions)
      subscription.start

      loggerFactory.assertLogs(
        for {
          subscription1 <- subscription1F
          // fail this subscription
          _ = subscription1.closeWithExn(RetryableExn)
          subscription2 <- subscription2F
          _ = subscription2.closeWithExn(FatalExn)
          // wait for the next subscription to occur
          closeReason <- subscription.closeReason.failed
        } yield { closeReason shouldBe FatalExn },
        _.warningMessage should include(
          "The sequencer subscription encountered an exception and will be restarted"
        ),
        _.errorMessage should include("Closing resilient sequencer subscription due to exception"),
        _.warningMessage should include("Underlying subscription failed to close"),
      )
    }

    "restart from last received counter" in {
      val testSubscriptions = SubscriptionTestFactory.mocked
      val subscription1F = testSubscriptions.addRunning()
      val subscription2F = testSubscriptions.addRunning()

      val subscription = createSubscription(testSubscriptions)
      subscription.start

      for {
        subscription1 <- subscription1F
        _ = subscription1.subscribedCounter shouldBe 0L
        // indicate that we've processed the next event
        _ <- subscription1.handleCounter(43L)
        // fail this subscription
        _ = subscription1.closeWithReason(RetryableError)
        // wait for the next subscription to occur
        subscription2 <- subscription2F
      } yield subscription2.subscribedCounter shouldBe 43L
    }

    "correctly indicates whether we've received items when calculating the next retry delay" in {
      val hasReceivedEventsCalls = mutable.Buffer[Boolean]()
      val captureHasEvent = new SubscriptionRetryDelayRule {
        override def nextDelay(
            previousDelay: FiniteDuration,
            hasReceivedEvent: Boolean,
        ): FiniteDuration = {
          hasReceivedEventsCalls += hasReceivedEvent
          1.milli
        }
        override val initialDelay: FiniteDuration = 1.milli
        override val warnDelayDuration: FiniteDuration = 100.millis
      }
      val testSubscriptions = SubscriptionTestFactory.mocked
      val subscription1F = testSubscriptions.addRunning()
      val subscription2F = testSubscriptions.addRunning()
      val subscription3F = testSubscriptions.addRunning()

      val subscription = createSubscription(testSubscriptions, retryDelayRule = captureHasEvent)
      subscription.start

      for {
        subscription1 <- subscription1F
        _ = {
          // provide an event then close with a recoverable error
          subscription1.handleCounter(1L)
          subscription1.closeWithReason(RetryableError)
        }
        subscription2 <- subscription2F
        _ = {
          // don't provide an event and close immediately
          subscription2.closeWithReason(RetryableError)
        }
        subscription3 <- subscription3F
        _ = subscription3.closeWithReason(SubscriptionCloseReason.Closed)
        closeReason <- subscription.closeReason
      } yield {
        closeReason should be(SubscriptionCloseReason.Closed)
        hasReceivedEventsCalls.toList should contain theSameElementsInOrderAs List(true, false)
      }
    }

    "close underlying subscription even if created after we close" in {
      val askedForSubscriptionPromise = Promise[Unit]()
      val underlyingSubscriptionPromise =
        Promise[
          Either[SequencerSubscriptionCreationError, SequencerSubscription[TestHandlerError]]
        ]()
      val resilientSequencerSubscription = new ResilientSequencerSubscription[TestHandlerError](
        "test",
        0L,
        _ => Future.successful[Either[TestHandlerError, Unit]](Right(())),
        (_, _) =>
          _ => {
            askedForSubscriptionPromise.success(())
            EitherT(underlyingSubscriptionPromise.future)
          },
        TestSubscriptionError.retryRule,
        retryDelay(),
        timeouts,
        loggerFactory,
      )

      val subscription = new SequencerSubscription[TestHandlerError] {
        override protected def timeouts = ResilientSequencerSubscriptionTest.this.timeouts
        override protected def loggerFactory: NamedLoggerFactory =
          ResilientSequencerSubscriptionTest.this.loggerFactory
      }

      // kick off
      resilientSequencerSubscription.start

      for {
        _ <- askedForSubscriptionPromise.future
        // close will block waiting for the subscription request, so start in a future but defer waiting for its completion until after its resolved
        closeF = Future { resilientSequencerSubscription.close() }
        _ = underlyingSubscriptionPromise.success(Right(subscription))
        _ <- closeF
      } yield subscription.isClosing shouldBe true // should have called close on underlying subscription
    }
  }
}

// these tests require a parallel execution context so are separated from the main group of tests
class ResilientSequencerSubscriptionRetryTimingTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with ResilientSequencerSubscriptionTestUtils {

  "retry until closing if the sequencer is permanently unavailable" in {
    val startTime = Deadline.now
    val maxDelay = 100.milliseconds

    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        val subscription =
          createSubscription(
            SubscriptionTestFactory.alwaysCloseWith(RetryableError),
            retryDelayRule = retryDelay(maxDelay),
          )

        subscription.start

        eventually() {
          loggerFactory.numberOfRecordedEntries should be > 0
        }

        subscription.close()
      },
      logEntries => {
        forEvery(logEntries) {
          _.warningMessage should (include(s"Waiting $maxDelay before reconnecting") or include(
            LostSequencerSubscription.id
          ))
        }
        logEntries should not be empty
      },
    )
    // Check that it has hit MaxDelay
    -startTime.timeLeft should (be >= maxDelay and be <= maxDelay * 25)
  }
}

trait ResilientSequencerSubscriptionTestUtils {
  this: BaseTest with HasExecutionContext =>

  // very short to speedup test
  val InitialDelay: FiniteDuration = 1.millisecond
  val MaxDelay: FiniteDuration =
    1025.millis // 1 + power of 2 because InitialDelay keeps being doubled

  def retryDelay(maxDelay: FiniteDuration = MaxDelay) =
    SubscriptionRetryDelayRule(InitialDelay, maxDelay, maxDelay)

  def createSubscription(
      subscriptionTestFactory: SubscriptionTestFactory,
      retryDelayRule: SubscriptionRetryDelayRule = retryDelay(),
  ): ResilientSequencerSubscription[TestHandlerError] = {
    val subscription = new ResilientSequencerSubscription(
      "test",
      0L,
      _ => Future.successful[Either[TestHandlerError, Unit]](Right(())),
      subscriptionTestFactory.createET,
      TestSubscriptionError.retryRule,
      retryDelayRule,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    )

    subscription
  }

  trait SubscriptionTestFactory {
    def create(counter: SequencerCounter, _handler: SerializedEventHandler[TestHandlerError])(
        traceContext: TraceContext
    ): SequencerSubscription[TestHandlerError]

    def createET(counter: SequencerCounter, _handler: SerializedEventHandler[TestHandlerError])(
        traceContext: TraceContext
    ): EitherT[Future, SequencerSubscriptionCreationError, SequencerSubscription[
      TestHandlerError
    ]] = {
      EitherT.pure[Future, SequencerSubscriptionCreationError](
        create(counter, _handler)(traceContext)
      )
    }
  }

  object SubscriptionTestFactory {
    def mocked: MockedSubscriptions = new MockedSubscriptions

    def alwaysCloseWith(
        reason: SubscriptionCloseReason[TestHandlerError]
    ): SubscriptionTestFactory =
      new SubscriptionTestFactory {
        override def create(
            counter: SequencerCounter,
            _handler: SerializedEventHandler[TestHandlerError],
        )(traceContext: TraceContext) =
          new SequencerSubscription[TestHandlerError] {
            override protected def loggerFactory: NamedLoggerFactory =
              ResilientSequencerSubscriptionTestUtils.this.loggerFactory
            closeReasonPromise.trySuccess(reason)
            override protected def timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing
            override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Nil
          }
      }
  }

  class MockSubscriptionResponse(
      mockCloseReason: Option[SubscriptionCloseReason[TestHandlerError]] = None
  ) {
    type SubscriberDetails =
      (SequencerCounter, SerializedEventHandler[TestHandlerError], MockedSequencerSubscription)
    private val activeSubscription =
      new AtomicReference[Option[SubscriberDetails]](None)
    private val subscribedP: Promise[Unit] = Promise()
    val subscribed = subscribedP.future

    class MockedSequencerSubscription(
        counter: SequencerCounter,
        handler: SerializedEventHandler[TestHandlerError],
    ) extends SequencerSubscription[TestHandlerError] {
      override protected def timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing
      override protected def loggerFactory: NamedLoggerFactory =
        ResilientSequencerSubscriptionTestUtils.this.loggerFactory

      def closeWithReason(reason: SubscriptionCloseReason[TestHandlerError]): Boolean =
        closeReasonPromise.trySuccess(reason)
      def closeWithExn(exn: Throwable): Boolean = closeReasonPromise.tryFailure(exn)

      if (!activeSubscription.compareAndSet(None, Some((counter, handler, this)))) {
        fail("subscription has been created more than once")
      } else {
        subscribedP.trySuccess(())
      }

      override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Nil

      // immediately close if a close reason was specified
      mockCloseReason foreach closeReasonPromise.trySuccess
    }

    def create(
        counter: SequencerCounter,
        handler: SerializedEventHandler[TestHandlerError],
    ): SequencerSubscription[TestHandlerError] =
      new MockedSequencerSubscription(counter, handler)

    private def fromSubscriber[A](getter: SubscriberDetails => A): A =
      activeSubscription.get() match {
        case Some(details) => getter(details)
        case None => fail("subscriber has not yet subscribed")
      }

    def handleCounter(counter: SequencerCounter): Future[Either[TestHandlerError, Unit]] =
      fromSubscriber(_._2)(OrdinarySequencedEvent(deliverEvent(counter))(traceContext))

    def subscribedCounter: SequencerCounter = fromSubscriber(_._1)

    def closeWithReason(reason: SubscriptionCloseReason[TestHandlerError]): Boolean =
      fromSubscriber(_._3).closeWithReason(reason)

    def closeWithExn(exn: Throwable): Boolean =
      fromSubscriber(_._3).closeWithExn(exn)

    def subscription: MockedSequencerSubscription = fromSubscriber(_._3)

    private def deliverEvent(
        counter: SequencerCounter
    ): SignedContent[SequencedEvent[ClosedEnvelope]] = {
      val deliver = SequencerTestUtils.mockDeliver(counter)
      SignedContent(deliver, SymbolicCrypto.emptySignature, None)
    }
  }

  class MockedSubscriptions extends SubscriptionTestFactory {
    private val subscriptions = scala.collection.mutable.Buffer[MockSubscriptionResponse]()
    private val nextSubscription = new AtomicInteger(0)

    def addRecoverable(): MockedSubscriptions = addClosed(RetryableError)

    def addUnrecoverable(): MockedSubscriptions = addClosed(UnretryableError)

    def addRunning(): Future[MockSubscriptionResponse] = {
      val mockResponse = new MockSubscriptionResponse()
      add(mockResponse)
      mockResponse.subscribed.map(_ => mockResponse)
    }

    def addClosed(reason: SubscriptionCloseReason[TestHandlerError]): MockedSubscriptions = {
      add(new MockSubscriptionResponse(Some(reason)))
    }

    def add(mockSubscriptionResponse: MockSubscriptionResponse) = {
      subscriptions += mockSubscriptionResponse
      this
    }

    def create(counter: SequencerCounter, _handler: SerializedEventHandler[TestHandlerError])(
        traceContext: TraceContext
    ): SequencerSubscription[TestHandlerError] =
      subscriptions(nextSubscription.getAndIncrement()).create(counter, _handler)

    def allShouldHaveBeenUsed: Assertion = nextSubscription.get() shouldBe subscriptions.length
  }

}
