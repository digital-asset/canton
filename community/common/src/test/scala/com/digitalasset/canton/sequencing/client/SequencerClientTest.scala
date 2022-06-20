// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import cats.syntax.foldable._
import com.codahale.metrics.MetricRegistry
import com.daml.metrics.MetricName
import com.digitalasset.canton._
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{
  DefaultProcessingTimeouts,
  LoggingConfig,
  ProcessingTimeout,
  TestingConfigInternal,
}
import com.digitalasset.canton.crypto.{Hash, TestHash}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.{CommonMockMetrics, SequencerClientMetrics}
import com.digitalasset.canton.protocol.TestDomainParameters
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing._
import com.digitalasset.canton.sequencing.client.SequencedEventValidationError.GapInSequencerCounter
import com.digitalasset.canton.sequencing.client.SequencerClient.CloseReason.{
  ClientShutdown,
  UnrecoverableError,
}
import com.digitalasset.canton.sequencing.client.SequencerClientSubscriptionError.{
  ApplicationHandlerException,
  EventValidationError,
}
import com.digitalasset.canton.sequencing.client.SubscriptionCloseReason.{
  HandlerError,
  HandlerException,
}
import com.digitalasset.canton.sequencing.client.transports.SequencerClientTransport
import com.digitalasset.canton.sequencing.handshake.HandshakeRequestError
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.store.memory.{
  InMemorySendTrackerStore,
  InMemorySequencedEventStore,
  InMemorySequencerCounterTrackerStore,
}
import com.digitalasset.canton.store.{
  CursorPrehead,
  SequencedEventStore,
  SequencerCounterTrackerStore,
}
import com.digitalasset.canton.time.{
  DomainTimeTracker,
  DomainTimeTrackerConfig,
  MockTimeRequestSubmitter,
  SimClock,
}
import com.digitalasset.canton.topology.DefaultTestIdentities.participant1
import com.digitalasset.canton.topology.{DefaultTestIdentities, Member}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class SequencerClientTest extends AsyncWordSpec with BaseTest with HasExecutorService {

  lazy val metrics =
    new SequencerClientMetrics(MetricName("SequencerClientTest"), new MetricRegistry())
  lazy val deliver: Deliver[Nothing] =
    SequencerTestUtils.mockDeliver(42L, CantonTimestamp.Epoch, DefaultTestIdentities.domainId)
  lazy val signedDeliver: OrdinarySerializedEvent = {
    OrdinarySequencedEvent(SequencerTestUtils.sign(deliver))(traceContext)
  }

  lazy val nextDeliver: Deliver[Nothing] = SequencerTestUtils.mockDeliver(
    43L,
    CantonTimestamp.ofEpochSecond(1),
    DefaultTestIdentities.domainId,
  )
  lazy val deliver44: Deliver[Nothing] = SequencerTestUtils.mockDeliver(
    44L,
    CantonTimestamp.ofEpochSecond(2),
    DefaultTestIdentities.domainId,
  )
  lazy val deliver45: Deliver[Nothing] = SequencerTestUtils.mockDeliver(
    45,
    CantonTimestamp.ofEpochSecond(3),
    DefaultTestIdentities.domainId,
  )
  lazy val dummyHash: Hash = TestHash.digest(0)

  lazy val alwaysSuccessfulHandler: PossiblyIgnoredApplicationHandler[ClosedEnvelope] =
    ApplicationHandler.success()
  lazy val failureException = new IllegalArgumentException("application handler failed")
  lazy val alwaysFailingHandler: PossiblyIgnoredApplicationHandler[ClosedEnvelope] =
    ApplicationHandler.create("always-fails")(_ =>
      HandlerResult.synchronous(FutureUnlessShutdown.failed(failureException))
    )

  "subscribe" should {
    "throws if more than one handler is subscribed" in {
      for {
        env <- Env.create()
        _ <- env.subscribeAfter()
        error <- loggerFactory
          .assertLogs(
            env.subscribeAfter(CantonTimestamp.MinValue, alwaysSuccessfulHandler),
            _.warningMessage shouldBe "Cannot create additional subscriptions to the sequencer from the same client",
          )
          .failed
      } yield error shouldBe a[RuntimeException]
    }

    "start from genesis if there is no recorded event" in {
      for {
        env <- Env.create()
        _ <- env.subscribeAfter()
      } yield {
        env.transport.subscriber.value.request.counter shouldBe GenesisSequencerCounter
      }
    }

    "starts subscription at last stored event (for fork verification)" in {
      for {
        env <- Env.create(
          storedEvents = Seq(deliver)
        )
        _ <- env.subscribeAfter()
      } yield {
        env.transport.subscriber.value.request.counter shouldBe deliver.counter
      }
    }

    "stores the event in the SequencedEventStore" in {
      for {
        env @ Env(client, transport, _, sequencedEventStore, _) <- Env.create()

        _ <- env.subscribeAfter()
        _ <- transport.subscriber.value.handler(signedDeliver)
        _ <- client.flush()
        storedEvent <- sequencedEventStore.sequencedEvents()
      } yield storedEvent shouldBe Seq(signedDeliver)
    }

    "stores the event even if the handler fails" in {
      for {
        env @ Env(client, transport, _, sequencedEventStore, _) <- Env.create()

        _ <- env.subscribeAfter(eventHandler = alwaysFailingHandler)
        _ <- loggerFactory.assertLogs(
          {
            for {
              _ <- transport.subscriber.value.handler(signedDeliver)
              _ <- client.flush()
            } yield ()
          },
          logEntry => {
            logEntry.errorMessage should be(
              "Synchronous event processing failed for event batch with sequencer counters 42 to 42."
            )
            logEntry.throwable.value shouldBe failureException
          },
        )
        storedEvent <- sequencedEventStore.sequencedEvents()
      } yield storedEvent shouldBe Seq(signedDeliver)
    }

    "doesn't give prior event to the application handler" in {
      val validated = new AtomicBoolean()
      val processed = new AtomicBoolean()

      for {
        env @ Env(client, transport, _, _, _) <- Env.create(
          eventValidator = event => {
            validated.set(true)
            Env.eventAlwaysValid.validate(event)
          },
          storedEvents = Seq(deliver),
        )
        _ <- env.subscribeAfter(
          deliver.timestamp,
          ApplicationHandler.create("") { events =>
            processed.set(true)
            alwaysSuccessfulHandler(events)
          },
        )
        _ = transport.subscriber.value.request.counter shouldBe deliver.counter
        _ <- transport.subscriber.value.handler(signedDeliver)
      } yield {
        validated.get() shouldBe true
        processed.get() shouldBe false
      }
    }

    "picks the last prior event" in {
      val triggerNextDeliverHandling = new AtomicBoolean()
      for {
        env <- Env.create(
          storedEvents = Seq(deliver, nextDeliver, deliver44)
        )
        _ <- env.subscribeAfter(
          nextDeliver.timestamp.immediatePredecessor,
          ApplicationHandler.create("") { events =>
            if (events.value.exists(_.counter == nextDeliver.counter)) {
              triggerNextDeliverHandling.set(true)
            }
            HandlerResult.done
          },
        )
      } yield {
        triggerNextDeliverHandling.get shouldBe true
      }
    }

    "completes the sequencer client if the subscription closes due to an error" in {
      val error = EventValidationError(GapInSequencerCounter(666, 0))
      for {
        env @ Env(client, transport, _, _, _) <- Env.create(useParallelExecutionContext = true)

        _ <- env.subscribeAfter(CantonTimestamp.MinValue, alwaysSuccessfulHandler)
        subscription = transport.subscriber
          // we know the resilient sequencer subscription is using this type
          .map(_.subscription.asInstanceOf[MockSubscription[SequencerClientSubscriptionError]])
          .value
        closeReason <- loggerFactory.assertLogs(
          {
            subscription.closeSubscription(error)
            client.completion
          },
          _.warningMessage should include("sequencer"),
        )
      } yield closeReason should matchPattern {
        case e: UnrecoverableError if e.cause == s"handler returned error: $error" =>
      }

    }

    "completes the sequencer client if the application handler fails" in {
      val error = new RuntimeException("failed handler")
      val syncError = ApplicationHandlerException(error, deliver.counter, deliver.counter)
      val handler: PossiblyIgnoredApplicationHandler[ClosedEnvelope] =
        ApplicationHandler.create("async-failure")(_ =>
          FutureUnlessShutdown.failed[AsyncResult](error)
        )
      for {
        env @ Env(client, transport, _, _, _) <- Env.create(useParallelExecutionContext = true)
        _ <- env.subscribeAfter(CantonTimestamp.MinValue, handler)
        closeReason <- loggerFactory.assertLogs(
          {
            for {
              _ <- transport.subscriber.value.sendToHandler(deliver)
              // Send the next event so that the client notices that an error has occurred.
              _ <- client.flush()
              _ <- transport.subscriber.value.sendToHandler(nextDeliver)
              // wait until the subscription is closed (will emit an error)
              closeReason <- client.completion
            } yield closeReason
          },
          logEntry => {
            logEntry.errorMessage should be(
              s"Synchronous event processing failed for event batch with sequencer counters ${deliver.counter} to ${deliver.counter}."
            )
            logEntry.throwable shouldBe Some(error)
          },
          _.warningMessage should include(
            s"Closing resilient sequencer subscription due to error: HandlerError($syncError)"
          ),
        )

      } yield {
        client.close() // make sure that we can still close the sequencer client
        closeReason should matchPattern {
          case e: UnrecoverableError if e.cause == s"handler returned error: $syncError" =>
        }
      }
    }

    "completes the sequencer client if the application handler shuts down synchronously" in {
      val handler: PossiblyIgnoredApplicationHandler[ClosedEnvelope] =
        ApplicationHandler.create("shutdown")(_ => FutureUnlessShutdown.abortedDueToShutdown)
      for {
        env @ Env(client, transport, _, _, _) <- Env.create(useParallelExecutionContext = true)
        _ <- env.subscribeAfter(eventHandler = handler)
        closeReason <- {
          for {
            _ <- transport.subscriber.value.sendToHandler(deliver)
            // Send the next event so that the client notices that an error has occurred.
            _ <- client.flush()
            _ <- transport.subscriber.value.sendToHandler(nextDeliver)
            closeReason <- client.completion
          } yield closeReason
        }
      } yield {
        client.close() // make sure that we can still close the sequencer client
        closeReason shouldBe ClientShutdown
      }
    }

    "completes the sequencer client if asynchronous event processing fails" in {
      val error = new RuntimeException("asynchronous failure")
      val asyncFailure = HandlerResult.asynchronous(FutureUnlessShutdown.failed(error))
      val asyncException = ApplicationHandlerException(error, deliver.counter, deliver.counter)
      for {
        env @ Env(client, transport, _, _, _) <- Env.create(useParallelExecutionContext = true)
        _ <- env.subscribeAfter(
          eventHandler = ApplicationHandler.create("async-failure")(_ => asyncFailure)
        )
        closeReason <- loggerFactory.assertLogs(
          {
            for {
              _ <- transport.subscriber.value.sendToHandler(deliver)
              // Make sure that the asynchronous error has been noticed
              // We intentionally do two flushes. The first captures `handleReceivedEventsUntilEmpty` completing.
              // During this it may addToFlush a future for capturing `asyncSignalledF` however this may occur
              // after we've called `flush` and therefore won't guarantee completing all processing.
              // So our second flush will capture `asyncSignalledF` for sure.
              _ <- client.flush()
              _ <- client.flush()
              // Send the next event so that the client notices that an error has occurred.
              _ <- transport.subscriber.value.sendToHandler(nextDeliver)
              _ <- client.flush()
              // wait until client completed (will write an error)
              closeReason <- client.completion
              _ = client.close() // make sure that we can still close the sequencer client
            } yield closeReason
          },
          logEntry => {
            logEntry.errorMessage should include(
              s"Asynchronous event processing failed for event batch with sequencer counters ${deliver.counter} to ${deliver.counter}"
            )
            logEntry.throwable shouldBe Some(error)
          },
          _.warningMessage should include(
            s"Closing resilient sequencer subscription due to error: HandlerError($asyncException)"
          ),
        )
      } yield {
        closeReason should matchPattern {
          case e: UnrecoverableError if e.cause == s"handler returned error: $asyncException" =>
        }
      }
    }

    "completes the sequencer client if asynchronous event processing shuts down" in {
      val asyncShutdown = HandlerResult.asynchronous(FutureUnlessShutdown.abortedDueToShutdown)
      for {
        env @ Env(client, transport, _, _, _) <- Env.create(useParallelExecutionContext = true)
        _ <- env.subscribeAfter(
          CantonTimestamp.MinValue,
          ApplicationHandler.create("async-shutdown")(_ => asyncShutdown),
        )
        closeReason <- {
          for {
            _ <- transport.subscriber.value.sendToHandler(deliver)
            _ <- client.flush() // Make sure that the asynchronous error has been noticed
            // Send the next event so that the client notices that an error has occurred.
            _ <- transport.subscriber.value.sendToHandler(nextDeliver)
            _ <- client.flush()
            closeReason <- client.completion
          } yield closeReason
        }
      } yield {
        client.close() // make sure that we can still close the sequencer client
        closeReason shouldBe ClientShutdown
      }
    }

    "replays messages from the SequencedEventStore" in {
      val processedEvents = new ConcurrentLinkedQueue[SequencerCounter]
      for {
        env <- Env.create(
          storedEvents = Seq(deliver, nextDeliver, deliver44)
        )
        _ <- env.subscribeAfter(
          deliver.timestamp,
          ApplicationHandler.create("") { events =>
            events.value.foreach(event => processedEvents.add(event.counter))
            alwaysSuccessfulHandler(events)
          },
        )
      } yield {
        import scala.jdk.CollectionConverters._
        processedEvents.iterator().asScala.toSeq shouldBe Seq(
          nextDeliver.counter,
          deliver44.counter,
        )
      }
    }

    "propagates errors during replay" in {
      val syncError =
        ApplicationHandlerException(failureException, nextDeliver.counter, deliver44.counter)
      val syncExc = SequencerClientSubscriptionException(syncError)
      for {
        env <- Env.create(
          storedEvents = Seq(deliver, nextDeliver, deliver44)
        )
        error <- loggerFactory.assertLogs(
          env.subscribeAfter(deliver.timestamp, alwaysFailingHandler).failed,
          logEntry => {
            logEntry.errorMessage shouldBe "Synchronous event processing failed for event batch with sequencer counters 43 to 44."
            logEntry.throwable shouldBe Some(failureException)
          },
          logEntry => {
            logEntry.errorMessage should include("Sequencer subscription failed")
            logEntry.throwable shouldBe Some(syncExc)
          },
        )
      } yield {
        error shouldBe syncExc
      }
    }
  }

  "subscribeTracking" should {
    "updates sequencer counter prehead" in {
      for {
        Env(client, transport, sequencerCounterTrackerStore, _, timeTracker) <- Env.create()

        _ <- client.subscribeTracking(
          sequencerCounterTrackerStore,
          alwaysSuccessfulHandler,
          timeTracker,
        )
        _ <- transport.subscriber.value.handler(signedDeliver)
        _ <- client.flush()
        preHead <- sequencerCounterTrackerStore.preheadSequencerCounter
      } yield preHead.value shouldBe CursorPrehead(deliver.counter, deliver.timestamp)
    }

    "replays from the sequencer counter prehead" in {
      val processedEvents = new ConcurrentLinkedQueue[SequencerCounter]
      for {
        Env(client, transport, sequencerCounterTrackerStore, _, timeTracker) <- Env.create(
          storedEvents = Seq(deliver, nextDeliver, deliver44, deliver45),
          cleanPrehead = Some(CursorPrehead(nextDeliver.counter, nextDeliver.timestamp)),
        )

        _ <- client.subscribeTracking(
          sequencerCounterTrackerStore,
          ApplicationHandler.create("") { events =>
            events.value.foreach(event => processedEvents.add(event.counter))
            alwaysSuccessfulHandler(events)
          },
          timeTracker,
        )
        _ <- client.flush()
        prehead <- sequencerCounterTrackerStore.preheadSequencerCounter
      } yield {
        import scala.jdk.CollectionConverters._
        processedEvents.iterator().asScala.toSeq shouldBe Seq(
          deliver44.counter,
          deliver45.counter,
        )
        prehead.value shouldBe CursorPrehead(deliver45.counter, deliver45.timestamp)
      }
    }

    "resubscribes after replay" in {
      val processedEvents = new ConcurrentLinkedQueue[SequencerCounter]
      for {
        Env(client, transport, sequencerCounterTrackerStore, _, timeTracker) <- Env.create(
          storedEvents = Seq(deliver, nextDeliver, deliver44),
          cleanPrehead = Some(CursorPrehead(nextDeliver.counter, nextDeliver.timestamp)),
        )
        _ <- client.subscribeTracking(
          sequencerCounterTrackerStore,
          ApplicationHandler.create("") { events =>
            events.value.foreach(event => processedEvents.add(event.counter))
            alwaysSuccessfulHandler(events)
          },
          timeTracker,
        )
        _ <- transport.subscriber.value.sendToHandler(deliver45)
        _ <- client.flush()
        prehead <- sequencerCounterTrackerStore.preheadSequencerCounter
      } yield {
        import scala.jdk.CollectionConverters._
        processedEvents.iterator().asScala.toSeq shouldBe Seq(
          deliver44.counter,
          deliver45.counter,
        )
        prehead.value shouldBe CursorPrehead(deliver45.counter, deliver45.timestamp)
      }
    }

    "does not update the prehead if the application handler fails" in {
      for {
        Env(client, transport, sequencerCounterTrackerStore, _, timeTracker) <- Env.create()

        _ <- client.subscribeTracking(
          sequencerCounterTrackerStore,
          alwaysFailingHandler,
          timeTracker,
        )
        _ <- loggerFactory.assertLogs(
          {
            for {
              _ <- transport.subscriber.value.handler(signedDeliver)
              _ <- client.flush()
            } yield ()
          },
          logEntry => {
            logEntry.errorMessage should be(
              "Synchronous event processing failed for event batch with sequencer counters 42 to 42."
            )
            logEntry.throwable.value shouldBe failureException
          },
        )
        preHead <- sequencerCounterTrackerStore.preheadSequencerCounter
      } yield preHead shouldBe None
    }

    "updates the prehead only after the asynchronous processing has been completed" in {
      val promises = Map[SequencerCounter, Promise[UnlessShutdown[Unit]]](
        nextDeliver.counter -> Promise[UnlessShutdown[Unit]](),
        deliver44.counter -> Promise[UnlessShutdown[Unit]](),
      )

      def handler: PossiblyIgnoredApplicationHandler[ClosedEnvelope] =
        ApplicationHandler.create("") { events =>
          assert(events.value.size == 1)
          promises.get(events.value(0).counter) match {
            case None => HandlerResult.done
            case Some(promise) => HandlerResult.asynchronous(FutureUnlessShutdown(promise.future))
          }
        }

      for {
        Env(client, transport, sequencerCounterTrackerStore, _, timeTracker) <- Env.create(
          options = SequencerClientConfig(eventInboxSize = PositiveInt.tryCreate(1))
        )
        _ <- client.subscribeTracking(sequencerCounterTrackerStore, handler, timeTracker)
        _ <- transport.subscriber.value.sendToHandler(deliver)
        _ <- client.flush()
        prehead42 <- sequencerCounterTrackerStore.preheadSequencerCounter
        _ <- transport.subscriber.value.sendToHandler(nextDeliver)
        prehead43 <- sequencerCounterTrackerStore.preheadSequencerCounter
        _ <- transport.subscriber.value.sendToHandler(deliver44)
        _ = promises(deliver44.counter).success(UnlessShutdown.unit)
        prehead43a <- sequencerCounterTrackerStore.preheadSequencerCounter
        _ = promises(nextDeliver.counter).success(
          UnlessShutdown.unit
        ) // now we can advance the prehead
        _ <- client.flush()
        prehead44 <- sequencerCounterTrackerStore.preheadSequencerCounter
      } yield {
        prehead42 shouldBe Some(CursorPrehead(deliver.counter, deliver.timestamp))
        prehead43 shouldBe Some(CursorPrehead(deliver.counter, deliver.timestamp))
        prehead43a shouldBe Some(CursorPrehead(deliver.counter, deliver.timestamp))
        prehead44 shouldBe Some(CursorPrehead(deliver44.counter, deliver44.timestamp))
      }
    }
  }

  "changeTransport" should {

    "create second subscription from the same counter as the previous one when there are no events" in {
      val secondTransport = new MockTransport
      for {
        env <- Env.create(useParallelExecutionContext = true)
        _ <- env.subscribeAfter()
        _ <- env.changeTransport(secondTransport)
      } yield {
        val originalSubscriber = env.transport.subscriber.value
        originalSubscriber.request.counter shouldBe GenesisSequencerCounter
        originalSubscriber.subscription.isClosing shouldBe true // old subscription gets closed
        env.transport.isClosing shouldBe true

        val newSubscriber = secondTransport.subscriber.value
        newSubscriber.request.counter shouldBe GenesisSequencerCounter
        newSubscriber.subscription.isClosing shouldBe false
        secondTransport.isClosing shouldBe false

        env.client.completion.isCompleted shouldBe false
      }
    }

    "create second subscription from the same counter as the previous one when there are events" in {
      val secondTransport = new MockTransport
      for {
        env <- Env.create(useParallelExecutionContext = true)
        _ <- env.subscribeAfter()

        _ <- env.transport.subscriber.value.handler(signedDeliver)
        _ <- env.client.flush()

        _ <- env.changeTransport(secondTransport)
      } yield {
        val originalSubscriber = env.transport.subscriber.value
        originalSubscriber.request.counter shouldBe GenesisSequencerCounter

        val newSubscriber = secondTransport.subscriber.value
        newSubscriber.request.counter shouldBe deliver.counter

        env.client.completion.isCompleted shouldBe false
      }
    }

    "have new transport be used for sends" in {
      val secondTransport = new MockTransport

      for {
        env <- Env.create(useParallelExecutionContext = true)
        _ <- env.changeTransport(secondTransport)
        _ <- env.sendAsync(Batch.empty(defaultProtocolVersion))
      } yield {
        env.transport.lastSend.get() shouldBe None
        secondTransport.lastSend.get() should not be None

        env.transport.isClosing shouldBe true
        secondTransport.isClosing shouldBe false
      }
    }

    "have new transport be used for sends when there is subscription" in {
      val secondTransport = new MockTransport

      for {
        env <- Env.create(useParallelExecutionContext = true)
        _ <- env.subscribeAfter()
        _ <- env.changeTransport(secondTransport)
        _ <- env.sendAsync(Batch.empty(defaultProtocolVersion))
      } yield {
        env.transport.lastSend.get() shouldBe None
        secondTransport.lastSend.get() should not be None
      }
    }

  }

  case class Subscriber[E](
      request: SubscriptionRequest,
      handler: SerializedEventHandler[E],
      subscription: MockSubscription[E],
  ) {
    def sendToHandler(event: SequencedEvent[ClosedEnvelope]): Future[Unit] = {
      handler(OrdinarySequencedEvent(SequencerTestUtils.sign(event))(traceContext)).transform {
        case Success(Right(_)) => Success(())
        case Success(Left(err)) =>
          subscription.closeSubscription(err)
          Success(())
        case Failure(ex) =>
          subscription.closeSubscription(ex)
          Success(())
      }
    }
  }

  case class Env(
      client: SequencerClient,
      transport: MockTransport,
      sequencerCounterTrackerStore: SequencerCounterTrackerStore,
      sequencedEventStore: SequencedEventStore,
      timeTracker: DomainTimeTracker,
  ) {

    def subscribeAfter(
        priorTimestamp: CantonTimestamp = CantonTimestamp.MinValue,
        eventHandler: PossiblyIgnoredApplicationHandler[ClosedEnvelope] = alwaysSuccessfulHandler,
    ): Future[Unit] =
      client.subscribeAfter(
        priorTimestamp,
        None,
        eventHandler,
        timeTracker,
        PeriodicAcknowledgements.noAcknowledgements,
      )

    def changeTransport(newTransport: SequencerClientTransport): Future[Unit] =
      client.changeTransport(newTransport)

    def sendAsync(
        batch: Batch[DefaultOpenEnvelope]
    )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit] =
      client.sendAsync(batch)

  }

  class MockSubscription[E] extends SequencerSubscription[E] {
    override protected def loggerFactory: NamedLoggerFactory =
      SequencerClientTest.this.loggerFactory
    override protected def timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing
    def closeSubscription(reason: E): Unit = this.closeReasonPromise.success(HandlerError(reason))
    def closeSubscription(error: Throwable): Unit =
      this.closeReasonPromise.success(HandlerException(error))
  }

  class MockTransport extends SequencerClientTransport with NamedLogging {

    override protected def timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing

    private val subscriberRef = new AtomicReference[Option[Subscriber[_]]](None)
    def subscriber = subscriberRef.get()

    val lastSend = new AtomicReference[Option[SubmissionRequest]](None)

    override def acknowledge(member: Member, timestamp: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): Future[Unit] =
      Future.unit

    override def sendAsync(
        request: SubmissionRequest,
        timeout: Duration,
        protocolVersion: ProtocolVersion,
    )(implicit
        traceContext: TraceContext
    ): EitherT[Future, SendAsyncClientError, Unit] = {
      lastSend.set(Some(request))
      EitherT.rightT(())
    }

    override def sendAsyncUnauthenticated(
        request: SubmissionRequest,
        timeout: Duration,
        protocolVersion: ProtocolVersion,
    )(implicit
        traceContext: TraceContext
    ): EitherT[Future, SendAsyncClientError, Unit] = ???

    override def subscribe[E](request: SubscriptionRequest, handler: SerializedEventHandler[E])(
        implicit traceContext: TraceContext
    ): SequencerSubscription[E] = {
      val subscription = new MockSubscription[E]

      if (!subscriberRef.compareAndSet(None, Some(Subscriber(request, handler, subscription)))) {
        fail("subscribe has already been called by this client")
      }

      subscription
    }

    override def subscriptionRetryPolicy: SubscriptionErrorRetryPolicy =
      SubscriptionErrorRetryPolicy.never
    override def handshake(request: HandshakeRequest)(implicit
        traceContext: TraceContext
    ): EitherT[Future, HandshakeRequestError, HandshakeResponse] = ???
    override protected def loggerFactory: NamedLoggerFactory =
      SequencerClientTest.this.loggerFactory

    override def subscribeUnauthenticated[E](
        request: SubscriptionRequest,
        handler: SerializedEventHandler[E],
    )(implicit traceContext: TraceContext): SequencerSubscription[E] = ???
  }

  object Env {
    val eventAlwaysValid: ValidateSequencedEvent = _ => EitherT.rightT(())

    /** @param useParallelExecutionContext Set to true to use a parallel execution context which is handy for
      *                                    verifying close behavior that can involve an Await that would deadlock
      *                                    on ScalaTest's default serial execution context. However by enabling this
      *                                    it means the order of asynchronous operations within the SequencerClient
      *                                    will no longer be deterministic.
      */
    def create(
        storedEvents: Seq[SequencedEvent[ClosedEnvelope]] = Seq.empty,
        cleanPrehead: Option[CursorPrehead[SequencerCounter]] = None,
        eventValidator: ValidateSequencedEvent = eventAlwaysValid,
        options: SequencerClientConfig = SequencerClientConfig(),
        useParallelExecutionContext: Boolean = false,
    ): Future[Env] = {
      // if parallel execution is desired use the UseExecutorService executor service (which is a parallel execution context)
      // otherwise use the default serial execution context provided by ScalaTest
      implicit val executionContext: ExecutionContext =
        if (useParallelExecutionContext) SequencerClientTest.this.executorService
        else SequencerClientTest.this.executionContext
      val clock = new SimClock(loggerFactory = loggerFactory)
      val timeouts = DefaultProcessingTimeouts.testing
      val transport = new MockTransport
      val sendTrackerStore = new InMemorySendTrackerStore()
      val sequencedEventStore = new InMemorySequencedEventStore(loggerFactory)
      val sendTracker = new SendTracker(Map.empty, sendTrackerStore, metrics, loggerFactory)
      val sequencerCounterTrackerStore = new InMemorySequencerCounterTrackerStore(loggerFactory)
      val timeTracker =
        new DomainTimeTracker(
          DomainTimeTrackerConfig(),
          clock,
          new MockTimeRequestSubmitter(),
          timeouts,
          loggerFactory,
        )
      val domainParameters = TestDomainParameters.defaultStatic

      val client = new SequencerClient(
        DefaultTestIdentities.domainId,
        participant1,
        transport,
        options,
        TestingConfigInternal(),
        domainParameters,
        timeouts,
        _ => eventValidator,
        clock,
        sequencedEventStore,
        sendTracker,
        CommonMockMetrics.sequencerClient,
        None,
        false,
        LoggingConfig(),
        loggerFactory,
      )(executionContext, tracer)
      val signedEvents = storedEvents.map(SequencerTestUtils.sign)

      for {
        _ <- sequencedEventStore.store(
          signedEvents.map(OrdinarySequencedEvent(_)(TraceContext.empty))
        )
        _ <- cleanPrehead.traverse_(prehead =>
          sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(prehead)
        )
      } yield Env(client, transport, sequencerCounterTrackerStore, sequencedEventStore, timeTracker)
    }
  }
}
