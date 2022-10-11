// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import akka.actor.ActorSystem
import akka.stream.Materializer
import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.config.{DefaultProcessingTimeouts, ProcessingTimeout}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.http.HttpClientError
import com.digitalasset.canton.sequencing.client.SubscriptionCloseReason
import com.digitalasset.canton.sequencing.client.http.HttpSequencerClientError
import com.digitalasset.canton.sequencing.client.transports.HttpSequencerSubscriptionTest.Env
import com.digitalasset.canton.sequencing.protocol.SignedContent
import com.digitalasset.canton.sequencing.{
  OrdinarySerializedEvent,
  RawSignedContentSerializedEvent,
  SequencerTestUtils,
}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.tracing.Traced
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter}
import org.scalatest.Outcome
import org.scalatest.wordspec.FixtureAnyWordSpec

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.collection.mutable
import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}

class HttpSequencerSubscriptionTest
    extends FixtureAnyWordSpec
    with BaseTest
    with HasExecutionContext {

  implicit val executionContext = parallelExecutionContext

  override type FixtureParam = Env

  override def withFixture(test: OneArgTest): Outcome = {
    val env = new Env(loggerFactory)

    try super.withFixture(test.toNoArgTest(env))
    finally env.close()
  }

  // try not to delay tests too much
  private val pollingInterval = 10.milli

  "HttpSequencerSubscription" should {
    "continuously poll readNextEvent until an event is found" in { env =>
      import env.*

      val finished = Promise[Unit]()
      val pollingResults = PollingResultsBuilder()
        .empty()
        .event(0)
        .empty()
        .empty()
        .empty()
        .event(1)
        .empty()
        .event(2)
        .build

      // this should start the subscription
      val subscription = mkSubscription(pollingResults) { event =>
        if (event.content.counter == SequencerCounter(2)) finished.success(())
        Future.successful(Right(()))
      }

      // wait for us to consume all events
      finished.future.futureValue

      subscription.close()

      subscription.closeReason.futureValue shouldBe SubscriptionCloseReason.Closed

      pollingResults.receivedPollsForCounters shouldBe List(0, 0, 1, 1, 1, 1, 2, 2)
    }
  }

  "fail subscription with subscription error if readEvents returns error" in { env =>
    import env.*
    val called = new AtomicBoolean(false)

    val error =
      HttpSequencerClientError.ClientError(HttpClientError.HttpRequestNetworkError("test"))
    val pollingResults = PollingResultsBuilder().empty().error(error).build
    val subscription = mkSubscription(pollingResults) { _ =>
      called.set(true)
      Future.successful(Right(()))
    }

    subscription.closeReason.futureValue shouldBe SubscriptionReadError(error)
    pollingResults.receivedPollsForCounters shouldBe List(0, 0)
    called.get() shouldBe false
  }

  "fail subscription if handler returns a failed future" in { env =>
    import env.*

    val exception = new RuntimeException("handler exception")
    val pollingResults = PollingResultsBuilder().event(0).build
    val subscription = mkSubscription(pollingResults) { _ =>
      Future.failed(exception)
    }

    subscription.closeReason.futureValue shouldBe SubscriptionCloseReason.HandlerException(
      exception
    )
  }

  "fail subscription if the handler throws" in { env =>
    import env.*

    val exception = new RuntimeException("handler exception")
    val pollingResults = PollingResultsBuilder().event(0).build
    val subscription = mkSubscription(pollingResults) { _ =>
      throw exception
    }

    subscription.closeReason.futureValue shouldBe SubscriptionCloseReason.HandlerException(
      exception
    )
  }

  "subscription doesn't set closeReason until handler is complete" in { env =>
    import env.*

    val handlerInvoked = Promise[Unit]()
    val handlerCompleted = Promise[Either[String, Unit]]()

    val pollingResults = PollingResultsBuilder().event(0).build
    val subscription = mkSubscription(pollingResults) { _ =>
      handlerInvoked.success(())
      handlerCompleted.future
    }

    // Make sure that the handler has been invoked before doing the next step.
    handlerInvoked.future.futureValue

    // complete the handler
    handlerCompleted.success(Right(()))

    // invoke close
    subscription.close()

    // check we haven't yet propagated the close as the handler is still running
    eventuallyForever(timeUntilSuccess = 0.seconds, durationOfSuccess = 100.milliseconds) {
      !subscription.closeReason.isCompleted
    }

    subscription.closeReason.futureValue shouldBe SubscriptionCloseReason.Closed
  }

  private def mkSubscription(pollingResults: PollingResults)(
      handler: RawSignedContentSerializedEvent => Future[Either[String, Unit]]
  )(implicit materializer: Materializer): HttpSequencerSubscription[String] =
    new HttpSequencerSubscription(
      SequencerCounter(0),
      event => handler(event.signedEvent),
      pollingResults.readEvent,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
      pollingInterval,
    )

  def mkEvent(sc: Long): OrdinarySerializedEvent =
    OrdinarySequencedEvent(
      SignedContent(
        SequencerTestUtils.mockDeliver(sc, CantonTimestamp.Epoch, DefaultTestIdentities.domainId),
        SymbolicCrypto.emptySignature,
        None,
      )
    )(traceContext)

  class PollingResults(
      results: Seq[Either[HttpSequencerClientError, Option[OrdinarySerializedEvent]]]
  ) {
    private val nextResultIndex = new AtomicInteger()
    private val polls = mutable.Buffer[Long]()

    def receivedPollsForCounters: Seq[Long] = polls.toList.take(results.size)

    def readEvent(
        counter: Traced[SequencerCounter]
    ): EitherT[Future, HttpSequencerClientError, Option[OrdinarySerializedEvent]] = {
      polls += counter.value.v
      val resultIndex = nextResultIndex.getAndIncrement()

      // bit shady but just don't complete the future once we've run out of expected events
      val done: Future[Either[HttpSequencerClientError, Option[OrdinarySerializedEvent]]] =
        Future.never

      if (results.isDefinedAt(resultIndex)) {
        val definedEvent = results(resultIndex)
        definedEvent.toEitherT
      } else EitherT(done)
    }
  }

  case class PollingResultsBuilder(
      results: Seq[Either[HttpSequencerClientError, Option[OrdinarySerializedEvent]]] = Seq.empty
  ) {
    def event(sc: Long) = copy(results :+ Right(Some(mkEvent(sc))))
    def empty(): PollingResultsBuilder = copy(results :+ Right(None))
    def error(error: HttpSequencerClientError) = copy(results :+ Left(error))

    def build = new PollingResults(results)
  }
}

object HttpSequencerSubscriptionTest {

  class Env(protected val loggerFactory: NamedLoggerFactory)
      extends FlagCloseable
      with NamedLogging {
    implicit val system: ActorSystem = ActorSystem("HttpSequencerSubscriptionTest")
    implicit val materializer: Materializer = Materializer(system)

    override protected def timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing

    override protected def onClosed(): Unit =
      Lifecycle.close(
        Lifecycle.toCloseableMaterializer(materializer, "HttpSequencerSubscriptionTest"),
        Lifecycle.toCloseableActorSystem(system, logger, DefaultProcessingTimeouts.testing),
      )(logger)
  }
}
