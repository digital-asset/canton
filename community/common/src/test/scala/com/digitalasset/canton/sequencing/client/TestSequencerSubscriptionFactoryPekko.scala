// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.health.HealthComponent
import com.digitalasset.canton.health.HealthComponent.AlwaysHealthyComponent
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.SequencedSerializedEvent
import com.digitalasset.canton.sequencing.client.TestSubscriptionError.{
  RetryableError,
  RetryableExn,
  UnretryableError,
}
import com.digitalasset.canton.sequencing.protocol.{Batch, Deliver, SignedContent}
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.store.SequencedEventStore.SequencedEventWithTraceContext
import com.digitalasset.canton.topology.{DefaultTestIdentities, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import org.apache.pekko.stream.scaladsl.{Keep, Source}

import java.util.concurrent.atomic.AtomicReference

class TestSequencerSubscriptionFactoryPekko(
    health: HealthComponent,
    override protected val loggerFactory: NamedLoggerFactory,
) extends SequencerSubscriptionFactoryPekko[TestSubscriptionError]
    with NamedLogging {
  import TestSequencerSubscriptionFactoryPekko.*

  override def sequencerId: SequencerId = DefaultTestIdentities.daSequencerId

  private val sources = new AtomicReference[Seq[Option[CantonTimestamp] => Seq[Element]]](Seq.empty)

  def add(next: Element*): Unit = subscribe(_ => next)

  def subscribe(subscribe: Option[CantonTimestamp] => Seq[Element]): Unit =
    sources.getAndUpdate(_ :+ subscribe).discard

  override def create(startingTimestamp: Option[CantonTimestamp])(implicit
      traceContext: TraceContext
  ): SequencerSubscriptionPekko[TestSubscriptionError] = {
    val srcs = sources.getAndUpdate(_.drop(1))
    val subscribe = srcs.headOption.getOrElse(
      throw new IllegalStateException(
        "Requesting more resubscriptions than provided by the test setup"
      )
    )

    logger.debug(s"Creating SequencerSubscriptionPekko at starting timestamp $startingTimestamp")

    val source = Source(subscribe(startingTimestamp))
      // Add an incomplete unproductive source at the end to prevent automatic completion signals
      .concat(Source.never[Element])
      .withUniqueKillSwitchMat()(Keep.right)
      .mapConcat { withKillSwitch =>
        noTracingLogger.debug(s"Processing element ${withKillSwitch.value}")
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
      .watchTermination()(Keep.both)

    SequencerSubscriptionPekko[TestSubscriptionError](source, health)
  }

  override val retryPolicy: SubscriptionErrorRetryPolicyPekko[TestSubscriptionError] =
    new TestSubscriptionErrorRetryPolicyPekko
}

object TestSequencerSubscriptionFactoryPekko {
  def apply(loggerFactory: NamedLoggerFactory): TestSequencerSubscriptionFactoryPekko = {
    val alwaysHealthyComponent = new AlwaysHealthyComponent(
      "TestSequencerSubscriptionFactory",
      loggerFactory.getTracedLogger(classOf[TestSequencerSubscriptionFactoryPekko]),
    )
    new TestSequencerSubscriptionFactoryPekko(alwaysHealthyComponent, loggerFactory)
  }

  sealed trait Element extends Product with Serializable

  final case class Error(error: TestSubscriptionError) extends Element
  final case class Failure(exception: Exception) extends Element
  case object Complete extends Element
  final case class Event(
      timestamp: CantonTimestamp,
      signatures: NonEmpty[Seq[Signature]] = Signature.noSignatures,
  ) extends Element {
    def asOrdinarySerializedEvent: SequencedSerializedEvent =
      mkOrdinarySerializedEvent(timestamp, signatures)
  }

  def mkOrdinarySerializedEvent(
      timestamp: CantonTimestamp,
      signatures: NonEmpty[Seq[Signature]] = Signature.noSignatures,
  ): SequencedSerializedEvent = {
    val pts =
      if (timestamp == CantonTimestamp.Epoch) None else Some(timestamp.addMicros(-1L))
    val sequencedEvent = Deliver.create(
      pts,
      timestamp,
      DefaultTestIdentities.physicalSynchronizerId,
      None,
      Batch.empty(BaseTest.testedProtocolVersion),
      None,
      Option.empty[TrafficReceipt],
    )
    val signedContent =
      SignedContent.create(
        sequencedEvent,
        signatures,
        None,
        SignedContent.protocolVersionRepresentativeFor(BaseTest.testedProtocolVersion),
      )
    SequencedEventWithTraceContext(signedContent)(TraceContext.empty)
  }

  def genEvents(startTimestamp: Option[CantonTimestamp], count: Long): Seq[Event] =
    (0L until count).map(offset =>
      Event(startTimestamp.getOrElse(CantonTimestamp.Epoch).addMicros(offset))
    )

  private class TestSubscriptionErrorRetryPolicyPekko
      extends SubscriptionErrorRetryPolicyPekko[TestSubscriptionError] {
    override def retryOnError(subscriptionError: TestSubscriptionError, receivedItems: Boolean)(
        implicit loggingContext: ErrorLoggingContext
    ): Boolean =
      subscriptionError match {
        case RetryableError => true
        case UnretryableError => false
      }

    override def retryOnException(ex: Throwable)(implicit
        loggingContext: ErrorLoggingContext
    ): Boolean = ex match {
      case RetryableExn => true
      case _ => false
    }
  }
}
