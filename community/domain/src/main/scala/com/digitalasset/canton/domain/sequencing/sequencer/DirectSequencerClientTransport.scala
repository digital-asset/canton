// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import akka.stream.Materializer
import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.service.DirectSequencerSubscriptionFactory
import com.digitalasset.canton.lifecycle.{AsyncOrSyncCloseable, SyncCloseable}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.SerializedEventHandler
import com.digitalasset.canton.sequencing.client._
import com.digitalasset.canton.sequencing.client.transports.SequencerClientTransport
import com.digitalasset.canton.sequencing.handshake.HandshakeRequestError
import com.digitalasset.canton.sequencing.protocol.{
  HandshakeRequest,
  HandshakeResponse,
  SubmissionRequest,
  SubscriptionRequest,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** This transport is meant to be used to create a sequencer client that connects directly to an in-process sequencer.
  * Needed for cases when the sequencer node itself needs to listen to specific events such as identity events.
  */
class DirectSequencerClientTransport(
    sequencer: Sequencer,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit materializer: Materializer, ec: ExecutionContext)
    extends SequencerClientTransport
    with NamedLogging {

  private val subscriptionFactory =
    new DirectSequencerSubscriptionFactory(sequencer, timeouts, loggerFactory)

  override def sendAsync(
      request: SubmissionRequest,
      timeout: Duration,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncClientError, Unit] =
    sequencer
      .sendAsync(request)
      .leftMap(SendAsyncClientError.RequestRefused)

  override def sendAsyncUnauthenticated(
      request: SubmissionRequest,
      timeout: Duration,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncClientError, Unit] =
    EitherT.leftT(
      SendAsyncClientError.RequestInvalid("Direct client does not support unauthenticated sends")
    )

  override def acknowledge(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    sequencer.acknowledge(member, timestamp)

  override def subscribe[E](request: SubscriptionRequest, handler: SerializedEventHandler[E])(
      implicit traceContext: TraceContext
  ): SequencerSubscription[E] = new SequencerSubscription[E] {

    override protected def timeouts: ProcessingTimeout =
      DirectSequencerClientTransport.this.timeouts

    private val subscriptionRef = new AtomicReference[Option[SequencerSubscription[E]]](None)

    subscriptionFactory
      .create(request.counter, "direct", request.member, handler)
      .value
      .onComplete {
        case Success(Right(subscription)) =>
          closeReasonPromise.completeWith(subscription.closeReason)

          performUnlessClosing {
            subscriptionRef.set(Some(subscription))
          } onShutdown {
            subscription.close()
          }
        case Success(Left(value)) =>
          closeReasonPromise.trySuccess(Fatal(value.toString))
        case Failure(exception) =>
          closeReasonPromise.tryFailure(exception)
      }

    override protected val loggerFactory: NamedLoggerFactory =
      DirectSequencerClientTransport.this.loggerFactory

    override protected def closeAsync(): Seq[SyncCloseable] = Seq(
      SyncCloseable(
        "direct-sequencer-client-transport",
        subscriptionRef.get().foreach(_.close()),
      )
    )
  }

  override def subscribeUnauthenticated[E](
      request: SubscriptionRequest,
      handler: SerializedEventHandler[E],
  )(implicit traceContext: TraceContext): SequencerSubscription[E] = new SequencerSubscription[E] {
    override protected def timeouts: ProcessingTimeout =
      DirectSequencerClientTransport.this.timeouts
    override protected val loggerFactory: NamedLoggerFactory =
      DirectSequencerClientTransport.this.loggerFactory

    override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Nil

    closeReasonPromise.tryFailure(
      new RuntimeException("Direct client does not support unauthenticated subscriptions")
    )
  }

  override def subscriptionRetryPolicy: SubscriptionErrorRetryPolicy =
    new SubscriptionErrorRetryPolicy {
      override def retryOnError(
          subscriptionError: SubscriptionCloseReason.SubscriptionError,
          receivedItems: Boolean,
      )(implicit traceContext: TraceContext): Boolean =
        false // unlikely there will be any errors with this direct transport implementation
    }

  override def handshake(request: HandshakeRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, HandshakeRequestError, HandshakeResponse] =
    // never called - throwing an exception so tests fail if this ever changes
    throw new UnsupportedOperationException(
      "handshake is not implemented for DirectSequencerClientTransport"
    )

}
