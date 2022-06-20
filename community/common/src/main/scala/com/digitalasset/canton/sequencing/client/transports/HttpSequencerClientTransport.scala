// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports
import akka.actor.ActorSystem
import akka.stream.Materializer
import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.lifecycle.Lifecycle.{toCloseableActorSystem, toCloseableMaterializer}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.SerializedEventHandler
import com.digitalasset.canton.sequencing.client.http.{
  HttpSequencerClient,
  HttpSequencerClientError,
}
import com.digitalasset.canton.sequencing.client.{
  SendAsyncClientError,
  SequencerSubscription,
  SubscriptionCloseReason,
  SubscriptionErrorRetryPolicy,
}
import com.digitalasset.canton.sequencing.handshake.HandshakeRequestError
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

sealed trait HttpSubscriptionError extends SubscriptionCloseReason.SubscriptionError
case class UnexpectedSubscriptionException(exception: Throwable) extends HttpSubscriptionError
case class SubscriptionReadError(readError: HttpSequencerClientError) extends HttpSubscriptionError

class HttpSequencerClientTransport(
    client: HttpSequencerClient,
    val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SequencerClientTransport
    with NamedLogging {

  implicit private val system = ActorSystem("ccf-sequencer-client")
  implicit private val materializer = Materializer(system)

  /** Attempt to obtain a handshake response from the sequencer.
    * Transports can indicate in the error if the error is transient and could be retried.
    */
  override def handshake(request: HandshakeRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, HandshakeRequestError, HandshakeResponse] =
    client.handshakeUnauthenticated(request).leftMap(err => HandshakeRequestError(err.toString))

  override def sendAsync(
      request: SubmissionRequest,
      timeout: Duration,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncClientError, Unit] =
    client.sendAsync(request, requiresAuthentication = true)

  override def sendAsyncUnauthenticated(
      request: SubmissionRequest,
      timeout: Duration,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncClientError, Unit] =
    client.sendAsync(request, requiresAuthentication = false)

  override def subscribe[E](request: SubscriptionRequest, handler: SerializedEventHandler[E])(
      implicit traceContext: TraceContext
  ): SequencerSubscription[E] =
    HttpSequencerSubscription(
      request,
      handler,
      client,
      timeouts,
      requiresAuthentication = true,
      loggerFactory,
    )

  override def subscribeUnauthenticated[E](
      request: SubscriptionRequest,
      handler: SerializedEventHandler[E],
  )(implicit traceContext: TraceContext): SequencerSubscription[E] =
    HttpSequencerSubscription(
      request,
      handler,
      client,
      timeouts,
      requiresAuthentication = false,
      loggerFactory,
    )

  override val subscriptionRetryPolicy: SubscriptionErrorRetryPolicy =
    new HttpSubscriptionErrorRetryPolicy

  override protected def onClosed(): Unit =
    Lifecycle.close(
      toCloseableMaterializer(materializer, "http-sequencer-transport-materializer"),
      toCloseableActorSystem(system, logger, timeouts),
      client,
    )(logger)

  /** Acknowledge that we have successfully processed all events up to and including the given timestamp.
    * The client should then never subscribe for events from before this point.
    */
  override def acknowledge(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    // Pruning is currently not supported by the CCF sequencer and is the only use for acknowledgements,
    // so persisting acknowledgements is currently not supported.
    // Will revisit if CCF moves beyond beta integration level.
    logger.debug("acknowledgments for the http sequencer client transport are not yet implemented")
    Future.unit
  }

}
