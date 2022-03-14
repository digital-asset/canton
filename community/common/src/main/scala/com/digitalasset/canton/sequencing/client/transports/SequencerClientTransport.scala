// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import cats.data.EitherT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.sequencing.SerializedEventHandler
import com.digitalasset.canton.sequencing.client.{
  SendAsyncClientError,
  SequencerSubscription,
  SubscriptionErrorRetryPolicy,
}
import com.digitalasset.canton.sequencing.handshake.SupportsHandshake
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.Future
import scala.concurrent.duration.Duration

/** Implementation dependent operations for a client to read and write to a domain sequencer. */
trait SequencerClientTransport extends FlagCloseable with SupportsHandshake {

  /** Sends a submission request to the sequencer.
    * If we failed to make the request, an error will be returned.
    * If the sequencer accepted (or may have accepted) the request this call will return successfully.
    */
  def sendAsync(request: SubmissionRequest, timeout: Duration, protocolVersion: ProtocolVersion)(
      implicit traceContext: TraceContext
  ): EitherT[Future, SendAsyncClientError, Unit]

  def sendAsyncUnauthenticated(
      request: SubmissionRequest,
      timeout: Duration,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncClientError, Unit]

  /** Create a single subscription to read events from the Sequencer for this member starting from the counter defined in the request.
    * Transports are currently responsible for calling the supplied handler.
    * The handler must not be called concurrently and must receive events in-order.
    * If the handler fails with an exception the subscription should close with a [[com.digitalasset.canton.sequencing.client.SubscriptionCloseReason.HandlerError]].
    * If the subscription fails for a technical reason it should close with a [[com.digitalasset.canton.sequencing.client.SubscriptionCloseReason.SubscriptionError]].
    * The transport is not expected to provide retries of subscriptions.
    */
  def subscribe[E](request: SubscriptionRequest, handler: SerializedEventHandler[E])(implicit
      traceContext: TraceContext
  ): SequencerSubscription[E]

  def subscribeUnauthenticated[E](request: SubscriptionRequest, handler: SerializedEventHandler[E])(
      implicit traceContext: TraceContext
  ): SequencerSubscription[E]

  /** Acknowledge that we have successfully processed all events up to and including the given timestamp.
    * The client should then never subscribe for events from before this point.
    */
  def acknowledge(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** The transport can decide which errors will cause the sequencer client to not try to reestablish a subscription */
  def subscriptionRetryPolicy: SubscriptionErrorRetryPolicy
}
