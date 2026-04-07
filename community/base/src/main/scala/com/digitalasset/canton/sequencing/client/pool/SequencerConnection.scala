// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.pool

import cats.data.EitherT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.sequencing.SequencedEventHandler
import com.digitalasset.canton.sequencing.client.SendAsyncClientError.SendAsyncClientResponseError
import com.digitalasset.canton.sequencing.client.{
  SequencerSubscription,
  SequencerSubscriptionPekko,
  SubscriptionErrorRetryPolicy,
  SubscriptionErrorRetryPolicyPekko,
}
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  GetTrafficStateForMemberRequest,
  GetTrafficStateForMemberResponse,
  SignedContent,
  SubmissionRequest,
  SubscriptionRequest,
  TopologyStateForInitHashResponse,
  TopologyStateForInitRequest,
  TopologyStateForInitResponse,
}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.Status

import scala.concurrent.duration.Duration

import Connection.ConnectionConfig
import InternalSequencerConnection.{ConnectionAttributes, SequencerConnectionHealth}

/** A connection to a sequencer. This trait attempts to be independent of the underlying transport.
  *
  * NOTE: We currently make only a minimal effort to keep transport independence, and there are
  * obvious leaks. This will be extended when we need it.
  */
trait SequencerConnection extends FlagCloseable with NamedLogging {

  def name: String

  def health: SequencerConnectionHealth

  def config: ConnectionConfig

  def attributes: ConnectionAttributes

  def fail(reason: String)(implicit traceContext: TraceContext): Unit

  def fatal(reason: String)(implicit traceContext: TraceContext): Unit

  def sendAsync(
      request: SignedContent[SubmissionRequest],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SendAsyncClientResponseError, Unit]

  def acknowledgeSigned(signedRequest: SignedContent[AcknowledgeRequest], timeout: Duration)(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Boolean]

  def getTrafficStateForMember(
      request: GetTrafficStateForMemberRequest,
      timeout: Duration,
      logPolicy: CantonGrpcUtil.GrpcLogPolicy = CantonGrpcUtil.DefaultGrpcLogPolicy,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, GetTrafficStateForMemberResponse]

  def logout()(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, Status, Unit]

  /** Fetches the "current" sequencing time */
  def getTime(timeout: Duration)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Option[CantonTimestamp]]

  def downloadTopologyStateForInit(request: TopologyStateForInitRequest, timeout: Duration)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, TopologyStateForInitResponse]

  def downloadTopologyStateForInitHash(request: TopologyStateForInitRequest, timeout: Duration)(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, TopologyStateForInitHashResponse]

  def subscribe[E](
      request: SubscriptionRequest,
      handler: SequencedEventHandler[E],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): Either[String, SequencerSubscription[E]]

  /** Determine which errors will cause the sequencer client to not try to reestablish a
    * subscription
    */
  def subscriptionRetryPolicy: SubscriptionErrorRetryPolicy
}

/** A connection to a sequencer with an alternative `subscribe` method.
  *
  * This is only used for the direct connection from a sequencer to itself.
  */
trait SequencerConnectionWithPekkoSubscribe extends SequencerConnection {
  type SubscriptionError

  /** Create a single subscription to read events from the Sequencer for this member starting from
    * the counter defined in the request. The transport is not expected to provide retries of
    * subscriptions.
    */
  def subscribe(request: SubscriptionRequest)(implicit
      traceContext: TraceContext
  ): SequencerSubscriptionPekko[SubscriptionError]

  /** The transport can decide which errors will cause the sequencer client to not try to
    * reestablish a subscription
    */
  def subscriptionRetryPolicyPekko: SubscriptionErrorRetryPolicyPekko[SubscriptionError]
}

object SequencerConnectionWithPekkoSubscribe {
  type Aux[E] = SequencerConnectionWithPekkoSubscribe { type SubscriptionError = E }
}
