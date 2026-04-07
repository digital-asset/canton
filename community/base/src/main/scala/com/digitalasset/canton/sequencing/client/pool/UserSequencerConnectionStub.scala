// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.pool

import cats.data.EitherT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, GrpcError}
import com.digitalasset.canton.sequencer.api.v30.AcknowledgeSignedResponse
import com.digitalasset.canton.sequencing.SequencedEventHandler
import com.digitalasset.canton.sequencing.client.SequencerSubscription
import com.digitalasset.canton.sequencing.client.pool.UserSequencerConnectionStub.DefaultSendAsyncLogPolicy
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
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError.ExceededMaxSequencingTime
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.duration.Duration

/** A generic stub for user interactions with a sequencer. This trait attempts to be independent of
  * the underlying transport.
  *
  * NOTE: We currently make only a minimal effort to keep transport independence, and there are
  * obvious leaks. This will be extended when we need it.
  */
trait UserSequencerConnectionStub extends NamedLogging {
  import com.digitalasset.canton.sequencing.client.pool.SequencerConnectionStub.*

  def sendAsync(
      request: SignedContent[SubmissionRequest],
      timeout: Duration,
      retryPolicy: GrpcError => Boolean = CantonGrpcUtil.RetryPolicy.noRetry,
      logPolicy: CantonGrpcUtil.GrpcLogPolicy = DefaultSendAsyncLogPolicy,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionStubError.ConnectionError, Unit]

  def acknowledgeSigned(
      signedRequest: SignedContent[AcknowledgeRequest],
      timeout: Duration,
      retryPolicy: GrpcError => Boolean = CantonGrpcUtil.RetryPolicy.noRetry,
      logPolicy: CantonGrpcUtil.GrpcLogPolicy = CantonGrpcUtil.DefaultGrpcLogPolicy,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionStubError, AcknowledgeSignedResponse]

  def getTrafficStateForMember(
      request: GetTrafficStateForMemberRequest,
      timeout: Duration,
      retryPolicy: GrpcError => Boolean = CantonGrpcUtil.RetryPolicy.noRetry,
      logPolicy: CantonGrpcUtil.GrpcLogPolicy = CantonGrpcUtil.DefaultGrpcLogPolicy,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    SequencerConnectionStubError,
    GetTrafficStateForMemberResponse,
  ]

  def getTime(
      timeout: Duration,
      retryPolicy: GrpcError => Boolean = CantonGrpcUtil.RetryPolicy.noRetry,
      logPolicy: CantonGrpcUtil.GrpcLogPolicy = CantonGrpcUtil.DefaultGrpcLogPolicy,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionStubError, Option[CantonTimestamp]]

  def downloadTopologyStateForInit(request: TopologyStateForInitRequest, timeout: Duration)(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    SequencerConnectionStubError,
    TopologyStateForInitResponse,
  ]

  def subscribe[E](
      request: SubscriptionRequest,
      handler: SequencedEventHandler[E],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): Either[SequencerConnectionStubError, SequencerSubscription[E]]

  def downloadTopologyStateForInitHash(
      request: TopologyStateForInitRequest,
      timeout: Duration,
      retryPolicy: GrpcError => Boolean = CantonGrpcUtil.RetryPolicy.noRetry,
      logPolicy: CantonGrpcUtil.GrpcLogPolicy = CantonGrpcUtil.DefaultGrpcLogPolicy,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionStubError, TopologyStateForInitHashResponse]
}

object UserSequencerConnectionStub {

  /** Does not log if request is refused due to the max sequencing time having elapsed. */
  val DefaultSendAsyncLogPolicy = new CantonGrpcUtil.FilteredGrpcLogPolicy({ error =>
    error.code.id == ExceededMaxSequencingTime.code.id
  })
}
