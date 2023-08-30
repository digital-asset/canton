// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import akka.stream.scaladsl.Keep
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.akka.ClientAdapter
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.SequencerClientMetrics
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.client.{
  SequencerSubscriptionAkka,
  SubscriptionErrorRetryPolicy,
}
import com.digitalasset.canton.sequencing.protocol.{SubscriptionRequest, SubscriptionResponse}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.AkkaUtil.syntax.*
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.Context.CancellableContext
import io.grpc.stub.StreamObserver
import io.grpc.{CallOptions, Context, ManagedChannel}

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

class GrpcSequencerClientTransportAkka(
    channel: ManagedChannel,
    callOptions: CallOptions,
    clientAuth: GrpcSequencerClientAuth,
    metrics: SequencerClientMetrics,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
    protocolVersion: ProtocolVersion,
)(implicit executionContext: ExecutionContext, executionSequencerFactory: ExecutionSequencerFactory)
// TODO(#13789) Extend GrpcSequencerClientTransportCommon and drop support for non-Akka subscriptions
    extends GrpcSequencerClientTransport(
      channel,
      callOptions,
      clientAuth,
      metrics,
      timeouts,
      loggerFactory,
      protocolVersion,
    )
    with SequencerClientTransportAkka {

  import GrpcSequencerClientTransportAkka.*

  override type SubscriptionError = GrpcSequencerSubscriptionError

  override def subscribe(request: SubscriptionRequest)(implicit
      traceContext: TraceContext
  ): SequencerSubscriptionAkka[SubscriptionError] =
    subscribeInternal(request, requiresAuthentication = true)

  override def subscribeUnauthenticated(request: SubscriptionRequest)(implicit
      traceContext: TraceContext
  ): SequencerSubscriptionAkka[SubscriptionError] =
    subscribeInternal(request, requiresAuthentication = false)

  private def subscribeInternal(
      subscriptionRequest: SubscriptionRequest,
      requiresAuthentication: Boolean,
  )(implicit traceContext: TraceContext): SequencerSubscriptionAkka[SubscriptionError] = {

    val subscriptionRequestP = subscriptionRequest.toProtoV0

    def mkSubscription[Resp: HasProtoTraceContext](
        subscriber: (v0.SubscriptionRequest, StreamObserver[Resp]) => Unit
    )(
        parseResponse: (Resp, TraceContext) => ParsingResult[SubscriptionResponse]
    ): SequencerSubscriptionAkka[SubscriptionError] = {
      val source = ClientAdapter
        .serverStreaming[v0.SubscriptionRequest, Resp](
          subscriptionRequestP,
          stubWithFreshContext(subscriber),
        )
        .map(Right(_))
        .recover { case NonFatal(e) => Left(GrpcStreamFailure(e)) }
        // Everything up to here runs "synchronously" and can deal with cancellations
        // without causing shutdown synchronization problems
        // Technically, everything below until `takeUntilThenDrain` also could deal with
        // cancellations just fine, but we nevertheless establish the pattern here
        // to see how well it scales
        .withUniqueKillSwitchMat()(Keep.right)
        .map(
          _.map(
            _.flatMap(
              deserializeSubscriptionResponse(_)(parseResponse).leftMap(ResponseParseError)
            )
          )
        )
        // Stop emitting after the first parse error
        .takeUntilThenDrain(_.isLeft)
        .map(_.unwrap)
        .watchTermination()(Keep.both)
      SequencerSubscriptionAkka(source)
    }

    if (protocolVersion >= ProtocolVersion.v5) {
      val subscriber =
        if (requiresAuthentication) sequencerServiceClient.subscribeVersioned _
        else sequencerServiceClient.subscribeUnauthenticatedVersioned _

      mkSubscription(subscriber)(SubscriptionResponse.fromVersionedProtoV0(_)(_))
    } else {
      val subscriber =
        if (requiresAuthentication) sequencerServiceClient.subscribe _
        else sequencerServiceClient.subscribeUnauthenticated _
      mkSubscription(subscriber)(SubscriptionResponse.fromProtoV0(_)(_))
    }

  }

  private def stubWithFreshContext[Req, Resp](
      stub: (Req, StreamObserver[Resp]) => Unit
  )(req: Req, obs: StreamObserver[Resp])(implicit traceContext: TraceContext): Unit = {
    // we intentionally don't use `Context.current()` as we don't want to inherit the
    // cancellation scope from upstream requests
    val context: CancellableContext = Context.ROOT.withCancellation()

    context.run { () =>
      TraceContextGrpc.withGrpcContext(traceContext) {
        stub(req, obs)
      }
    }
  }

  private def deserializeSubscriptionResponse[R: HasProtoTraceContext](subscriptionResponseP: R)(
      fromProto: (R, TraceContext) => ParsingResult[SubscriptionResponse]
  ): ParsingResult[OrdinarySerializedEvent] = {
    // we take the unusual step of immediately trying to deserialize the trace-context
    // so it is available here for logging
    implicit val traceContext: TraceContext = SerializableTraceContext
      .fromProtoSafeV0Opt(noTracingLogger)(
        implicitly[HasProtoTraceContext[R]].traceContext(subscriptionResponseP)
      )
      .unwrap
    logger.debug("Received a message from the sequencer.")
    fromProto(subscriptionResponseP, traceContext).map { response =>
      OrdinarySequencedEvent(response.signedSequencedEvent, response.trafficState)(
        response.traceContext
      )
    }
  }

  override def subscriptionRetryPolicy: SubscriptionErrorRetryPolicy =
    new GrpcSubscriptionErrorRetryPolicy(loggerFactory)
}

object GrpcSequencerClientTransportAkka {
  sealed trait GrpcSequencerSubscriptionError extends Product with Serializable

  final case class GrpcStreamFailure(ex: Throwable) extends GrpcSequencerSubscriptionError
  final case class ResponseParseError(error: ProtoDeserializationError)
      extends GrpcSequencerSubscriptionError
}
