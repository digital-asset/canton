// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import io.grpc.*
import io.grpc.Context as GrpcContext
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener
import io.grpc.stub.AbstractStub
import io.opentelemetry.api.trace.{Span, Tracer}

import scala.util.{Try, Using}

/** Support for propagating TraceContext values across GRPC boundaries. Includes:
  *   - a client interceptor for setting context values when sending requests to a server
  *   - a server interceptor for receiving context values when receiving requests from a client
  */
object TraceContextGrpc {
  // value of trace context in the GRPC Context
  private val TraceContextKey =
    Context.keyWithDefault[TraceContext]("traceContext", TraceContext.empty)

  /** Value of the trace context in a call's [[io.grpc.CallOptions]].
    *
    * Unlike `TraceContextKey`, call options travel with the stub value rather than with the thread,
    * so they survive arbitrary thread hops and `Future` composition.
    */
  val TraceContextOptionsKey: CallOptions.Key[TraceContext] =
    CallOptions.Key.create[TraceContext]("traceContext")

  def fromGrpcContext: TraceContext = TraceContextKey.get()

  def fromGrpcContextOrNew(name: String): TraceContext = {
    val grpcTraceContext = TraceContextGrpc.fromGrpcContext
    if (grpcTraceContext.traceId.isDefined) {
      grpcTraceContext
    } else {
      TraceContext.withNewTraceContext(name)(identity)
    }
  }

  def withGrpcTraceContext[A](f: TraceContext => A): A = f(fromGrpcContext)

  def withGrpcContext[A](traceContext: TraceContext)(fn: => A): A = {
    val context = GrpcContext.current().withValue(TraceContextKey, traceContext)

    context.call(() => fn)
  }

  /** Attaches `traceContext` to the stub's call options, so that it is propagated to the server
    * even if the call is later issued from a different thread, e.g. from a `Future` callback.
    */
  def addTraceContextToCallOptions[T <: AbstractStub[T]](
      stub: T
  )(implicit traceContext: TraceContext): T =
    stub.withOption(TraceContextOptionsKey, traceContext)

  /** The trace context the caller of a GRPC call intended.
    *
    * Prefers the call option set by [[addTraceContextToCallOptions]] and falls back to the
    * thread-local [[io.grpc.Context]]. A call issued from a thread without an attached GRPC context
    * yields the empty trace context; callers that want a usable trace id regardless should use
    * [[inferClientRequestTraceContext]].
    */
  def inferCallerTraceContext(callOptions: CallOptions): TraceContext =
    Option(callOptions.getOption(TraceContextOptionsKey)).getOrElse(fromGrpcContext)

  /** The trace context to report an outgoing GRPC call under, or a new one named `name` if the
    * caller supplied none.
    */
  def inferClientRequestTraceContext(callOptions: CallOptions, name: String): TraceContext = {
    val callerTraceContext = inferCallerTraceContext(callOptions)
    if (callerTraceContext.traceId.isDefined) callerTraceContext
    else TraceContext.withNewTraceContext(name)(identity)
  }

  private implicit final class TryFailedOps[A](private val a: Try[A]) extends AnyVal {
    @inline
    def valueOrThrow: A = a.fold(throw _, identity)
  }

  /** Injects headers TraceContext from into headers
    * @param wrappedInterceptor
    *   an optional interceptor to wrap with traceContext.context.makeCurrent(), to allow
    *   interoperability with GrpcTelemetry or other context-propagating interceptors
    */
  def clientInterceptor(wrappedInterceptor: Option[ClientInterceptor] = None): ClientInterceptor =
    new TraceContextClientInterceptor(wrappedInterceptor)
  def serverInterceptor: ServerInterceptor = new TraceContextServerInterceptor

  def reportingServerInterceptor(tracer: Tracer): ServerInterceptor =
    new TraceContextReportingServerInterceptor(tracer)

  private class TraceContextClientInterceptor(wrappedInterceptor: Option[ClientInterceptor])
      extends ClientInterceptor {
    override def interceptCall[ReqT, RespT](
        method: MethodDescriptor[ReqT, RespT],
        callOptions: CallOptions,
        next: Channel,
    ): ClientCall[ReqT, RespT] = {
      val traceContext = inferClientRequestTraceContext(callOptions, "grpc-client")
      val contextToPropagate = traceContext.context

      def withPropagatedContext[T](fn: => T): T =
        wrappedInterceptor match {
          case Some(_) =>
            Using(contextToPropagate.makeCurrent()) { _ =>
              fn
            }.valueOrThrow
          case None => fn
        }

      val nextCall = withPropagatedContext {
        wrappedInterceptor.fold(next.newCall(method, callOptions))(
          _.interceptCall(method, callOptions, next)
        )
      }

      new SimpleForwardingClientCall[ReqT, RespT](nextCall) {

        override def start(
            responseListener: ClientCall.Listener[RespT],
            headers: Metadata,
        ): Unit = {

          W3CTraceContext.injectIntoGrpcMetadata(traceContext, headers)

          super.start(responseListener, headers)
        }
      }
    }
  }

  private class TraceContextServerInterceptor extends ServerInterceptor {
    override def interceptCall[ReqT, RespT](
        call: ServerCall[ReqT, RespT],
        headers: Metadata,
        next: ServerCallHandler[ReqT, RespT],
    ): ServerCall.Listener[ReqT] = {
      val traceContext = W3CTraceContext.fromGrpcMetadata(headers)
      val context = GrpcContext
        .current()
        .withValue(TraceContextKey, traceContext)
      Contexts.interceptCall(context, call, headers, next)
    }
  }

  private class TraceContextReportingServerInterceptor(tracer: Tracer) extends ServerInterceptor {
    override def interceptCall[ReqT, RespT](
        call: ServerCall[ReqT, RespT],
        headers: Metadata,
        next: ServerCallHandler[ReqT, RespT],
    ): ServerCall.Listener[ReqT] = {

      val traceContextFromWire = W3CTraceContext.fromGrpcMetadata(headers)
      val parentTraceContext =
        if (traceContextFromWire.traceId.isEmpty)
          TraceContext.withNewTraceContext("grpc-server")(identity)
        else
          traceContextFromWire

      val span = tracer
        .spanBuilder(call.getMethodDescriptor.getFullMethodName)
        .setParent(parentTraceContext.context)
        .startSpan()
      val traceContext = TraceContext(span.storeInContext(parentTraceContext.context))

      val context = GrpcContext
        .current()
        .withValue(TraceContextKey, traceContext)

      val nextServerCallListener = Contexts.interceptCall(context, call, headers, next)
      new ServerCallListener(nextServerCallListener, span)
    }

    /** Intercepts events sent by the client.
      */
    class ServerCallListener[ReqT, RespT](
        delegate: ServerCall.Listener[ReqT],
        span: Span,
    ) extends SimpleForwardingServerCallListener[ReqT](delegate) {

      /** Called when the server receives the request. */
      override def onMessage(message: ReqT): Unit =
        delegate.onMessage(message)

      /** Called when the client completed all message sending (except for cancellation). */
      override def onHalfClose(): Unit =
        delegate.onHalfClose()

      /** Called when the client cancels the call. */
      override def onCancel(): Unit = {
        delegate.onCancel()
        span.end()
      }

      /** Called when the server considers the call completed. */
      override def onComplete(): Unit = {
        delegate.onComplete()
        span.end()
      }

      override def onReady(): Unit =
        delegate.onReady()
    }
  }
}
