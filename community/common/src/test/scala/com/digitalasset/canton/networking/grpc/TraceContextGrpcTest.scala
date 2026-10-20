// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.digitalasset.canton.config.ApiLoggingConfig
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, SuppressionRule}
import com.digitalasset.canton.protobuf.HelloServiceGrpc.{HelloService, HelloServiceStub}
import com.digitalasset.canton.protobuf.{Hello, HelloServiceGrpc}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import io.grpc.ServerInterceptors.intercept
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.stub.StreamObserver
import io.grpc.{CallOptions, ManagedChannel, Server}
import org.scalatest.Outcome
import org.scalatest.wordspec.FixtureAnyWordSpec
import org.slf4j.event.Level

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}

object TraceContextGrpcTest {

  private val request: Hello.Request = Hello.Request("Hello server")
  private val response: Hello.Response = Hello.Response("Hello client")

  /** Records the trace context that the server observed for the most recent call. */
  final class RecordingHelloService extends HelloService {
    private val observed = new AtomicReference[TraceContext](TraceContext.empty)

    def observedTraceContext: TraceContext = observed.get()

    override def hello(request: Hello.Request): Future[Hello.Response] = {
      observed.set(TraceContextGrpc.fromGrpcContext)
      Future.successful(response)
    }

    override def helloStreamed(
        request: Hello.Request,
        responseObserver: StreamObserver[Hello.Response],
    ): Unit = {
      observed.set(TraceContextGrpc.fromGrpcContext)
      responseObserver.onNext(response)
      responseObserver.onCompleted()
    }
  }

  final class Env(loggerFactory: NamedLoggerFactory)(implicit ec: ExecutionContext) {
    val service = new RecordingHelloService

    private val channelName: String = InProcessServerBuilder.generateName()

    val server: Server = InProcessServerBuilder
      .forName(channelName)
      .addService(
        // the trace context interceptor wraps the request logger, similar to
        // CantonCommunityServerInterceptors#interceptForLogging
        intercept(
          intercept(
            HelloServiceGrpc.bindService(service, ec),
            new GrpcRequestLoggingInterceptor(
              loggerFactory,
              ApiLoggingConfig(debugInProcessRequests = true),
            ),
          ),
          TraceContextGrpc.serverInterceptor,
        )
      )
      .build()
      .start()

    val channel: ManagedChannel = InProcessChannelBuilder
      .forName(channelName)
      .intercept(TraceContextGrpc.clientInterceptor())
      .build()

    def stub: HelloServiceStub = HelloServiceGrpc.stub(channel)

    // a single-threaded execution context that never has a gRPC `Context` attached
    val otherThread: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())

    def close(): Unit = {
      channel.shutdownNow().discard
      server.shutdownNow()
      server.awaitTermination()
      otherThread.shutdownNow().discard
    }
  }
}

class TraceContextGrpcTest extends FixtureAnyWordSpec with BaseTest with HasExecutionContext {
  import com.digitalasset.canton.tracing.TraceContextGrpc as TCG
  import TraceContextGrpcTest.*

  override type FixtureParam = Env

  override def withFixture(test: OneArgTest): Outcome = {
    val env = new Env(loggerFactory)(parallelExecutionContext)
    try {
      withFixture(test.toNoArgTest(env))
    } finally {
      env.close()
    }
  }

  private def newTraceContext(name: String): TraceContext =
    TraceContext.withNewTraceContext(name)(identity)

  "TraceContextGrpc.inferCallerTraceContext" should {

    "read back the trace context attached to a stub's call options" in { env =>
      implicit val traceContext: TraceContext = newTraceContext("round-trip")

      val stub = TCG.addTraceContextToCallOptions(env.stub)

      TCG.inferCallerTraceContext(stub.getCallOptions) shouldBe traceContext
    }

    "yield the empty trace context when neither propagation mechanism carries one" in { _ =>
      TCG.inferCallerTraceContext(CallOptions.DEFAULT).traceId shouldBe empty
    }

    "fall back to the thread-local gRPC context" in { _ =>
      val traceContext = newTraceContext("thread-local")

      TCG.withGrpcContext(traceContext) {
        TCG.inferCallerTraceContext(CallOptions.DEFAULT)
      } shouldBe traceContext
    }

    "prefer the call option over the thread-local gRPC context" in { env =>
      val callOptionTraceContext = newTraceContext("call-option")
      val threadLocalTraceContext = newTraceContext("thread-local")

      val stub =
        TCG.addTraceContextToCallOptions(env.stub)(callOptionTraceContext)

      TCG.withGrpcContext(threadLocalTraceContext) {
        TCG.inferCallerTraceContext(stub.getCallOptions)
      } shouldBe callOptionTraceContext
    }
  }

  "TraceContextGrpc.inferClientRequestTraceContext" should {

    "generate a trace context when the caller supplied none" in { _ =>
      TCG
        .inferClientRequestTraceContext(CallOptions.DEFAULT, "fresh")
        .traceId shouldBe defined
    }

    "keep supplied trace context" in { env =>
      val traceContext = newTraceContext("caller")
      val stub = TCG.addTraceContextToCallOptions(env.stub)(traceContext)

      TCG.inferClientRequestTraceContext(
        stub.getCallOptions,
        "fresh",
      ) shouldBe traceContext
    }
  }

  "The client interceptor" should {

    "propagate the trace context attached to the stub's call options" in { env =>
      implicit val traceContext: TraceContext = newTraceContext("call-option")

      val stub = TCG.addTraceContextToCallOptions(env.stub)

      stub.hello(request).futureValue shouldBe response
      env.service.observedTraceContext.traceId shouldBe traceContext.traceId
    }

    "propagate the trace context across a Future boundary" in { env =>
      implicit val traceContext: TraceContext = newTraceContext("across-futures")

      // Mimics the ledger client: the trace context is in scope on the calling
      // thread, but the call itself is issued from a Future callback running on
      // a thread with no gRPC Context.
      val stub = TCG.withGrpcContext(traceContext) {
        TCG.addTraceContextToCallOptions(env.stub)
      }

      Future.unit
        .flatMap(_ => stub.hello(request))(env.otherThread)
        .futureValue shouldBe response
      env.service.observedTraceContext.traceId shouldBe traceContext.traceId
    }

    "not propagate the thread-local trace context across a Future boundary" in { env =>
      val traceContext = newTraceContext("across-futures-thread-local")

      val stub = TCG.withGrpcContext(traceContext)(env.stub)

      Future.unit
        .flatMap(_ => stub.hello(request))(env.otherThread)
        .futureValue shouldBe response
      env.service.observedTraceContext.traceId should not be traceContext.traceId
      // the client interceptor still produces a trace id
      env.service.observedTraceContext.traceId shouldBe defined
    }

    "fall back to the thread-local gRPC context" in { env =>
      val traceContext = newTraceContext("legacy")

      TCG
        .withGrpcContext(traceContext)(env.stub.hello(request))
        .futureValue shouldBe response
      env.service.observedTraceContext.traceId shouldBe traceContext.traceId
    }

    "prefer the call option over the thread-local gRPC context" in { env =>
      val callOptionTraceContext = newTraceContext("call-option")
      val threadLocalTraceContext = newTraceContext("thread-local")

      val stub =
        TCG.addTraceContextToCallOptions(env.stub)(callOptionTraceContext)

      TCG
        .withGrpcContext(threadLocalTraceContext)(stub.hello(request))
        .futureValue shouldBe response
      env.service.observedTraceContext.traceId shouldBe callOptionTraceContext.traceId
    }
  }

  "A server handler" should {

    "see a trace id when the caller supplied none" in { env =>
      // The caller has no trace context, so before the client created one the
      // request reached the server without a traceparent: the request logger
      // then logged under a trace id of its own, while the handler saw the
      // empty trace context.
      loggerFactory.assertLogsSeq(
        SuppressionRule.LevelAndAbove(Level.DEBUG) &&
          SuppressionRule.LoggerNameContains("ApiRequestLogger")
      )(
        env.stub.hello(request).futureValue shouldBe response,
        entries => {
          val handlerTraceId = env.service.observedTraceContext.traceId
          handlerTraceId shouldBe defined
          entries.flatMap(_.mdc.get("trace-id")).distinct shouldBe handlerTraceId.toList
        },
      )
    }
  }
}
