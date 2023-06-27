// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.grpc.multidomain

import akka.NotUsed
import akka.stream.*
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.ledger.error.{CommonErrors, LedgerApiErrors}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import io.grpc.{StatusException, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}

private[participant] trait GrpcStream extends AutoCloseable {

  /** Materializes the akka-stream Source, and wires it to the gRPC response stream.
    * After this call is finished, the source is materialized and starts pushing the data to the gRPC consumer.
    *
    * @param grpcObserver The stream observer which represents the streaming gRPC consumer.
    * @param source The akka-stream source representing the data-flow of the result.
    */
  def apply[T](
      grpcObserver: StreamObserver[T]
  )(source: Source[T, NotUsed])(implicit traceContext: TraceContext): Unit
}

private[participant] object GrpcStream {

  def apply(
      transportExecutionContext: ExecutionContext,
      materializer: Materializer,
      namedLoggerFactory: NamedLoggerFactory,
  ): GrpcStream = new GrpcStream with NamedLogging {
    private val killSwitch = KillSwitches.shared("gRPC-API")

    override def apply[T](
        grpcObserver: StreamObserver[T]
    )(source: Source[T, NotUsed])(implicit traceContext: TraceContext): Unit =
      source
        .via(killSwitch.flow)
        .runWith(
          grpcSink(grpcObserver, transportExecutionContext)(errorLoggingContext(traceContext))
        )(materializer)
        .discard

    // TODO(#11002) Ensure that not waiting for termination here is enough (authors comment: it seems to be in participant-integration-api)
    override def close(): Unit = TraceContext.withNewTraceContext(implicit tc =>
      killSwitch.abort(
        GrpcExceptionFactory(() => Right(CommonErrors.ServerIsShuttingDown.Reject().asGrpcError))
      )
    )

    override val loggerFactory: NamedLoggerFactory = namedLoggerFactory
  }

  private def grpcSink[T](
      grpcObserver: StreamObserver[T],
      transportExecutionContext: ExecutionContext,
  )(implicit errorLoggingContext: ErrorLoggingContext): Sink[T, NotUsed] = {
    val observer = tryCastToServerCallStreamObserver(grpcObserver)
    Flow[T]
      .mapAsync(1)(t =>
        // this is where scalaPB DTOs will be serialized, and transported to network
        Future(
          // if cancellation from downstream (like gRPC client), it is okay to skip, since the stream is already
          // being teared down
          if (!observer.isCancelled) {
            // it is okay to have onNext even if the gRPC downstream not ready. This will be buffered
            observer.onNext(t)
          }
        )(transportExecutionContext)
      )
      .to(
        // the sink only operates on Unit-s, but here is backpressure and akka/grpc stream synchronization wired
        Sink.fromGraph(
          new GraphStage[SinkShape[Unit]] {
            private val in = Inlet[Unit]("in")

            override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
              new GraphStageLogic(shape) with InHandler {
                override def onPush(): Unit = tryAkkaStreamGrpcTransfer()
                override def onUpstreamFinish(): Unit = observer.onCompleted()
                override def onUpstreamFailure(t: Throwable): Unit =
                  observer.onError(t match {
                    case grpcException: StatusRuntimeException => grpcException
                    case grpcException: StatusException => grpcException
                    case GrpcExceptionFactory(createGrpcException) =>
                      // In case of a GrpcExceptionFactory we create the gRPC error objects here.
                      // This is needed so we are capable to send one Throwable to many streams.
                      // (io.grpc Errors are not thread safe, each stream needs it's own instance)
                      createGrpcException().fold(identity, identity)
                    case throwable =>
                      LedgerApiErrors.InternalError
                        .UnexpectedOrUnknownException(throwable)
                        .asGrpcError
                  })

                override def preStart(): Unit = pull(in) // start pulling after initialization

                private def tryAkkaStreamGrpcTransfer(): Unit =
                  if (!observer.isCancelled && observer.isReady && isAvailable(in)) {
                    // backpressure: we only pull if we had an onNext running previously, and the observer is ready
                    grab(in)
                    tryPull(in)
                  }

                setHandler(in, this)

                private def asyncCallback(computation: => Unit): Runnable = {
                  // for thread safety the getAsyncCallback must be called at createLogic construction
                  val callback = getAsyncCallback[Unit](_ => computation)
                  () => callback.invoke(())
                }
                // If gRPC stream is ready to receive the next element, we tryAkkaStreamGrpcTransfer
                observer.setOnReadyHandler(asyncCallback(tryAkkaStreamGrpcTransfer()))
                observer.setOnCancelHandler(
                  asyncCallback(cancelStage(new RuntimeException("cancelled by gRPC client")))
                )
                observer.setOnCloseHandler(asyncCallback(completeStage()))
              }

            override val shape: SinkShape[Unit] = SinkShape.of(in)
          }
        )
      )
  }

  // the provided StreamObserver in case server side streaming scenario must be a ServerCallStreamObserver
  private def tryCastToServerCallStreamObserver[T](
      observer: StreamObserver[T]
  ): ServerCallStreamObserver[T] =
    observer match {
      case o: ServerCallStreamObserver[T] => o
      case invalid =>
        throw new IllegalStateException(
          s"ServerCallStreamObserver expected, but got: ${invalid.getClass.getName}"
        )
    }

  // Support for error broadcasting
  final case class GrpcExceptionFactory(
      createGrpcException: () => Either[StatusException, StatusRuntimeException]
  ) extends RuntimeException
}
