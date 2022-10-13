// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import cats.data.EitherT
import cats.implicits.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.DelayUtil
import com.digitalasset.canton.util.Thereafter.syntax.*
import io.grpc.*
import io.grpc.stub.AbstractStub

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object CantonGrpcUtil {

  /** A retry policy for never retrying */
  def neverRetry(_error: GrpcError): Boolean = false

  def wrapErr[T](value: ParsingResult[T])(implicit
      loggingContext: ErrorLoggingContext,
      ec: ExecutionContext,
  ): EitherT[Future, CantonError, T] = {
    wrapErr(EitherT.fromEither[Future](value))
  }
  def wrapErr[T](value: EitherT[Future, ProtoDeserializationError, T])(implicit
      loggingContext: ErrorLoggingContext,
      ec: ExecutionContext,
  ): EitherT[Future, CantonError, T] = {
    value.leftMap(x => ProtoDeserializationError.ProtoDeserializationFailure.Wrap(x): CantonError)
  }

  @Deprecated
  def mapErr[T, C](value: Either[T, C])(implicit
      ec: ExecutionContext
  ): EitherT[Future, StatusRuntimeException, C] =
    mapErr(EitherT.fromEither[Future](value))

  @Deprecated
  def mapErr[T, C](value: EitherT[Future, T, C])(implicit
      ec: ExecutionContext
  ): EitherT[Future, StatusRuntimeException, C] =
    value.leftMap(x => invalidArgument(x.toString))

  def mapErrNew[T <: CantonError, C](value: Either[T, C])(implicit
      ec: ExecutionContext
  ): EitherT[Future, StatusRuntimeException, C] =
    mapErrNew(EitherT.fromEither[Future](value))

  def mapErrNew[T <: CantonError, C](value: EitherT[Future, T, C])(implicit
      ec: ExecutionContext
  ): EitherT[Future, StatusRuntimeException, C] =
    value.leftMap(_.asGrpcError)

  @Deprecated
  def invalidArgument(err: String): StatusRuntimeException =
    Status.INVALID_ARGUMENT.withDescription(err).asRuntimeException()

  /** Wrapper method for sending a Grpc request.
    * Takes care of appropriate logging and retrying.
    *
    * NOTE that this will NOT WORK for requests with streamed responses, as such requests will report errors to the
    * corresponding [[io.grpc.stub.StreamObserver]]. You need to do error handling within the corresponding
    * [[io.grpc.stub.StreamObserver]].
    *
    * @param client             the Grpc client used to send the request
    * @param serverName         used for logging
    * @param send               the client method for sending the request
    * @param requestDescription used for logging
    * @param timeout            determines how long to retry or wait for a response.
    *                           Will retry until 70% of this timeout has elapsed.
    *                           Will wait for a response until this timeout has elapsed.
    * @param logPolicy          use this to configure log levels for errors
    * @param retryPolicy        invoked after an error to determine whether to retry
    */
  def sendGrpcRequest[Svc <: AbstractStub[Svc], Res](client: Svc, serverName: String)(
      send: Svc => Future[Res],
      requestDescription: String,
      timeout: Duration,
      logger: TracedLogger,
      logPolicy: GrpcError => TracedLogger => TraceContext => Unit = err =>
        logger => traceContext => err.log(logger)(traceContext),
      retryPolicy: GrpcError => Boolean = _.retry,
  )(implicit traceContext: TraceContext): EitherT[Future, GrpcError, Res] = {
    implicit val ec: ExecutionContext = DirectExecutionContext(logger)

    // depending on the desired timeout, use a deadline or not
    val (clientWithDeadline, calcEffectiveBackoff) = timeout match {
      case finite: FiniteDuration =>
        // The deadline for individual requests.
        val requestDeadline = Deadline.after(finite.length, finite.unit)
        // After this deadline, we won't retry anymore.
        // This deadline is significantly before `requestDeadline`, because we want to avoid DEADLINE_EXCEEDED due to overly short deadlines.
        val retryDeadline = requestDeadline.offset(-finite.toMillis * 3 / 10, TimeUnit.MILLISECONDS)
        (
          client.withDeadline(requestDeadline),
          (
              backoffMs => Math.min(backoffMs, retryDeadline.timeRemaining(TimeUnit.MILLISECONDS))
          ): Long => Long,
        )
      case Duration.Inf =>
        (client, (x => x): Long => Long)
      case _ =>
        logger.error(s"Ignoring unexpected timeout ${timeout} value.")
        (client, (x => x): Long => Long)
    }

    def go(backoffMs: Long): Future[Either[GrpcError, Res]] = {
      logger.debug(s"Sending request $requestDescription to $serverName.")
      traceContext.intoGrpcContext(send(clientWithDeadline)).transformWith {
        case Success(value) =>
          logger.debug(s"Request $requestDescription has succeeded for $serverName.")
          Future.successful(Right(value))
        case Failure(e: StatusRuntimeException) =>
          val error = GrpcError(requestDescription, serverName, e)
          logPolicy(error)(logger)(traceContext)
          if (retryPolicy(error)) {
            val effectiveBackoff = calcEffectiveBackoff(backoffMs)
            if (effectiveBackoff > 0) {
              logger.info(s"Waiting for ${effectiveBackoff}ms before retrying...")
              DelayUtil
                .delay(FiniteDuration.apply(effectiveBackoff, TimeUnit.MILLISECONDS))
                .flatMap { _ =>
                  logger.info(s"Retrying request $requestDescription for $serverName...")
                  go(backoffMs * 2)
                }
            } else {
              logger.warn("Retry timeout has elapsed, giving up.")
              Future.successful(Left(error))
            }
          } else {
            logger.debug(
              s"Retry has not been configured for ${error.getClass.getSimpleName}, giving up."
            )
            Future.successful(Left(error))
          }
        case Failure(e) =>
          logger
            .error(
              s"An unexpected exception has occurred while sending request $requestDescription to $serverName.",
              e,
            )
          Future.failed(e)
      }
    }
    EitherT(go(1))
  }

  /** Method to create a grpc channel and send a single request
    *
    * Based on [[sendGrpcRequest]]
    */
  def sendSingleGrpcRequest[Svc <: AbstractStub[Svc], Res](
      serverName: String,
      requestDescription: String,
      channel: ManagedChannel,
      stubFactory: Channel => Svc,
      timeout: Duration,
      logger: TracedLogger,
      logPolicy: GrpcError => TracedLogger => TraceContext => Unit = err =>
        logger => traceContext => err.log(logger)(traceContext),
      retryPolicy: GrpcError => Boolean = _.retry,
  )(
      send: Svc => Future[Res]
  )(implicit traceContext: TraceContext): EitherT[Future, GrpcError, Res] = {

    val closeableChannel = Lifecycle.toCloseableChannel(channel, logger, "sendSingleGrpcRequest")
    val stub = stubFactory(closeableChannel.channel)

    val res = sendGrpcRequest(stub, serverName)(
      send(_),
      requestDescription,
      timeout,
      logger,
      logPolicy,
      retryPolicy,
    )

    implicit val ec = DirectExecutionContext(logger)
    res.thereafter { _ =>
      closeableChannel.close()
    }
  }

  def silentLogPolicy(error: GrpcError)(logger: TracedLogger)(traceContext: TraceContext): Unit = {
    // Log an info, if a cause is defined to not discard the cause information
    Option(error.status.getCause).foreach { cause =>
      logger.info(error.toString, cause)(traceContext)
    }
  }

  def retryUnlessClosing(closing: () => Boolean)(error: GrpcError): Boolean = {
    !closing() && error.retry
  }

  object RetryPolicy {
    lazy val noRetry: GrpcError => Boolean = _ => false
  }

  /** The name of the service that is associated with the sequencer servers' health status.
    * This name can have no relation with the gRPC services that the server is running with, and can be anything
    * as long as the client and servers use the same value.
    */
  val sequencerHealthCheckServiceName = "sequencer-health-check-service"
}
