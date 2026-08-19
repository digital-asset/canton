// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.client

import cats.data.EitherT
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.{PositiveFiniteDuration, ProcessingTimeout}
import com.digitalasset.canton.ledger.error.CommonErrors.ServiceNotRunning
import com.digitalasset.canton.ledger.error.{CommonErrors, LedgerApiErrors}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  NamedLoggerFactory,
  NamedLogging,
  TracedLogger,
}
import com.digitalasset.canton.networking.grpc.{
  CantonGrpcUtil,
  GrpcClient,
  GrpcError,
  GrpcManagedChannel,
}
import com.digitalasset.canton.tea.TrafficEnforcementErrors
import com.digitalasset.canton.tea.v1.*
import com.digitalasset.canton.tea.v1.TrafficServiceGrpc.TrafficServiceStub
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.{ManagedChannel, StatusRuntimeException}

import scala.annotation.unused
import scala.concurrent.ExecutionContext

/** gRPC client to the external traffic service used for local traffic enforcement operations by the
  * participant node.
  *
  * As part of the proxied traffic service operations, this client handles retries with timeouts and
  * logging. It does not normalize failures: both methods return the raw `GrpcError`, and the caller
  * is responsible for passing it through `normalizeTeaError` before it reaches a Ledger API client.
  *
  * @param channel
  *   The managed channel to the traffic service. This channel is closed when this client is closed.
  */
class RichTrafficServiceClient(
    channel: ManagedChannel,
    override protected val timeouts: ProcessingTimeout,
    accountLookupTimeout: PositiveFiniteDuration,
    override val loggerFactory: NamedLoggerFactory,
) extends FlagCloseable
    with NamedLogging {

  private val serverName: String = "traffic-service"

  private val managedChannel: GrpcManagedChannel =
    GrpcManagedChannel("traffic-service-channel", channel, this, logger)

  private val client: GrpcClient[TrafficServiceStub] =
    GrpcClient.create(managedChannel, TrafficServiceGrpc.stub)

  def getAccount(
      request: GetAccountRequest
  )(implicit
      traceContext: TraceContext,
      @unused ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, GrpcError, GetAccountResponse] =
    CantonGrpcUtil.sendGrpcRequest(client, serverName)(
      _.getAccount(request),
      requestDescription = "get-account",
      timeout = accountLookupTimeout.duration,
      logger = logger,
      logPolicy = RichTrafficServiceClient.doNotLogRefusedRequests,
      retryPolicy = RichTrafficServiceClient.retryUnlessClientGaveUp,
    )

  def updateAccount(
      request: UpdateAccountRequest
  )(implicit
      traceContext: TraceContext,
      @unused ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, GrpcError, UpdateAccountResponse] =
    CantonGrpcUtil.sendGrpcRequest(client, serverName)(
      _.updateAccount(request),
      requestDescription = "update-account",
      timeout = timeouts.network.duration,
      logger = logger,
      logPolicy = RichTrafficServiceClient.doNotLogRefusedRequests,
      retryPolicy = RichTrafficServiceClient.retryUnlessClientGaveUp,
    )
}

object RichTrafficServiceClient {

  private[client] def retryUnlessClientGaveUp(error: GrpcError): Boolean = error match {
    case _: GrpcError.GrpcClientGaveUp => false
    case other => other.retry
  }

  private def isFromTrafficService(error: GrpcError): Boolean = error match {
    case _: GrpcError.GrpcClientError => true
    case other =>
      other.decodedCantonError.exists(decoded =>
        TrafficEnforcementErrors.allErrorIds.contains(decoded.code.id)
      )
  }

  /** TEA errors are passed through, so their id and retry info survive. Everything else isn't safe
    * to expose to a Ledger API client, so it's replaced with an error type we control.
    */
  private[apiserver] def normalizeTeaError(
      error: GrpcError
  )(implicit errorLoggingContext: ErrorLoggingContext): StatusRuntimeException =
    if (isFromTrafficService(error)) error.asRuntimeException
    else
      error match {
        case _: GrpcError.GrpcServiceUnavailable =>
          ServiceNotRunning.Reject("User traffic service").asGrpcError
        case _: GrpcError.GrpcClientGaveUp =>
          CommonErrors.RequestTimeOut
            .Reject(
              "The request to the traffic service did not complete in time.",
              definiteAnswer = false,
            )
            .asGrpcError
        case other =>
          // Redacted category: this cause reaches the log only, never the client.
          LedgerApiErrors.InternalError
            .Generic(s"Error in submitting request to traffic service:\n$other")
            .asGrpcError
      }

  /** A refused request is the caller's mistake, and the traffic service already logged it where it
    * was raised, so the participant doesn't need to log it again at ERROR. This only holds while
    * the server is in-process and logs into the same file.
    *
    * TODO(#34917): make this configurable once external TEA is supported, since its logs go
    * somewhere else.
    */
  private[client] val doNotLogRefusedRequests: CantonGrpcUtil.GrpcLogPolicy =
    new CantonGrpcUtil.GrpcLogPolicy {
      override def log(error: GrpcError, logger: TracedLogger)(implicit
          traceContext: TraceContext
      ): Unit = error match {
        case _: GrpcError.GrpcClientError => ()
        case other => other.log(logger)
      }
    }

  def toInternalServer(
      grpcChannelName: String,
      timeout: ProcessingTimeout,
      accountLookupTimeout: PositiveFiniteDuration,
      loggerFactory: NamedLoggerFactory,
  )(implicit executor: ExecutionContextIdlenessExecutorService): RichTrafficServiceClient = {
    val channel =
      InProcessChannelBuilder
        .forName(grpcChannelName)
        .executor(executor)
        .build()

    new RichTrafficServiceClient(
      channel,
      timeout,
      accountLookupTimeout,
      loggerFactory,
    )
  }
}
