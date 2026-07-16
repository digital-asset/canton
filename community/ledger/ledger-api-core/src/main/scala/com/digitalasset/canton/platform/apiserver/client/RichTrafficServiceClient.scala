// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.client

import cats.data.EitherT
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.ledger.error.CommonErrors.ServiceNotRunning
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.{
  CantonGrpcUtil,
  GrpcClient,
  GrpcError,
  GrpcManagedChannel,
}
import com.digitalasset.canton.tea.v1.*
import com.digitalasset.canton.tea.v1.TrafficServiceGrpc.TrafficServiceStub
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import io.grpc.ManagedChannel
import io.grpc.inprocess.InProcessChannelBuilder

import scala.concurrent.ExecutionContext

/** gRPC client to the external traffic service used for local traffic enforcement operations by the
  * participant node.
  *
  * As part of the proxied traffic service operations, this client handles retries with timeouts,
  * logging and graceful error handling.
  *
  * @param channel
  *   The managed channel to the traffic service. This channel is closed when this client is closed.
  */
class RichTrafficServiceClient(
    channel: ManagedChannel,
    override protected val timeouts: ProcessingTimeout,
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
      ec: ExecutionContext,
  ): FutureUnlessShutdown[GetAccountResponse] =
    toServerResponse(
      CantonGrpcUtil.sendGrpcRequest(client, serverName)(
        _.getAccount(request),
        requestDescription = "get-account",
        timeout = timeouts.network.duration,
        logger = logger,
      )
    )

  def updateAccount(
      request: UpdateAccountRequest
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[UpdateAccountResponse] =
    toServerResponse(
      CantonGrpcUtil.sendGrpcRequest(client, serverName)(
        _.updateAccount(request),
        requestDescription = "update-account",
        timeout = timeouts.network.duration,
        logger = logger,
      )
    )

  private def toServerResponse[T](
      value: EitherT[FutureUnlessShutdown, GrpcError, T]
  )(implicit traceContext: TraceContext, ec: ExecutionContext): FutureUnlessShutdown[T] =
    EitherTUtil.toFutureUnlessShutdown(value.leftMap {
      case serviceUnavailableError: GrpcError.GrpcServiceUnavailable =>
        logger.info(
          s"Failed to resolve account balance for:\n$serviceUnavailableError"
        )
        ServiceNotRunning.Reject("User traffic service").asGrpcError
      // TODO(#33681): Handle other errors more precisely
      case otherGrpcError =>
        LedgerApiErrors.InternalError
          .Generic(
            s"Error in submitting request to traffic service:\n$otherGrpcError"
          )
          .asGrpcError
    })
}

object RichTrafficServiceClient {
  def toInternalServer(
      grpcChannelName: String,
      timeout: ProcessingTimeout,
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
      loggerFactory,
    )
  }
}
