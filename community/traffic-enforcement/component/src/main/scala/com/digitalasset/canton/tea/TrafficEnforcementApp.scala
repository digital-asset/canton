// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea

import cats.data.EitherT
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.tea.TrafficEnforcementApp.TeaGrpcServerName
import com.digitalasset.canton.tea.v1.{
  GetAccountRequest,
  GetAccountResponse,
  UpdateAccountRequest,
  UpdateAccountResponse,
}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.inprocess.InProcessServerBuilder

import scala.concurrent.ExecutionContext

/** Top level TEA class. Creates an in-process gRPC server that exposes the
  * [[com.digitalasset.canton.tea.v1.TrafficServiceGrpc.TrafficService]] via
  * [[TrafficEnforcementServiceGrpc]].
  */
class TrafficEnforcementApp(
    service: TrafficEnforcementService,
    node: InstanceName,
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  private val server = InProcessServerBuilder
    .forName(s"$TeaGrpcServerName-$node")
    .addService(new TrafficEnforcementServiceGrpc(service, loggerFactory))
    .build()

  server.start().discard

  override def onClosed(): Unit =
    LifeCycle.close(LifeCycle.toCloseableServer(server, logger, TeaGrpcServerName), service)(logger)
}

object TrafficEnforcementApp {
  val TeaGrpcServerName = "TeaGrpcInProcServer"
  type TeaAppBuilder = () => Either[String, TrafficEnforcementApp]

  def internal(
      forNode: InstanceName,
      loggerFactory: NamedLoggerFactory,
      processingTimeouts: ProcessingTimeout,
  )(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, String, TeaAppBuilder] =
    EitherT.pure[FutureUnlessShutdown, String] { () =>
      val mockService = new TrafficEnforcementService {
        override def getAccount(request: GetAccountRequest)(implicit
            traceContext: TraceContext
        ): FutureUnlessShutdown[
          Either[TrafficEnforcementService.TrafficEnforcementServiceError, GetAccountResponse]
        ] =
          throw new UnsupportedOperationException("Not supported")
        override def updateAccount(request: UpdateAccountRequest)(implicit
            traceContext: TraceContext
        ): FutureUnlessShutdown[
          Either[TrafficEnforcementService.TrafficEnforcementServiceError, UpdateAccountResponse]
        ] =
          throw new UnsupportedOperationException("Not supported")
        override protected def timeouts: ProcessingTimeout = processingTimeouts
        override protected[this] def logger: TracedLogger = loggerFactory.getTracedLogger(getClass)
      }

      Right(new TrafficEnforcementApp(mockService, forNode, loggerFactory, processingTimeouts))
    }
}
