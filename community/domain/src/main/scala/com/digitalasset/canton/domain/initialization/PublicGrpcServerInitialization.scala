// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.initialization

import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.domain.config.{DomainConfig, DomainNodeParameters}
import com.digitalasset.canton.domain.sequencing.SequencerRuntime
import com.digitalasset.canton.domain.sequencing.authentication.grpc.SequencerConnectServerInterceptor
import com.digitalasset.canton.domain.sequencing.service.GrpcSequencerConnectService
import com.digitalasset.canton.domain.service.ServiceAgreementManager
import com.digitalasset.canton.domain.service.grpc.GrpcDomainService
import com.digitalasset.canton.lifecycle.Lifecycle.{CloseableServer, toCloseableServer}
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.metrics.MetricHandle
import com.digitalasset.canton.networking.grpc.CantonServerBuilder
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.ServerInterceptors
import io.grpc.protobuf.services.ProtoReflectionService

import scala.concurrent.ExecutionContextExecutorService

object PublicGrpcServerInitialization {

  def apply(
      config: DomainConfig,
      metrics: MetricHandle.Factory,
      cantonParameterConfig: DomainNodeParameters,
      loggerFactory: NamedLoggerFactory,
      logger: TracedLogger,
      sequencerRuntime: SequencerRuntime,
      domainId: DomainId,
      agreementManager: Option[ServiceAgreementManager],
      staticDomainParameters: StaticDomainParameters,
      cryptoApi: DomainSyncCryptoClient,
  )(implicit executionContext: ExecutionContextExecutorService): CloseableServer = {

    logger.info(s"Starting public services with config ${config.publicApi}")(TraceContext.empty)

    val serverBuilder = CantonServerBuilder
      .forConfig(
        config.publicApi,
        metrics,
        executionContext,
        loggerFactory,
        cantonParameterConfig.loggingConfig.api,
        cantonParameterConfig.tracing,
      )
      // Overriding the dummy setting from PublicServerConfig.
      .maxInboundMessageSize(config.domainParameters.maxInboundMessageSize)
      .addService(ProtoReflectionService.newInstance(), withLogging = false)

    // the server builder is mutable
    sequencerRuntime.registerPublicGrpcServices { service =>
      serverBuilder.addService(service).discard
    }

    val domainService = new GrpcDomainService(agreementManager, loggerFactory)

    val sequencerConnectService =
      new GrpcSequencerConnectService(
        domainId,
        staticDomainParameters,
        cryptoApi,
        agreementManager,
        loggerFactory,
      )

    serverBuilder
      .addService(v0.DomainServiceGrpc.bindService(domainService, executionContext))
      .addService(
        ServerInterceptors.intercept(
          v0.SequencerConnectServiceGrpc.bindService(sequencerConnectService, executionContext),
          new SequencerConnectServerInterceptor(loggerFactory),
        )
      )

    val server = serverBuilder.build.start()
    toCloseableServer(server, logger, "PublicServer")
  }

}
