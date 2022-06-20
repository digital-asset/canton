// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import akka.actor.ActorSystem
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.grpc.CantonMutableHandlerRegistry
import com.digitalasset.canton.participant.admin.grpc.{GrpcPackageService, GrpcPingService}
import com.digitalasset.canton.participant.admin.v0.{PackageServiceGrpc, PingServiceGrpc}
import com.digitalasset.canton.participant.admin.{AdminWorkflowServices, PackageService}
import com.digitalasset.canton.participant.config.{
  LocalParticipantConfig,
  ParticipantNodeParameters,
}
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TracerProvider
import io.opentelemetry.api.trace.Tracer

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.ExecutionContextExecutor

/** Holds all Canton services that use the Ledger Api and hence depend on the ledger api server to be up.
  * Used to close and restart those services when then ledger api server needs to be taken down temporarily,
  * e.g. for ledger pruning.
  */
class LedgerApiDependentCantonServices(
    config: LocalParticipantConfig,
    testingConfig: ParticipantNodeParameters,
    packageService: PackageService,
    syncService: CantonSyncService,
    participantId: ParticipantId,
    hashOps: HashOps,
    clock: Clock,
    registry: CantonMutableHandlerRegistry,
    adminToken: CantonAdminToken,
    loggerFactory: NamedLoggerFactory,
    tracerProvider: TracerProvider,
)(implicit
    ec: ExecutionContextExecutor,
    actorSystem: ActorSystem,
    tracer: Tracer,
    scheduler: ScheduledExecutorService,
    executionSequencerFactory: ExecutionSequencerFactory,
) extends AutoCloseable {
  val logger = loggerFactory.getLogger(getClass)
  val adminWorkflowServices =
    new AdminWorkflowServices(
      config,
      testingConfig,
      packageService,
      syncService,
      participantId.uid,
      hashOps,
      adminToken,
      loggerFactory,
      clock,
      tracerProvider,
    )

  private val adminGrpcServices = PackageServiceGrpc
    .bindService(
      new GrpcPackageService(packageService, adminWorkflowServices.darDistribution, loggerFactory),
      ec,
    )
  registry.addService(adminGrpcServices)

  private val pingGrpcService = PingServiceGrpc
    .bindService(new GrpcPingService(adminWorkflowServices.ping), ec)
  registry.addService(pingGrpcService)

  override def close(): Unit = {
    registry.removeService(pingGrpcService)
    registry.removeService(adminGrpcServices)
    adminWorkflowServices.close()
  }
}
