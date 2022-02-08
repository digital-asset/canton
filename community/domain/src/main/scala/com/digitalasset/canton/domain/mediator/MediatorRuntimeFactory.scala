// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.EitherT
import com.digitalasset.canton.DomainId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{LocalNodeParameters, ProcessingTimeout}
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.domain.admin.v0.EnterpriseMediatorAdministrationServiceGrpc
import com.digitalasset.canton.domain.api.v0.DomainTimeServiceGrpc
import com.digitalasset.canton.domain.mediator.store.{FinalizedResponseStore, MediatorState}
import com.digitalasset.canton.domain.metrics.MediatorMetrics
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.StaticGrpcServices
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.UnsignedProtocolEventHandler
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.store.{SequencedEventStore, SequencerCounterTrackerStore}
import com.digitalasset.canton.time.{Clock, DomainTimeTrackerConfig, GrpcDomainTimeService}
import com.digitalasset.canton.topology.MediatorId
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.ServerServiceDefinition
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContext, Future}

/** Mediator component and its supporting services */
trait MediatorRuntime extends FlagCloseable {
  def mediator: Mediator

  final def registerAdminGrpcServices(register: ServerServiceDefinition => Unit): Unit = {
    register(timeService)
    register(enterpriseAdministrationService)
  }

  def timeService: ServerServiceDefinition
  def enterpriseAdministrationService: ServerServiceDefinition

  override protected def onClosed(): Unit = mediator.close()
}

class CommunityMediatorRuntime(
    override val mediator: Mediator,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit protected val ec: ExecutionContext)
    extends MediatorRuntime
    with NamedLogging {
  override val timeService: ServerServiceDefinition = DomainTimeServiceGrpc.bindService(
    GrpcDomainTimeService.forDomainEntity(mediator.domain, mediator.timeTracker, loggerFactory),
    ec,
  )
  override val enterpriseAdministrationService: ServerServiceDefinition =
    StaticGrpcServices.notSupportedByCommunity(
      EnterpriseMediatorAdministrationServiceGrpc.SERVICE,
      logger,
    )
}

trait MediatorRuntimeFactory {
  def create(
      mediatorId: MediatorId,
      domainId: DomainId,
      storage: Storage,
      sequencerCounterTrackerStore: SequencerCounterTrackerStore,
      sequencedEventStore: SequencedEventStore,
      sequencerClient: SequencerClient,
      syncCrypto: DomainSyncCryptoClient,
      topologyClient: DomainTopologyClientWithInit,
      identityClientEventHandler: UnsignedProtocolEventHandler,
      timeTrackerConfig: DomainTimeTrackerConfig,
      nodeParameters: LocalNodeParameters,
      clock: Clock,
      metrics: MediatorMetrics,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      tracer: Tracer,
      traceContext: TraceContext,
  ): EitherT[Future, String, MediatorRuntime]
}

object CommunityMediatorRuntimeFactory extends MediatorRuntimeFactory {
  override def create(
      mediatorId: MediatorId,
      domainId: DomainId,
      storage: Storage,
      sequencerCounterTrackerStore: SequencerCounterTrackerStore,
      sequencedEventStore: SequencedEventStore,
      sequencerClient: SequencerClient,
      syncCrypto: DomainSyncCryptoClient,
      topologyClient: DomainTopologyClientWithInit,
      identityClientEventHandler: UnsignedProtocolEventHandler,
      timeTrackerConfig: DomainTimeTrackerConfig,
      nodeParameters: LocalNodeParameters,
      clock: Clock,
      metrics: MediatorMetrics,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      tracer: Tracer,
      traceContext: TraceContext,
  ): EitherT[Future, String, MediatorRuntime] = {
    val state =
      new MediatorState(
        FinalizedResponseStore(storage, syncCrypto.pureCrypto, loggerFactory),
        metrics,
        loggerFactory,
      )
    EitherT.pure[Future, String](
      new CommunityMediatorRuntime(
        new Mediator(
          domainId,
          mediatorId,
          sequencerClient,
          topologyClient,
          syncCrypto,
          identityClientEventHandler,
          timeTrackerConfig,
          state,
          sequencerCounterTrackerStore,
          sequencedEventStore,
          nodeParameters,
          clock,
          metrics,
          MediatorReadyCheck(mediatorId, syncCrypto, loggerFactory),
          loggerFactory,
        ),
        nodeParameters.processingTimeouts,
        loggerFactory,
      )
    )
  }
}
