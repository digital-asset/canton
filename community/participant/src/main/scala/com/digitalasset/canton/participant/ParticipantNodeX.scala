// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import akka.actor.ActorSystem
import cats.Eval
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.lf.engine.Engine
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.environment.{
  BootstrapStageOrLeaf,
  CantonNodeBootstrapCommonArguments,
  CantonNodeBootstrapX,
}
import com.digitalasset.canton.health.admin.data.NodeStatus
import com.digitalasset.canton.health.{GrpcHealthReporter, HealthReporting}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.ParticipantNodeBootstrap.CommunityParticipantFactoryCommon
import com.digitalasset.canton.participant.admin.ResourceManagementService
import com.digitalasset.canton.participant.config.LocalParticipantConfig
import com.digitalasset.canton.participant.ledger.api.CantonLedgerApiServerWrapper.IndexerLockIds
import com.digitalasset.canton.participant.ledger.api.*
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.scheduler.ParticipantSchedulersParameters
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey.CommunityKey
import com.digitalasset.canton.resource.*
import com.digitalasset.canton.scheduler.SchedulersWithPruning
import com.digitalasset.canton.time.{Clock, TestingTimeService}
import com.digitalasset.canton.topology.{Member, ParticipantId, TopologyManagerX, UniqueIdentifier}
import io.grpc.{BindableService, ServerServiceDefinition}

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.Future

class ParticipantNodeBootstrapX(
    arguments: CantonNodeBootstrapCommonArguments[
      LocalParticipantConfig,
      ParticipantNodeParameters,
      ParticipantMetrics,
    ],
    engine: Engine,
    testingTimeService: TestingTimeService,
    cantonSyncServiceFactory: CantonSyncService.Factory[CantonSyncService],
    setStartableStoppableLedgerApiAndCantonServices: (
        StartableStoppableLedgerApiServer,
        StartableStoppableLedgerApiDependentServices,
    ) => Unit,
    resourceManagementServiceFactory: Eval[ParticipantSettingsStore] => ResourceManagementService,
    replicationServiceFactory: Storage => ServerServiceDefinition,
    allocateIndexerLockIds: DbConfig => Either[String, Option[IndexerLockIds]],
    meteringReportKey: MeteringReportKey,
    additionalGrpcServices: (
        CantonSyncService,
        Eval[ParticipantNodePersistentState],
    ) => List[BindableService] = (_, _) => Nil,
    createSchedulers: ParticipantSchedulersParameters => Future[SchedulersWithPruning] = _ =>
      Future.successful(SchedulersWithPruning.noop),
    private[canton] val persistentStateFactory: ParticipantNodePersistentStateFactory,
)(implicit
    executionContext: ExecutionContextIdlenessExecutorService,
    scheduler: ScheduledExecutorService,
    actorSystem: ActorSystem,
) extends CantonNodeBootstrapX[
      ParticipantNodeX,
      LocalParticipantConfig,
      ParticipantNodeParameters,
      ParticipantMetrics,
    ](arguments)
    with ParticipantNodeBootstrapCommon {
  override protected def customNodeStages(
      storage: Storage,
      crypto: Crypto,
      nodeId: UniqueIdentifier,
      manager: TopologyManagerX,
      healthReporter: GrpcHealthReporter,
  ): BootstrapStageOrLeaf[ParticipantNodeX] = ???
  override protected def member(uid: UniqueIdentifier): Member = ParticipantId(uid)
  override protected def mkNodeHealthService(storage: Storage): HealthReporting.HealthService =
    HealthReporting.HealthService(
      "participant",
      criticalDependencies = Seq(storage),
      // The sync service won't be reporting Ok until the node is initialized, but that shouldn't prevent traffic from
      // reaching the node
      softDependencies = Seq(), // TODO(#11255) add health services
      // Seq(syncDomainHealth, syncDomainEphemeralHealth, syncDomainSequencerClientHealth),
    )
}

object ParticipantNodeBootstrapX {

  object CommunityParticipantFactory
      extends CommunityParticipantFactoryCommon[ParticipantNodeBootstrapX] {

    override protected def createNode(
        arguments: Arguments,
        testingTimeService: TestingTimeService,
    )(implicit
        executionContext: ExecutionContextIdlenessExecutorService,
        scheduler: ScheduledExecutorService,
        actorSystem: ActorSystem,
        executionSequencerFactory: ExecutionSequencerFactory,
    ): ParticipantNodeBootstrapX =
      new ParticipantNodeBootstrapX(
        arguments,
        createEngine(arguments),
        testingTimeService,
        CantonSyncService.DefaultFactory,
        (_ledgerApi, _ledgerApiDependentServices) => (),
        createResourceService(arguments),
        createReplicationServiceFactory(arguments),
        _dbConfig => Option.empty[IndexerLockIds].asRight,
        meteringReportKey = CommunityKey,
        persistentStateFactory = ParticipantNodePersistentStateFactory,
      )
  }
}

class ParticipantNodeX(val adminToken: CantonAdminToken) extends ParticipantNodeCommon {
  override def close(): Unit = ???

  /** The clock used to measure up-time */
  override protected def clock: Clock = ???

  override protected def loggerFactory: NamedLoggerFactory = ???

  override def status: Future[NodeStatus.Status] = ???

  override def isActive: Boolean = ???
}

object ParticipantNodeX {}
