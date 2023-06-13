// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import akka.actor.ActorSystem
import cats.Eval
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.participant.config.LocalParticipantConfig
import com.digitalasset.canton.participant.grpc.multidomain.{
  CommandCompletionGrpcService,
  GrpcStream,
  StateGrpcService,
  TransferSubmissionGrpcService,
  UpdateGrpcService,
}
import com.digitalasset.canton.participant.ledger.api.multidomain.{
  StateApiServiceImpl,
  TransferSubmissionApiServiceImpl,
  UpdateApiServiceImpl,
}
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.CantonSyncService
import io.grpc.BindableService

import scala.concurrent.ExecutionContext

object AdditionalMultiDomainServices {
  def get[PC <: LocalParticipantConfig](
      arguments: CantonNodeBootstrapCommonArguments[
        PC,
        ParticipantNodeParameters,
        ParticipantMetrics,
      ]
  )(implicit
      executionContext: ExecutionContext,
      actorSystem: ActorSystem,
  ): (CantonSyncService, Eval[ParticipantNodePersistentState]) => List[BindableService] =
    (cantonSyncService, participantNodePersistentStateEval) => {
      val participantNodePersistentState = participantNodePersistentStateEval.value
      val grpcStream = GrpcStream(
        transportExecutionContext = executionContext,
        materializer = implicitly,
        namedLoggerFactory = arguments.loggerFactory,
      )
      val updateApiService = new UpdateApiServiceImpl(
        multiDomainEventLog = participantNodePersistentState.multiDomainEventLog,
        namedLoggerFactory = arguments.loggerFactory,
      )
      val stateService = new StateApiServiceImpl(
        cantonSyncService = cantonSyncService,
        multiDomainEventLog = participantNodePersistentState.multiDomainEventLog,
        namedLoggerFactory = arguments.loggerFactory,
      )
      val transferSubmissionService = new TransferSubmissionApiServiceImpl(
        readySyncDomainById = cantonSyncService.readySyncDomainById,
        protocolVersionFor = cantonSyncService.protocolVersionGetter,
        submittingParticipant = cantonSyncService.participantId.toLf,
        namedLoggerFactory = arguments.loggerFactory,
      )

      List(
        UpdateGrpcService
          .bindableService(arguments.loggerFactory, grpcStream, updateApiService),
        StateGrpcService
          .bindableService(arguments.loggerFactory, grpcStream, stateService),
        CommandCompletionGrpcService
          .bindableService(
            arguments.loggerFactory,
            grpcStream,
            updateApiService,
          ),
        TransferSubmissionGrpcService.bindableService(
          arguments.loggerFactory
        )(transferSubmissionService),
      )
    }
}
