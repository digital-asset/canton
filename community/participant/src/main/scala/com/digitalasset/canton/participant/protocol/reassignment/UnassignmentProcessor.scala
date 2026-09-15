// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.bifunctor.*
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.{SynchronizerCryptoClient, SynchronizerSnapshotSyncCryptoApi}
import com.digitalasset.canton.data.ViewType.UnassignmentViewType
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, PromiseUnlessShutdownFactory}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.metrics.ReassignmentMetrics
import com.digitalasset.canton.participant.protocol.ProtocolProcessor
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.ReassignmentProcessorError
import com.digitalasset.canton.participant.protocol.submission.{
  InFlightSubmissionSynchronizerTracker,
  SeedGenerator,
}
import com.digitalasset.canton.participant.sync.SyncEphemeralState
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.sequencing.protocol.{Batch, MediatorGroupRecipient}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ContractValidator
import com.digitalasset.canton.util.ReassignmentTag.Source
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

class UnassignmentProcessor(
    synchronizerId: Source[PhysicalSynchronizerId],
    override val participantId: ParticipantId,
    staticSynchronizerParameters: Source[StaticSynchronizerParameters],
    reassignmentCoordination: ReassignmentCoordination,
    inFlightSubmissionSynchronizerTracker: InFlightSubmissionSynchronizerTracker,
    ephemeral: SyncEphemeralState,
    synchronizerCrypto: SynchronizerCryptoClient,
    contractValidator: ContractValidator,
    seedGenerator: SeedGenerator,
    sequencerClient: SequencerClient,
    clock: Clock,
    override protected val timeouts: ProcessingTimeout,
    sourceProtocolVersion: Source[ProtocolVersion],
    reassignmentMetrics: ReassignmentMetrics,
    loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    override val testingConfig: TestingConfigInternal,
    promiseFactory: PromiseUnlessShutdownFactory,
)(implicit ec: ExecutionContext)
    extends ProtocolProcessor[
      UnassignmentProcessingSteps.SubmissionParam,
      UnassignmentProcessingSteps.SubmissionResult,
      UnassignmentViewType,
      ReassignmentProcessorError,
    ](
      new UnassignmentProcessingSteps(
        synchronizerId,
        participantId,
        reassignmentCoordination,
        synchronizerCrypto,
        seedGenerator,
        staticSynchronizerParameters,
        contractValidator,
        clock,
        sourceProtocolVersion,
        reassignmentMetrics,
        loggerFactory,
      ),
      inFlightSubmissionSynchronizerTracker,
      ephemeral,
      synchronizerCrypto,
      sequencerClient,
      clock,
      loggerFactory,
      futureSupervisor,
      promiseFactory,
    ) {
  override protected def metricsContextForSubmissionParam(
      submissionParam: UnassignmentProcessingSteps.SubmissionParam
  ): MetricsContext =
    MetricsContext(
      "user-id" -> submissionParam.submitterMetadata.userId,
      "type" -> "unassignment",
    )

  def buildSubmissionBatch(
      submissionParam: UnassignmentProcessingSteps.SubmissionParam,
      mediator: MediatorGroupRecipient,
      recentSnapshot: SynchronizerSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Batch[DefaultOpenEnvelope]] =
    steps
      .createSubmission(
        submissionParam,
        mediator,
        ephemeral,
        recentSnapshot,
        sequencerClient.generateMaxSequencingTime,
      )
      .flatMap { case (submission, _pendingSubmissionData) =>
        submission match {
          case submission: steps.UntrackedSubmission =>
            EitherT.pure[FutureUnlessShutdown, ReassignmentProcessorError](submission.batch)
          case _illegal => // Unassignments are always untracked
            EitherT
              .leftT[FutureUnlessShutdown, Batch[DefaultOpenEnvelope]](
                UnassignmentProcessorError.AutomaticAssignmentError(
                  s"Unexpected submission type ${_illegal.getClass.getSimpleName} for unassignement"
                )
              )
              .leftWiden[ReassignmentProcessorError]
        }
      }
}
