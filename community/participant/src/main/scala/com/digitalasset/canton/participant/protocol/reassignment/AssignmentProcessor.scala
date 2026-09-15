// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.ReassignmentCounter
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.{SynchronizerCryptoClient, SynchronizerSnapshotSyncCryptoApi}
import com.digitalasset.canton.data.ViewType.AssignmentViewType
import com.digitalasset.canton.data.{
  CantonTimestamp,
  ContractReassignment,
  ContractsReassignmentBatch,
  ReassignmentSubmitterMetadata,
}
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
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.protocol.{
  ContractInstance,
  ReassignmentId,
  StaticSynchronizerParameters,
}
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.sequencing.protocol.{Batch, MediatorGroupRecipient}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ContractValidator
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

class AssignmentProcessor private (
    private val assignmentSteps: AssignmentProcessingSteps,
    override val participantId: ParticipantId,
    inFlightSubmissionSynchronizerTracker: InFlightSubmissionSynchronizerTracker,
    ephemeral: SyncEphemeralState,
    synchronizerCrypto: SynchronizerCryptoClient,
    sequencerClient: SequencerClient,
    clock: Clock,
    override protected val timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    override val testingConfig: TestingConfigInternal,
    promiseFactory: PromiseUnlessShutdownFactory,
)(implicit ec: ExecutionContext)
    extends ProtocolProcessor[
      AssignmentProcessingSteps.SubmissionParam,
      AssignmentProcessingSteps.SubmissionResult,
      AssignmentViewType,
      ReassignmentProcessorError,
    ](
      assignmentSteps,
      inFlightSubmissionSynchronizerTracker,
      ephemeral,
      synchronizerCrypto,
      sequencerClient,
      clock,
      loggerFactory,
      futureSupervisor,
      promiseFactory,
    ) {
  def this(
      synchronizerId: Target[PhysicalSynchronizerId],
      participantId: ParticipantId,
      staticSynchronizerParameters: Target[StaticSynchronizerParameters],
      reassignmentCoordination: ReassignmentCoordination,
      inFlightSubmissionSynchronizerTracker: InFlightSubmissionSynchronizerTracker,
      ephemeral: SyncEphemeralState,
      synchronizerCrypto: SynchronizerCryptoClient,
      contractValidator: ContractValidator,
      seedGenerator: SeedGenerator,
      sequencerClient: SequencerClient,
      clock: Clock,
      timeouts: ProcessingTimeout,
      targetProtocolVersion: Target[ProtocolVersion],
      reassignmentMetrics: ReassignmentMetrics,
      loggerFactory: NamedLoggerFactory,
      futureSupervisor: FutureSupervisor,
      testingConfig: TestingConfigInternal,
      promiseFactory: PromiseUnlessShutdownFactory,
  )(implicit ec: ExecutionContext) = this(
    new AssignmentProcessingSteps(
      synchronizerId,
      participantId,
      reassignmentCoordination,
      synchronizerCrypto,
      seedGenerator,
      contractValidator,
      staticSynchronizerParameters,
      clock,
      targetProtocolVersion,
      reassignmentMetrics,
      loggerFactory,
    ),
    participantId,
    inFlightSubmissionSynchronizerTracker,
    ephemeral,
    synchronizerCrypto,
    sequencerClient,
    clock,
    timeouts,
    loggerFactory,
    futureSupervisor,
    testingConfig,
    promiseFactory,
  )

  override protected def metricsContextForSubmissionParam(
      submissionParam: AssignmentProcessingSteps.SubmissionParam
  ): MetricsContext =
    MetricsContext(
      "user-id" -> submissionParam.submitterMetadata.userId,
      "type" -> "assignment",
    )

  /** Builds a submission batch for cost estimation.
    *
    * Note: This method is intended for cost estimation purposes only
    */
  def buildSubmissionBatchForCostEstimation(
      submitterMetadata: ReassignmentSubmitterMetadata,
      contracts: Seq[ContractInstance],
      sourceSynchronizer: Source[PhysicalSynchronizerId],
      sourceSnapshot: Source[TopologySnapshot],
      mediator: MediatorGroupRecipient,
      recentSnapshot: SynchronizerSnapshotSyncCryptoApi,
      unassignmentTs: CantonTimestamp,
      counter: ReassignmentCounter,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Batch[DefaultOpenEnvelope]] =
    contracts.map { contract =>
      ContractReassignment(
        contract,
        Source(contract.templateId.packageId),
        Target(contract.templateId.packageId),
        counter,
      )
    } match {
      case head +: tail =>
        val batch = ContractsReassignmentBatch.create(head, tail)

        val reassignmentId = ReassignmentId(
          Source(sourceSynchronizer.unwrap.logical),
          Target(assignmentSteps.psid.unwrap.logical),
          unassignmentTs,
          batch.contractIdCounters.toMap.forgetNE,
        )

        for {
          reassigningParticipants <- new ReassigningParticipantsComputation(
            stakeholders = batch.stakeholders,
            sourceTopology = sourceSnapshot,
            targetTopology = Target(recentSnapshot.ipsSnapshot),
          ).compute.leftMap(_.toSubmissionValidationError)
          data <- assignmentSteps.buildSubmissionData(
            reassignmentId,
            submitterMetadata,
            batch,
            sourceSynchronizer,
            mediator,
            reassigningParticipants,
            unassignmentTs,
            batch.stakeholders,
            recentSnapshot,
            ephemeral.sessionKeyStore,
            sequencerClient.generateMaxSequencingTime,
          )
        } yield Batch.of(assignmentSteps.protocolVersion.unwrap, data.messages*)
      case _ =>
        EitherT.leftT[FutureUnlessShutdown, Batch[DefaultOpenEnvelope]](
          UnassignmentProcessorError.AutomaticAssignmentError(
            "Cannot build assignment batch for cost estimation with empty contract list"
          )
        )
    }
}
