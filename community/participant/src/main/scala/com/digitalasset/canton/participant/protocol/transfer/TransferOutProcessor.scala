// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.ViewType.TransferOutViewType
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.ProtocolProcessor
import com.digitalasset.canton.participant.protocol.submission.{
  InFlightSubmissionTracker,
  SeedGenerator,
}
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.TransferProcessorError
import com.digitalasset.canton.participant.store.SyncDomainEphemeralState
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.{SourceDomainId, StaticDomainParameters}
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.version.Transfer.SourceProtocolVersion

import scala.concurrent.ExecutionContext

class TransferOutProcessor(
    domainId: SourceDomainId,
    override val participantId: ParticipantId,
    damle: DAMLe,
    staticDomainParameters: StaticDomainParameters,
    transferCoordination: TransferCoordination,
    inFlightSubmissionTracker: InFlightSubmissionTracker,
    ephemeral: SyncDomainEphemeralState,
    domainCrypto: DomainSyncCryptoClient,
    seedGenerator: SeedGenerator,
    sequencerClient: SequencerClient,
    override protected val timeouts: ProcessingTimeout,
    sourceProtocolVersion: SourceProtocolVersion,
    loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    override val testingConfig: TestingConfigInternal,
)(implicit ec: ExecutionContext)
    extends ProtocolProcessor[
      TransferOutProcessingSteps.SubmissionParam,
      TransferOutProcessingSteps.SubmissionResult,
      TransferOutViewType,
      TransferProcessorError,
    ](
      new TransferOutProcessingSteps(
        domainId,
        participantId,
        damle,
        transferCoordination,
        seedGenerator,
        staticDomainParameters,
        sourceProtocolVersion,
        loggerFactory,
      ),
      inFlightSubmissionTracker,
      ephemeral,
      domainCrypto,
      sequencerClient,
      domainId.unwrap,
      staticDomainParameters,
      sourceProtocolVersion.v,
      loggerFactory,
      futureSupervisor,
    ) {
  override protected def metricsContextForSubmissionParam(
      submissionParam: TransferOutProcessingSteps.SubmissionParam
  ): MetricsContext =
    MetricsContext(
      "application-id" -> submissionParam.submitterMetadata.applicationId,
      "type" -> "transfer-out",
    )
}
