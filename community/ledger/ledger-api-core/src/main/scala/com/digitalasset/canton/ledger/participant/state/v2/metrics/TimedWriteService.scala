// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.v2.metrics

import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.transaction.{GlobalKey, ProcessedDisclosedContract, SubmittedTransaction}
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext
import com.daml.metrics.{Metrics, Timed}
import com.daml.tracing.TelemetryContext
import com.digitalasset.canton.ledger.api.health.HealthStatus
import com.digitalasset.canton.ledger.configuration.Configuration
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.v2.{
  PruningResult,
  SubmissionResult,
  SubmitterInfo,
  TransactionMeta,
  WriteService,
}

import java.util.concurrent.CompletionStage

final class TimedWriteService(delegate: WriteService, metrics: Metrics) extends WriteService {

  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
      estimatedInterpretationCost: Long,
      globalKeyMapping: Map[GlobalKey, Option[Value.ContractId]],
      processedDisclosedContracts: ImmArray[ProcessedDisclosedContract],
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    Timed.timedAndTrackedCompletionStage(
      metrics.daml.services.write.submitTransaction,
      metrics.daml.services.write.submitTransactionRunning,
      delegate.submitTransaction(
        submitterInfo,
        transactionMeta,
        transaction,
        estimatedInterpretationCost,
        globalKeyMapping,
        processedDisclosedContracts,
      ),
    )

  override def uploadPackages(
      submissionId: Ref.SubmissionId,
      archives: List[DamlLf.Archive],
      sourceDescription: Option[String],
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    Timed.completionStage(
      metrics.daml.services.write.uploadPackages,
      delegate.uploadPackages(submissionId, archives, sourceDescription),
    )

  override def allocateParty(
      hint: Option[Ref.Party],
      displayName: Option[String],
      submissionId: Ref.SubmissionId,
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    Timed.completionStage(
      metrics.daml.services.write.allocateParty,
      delegate.allocateParty(hint, displayName, submissionId),
    )

  override def submitConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: Ref.SubmissionId,
      config: Configuration,
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    Timed.completionStage(
      metrics.daml.services.write.submitConfiguration,
      delegate.submitConfiguration(maxRecordTime, submissionId, config),
    )

  override def prune(
      pruneUpToInclusive: Offset,
      submissionId: Ref.SubmissionId,
      pruneAllDivulgedContracts: Boolean,
  ): CompletionStage[PruningResult] =
    Timed.completionStage(
      metrics.daml.services.write.prune,
      delegate.prune(pruneUpToInclusive, submissionId, pruneAllDivulgedContracts),
    )

  override def currentHealth(): HealthStatus =
    delegate.currentHealth()
}