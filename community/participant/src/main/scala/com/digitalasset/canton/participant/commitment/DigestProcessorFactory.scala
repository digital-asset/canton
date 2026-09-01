// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.InternalIndexService
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.commitment.SynchronizerCommitmentState.TickSignaller
import com.digitalasset.canton.participant.config.AcsCommitmentConfig
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.metrics.CommitmentMetrics
import com.digitalasset.canton.participant.store.AcsDigestStore.allCheckpointsFilter
import com.digitalasset.canton.participant.store.{AcsCommitmentPeriodStore, AcsDigestStore}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.stream.Materializer

import scala.concurrent.ExecutionContext

/** Factory for creating digest processors.
  */
trait DigestProcessorFactory {
  def createRunningDigestProcessor(
      synchronizerAlias: SynchronizerAlias,
      synchronizerId: SynchronizerId,
      tickSignaller: TickSignaller,
  )(implicit traceContext: TraceContext): RunningDigestProcessor

  def createReinitializingDigestProcessor(
      synchronizerAlias: SynchronizerAlias,
      synchronizerId: SynchronizerId,
  )(implicit traceContext: TraceContext): ReinitializingDigestProcessor

  /** Returns whether the running digest store of the given synchronizer contains any checkpoint.
    * Returns `false` if there is no running digest store for the given synchronizer.
    */
  def needsReinitialization(
      synchronizerId: SynchronizerId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean]
}

class DigestProcessorFactoryImpl(
    participantId: ParticipantId,
    acsDigestStoreLookup: SynchronizerId => Option[AcsDigestStore],
    acsCommitmentPeriodStoreLookup: SynchronizerId => Option[AcsCommitmentPeriodStore],
    digestProcessorTopologyLookup: DigestProcessorTopologyLookup,
    internalIndexService: InternalIndexService,
    ledgerApiStore: LedgerApiStore,
    stringInterning: StringInterning,
    acsCommitmentConfig: AcsCommitmentConfig,
    metricsLookup: SynchronizerAlias => CommitmentMetrics,
    enableAdditionalConsistencyChecks: Boolean,
    timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, mat: Materializer)
    extends DigestProcessorFactory
    with NamedLogging {

  private def createDigestAccumulator(
      acsDigestStore: AcsDigestStore,
      metrics: CommitmentMetrics,
      loggerFactoryWithSynchronizer: NamedLoggerFactory,
  ): DigestAccumulator =
    if (acsCommitmentConfig.useSequentialDigestAccumulator) {
      new SequentialDigestAccumulator(
        acsDigestStore,
        stringInterning,
        acsCommitmentConfig.tracing,
        metrics,
        loggerFactoryWithSynchronizer,
      )
    } else {
      new InMemoryDigestAccumulator(
        acsDigestStore,
        loggerFactoryWithSynchronizer,
        stringInterning,
        maxNumLoadedDigests = acsCommitmentConfig.maxNumLoadedDigests.unwrap,
        digestUpdatePersistenceBatchFactor =
          acsCommitmentConfig.digestUpdatePersistenceBatchFactor.unwrap,
        digestLoadParallelism = acsCommitmentConfig.digestLoadParallelism.unwrap,
        digestComputeParallelism = acsCommitmentConfig.digestComputeParallelism.unwrap,
        bufferSize = acsCommitmentConfig.digestPipelineBufferSize.unwrap,
        tracingMode = acsCommitmentConfig.tracing,
        enableConsistencyChecks = enableAdditionalConsistencyChecks,
        metrics = metrics,
      )
    }

  override def createRunningDigestProcessor(
      synchronizerAlias: SynchronizerAlias,
      synchronizerId: SynchronizerId,
      tickSignaller: TickSignaller,
  )(implicit traceContext: TraceContext): RunningDigestProcessor = {
    val acsDigestStore = acsDigestStoreLookup(synchronizerId).getOrElse(
      ErrorUtil.invalidState("AcsDigestStore not initialized")
    )
    val acsCommitmentPeriodStore = acsCommitmentPeriodStoreLookup(synchronizerId).getOrElse(
      ErrorUtil.invalidState("AcsCommitmentPeriodStore not initialized")
    )
    val loggerFactoryWithSynchronizer =
      loggerFactory.append("synchronizer", synchronizerId.toString)

    val metrics = metricsLookup(synchronizerAlias)

    val digestAccumulator =
      createDigestAccumulator(acsDigestStore, metrics, loggerFactoryWithSynchronizer)

    val periodWriter =
      new AcsCommitmentPeriodWriter(acsDigestStore, acsCommitmentPeriodStore, loggerFactory)

    new RunningDigestProcessorImpl(
      participantId,
      synchronizerId,
      acsCommitmentConfig,
      digestAccumulator,
      acsDigestStore,
      tickSignaller,
      internalIndexService,
      digestProcessorTopologyLookup,
      enableAdditionalConsistencyChecks = enableAdditionalConsistencyChecks,
      periodWriter,
      metrics,
      timeouts,
      loggerFactoryWithSynchronizer,
    )
  }

  def createReinitializingDigestProcessor(
      synchronizerAlias: SynchronizerAlias,
      synchronizerId: SynchronizerId,
  )(implicit traceContext: TraceContext): ReinitializingDigestProcessor = {
    val acsDigestStore = acsDigestStoreLookup(synchronizerId).getOrElse(
      ErrorUtil.invalidState("AcsDigestStore not initialized")
    )
    val loggerFactoryWithSynchronizer =
      loggerFactory.append("synchronizer", synchronizerId.toString)

    val metrics = metricsLookup(synchronizerAlias)
    val digestAccumulator =
      createDigestAccumulator(acsDigestStore, metrics, loggerFactoryWithSynchronizer)

    val reinitializingTimepoint =
      DigestProcessorFactoryImpl.reinitializationTimepoint(ledgerApiStore, synchronizerId)

    new ReinitializingDigestProcessorImpl(
      participantId,
      synchronizerId,
      acsCommitmentConfig,
      digestAccumulator,
      acsDigestStore,
      indexService = internalIndexService,
      digestProcessorTopologyLookup,
      reinitializingTimepoint,
      enableAdditionalConsistencyChecks = enableAdditionalConsistencyChecks,
      metrics,
      timeouts,
      loggerFactory = loggerFactory.append("synchronizer", synchronizerId.toString),
    )
  }

  override def needsReinitialization(
      synchronizerId: SynchronizerId
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    acsDigestStoreLookup(synchronizerId) match {
      case None => FutureUnlessShutdown.pure(false)
      case Some(store) =>
        store.latestCheckpointUpTo(Offset.MaxValue, allCheckpointsFilter).map(_.isEmpty)
    }

}

object DigestProcessorFactoryImpl {
  @VisibleForTesting
  private[commitment] def reinitializationTimepoint(
      ledgerApiStore: LedgerApiStore,
      synchronizerId: SynchronizerId,
  )(implicit errorLoggingContext: ErrorLoggingContext): Timepoint = {
    // TODO(#33422) - Once the Github issue 27992 is solved, switch to new method
    val timepointO = for {
      end <- ledgerApiStore.ledgerEnd
      index <- end.synchronizerIndices.get(synchronizerId)
    } yield Timepoint(end.lastOffset)(index.recordTime)
    timepointO.getOrElse(
      ErrorUtil.invalidState(
        s"There is no suitable last offset for synchronizer $synchronizerId in the Ledger"
      )
    )
  }
}
