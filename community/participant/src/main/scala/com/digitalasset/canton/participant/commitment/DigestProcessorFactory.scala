// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.ledger.participant.state.InternalIndexService
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.commitment.SynchronizerCommitmentState.TickSignaller
import com.digitalasset.canton.participant.config.AcsCommitmentConfig
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.metrics.CommitmentMetrics
import com.digitalasset.canton.participant.store.{AcsCommitmentPeriodStore, AcsDigestStore}
import com.digitalasset.canton.participant.topology.TopologyLookup
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}
import org.apache.pekko.stream.Materializer

import scala.concurrent.ExecutionContext

/** Factory for creating digest processors.
  */
trait DigestProcessorFactory {
  def createRunningDigestProcessor(
      synchronizerAlias: SynchronizerAlias,
      synchronizerId: SynchronizerId,
      tickSignaller: TickSignaller,
  )(implicit traceContext: TraceContext): BaseDigestProcessor

  def createReinitializingDigestProcessor(
      synchronizerAlias: SynchronizerAlias,
      synchronizerId: SynchronizerId,
  )(implicit traceContext: TraceContext): BaseDigestProcessor
}

class DigestProcessorFactoryImpl(
    participantId: ParticipantId,
    topologyLookup: TopologyLookup,
    acsDigestStoreLookup: SynchronizerId => Option[AcsDigestStore],
    acsCommitmentPeriodStoreLookup: SynchronizerId => Option[AcsCommitmentPeriodStore],
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
        participantId.toLf,
        acsDigestStore,
        stringInterning,
        acsCommitmentConfig.tracing,
        metrics,
        loggerFactoryWithSynchronizer,
      )
    } else {
      new InMemoryDigestAccumulator(
        participantId.toLf,
        acsDigestStore,
        loggerFactoryWithSynchronizer,
        stringInterning,
        acsUpdateBatchSize = acsCommitmentConfig.maxNumLoadedDigests.unwrap,
        digestLoadParallelism = acsCommitmentConfig.digestLoadParallelism.unwrap,
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

    new RunningDigestProcessor(
      participantId,
      synchronizerId,
      acsCommitmentConfig,
      digestAccumulator,
      acsDigestStore,
      tickSignaller,
      internalIndexService,
      getTopologySnapshot = tracedTimestamp =>
        EitherTUtil.toFutureUnlessShutdown(
          topologyLookup
            .maybeOfflineAwaitTopologySnapshot(synchronizerId, tracedTimestamp.value)(
              tracedTimestamp.traceContext
            )
            // TODO(#33084): cleanup error types
            .leftMap(_.asGrpcError)
        ),
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

    new ReinitializingDigestProcessor(
      participantId,
      synchronizerId,
      acsCommitmentConfig,
      digestAccumulator,
      acsDigestStore,
      indexService = internalIndexService,
      getTopologySnapshot = tracedTimestamp =>
        EitherTUtil.toFutureUnlessShutdown(
          topologyLookup
            .maybeOfflineAwaitTopologySnapshot(synchronizerId, tracedTimestamp.value)(
              tracedTimestamp.traceContext
            )
            // TODO(#33084): cleanup error types
            .leftMap(_.asGrpcError)
        ),
      ledgerApiStore,
      metrics,
      timeouts,
      loggerFactory = loggerFactory.append("synchronizer", synchronizerId.toString),
    )
  }

}
