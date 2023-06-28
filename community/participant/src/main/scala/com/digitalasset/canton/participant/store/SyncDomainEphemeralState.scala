// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.Eval
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.ComponentHealthState
import com.digitalasset.canton.health.HealthReporting.HealthComponent
import com.digitalasset.canton.lifecycle.{AsyncCloseable, FlagCloseable, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.metrics.SyncDomainMetrics
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ConflictDetector,
  NaiveRequestTracker,
  RequestTracker,
  RequestTrackerLookup,
}
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker.InFlightSubmissionTrackerDomainState
import com.digitalasset.canton.participant.protocol.submission.{
  InFlightSubmissionTracker,
  WatermarkLookup,
  WatermarkTracker,
}
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.PendingTransferSubmission
import com.digitalasset.canton.participant.protocol.{
  Phase37Synchronizer,
  ProcessingStartingPoints,
  RequestCounterAllocatorImpl,
  RequestJournal,
  SubmissionTracker,
}
import com.digitalasset.canton.participant.store.memory.TransferCache
import com.digitalasset.canton.protocol.RootHash
import com.digitalasset.canton.time.DomainTimeTracker
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

/** The state of a synchronization domain that is kept only in memory and must be reconstructed after crashes
  * and fatal errors from the [[SyncDomainPersistentState]]. The ephemeral state can be kept across network disconnects
  * provided that the local processing continues as far as possible.
  */
class SyncDomainEphemeralState(
    participantId: ParticipantId,
    persistentState: SyncDomainPersistentState,
    multiDomainEventLog: Eval[MultiDomainEventLog],
    inFlightSubmissionTracker: InFlightSubmissionTracker,
    val startingPoints: ProcessingStartingPoints,
    createTimeTracker: NamedLoggerFactory => DomainTimeTracker,
    metrics: SyncDomainMetrics,
    override val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
)(implicit executionContext: ExecutionContext)
    extends SyncDomainEphemeralStateLookup
    with FlagCloseable
    with NamedLogging
    with HealthComponent {

  override val name: String = SyncDomainEphemeralState.healthName
  override val initialHealthState: ComponentHealthState = ComponentHealthState.NotInitializedState
  override val closingState: ComponentHealthState =
    ComponentHealthState.failed("Disconnected from domain")

  // Key is the root hash of the transfer tree
  val pendingTransferOutSubmissions: TrieMap[RootHash, PendingTransferSubmission] =
    TrieMap.empty[RootHash, PendingTransferSubmission]
  val pendingTransferInSubmissions: TrieMap[RootHash, PendingTransferSubmission] =
    TrieMap.empty[RootHash, PendingTransferSubmission]

  val requestJournal =
    new RequestJournal(
      persistentState.requestJournalStore,
      metrics,
      loggerFactory,
      startingPoints.processing.nextRequestCounter,
      futureSupervisor,
    )
  val requestCounterAllocator = new RequestCounterAllocatorImpl(
    startingPoints.cleanReplay.nextRequestCounter,
    startingPoints.cleanReplay.nextSequencerCounter,
    loggerFactory,
  )
  val storedContractManager =
    new StoredContractManager(persistentState.contractStore, loggerFactory)
  val transferCache =
    new TransferCache(persistentState.transferStore, loggerFactory)

  val requestTracker: RequestTracker = {
    val conflictDetector = new ConflictDetector(
      persistentState.activeContractStore,
      persistentState.contractKeyJournal,
      transferCache,
      loggerFactory,
      persistentState.enableAdditionalConsistencyChecks,
      executionContext,
      timeouts,
      futureSupervisor,
    )

    new NaiveRequestTracker(
      startingPoints.cleanReplay.nextSequencerCounter,
      startingPoints.cleanReplay.prenextTimestamp,
      conflictDetector,
      metrics.conflictDetection,
      timeouts,
      loggerFactory,
      futureSupervisor,
    )
  }

  val recordOrderPublisher = {
    import TraceContext.Implicits.Empty.emptyTraceContext
    new RecordOrderPublisher(
      persistentState.domainId.item,
      startingPoints.processing.nextSequencerCounter,
      startingPoints.processing.prenextTimestamp,
      persistentState.eventLog,
      multiDomainEventLog,
      inFlightSubmissionTracker,
      metrics.recordOrderPublisher,
      timeouts,
      loggerFactory,
      futureSupervisor,
    )
  }

  val phase37Synchronizer =
    new Phase37Synchronizer(
      startingPoints.cleanReplay.nextRequestCounter,
      loggerFactory,
      futureSupervisor,
    )

  val observedTimestampTracker = new WatermarkTracker[CantonTimestamp](
    startingPoints.rewoundSequencerCounterPrehead.fold(CantonTimestamp.MinValue)(_.timestamp),
    loggerFactory,
    futureSupervisor,
  )

  // the time tracker, note, must be shutdown in sync domain as it is using the sequencer client to
  // request time proofs
  val timeTracker: DomainTimeTracker = createTimeTracker(loggerFactory)

  val submissionTracker: SubmissionTracker =
    SubmissionTracker(persistentState.protocolVersion)(
      participantId,
      persistentState.submissionTrackerStore,
      futureSupervisor,
      timeouts,
      loggerFactory,
    )

  def markAsRecovered()(implicit tc: TraceContext): Unit = {
    if (!resolveUnhealthy)
      throw new IllegalStateException("SyncDomainState has already been marked as recovered.")
  }

  override def onClosed(): Unit = {
    import com.digitalasset.canton.tracing.TraceContext.Implicits.Empty.*
    Lifecycle.close(
      requestTracker,
      recordOrderPublisher,
      submissionTracker,
      AsyncCloseable(
        "request-journal-flush",
        requestJournal.flush(),
        timeouts.shutdownProcessing.unwrap,
      ),
    )(logger)
  }

  lazy val inFlightSubmissionTrackerDomainState: InFlightSubmissionTrackerDomainState =
    InFlightSubmissionTrackerDomainState.fromSyncDomainState(persistentState, this)

}

object SyncDomainEphemeralState {
  val healthName: String = "sync-domain-ephemeral"
}

trait SyncDomainEphemeralStateLookup {
  this: SyncDomainEphemeralState =>

  def contractLookup: ContractLookup = storedContractManager

  def transferLookup: TransferLookup = transferCache

  def tracker: RequestTrackerLookup = requestTracker
  def observedTimestampLookup: WatermarkLookup[CantonTimestamp] = observedTimestampTracker
}
