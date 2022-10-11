// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{AsyncCloseable, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.metrics.SyncDomainMetrics
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.PendingRequestDataOrReplayData
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ConflictDetector,
  NaiveRequestTracker,
  RequestTracker,
}
import com.digitalasset.canton.participant.protocol.submission.{
  InFlightSubmissionTracker,
  WatermarkLookup,
  WatermarkTracker,
}
import com.digitalasset.canton.participant.protocol.transfer.TransferInProcessingSteps.PendingTransferIn
import com.digitalasset.canton.participant.protocol.transfer.TransferOutProcessingSteps.PendingTransferOut
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.PendingTransferSubmission
import com.digitalasset.canton.participant.protocol.validation.PendingTransaction
import com.digitalasset.canton.participant.protocol.{
  Phase37Synchronizer,
  ProcessingStartingPoints,
  RequestCounterAllocatorImpl,
  RequestJournal,
  SingleDomainCausalTracker,
}
import com.digitalasset.canton.participant.store.memory.TransferCache
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.time.DomainTimeTracker

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

/** The state of a synchronization domain that is kept only in memory and must be reconstructed after crashes
  * and fatal errors from the [[SyncDomainPersistentState]]. The ephemeral state can be kept across network disconnects
  * provided that the local processing continues as far as possible.
  */
class SyncDomainEphemeralState(
    persistentState: SyncDomainPersistentState,
    multiDomainEventLog: MultiDomainEventLog,
    val singleDomainCausalTracker: SingleDomainCausalTracker,
    inFlightSubmissionTracker: InFlightSubmissionTracker,
    val startingPoints: ProcessingStartingPoints,
    createTimeTracker: NamedLoggerFactory => DomainTimeTracker,
    metrics: SyncDomainMetrics,
    timeouts: ProcessingTimeout,
    useCausalityTracking: Boolean,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SyncDomainEphemeralStateLookup
    with AutoCloseable
    with NamedLogging {

  // Key is the root hash of the transfer tree
  val pendingTransferOutSubmissions: TrieMap[RootHash, PendingTransferSubmission] =
    TrieMap.empty[RootHash, PendingTransferSubmission]
  val pendingTransferInSubmissions: TrieMap[RootHash, PendingTransferSubmission] =
    TrieMap.empty[RootHash, PendingTransferSubmission]

  val pendingTransactions: TrieMap[RequestId, PendingRequestDataOrReplayData[PendingTransaction]] =
    TrieMap.empty[RequestId, PendingRequestDataOrReplayData[PendingTransaction]]
  val pendingTransferOuts: TrieMap[RequestId, PendingRequestDataOrReplayData[PendingTransferOut]] =
    TrieMap.empty[RequestId, PendingRequestDataOrReplayData[PendingTransferOut]]
  val pendingTransferIns: TrieMap[RequestId, PendingRequestDataOrReplayData[PendingTransferIn]] =
    TrieMap.empty[RequestId, PendingRequestDataOrReplayData[PendingTransferIn]]

  val requestJournal =
    new RequestJournal(
      persistentState.requestJournalStore,
      metrics,
      loggerFactory,
      startingPoints.processing.nextRequestCounter,
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
    )

    new NaiveRequestTracker(
      startingPoints.cleanReplay.nextSequencerCounter,
      startingPoints.cleanReplay.prenextTimestamp,
      conflictDetector,
      metrics.conflictDetection,
      timeouts,
      loggerFactory,
    )
  }

  val recordOrderPublisher =
    new RecordOrderPublisher(
      persistentState.domainId.item,
      startingPoints.processing.nextSequencerCounter,
      startingPoints.processing.prenextTimestamp,
      persistentState.eventLog,
      multiDomainEventLog,
      singleDomainCausalTracker,
      inFlightSubmissionTracker,
      metrics.recordOrderPublisher,
      timeouts,
      useCausalityTracking,
      loggerFactory,
    )

  val phase37Synchronizer =
    new Phase37Synchronizer(startingPoints.cleanReplay.nextRequestCounter, loggerFactory)

  val observedTimestampTracker = new WatermarkTracker[CantonTimestamp](
    startingPoints.rewoundSequencerCounterPrehead.fold(CantonTimestamp.MinValue)(_.timestamp),
    loggerFactory,
  )

  // the time tracker, note, must be shutdown in sync domain as it is using the sequencer client to
  // request time proofs
  val timeTracker: DomainTimeTracker = createTimeTracker(loggerFactory)

  private val recoveredF: AtomicBoolean = new AtomicBoolean(false)

  def recovered: Boolean = recoveredF.get

  def markAsRecovered(): Unit = {
    if (!recoveredF.compareAndSet(false, true))
      throw new IllegalStateException("SyncDomainState has already been marked as recovered.")
  }

  override def close(): Unit = {
    import com.digitalasset.canton.tracing.TraceContext.Implicits.Empty.*
    Lifecycle.close(
      requestTracker,
      recordOrderPublisher,
      AsyncCloseable(
        "request-journal-flush",
        requestJournal.flush(),
        timeouts.shutdownProcessing.unwrap,
      ),
    )(logger)
  }

}

trait SyncDomainEphemeralStateLookup {
  this: SyncDomainEphemeralState =>

  def contractLookup: ContractLookup = storedContractManager

  def transferLookup: TransferLookup = transferCache

  def causalityLookup: SingleDomainCausalTracker = singleDomainCausalTracker

  def observedTimestampLookup: WatermarkLookup[CantonTimestamp] = observedTimestampTracker
}
