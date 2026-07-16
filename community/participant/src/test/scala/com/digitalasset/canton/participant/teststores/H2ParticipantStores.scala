// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.teststores

import cats.Eval
import com.digitalasset.canton.TestEssentials
import com.digitalasset.canton.config.{BatchAggregatorConfig, BatchingConfig}
import com.digitalasset.canton.participant.store.db.*
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.store.teststores.H2StoresTest
import com.digitalasset.canton.store.{
  IndexedPhysicalSynchronizer,
  IndexedSynchronizer,
  PrunableByTimeParameters,
}
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import org.scalatest.Suite

trait H2ParticipantStores extends H2StoresTest {
  self: Suite & TestEssentials =>

  def createAcsCommitmentConfigStore(): DbAcsCommitmentConfigStore =
    new DbAcsCommitmentConfigStore(inMemoryH2Storage, timeouts, loggerFactory)(h2InMemoryEc)

  def createAcsCommitmentStore(
      indexedSynchronizer: IndexedSynchronizer,
      counterParticipantStore: DbAcsCommitmentConfigStore,
      stringInterning: Eval[StringInterning],
  ): DbAcsCommitmentStore =
    new DbAcsCommitmentStore(
      storage = inMemoryH2Storage,
      indexedSynchronizer = indexedSynchronizer,
      acsCounterParticipantConfigStore = counterParticipantStore,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
      stringInterningEval = stringInterning,
      batchingConfig = BatchingConfig(),
    )(h2InMemoryEc)

  def createInFlightSubmissionStore(): DbInFlightSubmissionStore =
    new DbInFlightSubmissionStore(
      storage = inMemoryH2Storage,
      registerBatchAggregatorConfig = BatchAggregatorConfig.defaultsForTesting,
      releaseProtocolVersion = testedReleaseProtocolVersion,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
    )(h2InMemoryEc)

  def createRequestJournalStore(
      indexedPhysicalSynchronizer: IndexedPhysicalSynchronizer
  ): DbRequestJournalStore = new DbRequestJournalStore(
    physicalSynchronizerIdx = indexedPhysicalSynchronizer,
    storage = inMemoryH2Storage,
    insertBatchAggregatorConfig = BatchAggregatorConfig.defaultsForTesting,
    replaceBatchAggregatorConfig = BatchAggregatorConfig.defaultsForTesting,
    timeouts = timeouts,
    loggerFactory = loggerFactory,
  )(h2InMemoryEc)

  def createCommandDeduplicationStore(): DbCommandDeduplicationStore =
    new DbCommandDeduplicationStore(
      storage = inMemoryH2Storage,
      timeouts = timeouts,
      releaseProtocolVersion = testedReleaseProtocolVersion,
      loggerFactory = loggerFactory,
    )(h2InMemoryEc)

  def createPackageStore(): DbDamlPackageStore = new DbDamlPackageStore(
    storage = inMemoryH2Storage,
    timeouts = timeouts,
    futureSupervisor = futureSupervisor,
    exitOnFatalFailures = true,
    loggerFactory = loggerFactory,
  )(h2InMemoryEc)

  def createSubmissionTrackingStore(
      indexedSynchronizer: IndexedPhysicalSynchronizer
  ): DbSubmissionTrackerStore =
    new DbSubmissionTrackerStore(
      storage = inMemoryH2Storage,
      indexedSynchronizer = indexedSynchronizer,
      batchingParametersConfig = PrunableByTimeParameters.testingParams,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
    )(h2InMemoryEc)

  def createRegisteredSynchronizersStore(): DbRegisteredSynchronizersStore =
    new DbRegisteredSynchronizersStore(inMemoryH2Storage, timeouts, loggerFactory)(h2InMemoryEc)

  def synchronizerConnectivityStatusStore(
      physicalSynchronizerId: PhysicalSynchronizerId
  ): DbSynchronizerConnectivityStatusStore = new DbSynchronizerConnectivityStatusStore(
    psid = physicalSynchronizerId,
    storage = inMemoryH2Storage,
    timeouts = timeouts,
    loggerFactory = loggerFactory,
  )(ec = h2InMemoryEc)

  override protected def tablesToClean: Seq[String] = super.tablesToClean ++ Seq(
    "par_contracts",
    "acs_slow_counter_participants",
    "acs_slow_participant_config",
    "acs_no_wait_counter_participants",
    "par_commitment_pruning",
    "par_computed_acs_commitments",
    "par_received_acs_commitments",
    "par_outstanding_acs_commitments",
    "par_last_computed_acs_commitments",
    "par_commitment_snapshot",
    "par_commitment_snapshot_time",
    "par_commitment_checkpoint_snapshot_time",
    "par_commitment_checkpoint_snapshot",
    "par_commitment_queue",
    "par_in_flight_submission",
    "par_journal_requests",
    "par_command_deduplication",
    "par_command_deduplication_pruning",
    "par_daml_packages",
    "par_dar_packages",
    "par_dars",
    "par_fresh_submitted_transaction",
    "par_fresh_submitted_transaction_pruning",
    "par_synchronizer_connectivity_status",
  )
}
