// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.teststores

import com.digitalasset.canton.TestEssentials
import com.digitalasset.canton.config.BatchingConfig
import com.digitalasset.canton.participant.store.db.{DbActiveContractStore, DbReassignmentStore}
import com.digitalasset.canton.store.teststores.H2StoresTest
import com.digitalasset.canton.store.{IndexedSynchronizer, PrunableByTimeParameters}
import com.digitalasset.canton.util.ReassignmentTag.Target
import org.scalatest.Suite

trait H2ParticipantStoresWithIndexedStringDependency extends H2StoresTest {
  self: Suite & TestEssentials & H2IndexedStringStore =>
  def createActiveContractStore(indexedSynchronizer: IndexedSynchronizer): DbActiveContractStore =
    new DbActiveContractStore(
      storage = inMemoryH2Storage,
      indexedSynchronizer = indexedSynchronizer,
      enableAdditionalConsistencyChecks = Some(20),
      batchingParametersConfig = PrunableByTimeParameters.testingParams,
      batchingConfig = BatchingConfig(),
      indexedStringStore = createIndexedStringStore(),
      timeouts = timeouts,
      loggerFactory = loggerFactory,
    )(h2InMemoryEc)

  def createReassignmentStore(
      targetIndexedSynchronizer: Target[IndexedSynchronizer]
  ): DbReassignmentStore = new DbReassignmentStore(
    storage = inMemoryH2Storage,
    indexedTargetSynchronizer = targetIndexedSynchronizer,
    indexedStringStore = createIndexedStringStore(),
    futureSupervisor = futureSupervisor,
    exitOnFatalFailures = false,
    batchingConfig = new BatchingConfig,
    timeouts = timeouts,
    loggerFactory = loggerFactory,
  )(h2InMemoryEc)

  override protected def tablesToClean: Seq[String] = super.tablesToClean ++ Seq(
    "par_active_contracts",
    "par_active_contract_pruning",
    "par_reassignments",
  )
}
