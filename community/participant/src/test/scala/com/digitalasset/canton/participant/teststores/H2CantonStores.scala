// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.teststores

import com.digitalasset.canton.TestEssentials
import com.digitalasset.canton.store.IndexedPhysicalSynchronizer
import com.digitalasset.canton.store.db.DbSequencedEventStore
import com.digitalasset.canton.store.teststores.H2StoresTest
import org.scalatest.Suite

trait H2CantonStores extends H2StoresTest {
  self: Suite & TestEssentials =>

  def createSequencedEventStore(
      indexedPhysicalSynchronizer: IndexedPhysicalSynchronizer
  ): DbSequencedEventStore =
    new DbSequencedEventStore(
      inMemoryH2Storage,
      indexedPhysicalSynchronizer,
      timeouts,
      loggerFactory,
    )(h2InMemoryEc)

  override protected def tablesToClean: Seq[String] =
    super.tablesToClean :+ "common_sequenced_events"
}
