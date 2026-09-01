// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ledger.participant.state.SynchronizerIndex
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.store.AcsDigestTestBase
import com.digitalasset.canton.platform.store.backend.LedgerEnd
import com.digitalasset.canton.topology.{DefaultTestIdentities, SynchronizerId, UniqueIdentifier}
import org.scalatest.wordspec.AnyWordSpec

class DigestProcessorFactoryImplTest extends AnyWordSpec with BaseTest with AcsDigestTestBase {

  private lazy val reinitTimepoint = tp(100)
  def testSynchronizerId = DefaultTestIdentities.synchronizerId
  private def mockLedgerApiStore(hasLedgerEnd: Boolean): LedgerApiStore = {
    val ledgerEndO = Option.when(hasLedgerEnd)(
      LedgerEnd(
        reinitTimepoint.offset,
        reinitTimepoint.offset.unwrap,
        reinitTimepoint.offset.unwrap.toInt,
        reinitTimepoint.recordTime,
        Map(
          testSynchronizerId -> SynchronizerIndex(
            repairIndex = None,
            sequencerIndex = Some(reinitTimepoint.recordTime),
            recordTime = reinitTimepoint.recordTime,
          )
        ),
      )
    )
    val mockStore = mock[LedgerApiStore]
    when(
      mockStore.ledgerEnd
    ).thenAnswer(ledgerEndO)
    mockStore
  }

  "reinitializationTimepoint" should {
    "give back proper reinit time" in {
      val store = mockLedgerApiStore(hasLedgerEnd = true)
      val reinitTp = DigestProcessorFactoryImpl.reinitializationTimepoint(store, testSynchronizerId)
      reinitTp shouldEqual reinitTimepoint
    }

    s"fail when the ledger end is not set" in {
      val store = mockLedgerApiStore(hasLedgerEnd = false)
      loggerFactory.assertInternalError[IllegalStateException](
        DigestProcessorFactoryImpl.reinitializationTimepoint(store, testSynchronizerId),
        _.getMessage should include(
          s"There is no suitable last offset for synchronizer $testSynchronizerId in the Ledger"
        ),
      )
    }

    "fail when the synchronizer is unknown" in {
      val store = mockLedgerApiStore(hasLedgerEnd = true)
      val otherSynchronizer =
        SynchronizerId(UniqueIdentifier.tryCreate("other", DefaultTestIdentities.namespace))
      loggerFactory.assertInternalError[IllegalStateException](
        DigestProcessorFactoryImpl.reinitializationTimepoint(store, otherSynchronizer),
        _.getMessage should include(
          s"There is no suitable last offset for synchronizer $otherSynchronizer in the Ledger"
        ),
      )
    }
  }
}
