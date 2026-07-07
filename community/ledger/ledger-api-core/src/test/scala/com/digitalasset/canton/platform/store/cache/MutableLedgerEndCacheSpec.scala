// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.SynchronizerIndex
import com.digitalasset.canton.platform.store.backend.LedgerEnd
import com.digitalasset.canton.topology.SynchronizerId
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MutableLedgerEndCacheSpec extends AnyWordSpec with Matchers with OptionValues {
  private val synchronizer1: SynchronizerId =
    SynchronizerId.tryFromString("x::synchronizer1")
  private val synchronizer2: SynchronizerId =
    SynchronizerId.tryFromString("x::synchronizer2")

  private def synchronizerIndex(recordTimeMicros: Long): SynchronizerIndex =
    SynchronizerIndex(None, None, CantonTimestamp.assertFromLong(recordTimeMicros))

  private def ledgerEndWithSynchronizerIndex(
      offsetValue: Long,
      synchronizerIndices: Map[SynchronizerId, SynchronizerIndex],
  ): LedgerEnd =
    LedgerEnd(
      lastOffset = Offset.tryFromLong(offsetValue),
      lastEventSeqId = 0L,
      lastStringInterningId = 0,
      lastPublicationTime = CantonTimestamp.MinValue,
      synchronizerIndices = synchronizerIndices,
    )

  "MutableLedgerEndCache" should {
    "start empty" in {
      MutableLedgerEndCache().apply() shouldBe None
    }

    "set" should {
      "replace the cached ledger end and offset" in {
        val cache = MutableLedgerEndCache()
        val initial = ledgerEndWithSynchronizerIndex(
          offsetValue = 1L,
          synchronizerIndices = Map(synchronizer1 -> synchronizerIndex(10L)),
        )
        cache.set(Some(initial))
        cache().value.lastOffset shouldBe Offset.tryFromLong(1L)

        val replacement = ledgerEndWithSynchronizerIndex(
          offsetValue = 2L,
          synchronizerIndices = Map(synchronizer2 -> synchronizerIndex(20L)),
        )
        cache.set(Some(replacement))
        cache().value.lastOffset shouldBe Offset.tryFromLong(2L)
        cache().value.synchronizerIndices shouldBe Map(synchronizer2 -> synchronizerIndex(20L))
      }
    }

    "update" should {
      "replace the ledger end offset and merge synchronizer indices" in {
        val cache = MutableLedgerEndCache()
        cache.set(
          Some(
            ledgerEndWithSynchronizerIndex(
              offsetValue = 1L,
              synchronizerIndices = Map(synchronizer1 -> synchronizerIndex(10L)),
            )
          )
        )

        cache.update(
          ledgerEnd = ledgerEndWithSynchronizerIndex(
            offsetValue = 5L,
            synchronizerIndices = Map(synchronizer1 -> synchronizerIndex(15L)),
          )
        )

        cache().value.lastOffset shouldBe Offset.tryFromLong(5L)
        cache().value.synchronizerIndices shouldBe Map(synchronizer1 -> synchronizerIndex(15L))
      }

      "leave synchronizer1 unchanged when only synchronizer2 is updated" in {
        val cache = MutableLedgerEndCache()
        cache.set(
          Some(
            ledgerEndWithSynchronizerIndex(
              offsetValue = 1L,
              synchronizerIndices = Map(
                synchronizer1 -> synchronizerIndex(10L)
              ),
            )
          )
        )

        cache.update(
          ledgerEnd = ledgerEndWithSynchronizerIndex(
            offsetValue = 2L,
            synchronizerIndices = Map(synchronizer2 -> synchronizerIndex(20L)),
          )
        )

        cache().value.lastOffset shouldBe Offset.tryFromLong(2L)
        cache().value.synchronizerIndices(synchronizer1) shouldBe synchronizerIndex(10L)
        cache().value.synchronizerIndices(synchronizer2) shouldBe synchronizerIndex(20L)
      }

      "not decrease a synchronizer index when the update is earlier than the current state" in {
        val cache = MutableLedgerEndCache()
        cache.set(
          Some(
            ledgerEndWithSynchronizerIndex(
              offsetValue = 1L,
              synchronizerIndices = Map(synchronizer1 -> synchronizerIndex(100L)),
            )
          )
        )

        cache.update(
          ledgerEnd = ledgerEndWithSynchronizerIndex(
            offsetValue = 3L,
            synchronizerIndices = Map(synchronizer1 -> synchronizerIndex(50L)),
          )
        )

        cache().value.lastOffset shouldBe Offset.tryFromLong(3L)
        cache().value.synchronizerIndices(synchronizer1) shouldBe synchronizerIndex(100L)
      }

      "update all synchronizers in a single batch" in {
        val cache = MutableLedgerEndCache()
        cache.set(
          Some(
            ledgerEndWithSynchronizerIndex(
              offsetValue = 1L,
              synchronizerIndices = Map(
                synchronizer1 -> synchronizerIndex(10L),
                synchronizer2 -> synchronizerIndex(20L),
              ),
            )
          )
        )

        cache.update(
          ledgerEnd = ledgerEndWithSynchronizerIndex(
            offsetValue = 7L,
            synchronizerIndices = Map(
              synchronizer1 -> synchronizerIndex(30L),
              synchronizer2 -> synchronizerIndex(40L),
            ),
          )
        )

        cache().value.lastOffset shouldBe Offset.tryFromLong(7L)
        cache().value.synchronizerIndices shouldBe Map(
          synchronizer1 -> synchronizerIndex(30L),
          synchronizer2 -> synchronizerIndex(40L),
        )
      }

      "update an empty cache" in {
        val cache = MutableLedgerEndCache()

        cache.update(
          ledgerEnd = ledgerEndWithSynchronizerIndex(
            offsetValue = 9L,
            synchronizerIndices = Map(synchronizer1 -> synchronizerIndex(11L)),
          )
        )

        cache().value.lastOffset shouldBe Offset.tryFromLong(9L)
        cache().value.synchronizerIndices shouldBe Map(synchronizer1 -> synchronizerIndex(11L))
      }
    }
  }
}
