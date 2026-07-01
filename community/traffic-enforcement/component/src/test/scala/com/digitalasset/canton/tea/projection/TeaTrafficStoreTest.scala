// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection

import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.{BaseTest, FailOnShutdown, ProtocolVersionChecksAsyncWordSpec}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID

trait TeaTrafficStoreTest
    extends BeforeAndAfterAll
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec
    with FailOnShutdown {
  this: AsyncWordSpec =>

  protected val clock = new SimClock(loggerFactory = loggerFactory)

  private def uniqueId = EventId.tryCreate(UUID.randomUUID().toString)

  private val eventType = EventType.Usage
  private val eventSource = EventSource.LedgerAPI
  private val alice = AccountId.tryCreate("alice")
  private val butternut = AccountId.tryCreate("butternut")

  def teaTrafficStore(mk: () => TeaTrafficStore): Unit = {
    "TeaTrafficStore" should {
      "return empty for unknown account" in {
        val store = mk()
        store.getBalance(AccountId.tryCreate("unknown")).value.map(_ shouldBe empty)
      }

      "insert events in the store" in {
        val store = mk()
        val timestamp1 = clock.now
        val timestamp2 = timestamp1.immediateSuccessor
        val timestamp3 = timestamp2.immediateSuccessor
        for {
          afterPersist1 <- store
            .persistDelta(alice, uniqueId, eventSource, eventType, 10, timestamp1)
            .value
          getBalance1 <- store.getBalance(alice).value
          afterPersist2 <- store
            .persistDelta(alice, uniqueId, eventSource, eventType, -5, timestamp2)
            .value
          getBalance2 <- store.getBalance(alice).value
          afterPersist3 <- store
            .persistDelta(alice, uniqueId, eventSource, eventType, -3, timestamp3)
            .value
          getBalance3 <- store.getBalance(alice).value
        } yield {
          val expected1 = AccountState(alice, 10, timestamp1)
          val expected2 = AccountState(alice, 5, 10, timestamp2)
          val expected3 = AccountState(alice, 8, 10, timestamp3)

          afterPersist1 shouldBe Some(expected1)
          getBalance1 shouldBe Some(expected1)
          afterPersist2 shouldBe Some(expected2)
          getBalance2 shouldBe Some(expected2)
          afterPersist3 shouldBe Some(expected3)
          getBalance3 shouldBe Some(expected3)
        }
      }

      "scope by accounts" in {
        val store = mk()
        val timestamp1 = clock.now
        val timestamp2 = timestamp1.immediateSuccessor
        for {
          _ <- store
            .persistDelta(alice, uniqueId, eventSource, eventType, 10, timestamp1)
            .value
          _ <- store
            .persistDelta(butternut, uniqueId, eventSource, eventType, -5, timestamp2)
            .value
          getBalanceAlice <- store.getBalance(alice).value
          getBalanceButternut <- store.getBalance(butternut).value
        } yield {
          val expectedAlice = AccountState(alice, 10, timestamp1)
          val expectedButternut =
            AccountState(butternut, totalDebits = 5, totalCredits = 0, timestamp2)

          getBalanceAlice shouldBe Some(expectedAlice)
          getBalanceButternut shouldBe Some(expectedButternut)
        }
      }

      "handle out of order update timestamps" in {
        val store = mk()
        val timestamp1 = clock.now
        val timestamp2 = timestamp1.immediatePredecessor
        for {
          afterPersist1 <- store
            .persistDelta(alice, uniqueId, eventSource, eventType, 10, timestamp1)
            .value
          getBalance1 <- store.getBalance(alice).value
          afterPersist2 <- store
            .persistDelta(alice, uniqueId, eventSource, eventType, -3, timestamp2)
            .value
          getBalance2 <- store.getBalance(alice).value
        } yield {
          val expected1 = AccountState(alice, 0, 10, timestamp1)
          // Should still be timestamp1 because it's more recent
          val expected2 = AccountState(alice, 3, 10, timestamp1)

          afterPersist1 shouldBe Some(expected1)
          getBalance1 shouldBe Some(expected1)
          afterPersist2 shouldBe Some(expected2)
          getBalance2 shouldBe Some(expected2)
        }
      }

      "support negative balances" in {
        val store = mk()
        val timestamp1 = clock.now
        for {
          afterPersist <- store
            .persistDelta(alice, uniqueId, eventSource, eventType, -10, timestamp1)
            .value
          getBalance <- store.getBalance(alice).value
        } yield {
          val expected = AccountState(alice, -10, timestamp1)

          afterPersist shouldBe Some(expected)
          getBalance shouldBe Some(expected)
        }
      }

      "retrieve events" in {
        val store = mk()
        val timestamp1 = clock.now
        val timestamp2 = timestamp1.immediateSuccessor
        for {
          _ <- store.persistDelta(alice, uniqueId, eventSource, eventType, 10, timestamp1).value
          events1 <- store.getEvents(alice, timestamp1)
          _ <- store.persistDelta(alice, uniqueId, eventSource, eventType, -5, timestamp2).value
          events2 <- store.getEvents(alice, timestamp1)
          // Filter by timestamp 2, should drop the first event
          events3 <- store.getEvents(alice, timestamp2)
        } yield {
          events1 should contain theSameElementsInOrderAs Seq(
            DeltaEvent(10, timestamp1, eventType, eventSource)
          )
          events2 should contain theSameElementsInOrderAs Seq(
            DeltaEvent(10, timestamp1, eventType, eventSource),
            DeltaEvent(-5, timestamp2, eventType, eventSource),
          )
          events3 should contain theSameElementsInOrderAs Seq(
            DeltaEvent(-5, timestamp2, eventType, eventSource)
          )
        }
      }

      "retrieve events ordered by timestamp" in {
        val store = mk()
        val timestamp1 = clock.now
        // Event 2 has a timestamp older than event 1
        val timestamp2 = timestamp1.immediatePredecessor
        for {
          _ <- store.persistDelta(alice, uniqueId, eventSource, eventType, 10, timestamp1).value
          _ <- store.persistDelta(alice, uniqueId, eventSource, eventType, -5, timestamp2).value
          events <- store.getEvents(alice, timestamp2)
        } yield {
          events should contain theSameElementsInOrderAs Seq(
            DeltaEvent(-5, timestamp2, eventType, eventSource),
            DeltaEvent(10, timestamp1, eventType, eventSource),
          )
        }
      }

      "deduplicate on (event_source, event_id)" in {
        val store = mk()
        val timestamp1 = clock.now
        for {
          _ <- store
            .persistDelta(alice, EventId.tryCreate("id-1"), eventSource, eventType, 10, timestamp1)
            .value
          events1 <- store.getEvents(alice, timestamp1)
          getBalance1 <- store.getBalance(alice).value
          _ <- store
            .persistDelta(alice, EventId.tryCreate("id-1"), eventSource, eventType, 20, timestamp1)
            .value
          events2 <- store.getEvents(alice, timestamp1)
          getBalance2 <- store.getBalance(alice).value
          // Unicity is per (event_source, event_id), so a different source should go through
          _ <- store
            .persistDelta(
              alice,
              EventId.tryCreate("id-1"),
              EventSource.TeaAPI,
              eventType,
              20,
              timestamp1,
            )
            .value
          events3 <- store.getEvents(alice, timestamp1)
          getBalance3 <- store.getBalance(alice).value
        } yield {
          getBalance1 shouldBe Some(AccountState(alice, 10, timestamp1))
          // Balance2 should not have changed
          getBalance2 shouldBe Some(AccountState(alice, 10, timestamp1))
          getBalance3 shouldBe Some(AccountState(alice, 30, timestamp1))

          val expectedEvents1 = Seq(DeltaEvent(10, timestamp1, eventType, eventSource))
          events1 should contain theSameElementsInOrderAs expectedEvents1
          events2 should contain theSameElementsInOrderAs expectedEvents1
          events3 should contain theSameElementsInOrderAs expectedEvents1 :+ DeltaEvent(
            20,
            timestamp1,
            eventType,
            EventSource.TeaAPI,
          )
        }
      }
    }
  }

}
