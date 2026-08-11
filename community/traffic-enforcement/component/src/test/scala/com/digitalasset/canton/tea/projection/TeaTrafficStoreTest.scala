// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection

import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tea.TrafficEnforcementErrors
import com.digitalasset.canton.tea.TrafficEnforcementErrors.TrafficEnforcementError
import com.digitalasset.canton.tea.projection.TrafficDelta.{creditBalanceDelta, debitBalanceDelta}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.{BaseTest, FailOnShutdown, ProtocolVersionChecksAsyncWordSpec}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.language.implicitConversions

trait TeaTrafficStoreTest
    extends BeforeAndAfterAll
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec
    with FailOnShutdown {
  this: AsyncWordSpec =>

  // Test convenience: let plain numeric literals stand in for NonNegativeLong balances.
  private implicit def intToNonNegativeLong(value: Int): NonNegativeLong =
    NonNegativeLong.tryCreate(value.toLong)
  private implicit def longToNonNegativeLong(value: Long): NonNegativeLong =
    NonNegativeLong.tryCreate(value)

  protected val clock = new SimClock(loggerFactory = loggerFactory)

  private def uniqueId = EventId.tryCreate(UUID.randomUUID().toString)

  private val ledgerApi = EventSource.LedgerAPICompletions
  private val teaApi = EventSource.TeaAPI
  private val alice = AccountId.tryCreate("alice")
  private val butternut = AccountId.tryCreate("butternut")
  private val unknown = AccountId.tryCreate("unknown")

  /** Small syntax helper to cut down on the boilerplate of calling `persistTrafficDelta`. Only the
    * account, delta and timestamp usually matter to a given test, everything else defaults to
    * sensible values that can be overridden when a test cares about them.
    */
  private implicit class StoreOps(store: TeaTrafficStore) {
    def persist(
        account: AccountId,
        delta: TrafficDelta,
        timestamp: CantonTimestamp,
        eventId: EventId = uniqueId,
        source: EventSource = ledgerApi,
    ): FutureUnlessShutdown[Either[TrafficEnforcementError, Option[AccountState]]] =
      store
        .persistTrafficDelta(account, eventId, source, delta, timestamp)
        .value

    /** Happy path convenience: asserts the persist was not rejected and returns the optional
      * account state.
      */
    def persistOk(
        account: AccountId,
        delta: TrafficDelta,
        timestamp: CantonTimestamp,
        eventId: EventId = uniqueId,
        source: EventSource = ledgerApi,
    ): FutureUnlessShutdown[Option[AccountState]] =
      persist(account, delta, timestamp, eventId, source).map {
        case Right(state) => state
        case Left(err) => fail(s"expected the delta to be accepted, but it was rejected: $err")
      }

    def balance(account: AccountId): FutureUnlessShutdown[Option[AccountState]] =
      store.getBalance(account).value
  }

  private def deltaEvent(
      delta: TrafficDelta,
      timestamp: CantonTimestamp,
      source: EventSource = ledgerApi,
  ): DeltaEvent = DeltaEvent(delta, timestamp, source)

  /** Shared assertion for the dedup tests: alice must have exactly one credit of 10, meaning any
    * duplicate (event_source, event_id) persist was ignored.
    */
  private def assertAliceHasInitialCredit(
      store: TeaTrafficStore,
      timestamp: CantonTimestamp,
  ): FutureUnlessShutdown[Unit] =
    for {
      balance <- store.balance(alice)
      events <- store.getEvents(alice, timestamp)
    } yield {
      balance shouldBe Some(AccountState.credits(alice, 10, timestamp))
      events should contain theSameElementsInOrderAs Seq(
        deltaEvent(creditBalanceDelta(10), timestamp)
      )
      ()
    }

  def teaTrafficStore(mk: () => TeaTrafficStore): Unit = {
    "TeaTrafficStore" should {
      "return empty for unknown account" in {
        val store = mk()
        store.balance(unknown).map(_ shouldBe empty)
      }

      "return empty events for unknown account" in {
        val store = mk()
        store.getEvents(unknown, clock.now).map(_ shouldBe empty)
      }

      "insert events in the store" in {
        val store = mk()
        val timestamp1 = clock.now
        val timestamp2 = timestamp1.immediateSuccessor
        val timestamp3 = timestamp2.immediateSuccessor
        for {
          afterPersist1 <- store.persistOk(alice, creditBalanceDelta(10), timestamp1)
          getBalance1 <- store.balance(alice)
          afterPersist2 <- store.persistOk(alice, debitBalanceDelta(5), timestamp2)
          getBalance2 <- store.balance(alice)
          afterPersist3 <- store.persistOk(alice, debitBalanceDelta(3), timestamp3)
          getBalance3 <- store.balance(alice)
        } yield {
          val expected1 = AccountState.credits(alice, 10, timestamp1)
          val expected2 = expected1.copy(totalDebits = 5L, updatedAt = timestamp2)
          val expected3 = expected2.copy(totalDebits = 8L, updatedAt = timestamp3)

          afterPersist1 shouldBe Some(expected1)
          getBalance1 shouldBe Some(expected1)
          afterPersist2 shouldBe Some(expected2)
          getBalance2 shouldBe Some(expected2)
          afterPersist3 shouldBe Some(expected3)
          getBalance3 shouldBe Some(expected3)
        }
      }

      "accumulate credits and debits and expose the balance" in {
        val store = mk()
        val timestamp1 = clock.now
        val timestamp2 = timestamp1.immediateSuccessor
        val timestamp3 = timestamp2.immediateSuccessor
        for {
          _ <- store.persistOk(alice, creditBalanceDelta(100), timestamp1)
          _ <- store.persistOk(alice, debitBalanceDelta(30), timestamp2)
          _ <- store.persistOk(alice, debitBalanceDelta(20), timestamp3)
          finalBalance <- store.balance(alice)
        } yield {
          val state = finalBalance.value
          state.totalCredits.value shouldBe 100L
          state.totalDebits.value shouldBe 50L
          state.balance shouldBe 50L
          state.updatedAt shouldBe timestamp3
        }
      }

      "not overflow the total credit value" in {
        val store = mk()
        for {
          accountFetch <- store.persistOk(alice, creditBalanceDelta(Long.MaxValue), clock.now)
          // Going 1 above Long.MaxValue should be rejected
          rejected <- store.persist(alice, creditBalanceDelta(1L), clock.now)
        } yield {
          accountFetch.value.totalCredits.value shouldBe Long.MaxValue
          val error = rejected.left.value
          error.code.id shouldBe TrafficEnforcementErrors.TrafficUpdateOutOfBound.id
          error.context should contain("accountId" -> alice.toString)
        }
      }

      "not overflow the total debit value" in {
        val store = mk()
        for {
          accountFetch <- store.persistOk(alice, debitBalanceDelta(Long.MaxValue), clock.now)
          // Going 1 above Long.MaxValue should be rejected
          rejected <- store.persist(alice, debitBalanceDelta(Long.MaxValue), clock.now)
        } yield {
          accountFetch.value.totalDebits.value shouldBe Long.MaxValue
          val error = rejected.left.value
          error.code.id shouldBe TrafficEnforcementErrors.TrafficUpdateOutOfBound.id
          error.context should contain("accountId" -> alice.toString)
        }
      }

      "not allow negative total credit value" in {
        val store = mk()
        for {
          // The totalCredit on the account is 0, going -1 would bring it to negative
          rejected <- store.persist(alice, creditBalanceDelta(-1L), clock.now)
          events <- store.getEvents(alice, CantonTimestamp.MinValue)
        } yield {
          events shouldBe empty
          val error = rejected.left.value
          error.code.id shouldBe TrafficEnforcementErrors.TrafficUpdateOutOfBound.id
          error.context should contain("accountId" -> alice.toString)
        }
      }

      "not allow negative total debit value" in {
        val store = mk()
        for {
          // The totalDebit on the account is 0, going -1 would bring it to negative
          rejected <- store.persist(alice, debitBalanceDelta(-1L), clock.now)
          events <- store.getEvents(alice, CantonTimestamp.MinValue)
        } yield {
          events shouldBe empty
          val error = rejected.left.value
          error.code.id shouldBe TrafficEnforcementErrors.TrafficUpdateOutOfBound.id
          error.context should contain("accountId" -> alice.toString)
        }
      }

      "leave the balance untouched when an update is rejected" in {
        val store = mk()
        val timestamp1 = clock.now
        for {
          _ <- store.persistOk(alice, creditBalanceDelta(Long.MaxValue), timestamp1)
          rejected <- store.persist(alice, creditBalanceDelta(1L), timestamp1.immediateSuccessor)
          getBalance <- store.balance(alice)
        } yield {
          // The rejected update must not have been partially applied
          rejected.isLeft shouldBe true
          getBalance shouldBe Some(AccountState.credits(alice, Long.MaxValue, timestamp1))
        }
      }

      "scope by accounts" in {
        val store = mk()
        val timestamp1 = clock.now
        val timestamp2 = timestamp1.immediateSuccessor
        for {
          _ <- store.persistOk(alice, creditBalanceDelta(10), timestamp1)
          _ <- store.persistOk(butternut, debitBalanceDelta(5), timestamp2)
          getBalanceAlice <- store.balance(alice)
          getBalanceButternut <- store.balance(butternut)
        } yield {
          getBalanceAlice shouldBe Some(AccountState.credits(alice, 10, timestamp1))
          getBalanceButternut shouldBe Some(AccountState.debits(butternut, 5, timestamp2))
        }
      }

      "keep the most recent timestamp for out of order updates" in {
        val store = mk()
        val timestamp1 = clock.now
        val older = timestamp1.immediatePredecessor
        for {
          afterPersist1 <- store.persistOk(alice, creditBalanceDelta(10), timestamp1)
          getBalance1 <- store.balance(alice)
          // An older event arrives after a newer one
          afterPersist2 <- store.persistOk(alice, debitBalanceDelta(3), older)
          getBalance2 <- store.balance(alice)
        } yield {
          val expected1 = AccountState(alice, totalDebits = 0, totalCredits = 10, timestamp1)
          // Should still be timestamp1 because it's more recent
          val expected2 = AccountState(alice, totalDebits = 3, totalCredits = 10, timestamp1)

          afterPersist1 shouldBe Some(expected1)
          getBalance1 shouldBe Some(expected1)
          afterPersist2 shouldBe Some(expected2)
          getBalance2 shouldBe Some(expected2)
        }
      }

      "advance the timestamp when a newer update arrives" in {
        val store = mk()
        val timestamp1 = clock.now
        val newer = timestamp1.immediateSuccessor
        for {
          _ <- store.persistOk(alice, creditBalanceDelta(10), timestamp1)
          _ <- store.persistOk(alice, debitBalanceDelta(3), newer)
          getBalance <- store.balance(alice)
        } yield getBalance shouldBe Some(
          AccountState(alice, totalDebits = 3, totalCredits = 10, newer)
        )
      }

      "support negative balance deltas" in {
        val store = mk()
        val timestamp1 = clock.now
        for {
          _ <- store.persistOk(alice, debitBalanceDelta(10), timestamp1)
          negativeDebit <- store.persistOk(alice, debitBalanceDelta(-5), timestamp1)
          getBalanceDebit <- store.balance(alice)
          _ <- store.persistOk(butternut, creditBalanceDelta(10), timestamp1)
          negativeCredit <- store.persistOk(butternut, creditBalanceDelta(-3), timestamp1)
          getBalanceCredit <- store.balance(butternut)
        } yield {
          val expectedDebit = AccountState.debits(alice, 5, timestamp1)
          negativeDebit shouldBe Some(expectedDebit)
          getBalanceDebit shouldBe Some(expectedDebit)
          getBalanceDebit.value.balance shouldBe -5L

          val expectedCredit = AccountState.credits(butternut, 7, timestamp1)
          negativeCredit shouldBe Some(expectedCredit)
          getBalanceCredit shouldBe Some(expectedCredit)
          getBalanceCredit.value.balance shouldBe 7L
        }
      }

      "retrieve events" in {
        val store = mk()
        val timestamp1 = clock.now
        val timestamp2 = timestamp1.immediateSuccessor
        for {
          _ <- store.persistOk(alice, creditBalanceDelta(10), timestamp1)
          events1 <- store.getEvents(alice, timestamp1)
          _ <- store.persistOk(alice, debitBalanceDelta(5), timestamp2)
          events2 <- store.getEvents(alice, timestamp1)
          // Filter by timestamp 2, should drop the first event
          events3 <- store.getEvents(alice, timestamp2)
          // Filter strictly after all events, should be empty
          events4 <- store.getEvents(alice, timestamp2.immediateSuccessor)
        } yield {
          events1 should contain theSameElementsInOrderAs Seq(
            deltaEvent(creditBalanceDelta(10), timestamp1)
          )
          events2 should contain theSameElementsInOrderAs Seq(
            deltaEvent(creditBalanceDelta(10), timestamp1),
            deltaEvent(debitBalanceDelta(5), timestamp2),
          )
          events3 should contain theSameElementsInOrderAs Seq(
            deltaEvent(debitBalanceDelta(5), timestamp2)
          )
          events4 shouldBe empty
        }
      }

      "retrieve events ordered by timestamp" in {
        val store = mk()
        val timestamp1 = clock.now
        // Event 2 has a timestamp older than event 1
        val timestamp2 = timestamp1.immediatePredecessor
        for {
          _ <- store.persistOk(alice, creditBalanceDelta(10), timestamp1)
          _ <- store.persistOk(alice, debitBalanceDelta(5), timestamp2)
          events <- store.getEvents(alice, timestamp2)
        } yield events should contain theSameElementsInOrderAs Seq(
          deltaEvent(debitBalanceDelta(5), timestamp2),
          deltaEvent(creditBalanceDelta(10), timestamp1),
        )
      }

      // Difference with the test below: here the duplicate targets the SAME account, so we check
      // that the second persist is ignored while a different source still goes through.
      "deduplicate on (event_source, event_id)" in {
        val store = mk()
        val timestamp1 = clock.now
        val id1 = EventId.tryCreate("id-1")
        for {
          first <- store.persistOk(
            alice,
            creditBalanceDelta(10),
            timestamp1,
            eventId = id1,
            source = ledgerApi,
          )
          // Same (source, id): must be a no-op
          dedup <- store.persistOk(
            alice,
            creditBalanceDelta(20),
            timestamp1,
            eventId = id1,
            source = ledgerApi,
          )
          _ <- assertAliceHasInitialCredit(store, timestamp1)
          // Unicity is per (event_source, event_id), so a different source should go through
          _ <- store.persistOk(
            alice,
            creditBalanceDelta(20),
            timestamp1,
            eventId = id1,
            source = teaApi,
          )
          events <- store.getEvents(alice, timestamp1)
          getBalance <- store.balance(alice)
        } yield {
          first shouldBe Some(AccountState.credits(alice, 10, timestamp1))
          // A duplicate persist returns the current, unchanged balance
          dedup shouldBe first
          getBalance shouldBe Some(AccountState.credits(alice, 30, timestamp1))
          events should contain theSameElementsInOrderAs
            Seq(deltaEvent(creditBalanceDelta(10), timestamp1)) :+
            deltaEvent(creditBalanceDelta(20), timestamp1, source = teaApi)
        }
      }

      // Difference with the test above: here the duplicate targets a DIFFERENT account, so we check
      // that dedup is global (per (source, id)) and not scoped per account, butternut is untouched.
      "deduplicate the same (event_source, event_id) across accounts" in {
        val store = mk()
        val timestamp1 = clock.now
        val sharedId = EventId.tryCreate("shared-id")
        for {
          first <- store.persistOk(alice, creditBalanceDelta(10), timestamp1, eventId = sharedId)
          // Same (source, id) globally: this is a no-op, butternut is never created
          dedup <- store.persistOk(
            butternut,
            creditBalanceDelta(20),
            timestamp1,
            eventId = sharedId,
          )
          _ <- assertAliceHasInitialCredit(store, timestamp1)
          getBalanceButternut <- store.balance(butternut)
          eventsButternut <- store.getEvents(butternut, timestamp1)
        } yield {
          first shouldBe Some(AccountState.credits(alice, 10, timestamp1))
          // The duplicate returns the (non-existent) balance of the account it targeted
          dedup shouldBe empty
          getBalanceButternut shouldBe empty
          eventsButternut shouldBe empty
        }
      }
    }
  }

}
