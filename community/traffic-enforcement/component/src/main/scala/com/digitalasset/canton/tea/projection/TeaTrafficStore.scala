// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection

import cats.data.OptionT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tracing.TraceContext

/** Persistence store for the TEA. Provides methods to update and retrieve traffic for accounts.
  */
trait TeaTrafficStore {

  /** Return the current balance for an account
    * @param accountId
    *   account to retrieve
    * @return
    *   optional account state
    */
  def getBalance(accountId: AccountId)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, AccountState]

  /** Insert a new event into the event table, and updates the corresponding account state. The new
    * balance be current balance + delta. Delta is positive for credits and negative for debits.
    *
    * Note: the account state returned may have a timestamp higher than this timestamp. That's
    * because events can arrive out of order from different sources from different clocks. To avoid
    * the account state timestamp moving back and forth, its timestamp is always kept to the most
    * recent event that updated it.
    *
    * @param accountId
    *   account to update
    * @param delta
    *   delta to apply
    * @param timestamp
    *   timestamp of the update. There's no guarantee that the timestamp is strictly higher than
    *   previous entries.
    * @return
    *   optional account state
    */
  def persistDelta(
      accountId: AccountId,
      eventId: EventId,
      eventSource: EventSource,
      eventType: EventType,
      delta: Long,
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, AccountState]

  // Note: only used internally for testing, need to add pagination and / or streaming when exposed
  /** Return events, ordered by timestamp, for an account from the given timestamp forward
    * (inclusive). Note that this might NOT be the order in which events were applied to the
    * balance, as there's no single clock all the event timestamps come from.
    *
    * @param accountId
    *   account to update
    * @param fromInclusive
    *   timestamp to retrieve events from (inclusive)
    * @return
    *   events since fromInclusive for accountId
    */
  def getEvents(accountId: AccountId, fromInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[DeltaEvent]]
}
