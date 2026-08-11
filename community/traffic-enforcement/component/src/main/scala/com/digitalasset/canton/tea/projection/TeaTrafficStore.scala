// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tea.TrafficEnforcementErrors.TrafficEnforcementError
import com.digitalasset.canton.tracing.TraceContext

/** Persistence store for the TEA. Provides methods to update and retrieve traffic for accounts.
  */
trait TeaTrafficStore {

  /** Return the current balance for an account and traffic type
    * @param accountId
    *   account to retrieve
    * @return
    *   optional account state
    */
  def getBalance(accountId: AccountId)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, AccountState]

  /** Insert a new event into the event table, and updates the corresponding credit account state.
    * The new total credit will be existing + delta. The total debit stays unchanged. Delta may be a
    * negative value. This means the total credit value may be negative.
    *
    * Note: the account state returned may have a timestamp higher than this timestamp. That's
    * because events can arrive out of order from different sources from different clocks. To avoid
    * the account state timestamp moving back and forth, its timestamp is always kept to the most
    * recent event that updated it.
    *
    * @param accountId
    *   account to update
    * @param timestamp
    *   timestamp of the update. There's no guarantee that the timestamp is strictly higher than
    *   previous entries.
    * @param eventId
    *   eventId uniquely identifying this update
    * @param eventSource
    *   source of the event
    * @return
    *   a `Left` if the delta would push the running total negative or overflow it, otherwise a
    *   `Right` with an optional account state: `None` means the (event_source, event_id) pair was
    *   already seen.
    */
  def persistTrafficDelta(
      accountId: AccountId,
      eventId: EventId,
      eventSource: EventSource,
      trafficDelta: TrafficDelta,
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficEnforcementError, Option[AccountState]]

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
