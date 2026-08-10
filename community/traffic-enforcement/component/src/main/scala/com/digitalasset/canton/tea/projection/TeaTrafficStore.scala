// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection

import cats.data.OptionT
import com.digitalasset.base.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.TrafficEnforcementErrorGroup
import com.digitalasset.canton.error.{CantonError, ContextualizedCantonError}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
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
    *   optional account state
    */
  def persistTrafficDelta(
      accountId: AccountId,
      eventId: EventId,
      eventSource: EventSource,
      trafficDelta: TrafficDelta,
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

object TeaTrafficStore {
  trait TeaTrafficStoreError extends Product with Serializable with ContextualizedCantonError

  object TeaTrafficStoreError extends TrafficEnforcementErrorGroup {
    @Explanation(
      "This error indicates that a traffic delta could not be applied, as it would overflow the current credit balance."
    )
    @Resolution(
      "Use a lower (absolute) delta value."
    )
    object TrafficUpdateOutOfBound
        extends ErrorCode(
          "TRAFFIC_UPDATE_OUT_OF_BOUND",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(accountId: AccountId, delta: TrafficDelta)(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause =
              s"The traffic delta $delta cannot be applied to the current balance of $accountId without the credit balance exceeding its maximum value."
          )
          with TeaTrafficStoreError
    }
  }
}
