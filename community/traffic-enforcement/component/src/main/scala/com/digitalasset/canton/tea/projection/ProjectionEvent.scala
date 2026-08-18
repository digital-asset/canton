// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection

import com.digitalasset.canton.config.CantonRequireTypes.{
  LengthLimitedStringWrapper,
  LengthLimitedStringWrapperCompanion,
  String255,
}
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import slick.jdbc.{GetResult, SetParameter}

/** Core data model for traffic accounting used by the projection and its persistence layer.
  *
  * Everything is expressed as append-only deltas that are folded into a per-account state:
  *
  *   - [[TrafficDelta]] is the atomic change to apply. Rather than a single signed balance, it
  *     carries an [[EventType]] (credit vs debit) so that the same value can be routed to either
  *     the total credits or the total debits column (see `debitAndCreditDeltas`). This keeps
  *     credits and debits auditable independently while still allowing negative values (clawbacks /
  *     refunds).
  *   - [[DeltaEvent]] wraps a [[TrafficDelta]] with the metadata needed to persist and interpret
  *     it: the CantonTimestamp and the [[EventSource]] (where the event originated, e.g. Ledger API
  *     completions or the TEA API).
  *   - [[OffsetDeltaEvent]] adds the input-stream `offset`, and [[ProjectionEvent]] ties that event
  *     to an [[AccountId]]. These are the units flowing through the ingestion sources and the Pekko
  *     projection.
  *   - [[AccountState]] is the derived, cumulative view: total debits, total credits and the last
  *     update time for an account, with `balance = totalCredits - totalDebits`.
  */
object TrafficDelta {

  /** Constructs a TrafficDelta which value is to be added the [[AccountState.totalDebits]] balance
    * of the account.
    * @param value
    *   delta value by which to move the [[AccountState.totalDebits]] of the account. Can be
    *   negative.
    * @return
    *   a TrafficDelta
    */
  def debitBalanceDelta(value: Long): TrafficDelta = TrafficDelta(value, EventType.TotalDebitDelta)

  /** Constructs a TrafficDelta which value is to be added the [[AccountState.totalCredits]] balance
    * of the account.
    * @param value
    *   delta value by which to move the [[AccountState.totalCredits]] of the account. Can be
    *   negative.
    * @return
    *   a TrafficDelta
    */
  def creditBalanceDelta(value: Long): TrafficDelta =
    TrafficDelta(value, EventType.TotalCreditDelta)
}

/** A traffic delta to be applied to an account
  * @param value
  *   value to apply to the balance. Can be negative
  * @param eventType
  *   type of event: encodes how the event should be applied to the balance.
  */
final case class TrafficDelta(value: Long, eventType: EventType) extends PrettyPrinting {
  override protected def pretty: Pretty[TrafficDelta] =
    prettyOfClass(
      param("value", _.value),
      param("eventType", _.eventType),
    )

  def debitAndCreditDeltas: (Long, Long) = eventType match {
    case EventType.TotalCreditDelta => (0L, value)
    case EventType.TotalDebitDelta => (value, 0L)
  }
}

/** Event changing the balance of an account
  *
  * @param delta
  *   delta to apply to the balance
  * @param timestamp
  *   timestamp of the event
  */
final case class DeltaEvent(
    delta: TrafficDelta,
    timestamp: CantonTimestamp,
    eventSource: EventSource,
)

object DeltaEvent {
  implicit val getDeltaEvent: GetResult[DeltaEvent] = GetResult { r =>
    val delta = r.<<[Long]
    val updatedAt = r.<<[CantonTimestamp]
    val eventType = r.<<[EventType]
    val eventSource = r.<<[EventSource]

    DeltaEvent(TrafficDelta(delta, eventType), updatedAt, eventSource)
  }
}

/** Delta event with an offset
  * @param deltaEvent
  *   delta event
  * @param offset
  *   offset of the event
  */
final case class OffsetDeltaEvent(deltaEvent: DeltaEvent, offset: Long)

/** A projection event coming from an input stream
  * @param account
  *   account tied to the event
  * @param event
  *   event
  */
final case class ProjectionEvent(account: AccountId, event: OffsetDeltaEvent)

/** State of an account at a given point
  * @param account
  *   account tied to the state
  * @param totalDebits
  *   total debits on the account at this time
  * @param totalCredits
  *   total credits on the account at this time
  * @param updatedAt
  *   timestamp at which the state was updated
  */
final case class AccountState(
    account: AccountId,
    totalDebits: NonNegativeLong,
    totalCredits: NonNegativeLong,
    updatedAt: CantonTimestamp,
) {

  /** Traffic balance
    */
  def balance: Long = totalCredits.value - totalDebits.value
}
object AccountState {
  def credits(
      account: AccountId,
      balance: NonNegativeLong,
      updatedAt: CantonTimestamp,
  ): AccountState =
    AccountState(
      account = account,
      totalDebits = NonNegativeLong.zero,
      totalCredits = balance,
      updatedAt = updatedAt,
    )

  def debits(
      account: AccountId,
      balance: NonNegativeLong,
      updatedAt: CantonTimestamp,
  ): AccountState =
    AccountState(
      account = account,
      totalDebits = balance,
      totalCredits = NonNegativeLong.zero,
      updatedAt = updatedAt,
    )

  implicit val getAccountStateResult: GetResult[AccountState] = GetResult { r =>
    val account = r.<<[AccountId]
    val totalDebits = r.<<[NonNegativeLong]
    val totalCredits = r.<<[NonNegativeLong]
    val updatedAt = r.<<[CantonTimestamp]
    AccountState(account, totalDebits, totalCredits, updatedAt)
  }
}

/** Indicates how an event should be applied to the account state and how it affects its balance.
  * @param code
  *   unique code per balance type
  */
sealed abstract class EventType(val code: Short) extends PrettyPrinting
object EventType {

  /** Event type that models a delta to be applied to the [[AccountState.totalCredits]] of the
    * account. Associated to a positive value, it is a standard debit (typically traffic credited to
    * the account): it ADDS to [[AccountState.totalCredits]] Associated to a negative value, it is a
    * credit clawback: it REMOVES from [[AccountState.totalCredits]]
    */
  case object TotalCreditDelta extends EventType(0) {
    override protected def pretty: Pretty[TotalCreditDelta.this.type] =
      prettyOfString(_ => "credit_delta")
  }

  /** Event type that models a delta to be applied to the [[AccountState.totalDebits]] of the
    * account. Associated to a positive value, it is a standard debit (typically traffic consumed on
    * the account): it ADDS to [[AccountState.totalDebits]] Associated to a negative value, it is a
    * debit refund: it REMOVES from [[AccountState.totalDebits]]
    */
  case object TotalDebitDelta extends EventType(1) {
    override protected def pretty: Pretty[TotalDebitDelta.this.type] =
      prettyOfString(_ => "debit_delta")
  }

  val values: Set[EventType] = Set(TotalCreditDelta, TotalDebitDelta)
  private def fromCode(code: Short): EventType =
    values
      .find(_.code == code)
      .getOrElse(
        throw new IllegalArgumentException(s"Unknown EventType code from DB: $code")
      )

  implicit val setEventType: SetParameter[EventType] =
    SetParameter { (eventType, positionedParameters) =>
      positionedParameters.setShort(eventType.code)
    }

  implicit val getEventType: GetResult[EventType] =
    GetResult { positionedResult =>
      EventType.fromCode(positionedResult.nextShort())
    }
}

/** Represents where the event came from.
  * @param code
  *   unique code per event source
  */
sealed abstract class EventSource(val code: Short)
object EventSource {

  /** Events coming from the Ledger API completions: records traffic consumed on an account from
    * ledger events.
    */
  case object LedgerAPICompletions extends EventSource(0)

  /** Events coming from the TEA API (UpdateAccount RPC)
    */
  case object TeaAPI extends EventSource(1)

  val values: Set[EventSource] = Set(LedgerAPICompletions, TeaAPI)

  private def fromCode(code: Short): EventSource =
    values
      .find(_.code == code)
      .getOrElse(
        throw new IllegalArgumentException(s"Unknown EventSource code from DB: $code")
      )

  implicit val setEventSource: SetParameter[EventSource] =
    SetParameter { (eventSource, positionedParameters) =>
      positionedParameters.setShort(eventSource.code)
    }

  implicit val getEventSource: GetResult[EventSource] =
    GetResult { positionedResult =>
      EventSource.fromCode(positionedResult.nextShort())
    }
}

/** Account Id wrapper class
  * @param str
  *   account Id, limited to 255 characters
  */
final case class AccountId(str: String255) extends LengthLimitedStringWrapper

object AccountId extends LengthLimitedStringWrapperCompanion[String255, AccountId] {
  override def instanceName: String = "AccountId"
  override protected def companion: String255.type = String255
  override protected def factoryMethodWrapper(str: String255): AccountId = AccountId(str)
}

/** Event Id wrapper class
  * @param str
  *   event Id, limited to 255 characters
  */
final case class EventId(str: String255) extends LengthLimitedStringWrapper

object EventId extends LengthLimitedStringWrapperCompanion[String255, EventId] {
  override def instanceName: String = "EventId"
  override protected def companion: String255.type = String255
  override protected def factoryMethodWrapper(str: String255): EventId = EventId(str)
}
