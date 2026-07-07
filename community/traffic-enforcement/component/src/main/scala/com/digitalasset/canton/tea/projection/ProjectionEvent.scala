// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection

import com.digitalasset.canton.config.CantonRequireTypes.{
  LengthLimitedStringWrapper,
  LengthLimitedStringWrapperCompanion,
  String255,
}
import com.digitalasset.canton.data.CantonTimestamp
import slick.jdbc.{GetResult, SetParameter}

/** Event changing the balance of an account
  *
  * @param delta
  *   amount by which the balance changes. Positive for credits, negative for debits.
  * @param timestamp
  *   timestamp of the event
  */
final case class DeltaEvent(
    delta: Long,
    timestamp: CantonTimestamp,
    eventType: EventType,
    eventSource: EventSource,
)
object DeltaEvent {
  implicit val getDeltaEvent: GetResult[DeltaEvent] = GetResult { r =>
    val delta = r.<<[Long]
    val updatedAt = r.<<[CantonTimestamp]
    val eventType = r.<<[EventType]
    val eventSource = r.<<[EventSource]
    DeltaEvent(delta, updatedAt, eventType, eventSource)
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
    totalDebits: Long,
    totalCredits: Long,
    updatedAt: CantonTimestamp,
) {

  /** Traffic balance
    */
  def balance: Long = totalCredits - totalDebits
}
object AccountState {
  def apply(account: AccountId, balance: Long, updatedAt: CantonTimestamp): AccountState =
    AccountState(
      account = account,
      totalDebits = if (balance < 0) -balance else 0L,
      totalCredits = if (balance > 0) balance else 0L,
      updatedAt = updatedAt,
    )

  implicit val getAccountStateResult: GetResult[AccountState] = GetResult { r =>
    val account = r.<<[AccountId]
    val totalDebits = r.<<[Long]
    val totalCredits = r.<<[Long]
    val updatedAt = r.<<[CantonTimestamp]
    AccountState(account, totalDebits, totalCredits, updatedAt)
  }
}

/** Represents a type of event. For now only "Usage" is supported which describes standard traffic
  * usage.
  * @param code
  *   unique code per event type
  */
sealed abstract class EventType(val code: Short)
object EventType {

  /** Standard traffic usage from transaction processing
    */
  case object Usage extends EventType(0)

  val values: Set[EventType] = Set(Usage)
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

  /** Events coming from the Ledger API (completion streams for debits so far)
    */
  case object LedgerAPI extends EventSource(0)

  /** Events coming from the TEA API (UpdateAccount RPC)
    */
  case object TeaAPI extends EventSource(1)

  val values: Set[EventSource] = Set(LedgerAPI, TeaAPI)

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
