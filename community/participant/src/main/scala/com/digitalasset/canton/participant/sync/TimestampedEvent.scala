// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.syntax.either._
import cats.syntax.option._
import com.daml.ledger.participant.state.v2.Update.TransactionAccepted
import com.daml.lf.data.ImmArray
import com.digitalasset.canton.config.RequireTypes.String300
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.protocol.CausalityUpdate
import com.digitalasset.canton.participant.store.InFlightSubmissionStore.InFlightByMessageId
import com.digitalasset.canton.participant.sync.TimestampedEvent.EventId
import com.digitalasset.canton.participant.{LedgerSyncEvent, LocalOffset}
import com.digitalasset.canton.sequencing.protocol.MessageId
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil._
import com.digitalasset.canton.{DomainId, LedgerTransactionId, SequencerCounter, checked}
import slick.jdbc.{GetResult, SetParameter}

import java.util.UUID

case class TimestampedEvent(
    event: LedgerSyncEvent,
    localOffset: LocalOffset,
    requestSequencerCounter: Option[SequencerCounter],
    eventId: Option[EventId],
)(implicit val traceContext: TraceContext)
    extends PrettyPrinting {

  val timestamp: CantonTimestamp = CantonTimestamp(event.recordTime)

  /** Yields a version of this event that is suitable for comparison.
    * I.e., to check for equality call `event1.normalized == event2.normalized`.
    */
  def normalized: TimestampedEvent = {
    val normalizeEvent = event match {
      case ta: TransactionAccepted =>
        val normalizedTransactionMeta = ta.transactionMeta.optNodeSeeds match {
          case _: Some[_] => ta.transactionMeta
          case None => ta.transactionMeta.copy(optNodeSeeds = Some(ImmArray.empty))
        }
        ta.copy(transactionMeta = normalizedTransactionMeta)

      case _ => event
    }

    copy(event = normalizeEvent)
  }

  def eventSize: Int = TimestampedEvent.eventSize(event)

  override def pretty: Pretty[TimestampedEvent] = prettyOfParam(_.event)
}

object TimestampedEvent {
  def apply(
      event: LedgerSyncEvent,
      localOffset: LocalOffset,
      requestSequencerCounter: Option[SequencerCounter],
  )(implicit traceContext: TraceContext): TimestampedEvent =
    TimestampedEvent(
      event,
      localOffset,
      requestSequencerCounter,
      EventId.fromLedgerSyncEvent(event),
    )

  implicit def getResultTimestampedEvent(implicit
      readEvent: GetResult[LedgerSyncEvent],
      getResultByteArray: GetResult[Array[Byte]],
  ): GetResult[TimestampedEvent] =
    GetResult { r =>
      import TraceContext.readTraceContextFromDb

      val requestCounter = r.nextLong()
      val requestSequencerCounter = r.nextLongOption()
      val eventId = GetResult[Option[EventId]].apply(r)
      val event = implicitly[GetResult[LedgerSyncEvent]].apply(r)
      val traceContext = implicitly[GetResult[TraceContext]].apply(r)
      TimestampedEvent(event, requestCounter, requestSequencerCounter, eventId)(traceContext)
    }

  /** The size of the event for the metric in the [[com.digitalasset.canton.participant.store.MultiDomainEventLog]]. */
  def eventSize(event: LedgerSyncEvent): Int = event match {
    case event: TransactionAccepted => event.transaction.roots.length
    case _ => 1
  }

  sealed trait EventId extends PrettyPrinting with Product with Serializable {
    def asString300: String300
    def associatedDomain: Option[DomainId]
  }

  object EventId {
    val transactionEventIdPrefix = "T"
    val timelyRejectionEventIdPrefix = "M"

    /** Separator between the [[com.digitalasset.canton.DomainId]] and the [[com.digitalasset.canton.sequencing.protocol.MessageId]].
      * Since a [[com.digitalasset.canton.DomainId]] is a [[com.digitalasset.canton.topology.SafeSimpleString]],
      * it cannot contain a #.
      */
    val timelyRejectionEventSeparator = "#"

    private val TransactionEventIdRegex = s"$transactionEventIdPrefix(.*)".r
    private val TimelyRejectionEventIdRegex =
      s"$timelyRejectionEventIdPrefix([^$timelyRejectionEventSeparator]*)$timelyRejectionEventSeparator(.*)".r

    implicit val eventIdSetParameter: SetParameter[EventId] = (v, pp) =>
      pp.setString(v.asString300.unwrap)

    implicit val optionEventIdSetParameter: SetParameter[Option[EventId]] = (v, pp) =>
      pp.setStringOption(v.map(_.asString300.unwrap))

    implicit val optionEventIdGetResult: GetResult[Option[EventId]] = GetResult { r =>
      val eventIdO = r.nextStringOption()
      eventIdO.map {
        case TransactionEventIdRegex(transactionIdS) =>
          val transactionId = LedgerTransactionId
            .fromString(transactionIdS)
            .valueOr(err =>
              throw new DbDeserializationException(s"Unable to deserialize transaction ID: $err")
            )
          TransactionEventId(transactionId)
        case TimelyRejectionEventIdRegex(domainIdS, uuidS) =>
          val domainId = DomainId
            .fromProtoPrimitive(domainIdS, "event ID")
            .valueOr(err =>
              throw new DbDeserializationException(s"Unable to parse domain ID: $err")
            )
          val uuid = Either
            .catchOnly[IllegalArgumentException](UUID.fromString(uuidS))
            .valueOr(err => throw new DbDeserializationException(show"Unable to parse UUID", err))
          TimelyRejectionEventId(domainId, uuid)
        case notAnEventId =>
          throw new DbDeserializationException(s"Unable to parse event id $notAnEventId")
      }
    }

    def fromLedgerSyncEvent(event: LedgerSyncEvent): Option[EventId] = {
      val optTransactionId = event match {
        case at: TransactionAccepted => Some(at.transactionId)
        case _ => None
      }
      optTransactionId.map(TransactionEventId)
    }
  }

  /** The transaction ID of a `TransactionAccepted` event */
  case class TransactionEventId(transactionId: LedgerTransactionId) extends EventId {
    override def asString300: String300 = checked(
      String300.tryCreate("T" + transactionId, "TransactionEventId".some)
    )

    override def associatedDomain: Option[DomainId] = None

    override def pretty: Pretty[TransactionEventId] = prettyOfClass(
      unnamedParam(_.transactionId)
    )
  }

  /** Event identifier for an event that comes in as a timely rejection via the
    * [[com.digitalasset.canton.participant.store.InFlightSubmissionStore]].
    * We use a [[java.util.UUID]] instead of a general [[com.digitalasset.canton.sequencing.protocol.MessageId]]
    * so that we stay below the 300 character limit.
    */
  case class TimelyRejectionEventId(domainId: DomainId, uuid: UUID) extends EventId {
    override def asString300: String300 = {
      // Maximum character length: 293
      // - Domain ID: 255
      // - UUID: 36
      // - prefix: 1
      // - separator: 1
      val string =
        EventId.timelyRejectionEventIdPrefix + domainId.toProtoPrimitive + EventId.timelyRejectionEventSeparator + uuid.toString
      checked(String300.tryCreate(string))
    }

    override def associatedDomain: Option[DomainId] = Some(domainId)

    def asInFlightReference: InFlightByMessageId =
      InFlightByMessageId(domainId, MessageId.fromUuid(uuid))

    override def pretty: Pretty[TimelyRejectionEventId] = prettyOfClass(
      param("domain ID", _.domainId),
      param("UUID", _.uuid),
    )
  }
}
case class TimestampedEventAndCausalChange(
    tse: TimestampedEvent,
    causalityUpdate: Option[CausalityUpdate],
)

object TimestampedEventAndCausalChange {
  implicit def getResultTimestampedEventAndCausalChange(implicit
      getTuple: GetResult[(TimestampedEvent, Option[CausalityUpdate])]
  ): GetResult[TimestampedEventAndCausalChange] = {
    getTuple.andThen { case (event, maybeUpdate) =>
      TimestampedEventAndCausalChange(event, maybeUpdate)
    }
  }
}
