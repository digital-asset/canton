// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.OptionT
import cats.syntax.either._
import cats.syntax.option._
import com.daml.ledger.participant.state.v2.ChangeId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.protocol.submission.ChangeIdHash
import com.digitalasset.canton.participant.store.CommandDeduplicationStore.OffsetAndPublicationTime
import com.digitalasset.canton.participant.store.db.DbCommandDeduplicationStore
import com.digitalasset.canton.participant.store.memory.InMemoryCommandDeduplicationStore
import com.digitalasset.canton.protocol.StoredParties
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.{ApplicationId, CommandId, LedgerSubmissionId}
import slick.jdbc.GetResult

import scala.concurrent.{ExecutionContext, Future}

trait CommandDeduplicationStore {

  /** Returns the [[CommandDeduplicationData]] associated with the given
    * [[com.digitalasset.canton.participant.protocol.submission.ChangeIdHash]], if any.
    */
  def lookup(changeIdHash: ChangeIdHash)(implicit
      traceContext: TraceContext
  ): OptionT[Future, CommandDeduplicationData]

  /** Updates the [[com.digitalasset.canton.participant.protocol.submission.ChangeIdHash]]'s for the given
    * [[com.daml.ledger.participant.state.v2.ChangeId]]s with the given [[DefiniteAnswerEvent]]s.
    * The [[scala.Boolean]] specifies whether the definite answer is an acceptance.
    *
    * Does not overwrite the data if the existing data has a higher [[DefiniteAnswerEvent.offset]]. This should never
    * happen in practice.
    */
  def storeDefiniteAnswers(answers: Seq[(ChangeId, DefiniteAnswerEvent, Boolean)])(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Updates the [[com.digitalasset.canton.participant.protocol.submission.ChangeIdHash]]'s for the given
    * [[com.daml.ledger.participant.state.v2.ChangeId]] with the given [[DefiniteAnswerEvent]].
    *
    * Does not overwrite the data if the existing data has a higher [[DefiniteAnswerEvent.offset]]. This should never
    * happen in practice.
    */
  def storeDefiniteAnswer(
      changeId: ChangeId,
      definiteAnswerEvent: DefiniteAnswerEvent,
      accepted: Boolean,
  ): Future[Unit] =
    storeDefiniteAnswers(Seq((changeId, definiteAnswerEvent, accepted)))(
      definiteAnswerEvent.traceContext
    )

  /** Prunes all command deduplication entries whose [[CommandDeduplicationData.latestDefiniteAnswer]] offset
    * is less or equal to `upToInclusive`.
    *
    * @param prunedPublicationTime The publication time of the given offset in the [[MultiDomainEventLog]].
    */
  def prune(upToInclusive: GlobalOffset, prunedPublicationTime: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Returns the highest offset with which [[prune]] was called, and an upper bound on its publication time, if any.
    */
  def latestPruning()(implicit
      traceContext: TraceContext
  ): OptionT[Future, OffsetAndPublicationTime]
}

object CommandDeduplicationStore {

  def apply(storage: Storage, loggerFactory: NamedLoggerFactory)(implicit
      ec: ExecutionContext
  ): CommandDeduplicationStore =
    storage match {
      case _: MemoryStorage => new InMemoryCommandDeduplicationStore(loggerFactory)
      case jdbc: DbStorage => new DbCommandDeduplicationStore(jdbc, loggerFactory)
    }

  case class OffsetAndPublicationTime(offset: GlobalOffset, publicationTime: CantonTimestamp)
      extends PrettyPrinting {
    override def pretty: Pretty[OffsetAndPublicationTime] = prettyOfClass(
      param("offset", _.offset),
      param("publication time", _.publicationTime),
    )
  }

  object OffsetAndPublicationTime {
    implicit val getResultOffsetAndPublicationTime: GetResult[OffsetAndPublicationTime] =
      GetResult { r =>
        val offset = r.<<[GlobalOffset]
        val publicationTime = r.<<[CantonTimestamp]
        OffsetAndPublicationTime(offset, publicationTime)
      }
  }
}

/** The command deduplication data associated with a [[com.daml.ledger.participant.state.v2.ChangeId]].
  *
  * @param changeId The change ID this command deduplication data is associated with
  * @param latestDefiniteAnswer The latest definite answer for the change ID
  * @param latestAcceptance The latest accepting completion for the change ID, if any
  */
case class CommandDeduplicationData private (
    changeId: ChangeId,
    latestDefiniteAnswer: DefiniteAnswerEvent,
    latestAcceptance: Option[DefiniteAnswerEvent],
) extends PrettyPrinting
    with NoCopy {
  latestAcceptance.foreach { acceptance =>
    if (acceptance.offset > latestDefiniteAnswer.offset) {
      throw CommandDeduplicationData.InvalidCommandDeduplicationData(
        s"The latest definite answer at offset ${latestDefiniteAnswer.offset} is before the acceptance at offset ${acceptance.offset}"
      )
    }
  }

  override def pretty: Pretty[CommandDeduplicationData] = prettyOfClass(
    param("change id", _.changeId),
    param("latest definite answer", _.latestDefiniteAnswer),
    paramIfDefined("latest acceptance", _.latestAcceptance),
  )
}

object CommandDeduplicationData {
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  case class InvalidCommandDeduplicationData(message: String, cause: Throwable = null)
      extends RuntimeException(message, cause)

  private[this] def apply(
      changeId: ChangeId,
      latestDefiniteAnswer: DefiniteAnswerEvent,
      latestAcceptance: Option[DefiniteAnswerEvent],
  ): CommandDeduplicationData =
    throw new UnsupportedOperationException("Use the create/tryCreate factory methods instead")

  def create(
      changeId: ChangeId,
      latestDefiniteAnswer: DefiniteAnswerEvent,
      latestAcceptance: Option[DefiniteAnswerEvent],
  ): Either[String, CommandDeduplicationData] =
    Either
      .catchOnly[InvalidCommandDeduplicationData](
        tryCreate(changeId, latestDefiniteAnswer, latestAcceptance)
      )
      .leftMap(_.getMessage)

  def tryCreate(
      changeId: ChangeId,
      latestDefiniteAnswer: DefiniteAnswerEvent,
      latestAcceptance: Option[DefiniteAnswerEvent],
  ): CommandDeduplicationData =
    new CommandDeduplicationData(changeId, latestDefiniteAnswer, latestAcceptance)

  implicit def getResultCommandDeduplicationData(implicit
      getResultByteArray: GetResult[Array[Byte]],
      getResultByteArrayO: GetResult[Option[Array[Byte]]],
  ): GetResult[CommandDeduplicationData] = GetResult { r =>
    val applicationId = r.<<[ApplicationId]
    val commandId = r.<<[CommandId]
    val actAs = r.<<[StoredParties]
    val latestDefiniteAnswer = r.<<[DefiniteAnswerEvent]
    val latestAcceptance = r.<<[Option[DefiniteAnswerEvent]]
    val changeId = ChangeId(applicationId.unwrap, commandId.unwrap, actAs.parties)
    create(changeId, latestDefiniteAnswer, latestAcceptance).valueOr(err =>
      throw new DbDeserializationException(
        s"Failed to deserialize command deduplication data: $err"
      )
    )
  }
}

/** @param offset A completion offset in the [[MultiDomainEventLog]]
  * @param publicationTime The publication time associated with the `offset`
  * @param traceContext The trace context that created the completion offset.
  */
case class DefiniteAnswerEvent(
    offset: GlobalOffset,
    publicationTime: CantonTimestamp,
    submissionId: Option[LedgerSubmissionId],
    traceContext: TraceContext,
    // TODO(#7348) add submission rank
) extends PrettyPrinting {

  def serializableSubmissionId: Option[SerializableSubmissionId] =
    submissionId.map(SerializableSubmissionId(_))

  override def pretty: Pretty[DefiniteAnswerEvent] = prettyOfClass(
    param("offset", _.offset),
    param("publication time", _.publicationTime),
    paramIfNonEmpty("submission id", _.submissionId),
    param("trace context", _.traceContext),
  )
}

object DefiniteAnswerEvent {
  import TraceContext._

  implicit def getResultDefiniteAnswerEvent(implicit
      getResultByteArray: GetResult[Array[Byte]]
  ): GetResult[DefiniteAnswerEvent] = GetResult { r =>
    val offset = r.<<[GlobalOffset]
    val publicationTime = r.<<[CantonTimestamp]
    val submissionIdO = r.<<[Option[SerializableSubmissionId]]
    val traceContext = r.<<[TraceContext]
    DefiniteAnswerEvent(offset, publicationTime, submissionIdO.map(_.submissionId), traceContext)
  }

  implicit def getResultDefinitionAnswerEventOption(implicit
      getResultByteArrayO: GetResult[Option[Array[Byte]]]
  ): GetResult[Option[DefiniteAnswerEvent]] =
    GetResult { r =>
      val offsetO = r.<<[Option[GlobalOffset]]
      val publicationTimeO = r.<<[Option[CantonTimestamp]]
      val submissionIdO = r.<<[Option[SerializableSubmissionId]]
      val traceContextO = r.<<[Option[TraceContext]]
      (offsetO, publicationTimeO, submissionIdO, traceContextO) match {
        case (Some(offset), Some(publicationTime), submissionId, Some(traceContext)) =>
          DefiniteAnswerEvent(
            offset,
            publicationTime,
            submissionId.map(_.submissionId),
            traceContext,
          ).some
        case (None, None, None, None) => None
        case _ =>
          throw new DbDeserializationException(
            s"Invalid definite answer event (should be all None or all defined): $offsetO, $publicationTimeO, $traceContextO"
          )
      }
    }
}
