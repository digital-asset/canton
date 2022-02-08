// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.DomainId
import com.digitalasset.canton.config.BatchAggregatorConfig
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.protocol.submission._
import com.digitalasset.canton.participant.store.InFlightSubmissionStore.InFlightReference
import com.digitalasset.canton.participant.store.db.DbInFlightSubmissionStore
import com.digitalasset.canton.participant.store.memory.InMemoryInFlightSubmissionStore
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.sequencing.protocol.MessageId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

/** Backing store for [[com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker]].
  *
  * An in-flight submission is uniquely identified by its
  * [[com.daml.ledger.participant.state.v2.ChangeId]].
  * [[com.digitalasset.canton.sequencing.protocol.MessageId]]s should be unique too,
  * but this is not enforced by the store.
  *
  * Every change to an individual submissions must execute atomically.
  * Bulk operations may interleave arbitrarily the atomic changes of the affected individual submissions
  * and therefore need not be atomic as a whole.
  */
trait InFlightSubmissionStore {

  /** Retrieves the in-flight submission for the given
    * [[com.daml.ledger.participant.state.v2.ChangeId]] if one exists.
    */
  def lookup(changeIdHash: ChangeIdHash)(implicit
      traceContext: TraceContext
  ): OptionT[Future, InFlightSubmission[SubmissionSequencingInfo]]

  /** Returns all unsequenced in-flight submissions on the given domain
    * whose [[com.digitalasset.canton.participant.protocol.submission.UnsequencedSubmission.timeout]]
    * is no later than `observedSequencingTime`.
    *
    * The in-flight submissions are not returned in any specific order.
    */
  def lookupUnsequencedUptoUnordered(domainId: DomainId, observedSequencingTime: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): Future[Seq[InFlightSubmission[UnsequencedSubmission]]]

  /** Returns all sequenced in-flight submissions on the given domain
    * whose [[com.digitalasset.canton.participant.protocol.submission.SequencedSubmission.sequencingTime]]
    * is no later than `sequencingTimeInclusive`.
    *
    * The in-flight submissions are not returned in any specific order.
    */
  def lookupSequencedUptoUnordered(domainId: DomainId, sequencingTimeInclusive: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): Future[Seq[InFlightSubmission[SequencedSubmission]]]

  /** Returns one of the in-flight submissions with the given [[com.digitalasset.canton.DomainId]]
    * and [[com.digitalasset.canton.sequencing.protocol.MessageId]], if any.
    */
  def lookupSomeMessageId(domainId: DomainId, messageId: MessageId)(implicit
      traceContext: TraceContext
  ): Future[Option[InFlightSubmission[SubmissionSequencingInfo]]]

  /** Returns the earliest [[com.digitalasset.canton.participant.protocol.submission.UnsequencedSubmission.timeout]]
    * or [[com.digitalasset.canton.participant.protocol.submission.SequencedSubmission.sequencingTime]] in the store, if any,
    * for the given domain.
    */
  def lookupEarliest(domainId: DomainId)(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]]

  /** Registers the given submission as being in flight and unsequenced
    * unless there already is an in-flight submission for the same change ID.
    *
    * This method MUST NOT run concurrently with a [[delete]] query for the same change ID and message ID.
    * When this method fails with an exception, it is unknown whether the submission was registered.
    *
    * @return A [[scala.Left$]] of the existing in-flight submission with the same change ID
    *         and a different [[com.digitalasset.canton.sequencing.protocol.MessageId]] if there is any.
    */
  def register(
      submission: InFlightSubmission[UnsequencedSubmission]
  ): EitherT[FutureUnlessShutdown, InFlightSubmission[SubmissionSequencingInfo], Unit]

  /** Moves the submissions to the given domain
    * with the given [[com.digitalasset.canton.sequencing.protocol.MessageId]]s
    * from [[com.digitalasset.canton.participant.protocol.submission.UnsequencedSubmission]]
    * to [[com.digitalasset.canton.participant.protocol.submission.SequencedSubmission]].
    */
  def observeSequencing(domainId: DomainId, submissions: Map[MessageId, SequencedSubmission])(
      implicit traceContext: TraceContext
  ): Future[Unit]

  /** Deletes the referred to in-flight submissions if there are any.
    *
    * If the [[com.digitalasset.canton.sequencing.protocol.MessageId]] in
    * [[com.digitalasset.canton.participant.store.InFlightSubmissionStore.InFlightByMessageId]] is not a UUID,
    * there cannot be a matching in-flight submission because [[register]] forces a UUID for the message ID.
    */
  // We do not provide a `deleteUnsequencedUpto` as a counterpart to `lookupUnsequencedUpto`
  // so that we do not have to guard against concurrent insertions between the two calls,
  // e.g., if there comes a submission whose max sequencing times is derived from a very early ledger time.
  // This is also the reason for why we combine the change ID with the message ID.
  def delete(submissions: Seq[InFlightReference])(implicit traceContext: TraceContext): Future[Unit]

  /** Update the in-flight submission identified by the given `changeId`
    * if `submissionDomain` and `messageId` match and it is unsequenced and
    * the existing [[com.digitalasset.canton.participant.protocol.submission.UnsequencedSubmission.timeout]]
    * is not earlier than the `newSequencingInfo`'s
    * [[com.digitalasset.canton.participant.protocol.submission.UnsequencedSubmission.timeout]].
    * Only the field [[com.digitalasset.canton.participant.protocol.submission.InFlightSubmission.sequencingInfo]]
    * is updated.
    *
    * This is useful to change when and how a rejection is reported, e.g.,
    * if the submission logic decided to not send the [[com.digitalasset.canton.sequencing.protocol.SubmissionRequest]]
    * to the sequencer after all.
    */
  def updateUnsequenced(
      changeId: ChangeIdHash,
      submissionDomain: DomainId,
      messageId: MessageId,
      newSequencingInfo: UnsequencedSubmission,
  )(implicit traceContext: TraceContext): Future[Unit]
}

object InFlightSubmissionStore {
  def apply(
      storage: Storage,
      maxItemsInSqlInClause: PositiveNumeric[Int],
      registerBatchAggregatorConfig: BatchAggregatorConfig,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): InFlightSubmissionStore = storage match {
    case _: MemoryStorage => new InMemoryInFlightSubmissionStore(loggerFactory)
    case jdbc: DbStorage =>
      new DbInFlightSubmissionStore(
        jdbc,
        maxItemsInSqlInClause,
        registerBatchAggregatorConfig,
        loggerFactory,
      )
  }

  /** Reference to an in-flight submission */
  sealed trait InFlightReference extends Product with Serializable with PrettyPrinting {
    def domainId: DomainId
    def toEither: Either[InFlightByMessageId, InFlightBySequencingInfo]
  }

  /** Identifies an in-flight submission via the [[com.digitalasset.canton.sequencing.protocol.MessageId]] */
  case class InFlightByMessageId(override val domainId: DomainId, messageId: MessageId)
      extends InFlightReference {
    override def toEither: Either[InFlightByMessageId, InFlightBySequencingInfo] = Left(this)

    override def pretty: Pretty[InFlightByMessageId] = prettyOfClass(
      param("domain id", _.domainId),
      param("message id", _.messageId),
    )
  }

  /** Identifies an in-flight submission via the
    * [[com.digitalasset.canton.participant.protocol.submission.SequencedSubmission]]
    */
  case class InFlightBySequencingInfo(
      override val domainId: DomainId,
      sequenced: SequencedSubmission,
  ) extends InFlightReference {
    override def toEither: Either[InFlightByMessageId, InFlightBySequencingInfo] = Right(this)

    override def pretty: Pretty[InFlightBySequencingInfo] = prettyOfClass(
      param("domain id", _.domainId),
      param("sequenced", _.sequenced),
    )
  }

}
