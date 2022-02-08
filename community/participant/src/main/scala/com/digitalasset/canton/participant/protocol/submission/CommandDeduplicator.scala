// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.syntax.either._
import cats.syntax.functorFilter._
import com.daml.ledger.api.DeduplicationPeriod
import com.digitalasset.canton.LedgerSubmissionId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.CommandDeduplicator.{
  AlreadyExists,
  DeduplicationFailed,
  DeduplicationPeriodTooEarly,
  MalformedOffset,
}
import com.digitalasset.canton.participant.store.CommandDeduplicationStore.OffsetAndPublicationTime
import com.digitalasset.canton.participant.store._
import com.digitalasset.canton.participant.sync.UpstreamOffsetConvert
import com.digitalasset.canton.participant.{GlobalOffset, LedgerSyncOffset}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil

import scala.concurrent.{ExecutionContext, Future}

/** Implements the command deduplication logic.
  *
  * All method calls should be coordinated by the [[InFlightSubmissionTracker]].
  * In particular, `checkDeduplication` must not be called concurrently with
  * `processPublications` for the same [[com.daml.ledger.participant.state.v2.ChangeId]]s.
  */
trait CommandDeduplicator {

  /** Register the publication of the events in the [[com.digitalasset.canton.participant.store.CommandDeduplicationStore]] */
  def processPublications(publications: Seq[MultiDomainEventLog.OnPublish.Publication])(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Perform deduplication for the given [[com.digitalasset.canton.participant.protocol.submission.ChangeIdHash]]
    * and [[com.daml.ledger.api.DeduplicationPeriod]].
    *
    * @param changeIdHash The change ID hash of the submission to be deduplicated
    * @param deduplicationPeriod The deduplication period specified with the submission
    *
    * @return The [[com.daml.ledger.api.DeduplicationPeriod.DeduplicationOffset]]
    *         to be included in the command completion's [[com.daml.ledger.participant.state.v2.CompletionInfo]].
    *         Canton always returns a [[com.daml.ledger.api.DeduplicationPeriod.DeduplicationOffset]]
    *         because it cannot meet the the record time requirements for the other kinds of
    *         [[com.daml.ledger.api.DeduplicationPeriod]]s.
    */
  def checkDuplication(changeIdHash: ChangeIdHash, deduplicationPeriod: DeduplicationPeriod)(
      implicit traceContext: TraceContext
  ): EitherT[Future, DeduplicationFailed, DeduplicationPeriod.DeduplicationOffset]
}

object CommandDeduplicator {
  sealed trait DeduplicationFailed extends Product with Serializable

  case class MalformedOffset(error: String) extends DeduplicationFailed

  case class AlreadyExists(
      completionOffset: GlobalOffset,
      accepted: Boolean,
      existingSubmissionId: Option[LedgerSubmissionId],
  ) extends DeduplicationFailed

  case class DeduplicationPeriodTooEarly(
      requested: DeduplicationPeriod,
      earliestDeduplicationStart: DeduplicationPeriod,
  ) extends DeduplicationFailed
}

class CommandDeduplicatorImpl(
    store: CommandDeduplicationStore,
    clock: Clock,
    publicationTimeLowerBound: => CantonTimestamp,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends CommandDeduplicator
    with NamedLogging {

  override def processPublications(
      publications: Seq[MultiDomainEventLog.OnPublish.Publication]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val offsetsAndComletionInfos = publications.mapFilter {
      case MultiDomainEventLog.OnPublish.Publication(
            globalOffset,
            publicationTime,
            _inFlightReference,
            deduplicationInfo,
          ) =>
        deduplicationInfo.map { dedupInfo =>
          (
            dedupInfo.changeId,
            DefiniteAnswerEvent(
              globalOffset,
              publicationTime,
              dedupInfo.submissionId,
              dedupInfo.eventTraceContext,
            ),
            dedupInfo.acceptance,
          )
        }
    }
    store.storeDefiniteAnswers(offsetsAndComletionInfos)
  }

  override def checkDuplication(
      changeIdHash: ChangeIdHash,
      deduplicationPeriod: DeduplicationPeriod,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, DeduplicationFailed, DeduplicationPeriod.DeduplicationOffset] = {

    def dedupPeriodTooEarly(prunedOffset: GlobalOffset): DeduplicationFailed =
      DeduplicationPeriodTooEarly(
        deduplicationPeriod,
        DeduplicationPeriod.DeduplicationOffset(
          // We may hand out the latest pruned offset because deduplication offsets are exclusive
          UpstreamOffsetConvert.fromGlobalOffset(prunedOffset)
        ),
      )

    // If we haven't pruned the command deduplication store, this store contains all command deduplication entries
    // and can handle any reasonable timestamp and offset.
    // Ideally, we would report the predecessor to the first ledger offset
    // as deduplication offsets are exclusive, but this would be an invalid Canton global offset.
    // So we report the first ledger offset as the deduplication start instead.
    // This difference does not matter in practice for command deduplication
    // as the first offset always contains the ledger configuration and can therefore never be a command completion.
    def unprunedDedupOffset: GlobalOffset = MultiDomainEventLog.ledgerFirstOffset

    def dedupDuration(
        duration: java.time.Duration
    ): EitherT[Future, DeduplicationFailed, GlobalOffset] = {
      // Convert the duration into a timestamp based on the local participant clock
      // and check against the publication time of the definite answer events.
      //
      // Set the baseline to at least the to-be-assigned publication time
      // so that durations up to the max deduplication time always fall
      // into the unpruned window.
      //
      // By including `clock.now`, it may happen that the assigned publication time is actually lower than
      // the baseline, e.g., if the request is sequenced and then processing fails over to a participant
      // where the clock lags behind.  We accept this for now as the deduplication guarantee
      // does not forbid clocks that run backwards. Including `clock.now` ensures that time advances
      // for deduplication even if no events happen on the domain.
      val baseline = Ordering[CantonTimestamp].max(clock.now, publicationTimeLowerBound).toInstant

      def checkAgainstPruning(
          deduplicationStart: CantonTimestamp
      ): EitherT[Future, DeduplicationFailed, GlobalOffset] = {
        EitherTUtil.leftSubflatMap(store.latestPruning().toLeft(unprunedDedupOffset)) {
          case OffsetAndPublicationTime(prunedOffset, prunedPublicationTime) =>
            Either.cond(
              deduplicationStart > prunedPublicationTime,
              prunedOffset,
              dedupPeriodTooEarly(prunedOffset),
            )
        }
      }

      for {
        deduplicationStart <- EitherT.fromEither[Future](
          CantonTimestamp.fromInstant(baseline.minus(duration)).leftMap[DeduplicationFailed] {
            err =>
              logger.info(s"Deduplication period underflow: $err")
              val longestDeduplicationPeriod =
                java.time.Duration.between(CantonTimestamp.MinValue.toInstant, baseline)
              DeduplicationPeriodTooEarly(
                deduplicationPeriod,
                DeduplicationPeriod.DeduplicationDuration(longestDeduplicationPeriod),
              )
          }
        )
        dedupEntryO <- EitherT.right(store.lookup(changeIdHash).value)
        reportedDedupOffset <- dedupEntryO match {
          case None =>
            checkAgainstPruning(deduplicationStart)
          case Some(CommandDeduplicationData(_changeId, _latestDefiniteAnswer, latestAcceptance)) =>
            // TODO(#7348) add submission rank check using latestDefiniteAnswer
            latestAcceptance match {
              case None => checkAgainstPruning(deduplicationStart)
              case Some(acceptance) =>
                EitherT.cond[Future](
                  acceptance.publicationTime < deduplicationStart,
                  // Use the found offset rather than deduplicationStart
                  // so that we don't have to check whether deduplicationStart is at most the ledger end
                  acceptance.offset,
                  AlreadyExists(
                    acceptance.offset,
                    accepted = true,
                    acceptance.submissionId,
                  ): DeduplicationFailed,
                )
            }
        }
      } yield reportedDedupOffset
    }

    def dedupOffset(
        offset: LedgerSyncOffset
    ): EitherT[Future, DeduplicationFailed, GlobalOffset] = {
      def checkAgainstPruning(
          dedupOffset: GlobalOffset
      ): EitherT[Future, DeduplicationFailed, GlobalOffset] = {
        EitherTUtil.leftSubflatMap(store.latestPruning().toLeft(unprunedDedupOffset)) {
          case OffsetAndPublicationTime(prunedOffset, prunedPublicationTime) =>
            Either.cond(
              prunedOffset <= dedupOffset,
              prunedOffset,
              dedupPeriodTooEarly(prunedOffset),
            )
        }
      }

      for {
        dedupOffset <- EitherT.fromEither[Future](
          UpstreamOffsetConvert
            .toGlobalOffset(offset)
            .leftMap[DeduplicationFailed](err => MalformedOffset(err))
        )
        dedupEntryO <- EitherT.right(store.lookup(changeIdHash).value)
        reportedDedupOffset <- dedupEntryO match {
          case None =>
            checkAgainstPruning(dedupOffset)
          case Some(CommandDeduplicationData(_changeId, _latestDefiniteAnswer, latestAcceptance)) =>
            // TODO(#7348) add submission rank check using latestDefiniteAnswer
            latestAcceptance match {
              case None => checkAgainstPruning(dedupOffset)
              case Some(acceptance) =>
                EitherT.cond[Future](
                  acceptance.offset <= dedupOffset,
                  acceptance.offset,
                  AlreadyExists(
                    acceptance.offset,
                    accepted = true,
                    acceptance.submissionId,
                  ): DeduplicationFailed,
                )
            }
        }
      } yield reportedDedupOffset
    }

    val dedupOffsetE = deduplicationPeriod match {
      case DeduplicationPeriod.DeduplicationDuration(duration) => dedupDuration(duration)
      case DeduplicationPeriod.DeduplicationOffset(offset) => dedupOffset(offset)
    }
    dedupOffsetE.map(dedupOffset =>
      DeduplicationPeriod.DeduplicationOffset(UpstreamOffsetConvert.fromGlobalOffset(dedupOffset))
    )
  }
}
