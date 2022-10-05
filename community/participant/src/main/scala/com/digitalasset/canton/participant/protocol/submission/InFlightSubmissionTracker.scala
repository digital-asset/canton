// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.syntax.bifunctor._
import cats.syntax.either._
import cats.syntax.foldable._
import cats.syntax.functorFilter._
import cats.syntax.option._
import cats.syntax.traverse._
import cats.syntax.traverseFilter._
import com.daml.ledger.api.DeduplicationPeriod
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.CommandDeduplicator.DeduplicationFailed
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker.InFlightSubmissionTrackerDomainState
import com.digitalasset.canton.participant.store.InFlightSubmissionStore.InFlightBySequencingInfo
import com.digitalasset.canton.participant.store.MultiDomainEventLog.{DeduplicationInfo, OnPublish}
import com.digitalasset.canton.participant.store._
import com.digitalasset.canton.participant.sync.ParticipantEventPublisher
import com.digitalasset.canton.participant.sync.TimestampedEvent.TimelyRejectionEventId
import com.digitalasset.canton.participant.{LedgerSyncEvent, LocalOffset}
import com.digitalasset.canton.sequencing.protocol.{DeliverError, MessageId}
import com.digitalasset.canton.time.DomainTimeTracker
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil._
import com.digitalasset.canton.util.retry.Policy
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil}
import io.functionmeta.functionFullName

import scala.collection.immutable.ArraySeq
import scala.concurrent.{ExecutionContext, Future}

/** Tracker for in-flight submissions backed by the [[com.digitalasset.canton.participant.store.InFlightSubmissionStore]].
  *
  * A submission is in flight if it is in the [[com.digitalasset.canton.participant.store.InFlightSubmissionStore]].
  * The tracker registers a submission
  * before the [[com.digitalasset.canton.sequencing.protocol.SubmissionRequest]]
  * is sent to the [[com.digitalasset.canton.sequencing.client.SequencerClient]] of a domain.
  * After the corresponding event has been published into the
  * [[com.digitalasset.canton.participant.store.MultiDomainEventLog]] state updates,
  * the submission will be removed from the [[com.digitalasset.canton.participant.store.InFlightSubmissionStore]] again.
  * This happens normally as part of request processing after phase 7.
  * If the submission has not been sequenced by the specified
  * [[com.digitalasset.canton.participant.protocol.submission.UnsequencedSubmission.timeout]],
  * say because the submission was lost on the way to the sequencer,
  * the participant generates an appropriate update because the submission will never reach request processing.
  * The latter must work even if the participant crashes (if run with persistence).
  *
  * @param domainStates The projection of the [[com.digitalasset.canton.participant.store.SyncDomainEphemeralState]]
  *                     to what the [[InFlightSubmissionTracker]] uses.
  */
class InFlightSubmissionTracker(
    store: InFlightSubmissionStore,
    participantEventPublisher: ParticipantEventPublisher,
    deduplicator: CommandDeduplicator,
    multiDomainEventLog: MultiDomainEventLog,
    domainStates: DomainId => Option[InFlightSubmissionTrackerDomainState],
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends FlagCloseable
    with NamedLogging {
  import InFlightSubmissionTracker._

  /** Registers the given submission as being in flight and unsequenced
    * unless there already is an in-flight submission for the same change ID
    * or the timeout has already elapsed.
    *
    * @return The actual deduplication offset that is being used for deduplication for this submission
    */
  def register(
      submission: InFlightSubmission[UnsequencedSubmission],
      deduplicationPeriod: DeduplicationPeriod,
  ): EitherT[FutureUnlessShutdown, InFlightSubmissionTrackerError, Either[
    DeduplicationFailed,
    DeduplicationPeriod.DeduplicationOffset,
  ]] = {
    implicit val traceContext: TraceContext = submission.submissionTraceContext

    for {
      domainState <- domainStateFor(submission.submissionDomain).mapK(FutureUnlessShutdown.outcomeK)
      _result <- domainState.observedTimestampTracker
        .runIfAboveWatermark(submission.sequencingInfo.timeout, store.register(submission)) match {
        case Left(markTooLow) =>
          EitherT.leftT[FutureUnlessShutdown, Unit](
            TimeoutTooLow(submission, markTooLow.highWatermark): InFlightSubmissionTrackerError
          )
        case Right(eitherT) =>
          eitherT
            .leftMap(SubmissionAlreadyInFlight(submission, _))
            .leftWiden[InFlightSubmissionTrackerError]
      }
      // It is safe to request a tick only after persisting the in-flight submission
      // because if we crash in between, crash recovery will request the tick.
      _ = domainState.domainTimeTracker.requestTick(submission.sequencingInfo.timeout)
      // After the registration of the in-flight submission, we want to deduplicate the command.
      // A command deduplication failure must be reported via a completion event
      // because we do not know whether we have already produced a timely rejection concurrently.
      deduplicationResult <- EitherT
        .right(deduplicator.checkDuplication(submission.changeIdHash, deduplicationPeriod).value)
        .mapK(FutureUnlessShutdown.outcomeK)
    } yield deduplicationResult
  }

  /** @see com.digitalasset.canton.participant.store.InFlightSubmissionStore.observeSequencing */
  def observeSequencing(domainId: DomainId, sequenceds: Map[MessageId, SequencedSubmission])(
      implicit traceContext: TraceContext
  ): Future[Unit] =
    store.observeSequencing(domainId, sequenceds)

  /** @see com.digitalasset.canton.participant.store.InFlightSubmissionStore.updateUnsequenced */
  def observeSubmissionError(
      changeIdHash: ChangeIdHash,
      domainId: DomainId,
      messageId: MessageId,
      newTrackingData: UnsequencedSubmission,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    store.updateUnsequenced(changeIdHash, domainId, messageId, newTrackingData).map { (_: Unit) =>
      // Request a tick for the new timestamp if we're still connected to the domain
      domainStates(domainId) match {
        case Some(domainState) =>
          domainState.domainTimeTracker.requestTick(newTrackingData.timeout)
        case None =>
          logger.debug(
            s"Skipping to request tick at ${newTrackingData.timeout} on $domainId as the domain is not available"
          )
      }
    }
  }

  /** Updates the unsequenced submission corresponding to the [[com.digitalasset.canton.sequencing.protocol.DeliverError]],
    * if any, using [[com.digitalasset.canton.participant.protocol.submission.SubmissionTrackingData.updateOnNotSequenced]].
    */
  def observeDeliverError(
      deliverError: DeliverError
  )(implicit traceContext: TraceContext): Future[Unit] = {
    def updatedTrackingData(
        inFlightO: Option[InFlightSubmission[SubmissionSequencingInfo]]
    ): Option[(ChangeIdHash, UnsequencedSubmission)] =
      for {
        inFlight <- inFlightO
        unsequencedO = inFlight.sequencingInfo.asUnsequenced
        _ = if (unsequencedO.isEmpty) {
          logger.warn(
            s"Received a deliver error for the sequenced submission $inFlight. Deliver error $deliverError"
          )
        }
        unsequenced <- unsequencedO
        newTrackingData <- unsequenced.trackingData.updateOnNotSequenced(
          deliverError.timestamp,
          deliverError.reason,
        )
      } yield (inFlight.changeIdHash, newTrackingData)

    val domainId = deliverError.domainId
    val messageId = deliverError.messageId

    for {
      inFlightO <- store.lookupSomeMessageId(domainId, messageId)
      toUpdateO = updatedTrackingData(inFlightO)
      _ <- toUpdateO.traverse_ { case (changeIdHash, newTrackingData) =>
        store.updateUnsequenced(changeIdHash, domainId, messageId, newTrackingData)
      }
    } yield ()
  }

  /** Marks the timestamp as having been observed on the domain. */
  // We could timely reject up to the `timestamp` here,
  // but we would have to implement our own batching.
  // Instead, we piggy-back on when the clean sequencer counter is advanced.
  def observeTimestamp(
      domainId: DomainId,
      timestamp: CantonTimestamp,
  ): EitherT[Future, UnknownDomain, Unit] =
    domainStateFor(domainId).semiflatMap { domainState =>
      domainState.observedTimestampTracker.increaseWatermark(timestamp)
    }

  /** Publishes the rejection events for all unsequenced submissions on `domainId` up to the given timestamp.
    * Does not remove the submissions from the in-flight table as this will happen by the
    * [[onPublishListener]] called by the [[com.digitalasset.canton.participant.store.MultiDomainEventLog]].
    */
  def timelyReject(domainId: DomainId, upToInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, UnknownDomain, Unit] = {
    domainStateFor(domainId).semiflatMap { domainState =>
      for {
        // Increase the watermark for two reasons:
        // 1. Below, we publish an event via the ParticipantEventPublisher. This will then call the
        //    onPublishListener, which deletes the entry from the store. So we must make sure that the deletion
        //    cannot interfere with a concurrent insertion.
        // 2. Timestamps are also observed via the RecordOrderPublisher. If the RecordOrderPublisher
        //    does not advance due to a long-running request-response-result cycle, we get here
        //    a second chance of observing the timestamp when the sequencer counter becomes clean.
        _ <- domainState.observedTimestampTracker.increaseWatermark(upToInclusive)
        timelyRejects <- store.lookupUnsequencedUptoUnordered(domainId, upToInclusive)
        events = timelyRejects.map(timelyRejectionEventFor)
        skippedE <- participantEventPublisher.publishWithIds(events).value
      } yield {
        skippedE.valueOr { skipped =>
          logger.info(
            show"Skipping publication of timely rejections with IDs ${skipped
                .map(_.eventId.showValueOrNone)} as they are already there at offsets ${skipped.map(_.localOffset)}"
          )
        }
      }
    }
  }

  private[this] def timelyRejectionEventFor(
      inFlight: InFlightSubmission[UnsequencedSubmission]
  ): Traced[(TimelyRejectionEventId, LedgerSyncEvent)] = {
    implicit val traceContext: TraceContext = inFlight.submissionTraceContext
    // Use the trace context from the submission for the rejection
    // because we don't have any other later trace context available
    val rejectionEvent = inFlight.sequencingInfo.trackingData
      .rejectionEvent(inFlight.associatedTimestamp)
    Traced(inFlight.timelyRejectionEventId -> rejectionEvent)
  }

  val onPublishListener: MultiDomainEventLog.OnPublish = new MultiDomainEventLog.OnPublish {
    override def notify(
        published: Seq[MultiDomainEventLog.OnPublish.Publication]
    )(implicit batchTraceContext: TraceContext): Unit = {
      def performNotification(): Future[Unit] = performUnlessClosingF(functionFullName) {
        for {
          _ <- deduplicator.processPublications(published)

          trackedReferences = published.collect {
            case MultiDomainEventLog.OnPublish.Publication(
                  _globalOffset,
                  _publicationTime,
                  Some(inFlightReference),
                  _deduplicationInfo,
                ) =>
              inFlightReference
          }
          _ <- store.delete(trackedReferences)
        } yield ()
      }.onShutdown {
        logger.info("Failed to complete and delete in-flight submission due to shutdown.")
      }

      val afterRetries = Policy
        .noisyInfiniteRetry(
          performNotification(),
          InFlightSubmissionTracker.this,
          timeouts.storageMaxRetryInterval.asFiniteApproximation,
          "notify command deduplicator and in-flight submission tracker",
          "Restart the participant if this error persists.",
        )
        .onShutdown(
          logger.info(
            "Failed to complete and delete in-flight submission due to shutdown."
          )
        )
      FutureUtil.doNotAwait(
        afterRetries,
        "Failed to retry notifying the command deduplicator and in-flight submission tracker",
      )
    }
  }

  /** Completes all unsequenced in-flight submissions for the given domains for which a timely rejection event
    * has been published in the [[com.digitalasset.canton.participant.store.MultiDomainEventLog]].
    */
  def recoverPublishedTimelyRejections(
      domains: Seq[DomainId]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    for {
      unsequenceds <- domains.traverse { domainId =>
        store.lookupUnsequencedUptoUnordered(domainId, CantonTimestamp.MaxValue)
      }
      unsequenced = unsequenceds.flatten
      eventIds = unsequenced.map(_.timelyRejectionEventId)
      byEventId <- multiDomainEventLog.lookupByEventIds(eventIds)
      (references, publications) = unsequenced.mapFilter { inFlight =>
        byEventId.get(inFlight.timelyRejectionEventId).map {
          case (globalOffset, timestampedEventAndCausalUpdate, publicationTime) =>
            val reference = inFlight.referenceByMessageId
            val publication = OnPublish.Publication(
              globalOffset,
              publicationTime,
              reference.some,
              DeduplicationInfo.fromTimestampedEvent(timestampedEventAndCausalUpdate.tse),
            )
            reference -> publication
        }
      }.unzip
      _ <- deduplicator.processPublications(publications)
      _ <- store.delete(references)
    } yield ()
  }

  /** Deletes the published, sequenced in-flight submissions with sequencing timestamps up to the given bound
    * and informs the [[CommandDeduplicator]] about the published events.
    *
    * @param upToInclusive Upper bound on the sequencing time of the submissions to be recovered.
    *                      The [[com.digitalasset.canton.participant.LedgerSyncEvent]]s for all sequenced submissions
    *                      up to this bound must have been published to the
    *                      [[com.digitalasset.canton.participant.store.MultiDomainEventLog]].
    *                      The [[com.digitalasset.canton.participant.LedgerSyncEvent]]s for all sequenced submissions
    *                      in the [[com.digitalasset.canton.participant.store.InFlightSubmissionStore]] must not yet
    *                      have been pruned from the [[com.digitalasset.canton.participant.store.MultiDomainEventLog]].
    */
  def recoverDomain(domainId: DomainId, upToInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    val domainState = domainStates(domainId).getOrElse(
      ErrorUtil.internalError(
        new IllegalStateException(s"Domain state for $domainId not found during crash recovery.")
      )
    )

    def localOffsetsFor(sequencedInFlight: Seq[InFlightSubmission[SequencedSubmission]]): Future[
      ArraySeq[(LocalOffset, InFlightSubmission[SequencedSubmission], Option[DeduplicationInfo])]
    ] = {
      if (sequencedInFlight.isEmpty) Future.successful(ArraySeq.empty)
      else {
        // Locate the offsets in single-dimension event log based on the sequencer timestamps
        // because that's what we have an index for
        val first = sequencedInFlight
          .minByOption(_.sequencingInfo.sequencingTime)
          .getOrElse(
            ErrorUtil.internalError(
              new RuntimeException("A non-empty sequence must contain a minimum.")
            )
          )
        val last = sequencedInFlight
          .maxByOption(_.sequencingInfo.sequencingTime)
          .getOrElse(
            ErrorUtil.internalError(
              new RuntimeException("A non-empty sequence must contain a maximum.")
            )
          )
        for {
          foundLocalEvents <- domainState.singleDimensionEventLog.lookupEventRange(
            fromInclusive = None,
            toInclusive = None,
            fromTimestampInclusive = first.sequencingInfo.sequencingTime.some,
            toTimestampInclusive = last.sequencingInfo.sequencingTime.some,
            limit = None,
          )
        } yield {
          val localOffsetsB =
            ArraySeq.newBuilder[
              (LocalOffset, InFlightSubmission[SequencedSubmission], Option[DeduplicationInfo])
            ]
          localOffsetsB.sizeHint(sequencedInFlight.size)
          sequencedInFlight.foreach { inFlight =>
            // We can't compare by timestamp because repair events may have the same timestamp as a sequenced event.
            // Instead we check the unique sequencer counters.
            val sequencerCounter = inFlight.sequencingInfo.sequencerCounter.some
            val eventO = foundLocalEvents.find { case (_localOffset, event) =>
              event.tse.requestSequencerCounter == sequencerCounter
            }
            eventO match {
              case None =>
                ErrorUtil.internalError(
                  new IllegalStateException(
                    s"Cannot find event for sequenced in-flight submission $inFlight. Command deduplication may fail for the change id hash ${inFlight.changeIdHash}."
                  )
                )
              case Some((localOffset, event)) =>
                val deduplicationInfo = DeduplicationInfo.fromTimestampedEvent(event.tse)
                localOffsetsB += ((localOffset, inFlight, deduplicationInfo))
            }
          }
          localOffsetsB.result()
        }
      }
    }

    def publicationsFor(
        localOffsets: ArraySeq[
          (LocalOffset, InFlightSubmission[SequencedSubmission], Option[DeduplicationInfo])
        ]
    ): Future[Seq[(InFlightBySequencingInfo, MultiDomainEventLog.OnPublish.Publication)]] = {
      EventLogId.forDomain(multiDomainEventLog.indexedStringStore)(domainId).flatMap { eventLogId =>
        localOffsets
          .traverseFilter { case (localOffset, inFlight, deduplicationInfo) =>
            multiDomainEventLog.globalOffsetFor(eventLogId, localOffset).map { optPublicationInfo =>
              optPublicationInfo.map { case (globalOffset, publicationTime) =>
                val info = inFlight.referenceBySequencingInfo
                info -> MultiDomainEventLog.OnPublish.Publication(
                  globalOffset,
                  publicationTime,
                  info.some,
                  deduplicationInfo,
                )
              }
            }
          }
      }
    }

    for {
      sequencedInFlight <- store.lookupSequencedUptoUnordered(
        domainId,
        sequencingTimeInclusive = upToInclusive,
      )
      _ = logger.debug(
        "Re-informing the command deduplicator about in-flight sequenced submissions with published events"
      )
      localOffsets <- localOffsetsFor(sequencedInFlight)
      sequencingInfoAndPublications <- publicationsFor(localOffsets)
      (toDelete, publications) = sequencingInfoAndPublications.unzip
      _ <- deduplicator.processPublications(publications)
      _ = logger.debug("Removing in-flight submissions from in-flight submission store")
      _ <- store.delete(toDelete)
      // Re-request ticks for all remaining unsequenced timestamps
      unsequencedInFlights <- store.lookupUnsequencedUptoUnordered(
        domainId,
        CantonTimestamp.MaxValue,
      )
      _ = if (unsequencedInFlights.nonEmpty) {
        domainState.domainTimeTracker.requestTicks(
          unsequencedInFlights.map(_.sequencingInfo.timeout)
        )
      }
    } yield ()
  }

  private def domainStateFor(
      domainId: DomainId
  ): EitherT[Future, UnknownDomain, InFlightSubmissionTrackerDomainState] =
    EitherT(Future.successful {
      domainStates(domainId).toRight(UnknownDomain(domainId))
    })
}

object InFlightSubmissionTracker {

  /** The portion of the [[com.digitalasset.canton.participant.store.SyncDomainEphemeralState]]
    * that is used by the [[InFlightSubmissionTracker]].
    *
    * @param observedTimestampTracker
    *   Tracks the observed timestamps per domain to synchronize submission registration with submission deletion.
    *   We track them per domain so that clock skew between different domains does not cause interferences.
    * @param domainTimeTracker
    *   Used to request a timestamp observation for the
    *   [[com.digitalasset.canton.participant.protocol.submission.UnsequencedSubmission.timeout]]s of
    *   [[com.digitalasset.canton.participant.protocol.submission.UnsequencedSubmission]]s.
    * @param singleDimensionEventLog
    *   Used to locate the offsets and their events of sequenced requests during crash recovery
    */
  case class InFlightSubmissionTrackerDomainState(
      observedTimestampTracker: WatermarkTracker[CantonTimestamp],
      domainTimeTracker: DomainTimeTracker,
      singleDimensionEventLog: SingleDimensionEventLogLookup,
  )

  object InFlightSubmissionTrackerDomainState {
    def fromSyncDomainState(
        persistent: SyncDomainPersistentState,
        ephemeral: SyncDomainEphemeralState,
    ): InFlightSubmissionTrackerDomainState = {
      InFlightSubmissionTrackerDomainState(
        ephemeral.observedTimestampTracker,
        ephemeral.timeTracker,
        persistent.eventLog,
      )
    }
  }

  sealed trait InFlightSubmissionTrackerError extends Product with Serializable

  case class SubmissionAlreadyInFlight(
      newSubmission: InFlightSubmission[UnsequencedSubmission],
      existingSubmission: InFlightSubmission[SubmissionSequencingInfo],
  ) extends InFlightSubmissionTrackerError

  case class TimeoutTooLow(
      submission: InFlightSubmission[UnsequencedSubmission],
      lowerBound: CantonTimestamp,
  ) extends InFlightSubmissionTrackerError

  case class UnknownDomain(domainId: DomainId) extends InFlightSubmissionTrackerError
}
