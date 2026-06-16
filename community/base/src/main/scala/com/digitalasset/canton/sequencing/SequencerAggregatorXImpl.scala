// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.data.EitherT
import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{CryptoPureApi, Hash, HashPurpose, Signature}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  HasCloseContext,
  PromiseUnlessShutdown,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.sequencing.SequencerAggregator.{
  MessageAggregationConfig,
  SequencerAggregatorError,
}
import com.digitalasset.canton.sequencing.SequencerAggregatorXImpl.EventAndOrdinal
import com.digitalasset.canton.sequencing.protocol.{Batch, Envelope, SignedContent}
import com.digitalasset.canton.store.SequencedEventStore.SequencedEventWithTraceContext
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherUtil, ErrorUtil, FutureUtil, Mutex}
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}
import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, blocking}

class SequencerAggregatorXImpl(
    postAggregationHandler: PostAggregationHandler,
    cryptoPureApi: CryptoPureApi,
    eventInboxSize: PositiveInt,
    pastEventsCacheSize: PositiveInt,
    val loggerFactory: NamedLoggerFactory,
    initialConfig: MessageAggregationConfig,
    updateSendTracker: Seq[SequencedEventWithTraceContext[Batch[Envelope[?]]]] => Unit,
    notifyNewEvent: EventAndOrdinal => Unit,
    override val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
) extends SequencerAggregator
    with HasCloseContext {

  import SequencerAggregatorXImpl.*

  /** Used to synchronize and coordinate access to the mutable references [[configRef]],
    * [[stateRef]], [[pastEventsCacheRef]] and [[latestProcessedEventORef]]
    */
  private val lock = Mutex()

  /** Reference to the currently active configuration */
  private val configRef = new AtomicReference[MessageAggregationConfig](initialConfig)

  /** Reference to the current event aggregation state.
    *
    * We're using an `Option` here, because the state carries a supervised promise which uses a
    * trace context for its logging. The appropriate trace context is only available when the first
    * contribution arrives.
    */
  private val stateRef = new AtomicReference[Option[AggregationState]](None)

  /** Reference to the cache of past events successfully aggregated */
  private val pastEventsCacheRef = new AtomicReference(SortedMap[CantonTimestamp, Hash]())

  /** Reference to the latest event aggregated. This is the same as the most recent entry in
    * [[pastEventsCacheRef]], but with the full event
    */
  private val latestProcessedEventORef = new AtomicReference[Option[EventAndOrdinal]](None)
  override def getLatestProcessedEventO: Option[EventAndOrdinal] =
    lock.exclusive(latestProcessedEventORef.get)

  /** Queue containing received and not yet handled events. Used for batched processing. */
  private val receivedEvents =
    new ArrayBlockingQueue[SequencedSerializedEvent](eventInboxSize.unwrap)

  @VisibleForTesting
  override def eventQueue: BlockingQueue[SequencedSerializedEvent] = receivedEvents

  override def clearEventQueue(): Unit = receivedEvents.clear()

  override def changeMessageAggregationConfig(
      newConfig: MessageAggregationConfig
  )(implicit traceContext: TraceContext, executionContext: ExecutionContext): Unit =
    lock.exclusive {
      configRef.set(newConfig)
      checkThreshold(context = "configuration change")
    }

  override def combine(
      messages: NonEmpty[Seq[SequencedSerializedEvent]]
  ): Either[SequencerAggregatorError, SequencedSerializedEvent] =
    throw new UnsupportedOperationException(
      """This is used only in `SequencerAggregatorTest`, but not for `SequencerAggregatorXImpl`
      |because signatures are organized a bit differently in this implementation, and an ad-hoc
      |implementation is used in the test instead.
      |This will be removed once the old aggregator is removed.
      |""".stripMargin
    )

  override def combineAndMergeEvent(sequencerId: SequencerId, newEvent: SequencedSerializedEvent)(
      implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Either[SequencerAggregatorError, Unit]] = {
    val context = sequencerId.uid.toString

    val eventHash = SignedContent.hashContent(
      cryptoPureApi,
      newEvent.signedEvent.content,
      HashPurpose.SequencerAggregatorAggregation,
    )

    logger.debug(s"[$context] Received event at ${newEvent.timestamp} with hash $eventHash")

    val lateOrMerged = lock.exclusive {
      val pastEventsCache = pastEventsCacheRef.get
      pastEventsCache.lastOption match {
        case Some((latestProcessedTs, _hash)) if newEvent.timestamp <= latestProcessedTs =>
          Left(pastEventsCache)

        case _ =>
          // Create a new state if needed
          val state = stateRef.get match {
            case None =>
              val newState =
                new AggregationState(mkPromise("sequencer-aggregator-decision", futureSupervisor))
              stateRef.set(Some(newState))
              newState
            case Some(currentState) => currentState
          }

          val contributorHash = state.contributors.get(sequencerId) match {
            case Some(existingHash) =>
              // This can happen if a subscription restarts while the decision on this event has not yet been taken
              logger.info(s"Ignoring duplicate contribution from $sequencerId")
              // Return the existing hash in case the new contribution has a different one (which we ignore)
              existingHash

            case None =>
              // Merge the event into the state
              state.hashToContributions
                .updateWith(eventHash) {
                  case Some(eventAndSignatures) =>
                    Some(eventAndSignatures.updated(sequencerId, newEvent))
                  case None =>
                    Some(EventAndSignatures.create(sequencerId, newEvent))
                }
                .discard
              state.contributors += sequencerId -> eventHash

              // This can clear `stateRef` if a decision is finalized
              checkThreshold(context)

              eventHash
          }

          Right((state.decisionP, contributorHash))
      }
    }

    lateOrMerged match {
      case Left(pastEventsCache) =>
        // The provided event was late -- ignore or return an error depending on whether it is valid.
        // In both cases, the returned Future does not depend on the decision.
        val eventIsValid = pastEventsCache.get(newEvent.timestamp).contains(eventHash)
        val (firstTs, lastTs) =
          (pastEventsCache.headOption.map(_._1), pastEventsCache.lastOption.map(_._1))

        EitherT
          .cond[FutureUnlessShutdown](
            eventIsValid,
            logger.debug(
              s"[$context] Ignoring past event (latest processed at $lastTs)"
            ), {
              logger.info(
                s"[$context] Rejecting unknown past event (not present in the last ${pastEventsCache.size}" +
                  s" aggregated events between $firstTs and $lastTs)"
              )
              SequencerAggregatorError.BogusEvent(sequencerId, newEvent)
            },
          )
          .value

      case Right((decisionP, contributorHash)) =>
        // The provided event has been merged -- return a future on the decision tailored for this sequencer
        decisionP.futureUS.map(winningHashE =>
          winningHashE.flatMap(winningHash =>
            EitherUtil.condUnit(
              contributorHash == winningHash,
              SequencerAggregatorError.BogusEvent(sequencerId, newEvent),
            )
          )
        )
    }
  }

  /** Check whether the threshold is reached.
    *
    * If the threshold is reached, forward the combined event to the application handler and
    * finalize the decision. If an aggregation error occurs, finalize the decision with this error.
    *
    * This must be called with the lock held.
    */
  private def checkThreshold(
      context: String
  )(implicit traceContext: TraceContext, executionContext: ExecutionContext): Unit = {

    /* Return
     *   - `None`, if the threshold is not reached
     *   - the event that reached the threshold, if the threshold is reached
     *   - an error if the threshold is not reachable
     */
    def getHashIfThresholdIsReached
        : Either[SequencerAggregatorError, Option[(Hash, EventAndSignatures)]] =
      stateRef.get match {
        case Some(state) =>
          val hashToContributions = state.hashToContributions
          // If every sequencer delivers a different hash, the following is a quadratic algorithm because we'll check for
          // each contribution to the aggregator all hashes in the state.
          // We could fix this with more bookkeeping at the expense of some more complexity.
          // Given that we don't expect many different hashes frequently, and that we expect the number of sequencers
          // to be low, we favor simplicity until this becomes a performance bottleneck.
          val hashToNbOfContributions =
            hashToContributions.map {
              case hash -> EventAndSignatures(_event, signaturesPerSequencer) =>
                hash -> signaturesPerSequencer.size
            }
          logger.debug(
            s"[$context] Event hashes received so far for the current aggregation + nb of sequencers who provided them: $hashToNbOfContributions"
          )

          val config = configRef.get
          val threshold = config.sequencerTrustThreshold.value

          val largestGroup = hashToNbOfContributions
            .maxByOption { case (_hash, nb) => nb }
            .getOrElse(ErrorUtil.invalidState(s"No max with non-empty state"))

          largestGroup match {
            case (hash, groupSize) if groupSize >= threshold =>
              Right(Some(hash -> hashToContributions(hash)))

            case (_hash, groupSize) =>
              val decided = state.contributors.size
              val undecided = config.maxNbOfContributions.value - decided

              Either.cond(
                groupSize + undecided >= threshold,
                None,
                SequencerAggregatorError.ThresholdUnreachable(
                  decided,
                  undecided,
                  groupSize,
                  threshold,
                ),
              )
          }

        case None =>
          // This can happen when triggered by a configuration change
          Right(None)
      }

    def finalizeDecision(result: Either[SequencerAggregatorError, Hash]): Unit = {
      logger.debug(s"[$context] Finalizing decision with $result")

      // Complete promise and prepare for new decision
      val state =
        stateRef.getAndSet(None).getOrElse(ErrorUtil.invalidState("Empty aggregation state"))
      state.decisionP.outcome_(result)
    }

    logger.debug(s"[$context] Checking whether the threshold has been reached")

    getHashIfThresholdIsReached match {
      case Left(error) =>
        logger.warn(s"[$context] Error during aggregation: $error")
        finalizeDecision(Left(error))

      case Right(None) => logger.debug(s"[$context] Threshold not yet reached")

      case Right(Some(hash -> eventAndSignatures)) =>
        logger.debug(s"[$context] Threshold reached with hash $hash")

        val combinedEvent = eventAndSignatures.combineSignatures

        // Update accepted event information
        pastEventsCacheRef.set {
          // We are only adding one element at a time, and therefore need to drop at most one element to keep the size limit
          val tmp = pastEventsCacheRef.get + (combinedEvent.timestamp -> hash)
          if (tmp.sizeCompare(pastEventsCacheSize.value) > 0) tmp.drop(1) else tmp
        }

        val newProcessedEventAndOrdinal = latestProcessedEventORef.get match {
          case None => EventAndOrdinal.first(combinedEvent)
          case Some(current) => current.next(combinedEvent)
        }
        latestProcessedEventORef.set(Some(newProcessedEventAndOrdinal))

        FutureUtil.doNotAwait(
          Future(notifyNewEvent(newProcessedEventAndOrdinal)),
          s"Notifying of new processed event with ordinal ${newProcessedEventAndOrdinal.ordinal}",
        )

        forwardEventToApplicationHandler(combinedEvent)

        finalizeDecision(Right(hash))
    }
  }

  /** Forward the event to the application handler. */
  private def forwardEventToApplicationHandler(event: SequencedSerializedEvent): Unit = {
    implicit val traceContext: TraceContext = event.traceContext

    logger.debug(
      s"Storing event in the event inbox:\n${event.signedEvent.content}"
    )

    updateSendTracker(Seq(event))

    // TODO(i33239): Do not block when signalling backpressure
    if (!receivedEvents.offer(event)) {
      logger.info(
        s"Event inbox is full. Blocking sequenced event with timestamp ${event.timestamp}."
      )
      blocking {
        receivedEvents.put(event)
      }
      logger.info(
        s"Unblocked sequenced event with timestamp ${event.timestamp}."
      )
    }

    logger.debug("Signalling the application handler")
    postAggregationHandler.signalHandler(receivedEvents)
  }
}

object SequencerAggregatorXImpl {

  /** State of the aggregation
    *
    * @param decisionP
    *   promise that will be fulfilled with the result on the current event
    */
  private class AggregationState(
      val decisionP: PromiseUnlessShutdown[Either[SequencerAggregatorError, Hash]]
  ) {

    /** map hash -> event, with currently collected sequencer signatures */
    val hashToContributions = mutable.HashMap.empty[Hash, EventAndSignatures]

    /** map sequencerId -> hash, with current contributors and their hash */
    val contributors = mutable.HashMap.empty[SequencerId, Hash]
  }

  private final case class EventAndSignatures private (
      event: SequencedSerializedEvent,
      sequencerWithSignatures: NonEmpty[Seq[(SequencerId, NonEmpty[Seq[Signature]], TraceContext)]],
  ) {
    def updated(
        sequencerId: SequencerId,
        newEvent: SequencedSerializedEvent,
    ): EventAndSignatures = copy(sequencerWithSignatures =
      (
        sequencerId,
        newEvent.signedEvent.signatures,
        newEvent.traceContext,
      ) +: sequencerWithSignatures
    )

    def combineSignatures: SequencedSerializedEvent = {
      val combinedSignatures = sequencerWithSignatures.flatMap {
        case (_sequencerId, signatures, _traceContext) =>
          signatures
      }

      // Node operators can opt to not propagate trace contexts, so we cannot use them in the event hashes.
      // Malicious sequencers can also provide incorrect trace contexts.
      // As a best effort, we use the most frequent trace context within the quorum.
      val mostFrequentTraceContext = sequencerWithSignatures.forgetNE
        .mapFilter { case (_sequencerId, _signatures, tc) =>
          Option.when(tc != TraceContext.empty)(tc) // Skip empty tcs
        }
        .groupMapReduce(identity)(_ => 1)(_ + _) // Count occurrences
        .maxByOption { case (_tc, count) => count } // Get most frequent
        .map { case (tc, _count) => tc }
        .getOrElse(TraceContext.empty) // Fall back to empty tc if there is really nothing else

      SequencedEventWithTraceContext(event.signedEvent.copy(signatures = combinedSignatures))(
        mostFrequentTraceContext
      )
    }
  }

  private object EventAndSignatures {
    def create(sequencerId: SequencerId, event: SequencedSerializedEvent): EventAndSignatures =
      EventAndSignatures(
        event,
        NonEmpty(Seq, (sequencerId, event.signedEvent.signatures, event.traceContext)),
      )
  }

  final case class EventAndOrdinal private (
      event: ProcessingSerializedEvent,
      ordinal: NonNegativeInt,
  ) {
    def next(event: ProcessingSerializedEvent): EventAndOrdinal =
      EventAndOrdinal(event = event, ordinal = ordinal.increment.toNonNegative)
  }

  object EventAndOrdinal {
    def zero(event: ProcessingSerializedEvent): EventAndOrdinal =
      EventAndOrdinal(event = event, ordinal = NonNegativeInt.zero)

    def first(event: ProcessingSerializedEvent): EventAndOrdinal =
      EventAndOrdinal(event = event, ordinal = NonNegativeInt.one)
  }
}
