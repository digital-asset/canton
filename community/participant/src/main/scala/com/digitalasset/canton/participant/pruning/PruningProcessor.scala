// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import cats.{Eval, Monad}
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond, Offset}
import com.digitalasset.canton.ledger.participant.state.SynchronizerIndex
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  LifeCycle,
}
import com.digitalasset.canton.logging.{
  HasLoggerName,
  NamedLoggerFactory,
  NamedLogging,
  NamedLoggingContext,
}
import com.digitalasset.canton.participant.Pruning
import com.digitalasset.canton.participant.Pruning.*
import com.digitalasset.canton.participant.metrics.PruningMetrics
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.CommitmentsPruningBound
import com.digitalasset.canton.participant.store.{
  AcsCommitmentStore,
  InFlightSubmissionStore,
  ParticipantNodePersistentState,
  RequestJournalStore,
  SynchronizerConnectionConfigStore,
}
import com.digitalasset.canton.participant.sync.SyncPersistentStateManager
import com.digitalasset.canton.pruning.ConfigForNoWaitCounterParticipants
import com.digitalasset.canton.scheduler.SafeToPruneCommitmentState
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{
  EitherTUtil,
  ErrorUtil,
  FutureUnlessShutdownUtil,
  MonadUtil,
  SimpleExecutionQueue,
}
import com.google.common.annotations.VisibleForTesting
import org.slf4j.event.Level

import scala.concurrent.ExecutionContext
import scala.math.Ordering.Implicits.*

/** The pruning processor coordinates the pruning of all participant node stores
  *
  * @param participantNodePersistentState
  *   the persistent state of the participant node that is not specific to a synchronizer
  * @param syncPersistentStateManager
  *   manager to provide a participant's state for a synchronizer used for pruning
  * @param maxPruningBatchSize
  *   size to which to break up pruning batches to limit (memory) resource consumption
  * @param metrics
  *   pruning metrics
  * @param exitOnFatalFailures
  *   whether to crash on failures
  */
class PruningProcessor(
    participantNodePersistentState: Eval[ParticipantNodePersistentState],
    syncPersistentStateManager: SyncPersistentStateManager,
    maxPruningBatchSize: PositiveInt,
    metrics: PruningMetrics,
    exitOnFatalFailures: Boolean,
    synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
    override protected val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext {
  import PruningProcessor.*

  private val executionQueue = new SimpleExecutionQueue(
    "pruning-processor-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = exitOnFatalFailures,
  )

  reportUnfinishedPruning()(TraceContext.empty)

  private val firstUnsafeOffsetComputation = new FirstUnsafeOffsetComputation(
    participantNodePersistentState,
    synchronizerConnectionConfigStore,
    syncPersistentStateManager,
    timeouts,
    loggerFactory,
  )

  /** Logs a warning if there is an unfinished pruning.
    */
  private def reportUnfinishedPruning()(implicit traceContext: TraceContext): Unit =
    FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
      executionQueue
        .executeUS(
          for {
            status <- participantNodePersistentState.value.pruningStore.pruningStatus()
          } yield {
            if (status.isInProgress)
              logger.warn(
                show"Unfinished pruning operation. The participant has been partially pruned up to ${status.startedO.showValue}. " +
                  show"The last successful pruning operation has deleted all events up to ${status.completedO.showValueOrNone}."
              )
            else logger.info(show"Pruning status: $status")
          },
          functionFullName,
        ),
      "Unable to retrieve pruning status.",
      level = if (isClosing) Level.INFO else Level.ERROR,
    )

  /** Prune ledger event stream of this participant up to the given global offset inclusively.
    * Returns the global offset of the last pruned event.
    *
    * Safe to call multiple times concurrently.
    */
  def pruneLedgerEvents(
      pruneUpToInclusive: Offset,
      safeToPruneCommitmentState: Option[SafeToPruneCommitmentState],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Unit] = {

    // Returns a Right(()) if done and a Left(Some(offset)) if pruning should
    // be done again using offset.
    def go(lastUpTo: Option[Offset]): FutureUnlessShutdown[Either[Option[Offset], Unit]] = {
      val pruneUpToNext = increaseByBatchSize(lastUpTo)
      val offset = pruneUpToNext.min(pruneUpToInclusive)
      val done = offset == pruneUpToInclusive

      pruneLedgerEventBatch(lastUpTo, offset).map(_ => Either.cond(done, (), Some(offset)))
    }

    def doPrune()(implicit
        executionContext: ExecutionContext
    ): EitherT[FutureUnlessShutdown, LedgerPruningError, Unit] =
      EitherTUtil.timed(metrics.overall)(
        for {
          pruningStatus <- EitherT
            .right(
              participantNodePersistentState.value.pruningStore.pruningStatus()
            )
          _ensuredSafeToPrune <- ensurePruningOffsetIsSafe(
            pruneUpToInclusive,
            safeToPruneCommitmentState,
          )

          _ <- EitherT.liftF[FutureUnlessShutdown, LedgerPruningError, Unit](
            Monad[FutureUnlessShutdown].tailRecM(pruningStatus.completedO)(go)
          )
        } yield ()
      )

    executionQueue.executeEUS(doPrune(), s"prune ledger events up to $pruneUpToInclusive")
  }

  /** Returns an offset of at most `boundInclusive` that is safe to prune and whose timestamp is
    * before or at `beforeOrAt`.
    *
    * @param boundInclusive
    *   The caller must choose a bound so that the ledger API server never requests an offset at or
    *   below `boundInclusive`. Offsets at or below ledger end are typically a safe choice
    * @param safeToPruneCommitmentState
    *   Optionally specify in which conditions counter-participants that have not sent matching
    *   commitments cannot block pruning.
    */
  def safeToPrune(
      beforeOrAt: CantonTimestamp,
      boundInclusive: Offset,
      safeToPruneCommitmentState: Option[SafeToPruneCommitmentState],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Option[Offset]] = EitherT(
    participantNodePersistentState.value.ledgerApiStore
      .lastSynchronizerOffsetBeforeOrAtPublicationTime(beforeOrAt)
      .map(_.map(_.offset))
      .flatMap {
        case Some(beforeOrAtOffset) =>
          // under the hood this computation not only pushes back the boundInclusive bound according to the beforeOrAt publication timestamp, but also pushes it back before the ledger-end
          val rewoundBoundInclusive: Offset = boundInclusive.min(beforeOrAtOffset)

          // wiring through, needed
          firstUnsafeOffsetComputation
            .perform(rewoundBoundInclusive, safeToPruneCommitmentState)
            .map { firstUnsafeOffset =>
              val result = firstUnsafeOffset
                .map(_.offset)
                .flatMap(_.decrement) // unsafe -> safe
                .map(_.min(rewoundBoundInclusive))

              logger.debug(
                s"BoundInclusive: $boundInclusive, beforeOrAtPublicationTime: $beforeOrAt, beforeOrAtOffset: $beforeOrAtOffset, rewoundBoundInclusive: $rewoundBoundInclusive, first unsafe offset for rewound-bound: $firstUnsafeOffset, result: $result"
              )
              result
            }
            .value

        case None =>
          // nothing to prune, beforeOrAt is too low
          FutureUnlessShutdown.pure(Left(LedgerPruningNothingToPrune))
      }
  )

  /** Purge all data of the specified synchronizer that must be inactive.
    */
  def purgeInactiveSynchronizer(lsid: SynchronizerId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Unit] = for {
    // Ensure all configs are inactive
    configs <- EitherT.fromEither[FutureUnlessShutdown](
      synchronizerConnectionConfigStore
        .getAllFor(lsid)
        .leftMap(_ => PurgingUnknownSynchronizer(lsid))
    )

    _ <- EitherT.fromEither[FutureUnlessShutdown](
      NonEmpty
        .from(configs.collect {
          case config if config.status != SynchronizerConnectionConfigStore.Inactive =>
            (config.configuredPsid, config.status)
        }.toSet)
        .toLeft(())
        .leftMap(PurgingOnlyAllowedOnInactiveSynchronizer(lsid, _))
    )

    _ <- EitherT.right(
      synchronizeWithClosing("Purge inactive synchronizer")(purgeSynchronizer(lsid))
    )
  } yield ()

  private def pruneLedgerEventBatch(
      lastUpTo: Option[Offset],
      pruneUpToInclusiveBatchEnd: Offset,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    synchronizeWithClosing(functionFullName) {
      logger.info(s"Start pruning up to $pruneUpToInclusiveBatchEnd")
      val pruningStore = participantNodePersistentState.value.pruningStore
      for {
        _ <- pruningStore.markPruningStarted(pruneUpToInclusiveBatchEnd)
        _ <- performPruning(lastUpTo, pruneUpToInclusiveBatchEnd)
        _ <- pruningStore.markPruningDone(pruneUpToInclusiveBatchEnd)
      } yield {
        logger.info(s"Pruned up to $pruneUpToInclusiveBatchEnd")
      }
    }

  private def getPruningCutoffs(
      pruneFromExclusive: Option[Offset],
      pruneUpToInclusive: Offset,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[PruningCutoffs] =
    for {
      lastOffsetBeforeOrAtPruneUptoInclusive <- participantNodePersistentState.value.ledgerApiStore
        .lastSynchronizerOffsetBeforeOrAt(pruneUpToInclusive)

      lastOffsetInPruningRange = lastOffsetBeforeOrAtPruneUptoInclusive
        .filter(synchronizerOffset => Option(synchronizerOffset.offset) > pruneFromExclusive)
        .map(synchronizerOffset =>
          (
            synchronizerOffset.offset,
            CantonTimestamp(synchronizerOffset.publicationTime),
          )
        )

      synchronizerOffsets <- syncPersistentStateManager.getAllLogical.keySet.toList
        .parTraverseFilter[FutureUnlessShutdown, PruningCutoffs.SynchronizerOffset] { lsid =>
          participantNodePersistentState.value.ledgerApiStore
            .lastSynchronizerOffsetBeforeOrAt(lsid, pruneUpToInclusive)
            .map(
              _.filter(synchronizerOffset => Option(synchronizerOffset.offset) > pruneFromExclusive)
                .map { synchronizerOffset =>
                  PruningCutoffs.SynchronizerOffset(
                    lsid = lsid,
                    lastRecordTime = CantonTimestamp(synchronizerOffset.recordTime),
                  )
                }
            )
        }
    } yield PruningCutoffs(
      lastOffsetInPruningRange,
      synchronizerOffsets,
    )

  private def ensurePruningOffsetIsSafe(
      offset: Offset,
      safeToPruneCommitmentState: Option[SafeToPruneCommitmentState],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Unit] =
    for {
      firstUnsafeOffsetO <- firstUnsafeOffsetComputation
        .perform(offset, safeToPruneCommitmentState)
        // if nothing to prune we go on with this iteration regardless to ensure that iterative and scheduled pruning is not stuck in a window where nothing to prune
        .recover { case LedgerPruningNothingToPrune => None }
      _ <- firstUnsafeOffsetO match {
        case None => EitherT.pure[FutureUnlessShutdown, LedgerPruningError](())
        case Some(unsafe) if unsafe.offset > offset =>
          EitherT.pure[FutureUnlessShutdown, LedgerPruningError](())
        case Some(unsafe) =>
          EitherT
            .leftT[FutureUnlessShutdown, Unit]
            .apply[LedgerPruningError](
              Pruning.LedgerPruningOffsetUnsafeToPrune(
                offset,
                unsafe.synchronizerId,
                unsafe.recordTime,
                unsafe.cause,
                unsafe.offset.decrement,
              )
            )
      }
    } yield ()

  private[pruning] def performPruning(
      fromExclusive: Option[Offset],
      upToInclusive: Offset,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    for {
      cutoffs <- getPruningCutoffs(fromExclusive, upToInclusive)
      _ <- cutoffs.synchronizerOffsets.parTraverse(pruneSynchronizer)
      _ <- cutoffs.globalOffsetO.fold(FutureUnlessShutdown.unit) {
        case (globalOffset, publicationTime) =>
          pruneDeduplicationStore(globalOffset, publicationTime)
      }
    } yield ()

  /** Prune the persistent states of all physical instances of the given lsid.
    */
  private def pruneSynchronizer(synchronizerOffset: PruningCutoffs.SynchronizerOffset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val PruningCutoffs.SynchronizerOffset(lsid, lastRecordTime) = synchronizerOffset

    def logical(): EitherT[FutureUnlessShutdown, LedgerPruningInternalError, Unit] = for {
      state <- syncPersistentStateManager.getAllLogical
        .get(lsid)
        .toRight(LedgerPruningInternalError(s"Unable to get persistent state for $lsid"))
        .toEitherT[FutureUnlessShutdown]

      _ <- EitherT.liftF(state.acsCommitmentStore.prune(lastRecordTime))
      // TODO(#2600) Prune the reassignment store
    } yield ()

    def physical(): FutureUnlessShutdown[Unit] =
      MonadUtil.sequentialTraverse_(syncPersistentStateManager.getAllFor(lsid)) { state =>
        logger.debug(s"Pruning sequenced event store of ${state.psid}")

        for {
          _ <- state.sequencedEventStore.prune(lastRecordTime)

          _ = logger.debug(s"Pruning request journal store of ${state.psid}")
          _ <- state.requestJournalStore.prune(lastRecordTime)
        } yield ()
      }

    logger.info(s"Pruning $lsid up to $lastRecordTime")

    for {
      _ <- physical()
      _ <- logical().fold(
        err => logger.warn(s"Unable to prune $lsid: $err"),
        _ => (),
      )
    } yield ()
  }

  /** Purge the persistent states of all physical instances of the given lsid.
    */
  private def purgeSynchronizer(lsid: SynchronizerId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    def logical(): EitherT[FutureUnlessShutdown, LedgerPruningInternalError, Unit] = for {
      state <- syncPersistentStateManager.getAllLogical
        .get(lsid)
        .toRight(LedgerPruningInternalError(s"Unable to get persistent state for $lsid"))
        .toEitherT[FutureUnlessShutdown]

      _ <- EitherT.liftF(state.activeContractStore.purge())

      // TODO(#2600) Purge the reassignment store
    } yield ()

    def physical(): FutureUnlessShutdown[Unit] =
      MonadUtil.sequentialTraverse_(syncPersistentStateManager.getAllFor(lsid)) { state =>
        logger.debug(s"Purging sequenced event store of ${state.psid}")

        for {
          _ <- state.sequencedEventStore.purge()

          _ = logger.debug(s"Purging request journal store of ${state.psid}")
          _ <- state.requestJournalStore.purge()

          _ = logger.debug(s"Purging submission of ${state.lsid}")
          _ <- state.submissionTrackerStore.purge()
        } yield ()
      }

    logger.info(s"Purging synchronizer $lsid")

    for {
      _ <- physical()
      _ <- logical().fold(
        err => logger.warn(s"Unable to purge $lsid: $err"),
        _ => (),
      )
    } yield ()
  }

  private def pruneDeduplicationStore(
      globalOffset: Offset,
      publicationTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    logger.debug(
      s"Pruning command deduplication table at $globalOffset with publication time $publicationTime..."
    )
    participantNodePersistentState.value.commandDeduplicationStore
      .prune(globalOffset, publicationTime)
  }

  override protected def onClosed(): Unit = LifeCycle.close(executionQueue)(logger)

  def acsSetNoWaitCommitmentsFrom(
      configs: Seq[ConfigForNoWaitCounterParticipants]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    participantNodePersistentState.value.acsCounterParticipantConfigStore
      .addNoWaitCounterParticipant(configs)

  def acsGetNoWaitCommitmentsFrom(
      synchronizers: Seq[SynchronizerId],
      participants: Seq[ParticipantId],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[ConfigForNoWaitCounterParticipants]] =
    participantNodePersistentState.value.acsCounterParticipantConfigStore
      .getAllActiveNoWaitCounterParticipants(
        synchronizers,
        participants,
      )

  def acsResetNoWaitCommitmentsFrom(
      configs: Seq[ConfigForNoWaitCounterParticipants]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    participantNodePersistentState.value.acsCounterParticipantConfigStore
      .removeNoWaitCounterParticipant(configs.map(_.synchronizerId), configs.map(_.participantId))

  /** Providing the next Offset for iterative pruning: computed by the current pruning Offset
    * increased by the max pruning batch size.
    */
  def findPruningOffsetForOneIteration(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Offset] =
    EitherT
      .right(
        participantNodePersistentState.value.pruningStore.pruningStatus()
      )
      .map(_.completedO)
      .map(increaseByBatchSize)

  private def increaseByBatchSize(offset: Option[Offset]): Offset =
    Offset.tryFromLong(offset.fold(0L)(_.unwrap) + maxPruningBatchSize.value)

}

private[pruning] object PruningProcessor extends HasLoggerName {

  /* Extracted to be able to test more easily */
  @VisibleForTesting
  private[pruning] def safeToPrune_(
      cleanReplayF: FutureUnlessShutdown[CantonTimestamp],
      commitmentsPruningBound: CommitmentsPruningBound,
      earliestInFlightSubmissionFUS: FutureUnlessShutdown[Option[CantonTimestamp]],
      sortedReconciliationIntervalsProvider: SortedReconciliationIntervalsProvider,
      synchronizerId: SynchronizerId,
  )(implicit
      ec: ExecutionContext,
      loggingContext: NamedLoggingContext,
  ): FutureUnlessShutdown[Option[CantonTimestampSecond]] =
    for {
      // This logic progressively lowers the timestamp based on the following constraints:
      // 1. Pruning must not delete data needed for recovery (after the clean replay timestamp)
      cleanReplayTs <- cleanReplayF

      // 2. Pruning must not delete events from the event log for which there are still in-flight submissions.
      // We check here the synchronizer related events only.
      //
      // Processing of sequenced events may concurrently move the earliest in-flight submission back in time
      // (from timeout to sequencing timestamp), but this can only happen if the corresponding request is not yet clean,
      // i.e., the sequencing timestamp is after `cleanReplayTs`. So this concurrent modification does not affect
      // the calculation below.
      inFlightSubmissionTs <- earliestInFlightSubmissionFUS

      getTickBeforeOrAt = (ts: CantonTimestamp) =>
        sortedReconciliationIntervalsProvider
          .reconciliationIntervals(ts)(loggingContext.traceContext)
          .map(_.tickBeforeOrAt(ts))
          .flatMap {
            case Some(tick) =>
              loggingContext.debug(
                s"Tick before or at $ts yields $tick on synchronizer $synchronizerId"
              )
              FutureUnlessShutdown.pure(tick)
            case None =>
              FutureUnlessShutdown.failed(
                new RuntimeException(
                  s"Unable to compute tick before or at `$ts` for synchronizer $synchronizerId"
                )
              )
          }

      // Latest potential pruning point is the ACS commitment tick before or at the "clean replay" timestamp
      // and strictly before the earliest timestamp associated with an in-flight submission.
      latestTickBeforeOrAt <- getTickBeforeOrAt(
        cleanReplayTs.min(
          inFlightSubmissionTs.fold(CantonTimestamp.MaxValue)(_.immediatePredecessor)
        )
      )

      // Only acs commitment ticks whose ACS commitment fully matches all counter participant ACS commitments are safe,
      // so look for the most recent such tick before latestTickBeforeOrAt if any.
      tsSafeToPruneUpTo <- commitmentsPruningBound match {
        case CommitmentsPruningBound.Outstanding(noOutstandingCommitmentsF) =>
          noOutstandingCommitmentsF(latestTickBeforeOrAt.forgetRefinement)
            .flatMap(
              _.traverse(getTickBeforeOrAt)
            )
        case CommitmentsPruningBound.LastComputedAndSent(lastComputedAndSentF) =>
          for {
            lastComputedAndSentO <- lastComputedAndSentF
            tickBeforeLastComputedAndSentO <- lastComputedAndSentO.traverse(getTickBeforeOrAt)
          } yield tickBeforeLastComputedAndSentO.map(_.min(latestTickBeforeOrAt))
      }

      _ = loggingContext.debug {
        val timestamps = Map(
          "cleanReplayTs" -> cleanReplayTs.toString,
          "inFlightSubmissionTs" -> inFlightSubmissionTs.toString,
          "latestTickBeforeOrAt" -> latestTickBeforeOrAt.toString,
          "tsSafeToPruneUpTo" -> tsSafeToPruneUpTo.toString,
        )

        s"Getting safe to prune commitment tick with data $timestamps on synchronizer $synchronizerId"
      }

      // Sanity check that safe pruning timestamp has not "increased" (which would be a coding bug).
      _ = tsSafeToPruneUpTo.foreach(ts =>
        ErrorUtil.requireState(
          ts <= latestTickBeforeOrAt,
          s"limit $tsSafeToPruneUpTo after $latestTickBeforeOrAt on synchronizer $synchronizerId",
        )
      )
    } yield tsSafeToPruneUpTo

  /** The latest commitment tick before or at the given time at which it is safe to prune. */
  def latestSafeToPruneTick(
      requestJournalStore: RequestJournalStore,
      synchronizerIndexO: Option[SynchronizerIndex],
      sortedReconciliationIntervalsProvider: SortedReconciliationIntervalsProvider,
      acsCommitmentStore: AcsCommitmentStore,
      inFlightSubmissionStore: InFlightSubmissionStore,
      synchronizerId: SynchronizerId,
      // TODO(#30038) remove checkForOutstandingCommitments, because all callers set it to false
      checkForOutstandingCommitments: Boolean,
  )(implicit
      ec: ExecutionContext,
      loggingContext: NamedLoggingContext,
  ): FutureUnlessShutdown[Option[CantonTimestampSecond]] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext
    val cleanReplayF = requestJournalStore
      .crashRecoveryPruningBoundInclusive(synchronizerIndexO)
      .map(_.getOrElse(CantonTimestamp.MinValue))

    val commitmentsPruningBound =
      if (checkForOutstandingCommitments)
        CommitmentsPruningBound.Outstanding(ts =>
          acsCommitmentStore.noOutstandingCommitments(ts, None)
        )
      else
        CommitmentsPruningBound.LastComputedAndSent(
          acsCommitmentStore.lastComputedAndSent.map(_.map(_.forgetRefinement))
        )

    val earliestInFlightF = inFlightSubmissionStore.lookupEarliest(synchronizerId)

    safeToPrune_(
      cleanReplayF,
      commitmentsPruningBound = commitmentsPruningBound,
      earliestInFlightF,
      sortedReconciliationIntervalsProvider,
      synchronizerId,
    )
  }

  final case class UnsafeOffset(
      offset: Offset,
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
      cause: String,
  )

  /** PruningCutoffs captures two "formats" of the same pruning cutoff: The global offset and
    * per-synchronizer local offsets.
    * @param synchronizerOffsets
    *   cutoff as synchronizer-local offsets used for canton-internal per-synchronizer pruning
    */
  final case class PruningCutoffs(
      globalOffsetO: Option[(Offset, CantonTimestamp)],
      synchronizerOffsets: List[PruningCutoffs.SynchronizerOffset],
  )

  object PruningCutoffs {

    /** @param lsid
      *   The id of the synchronizer. It is logical despite referring to a sequencing timestamp
      *   (which is physical) because it comes from the indexer.
      * @param lastRecordTime
      *   Last sequencing timestamp/record time below the given globalOffset
      */
    final case class SynchronizerOffset(
        lsid: SynchronizerId,
        lastRecordTime: CantonTimestamp,
    )
  }
}
