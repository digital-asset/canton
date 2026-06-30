// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import cats.syntax.traverseFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.SynchronizerIndex
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.Pruning
import com.digitalasset.canton.participant.Pruning.*
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.pruning.PruningProcessor.UnsafeOffset
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.{
  Active,
  HardMigratingSource,
  HardMigratingTarget,
  Inactive,
  LsuSource,
  LsuTarget,
  UnknownId,
}
import com.digitalasset.canton.participant.sync.SyncPersistentStateManager
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SynchronizerOffset
import com.digitalasset.canton.scheduler.SafeToPruneCommitmentState
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.util.ShowUtil.*

import scala.concurrent.ExecutionContext
import scala.math.Ordering.Implicits.*

/** The pruning processor coordinates the pruning of all participant node stores
  *
  * @param participantNodePersistentState
  *   the persistent state of the participant node that is not specific to a synchronizer
  */
class FirstUnsafeOffsetComputation(
    participantNodePersistentState: Eval[ParticipantNodePersistentState],
    synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
    syncPersistentStateManager: SyncPersistentStateManager,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext {
  import PruningProcessor.*

  /** List all synchronizer ids that need to be considered in the computation.
    *
    * For a given lsid:
    *   - If all configurations are
    *     [[com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.Inactive]]
    *     it should not be included in the unsafe to prune computation.
    *   - If there is one status
    *     [[com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.HardMigratingSource]]
    *     or
    *     [[com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.HardMigratingTarget]],
    *     then refuse pruning completely. This is because assignations of contracts is being
    *     changed. Note that it is just a sanity check; it does not prevent a migration from being
    *     started concurrently with pruning.
    *     - Otherwise, include the lsid in the computation of the unsafe to prune offset.
    */
  private def listAllRelevantSynchronizers()(implicit
      traceContext: TraceContext
  ): Either[LedgerPruningError, Set[SynchronizerId]] = {

    val lsids = synchronizerConnectionConfigStore.aliasResolution.logicalSynchronizerIds

    lsids.toSeq
      .traverseFilter { lsid =>
        synchronizerConnectionConfigStore.getAllStatusesFor(lsid) match {
          case Left(_: UnknownId) =>
            Left(LedgerPruningInternalError(s"No synchronizer status for $lsid"))

          case Right(configs) =>
            val hasAllInactive = configs.forall(_ == Inactive)

            if (hasAllInactive)
              Right(None)
            else
              configs.forgetNE
                .traverse_ {
                  case Active | Inactive | LsuSource | LsuTarget => Right(())
                  case migratingStatus @ (HardMigratingSource | HardMigratingTarget) =>
                    logger.info(
                      s"Unable to prune while $lsid is being migrated ($migratingStatus)"
                    )
                    Left(LedgerPruningNotPossibleDuringUpgrade(lsid, migratingStatus))
                }
                .map(_ => Some(lsid))
        }
      }
      .map(_.toSet)
  }

  /** Computes the first unsafe offset that is before or at pruneUptoInclusive. Returns none if
    * there is nothing to prune.
    */
  def perform(
      pruneUptoInclusive: Offset,
      safeToPruneCommitmentState: Option[SafeToPruneCommitmentState],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Option[UnsafeOffset]] = {
    logger.info(
      s"Computing unsafe offset with upper bound $pruneUptoInclusive and commitment state $safeToPruneCommitmentState"
    )

    for {
      _ <- EitherT.cond[FutureUnlessShutdown](
        participantNodePersistentState.value.ledgerApiStore
          .ledgerEndCache()
          .map(_.lastOffset)
          >= Some(pruneUptoInclusive),
        (),
        Pruning.LedgerPruningOffsetAfterLedgerEnd: LedgerPruningError,
      )

      allLsids <- EitherT.fromEither[FutureUnlessShutdown](listAllRelevantSynchronizers())
      synchronizerIndexes <- getSynchronizerIndexes(allLsids.toSeq, pruneUptoInclusive)

      _ = logger.debug(s"Synchronizer indexes: $synchronizerIndexes")

      // Compute unsafe offsets coming from logical stores (e.g., ACS)
      logicalPersistentStates: Map[SynchronizerId, LogicalSyncPersistentState] =
        syncPersistentStateManager.getAllLogical

      unsafeLogicalSynchronizerOffsets <- MonadUtil.sequentialTraverseFilter(
        synchronizerIndexes.toSeq
      ) { case (lsid, synchronizerIndex) =>
        for {
          state <- logicalPersistentStates
            .get(lsid)
            .toRight(
              LedgerPruningInternalError(
                s"Could not find logical persistent state for synchronizer $lsid"
              )
            )
            .toEitherT[FutureUnlessShutdown]

          offset <- FirstUnsafeOffsetComputation.firstUnsafeLogicalOffset(
            state,
            synchronizerIndex,
            participantNodePersistentState.value.ledgerApiStore,
            participantNodePersistentState.value.inFlightSubmissionStore,
            pruneUptoInclusive,
            safeToPruneCommitmentState,
          )
        } yield offset
      }

      // Compute unsafe offsets coming from physical stores
      physicalPersistentStates = getPhysicalPersistentStates(allLsids)

      unsafePhysicalSynchronizerOffsets <- MonadUtil.sequentialTraverseFilter(
        physicalPersistentStates.toSeq
      ) { physicalSyncPersistentState =>
        val lsid = physicalSyncPersistentState.psid.logical

        for {
          synchronizerIndex <- synchronizerIndexes
            .get(lsid)
            .toRight(
              Pruning.LedgerPruningInternalError(
                s"Unable to find synchronizer index for $lsid in the map"
              )
            )
            .toEitherT[FutureUnlessShutdown]

          offset <-
            FirstUnsafeOffsetComputation.firstUnsafePhysicalOffset(
              physicalSyncPersistentState,
              synchronizerIndex,
              participantNodePersistentState.value.ledgerApiStore,
            )
        } yield offset
      }

      // Other checks
      unsafeIncompleteReassignmentOffsets <- logicalPersistentStates.values.toSeq.parTraverseFilter(
        FirstUnsafeOffsetComputation.firstUnsafeReassignmentEventFor(
          _,
          participantNodePersistentState.value.ledgerApiStore,
        )
      )
      unsafeDedupOffset <- EitherT.right(firstUnsafeOffsetPublicationTime())
    } yield (unsafeLogicalSynchronizerOffsets.toList ++ unsafeDedupOffset ++ unsafePhysicalSynchronizerOffsets ++ unsafeIncompleteReassignmentOffsets)
      .minByOption(_.offset)
  }

  /** Computes the list of physical persistent state that need to be used in the computation.
    *
    * For a given logical synchronizer, old physical synchronizer states block pruning because some
    * watermarks don't progress anymore (e.g., request journal).
    *
    * Hence, for each lsid, we filter out states that have a psid that is smaller than the psid
    * corresponding to the active synchronizer connection.
    */
  private def getPhysicalPersistentStates(
      lsids: Set[SynchronizerId]
  ): Set[PhysicalSyncPersistentState] =
    lsids.flatMap { lsid =>
      val allStates = syncPersistentStateManager.getAllFor(lsid).map(_.physical)

      synchronizerConnectionConfigStore
        .getActive(lsid)
        .toOption
        .flatMap(_.configuredPsid.toOption) match {
        case Some(activePsid) => allStates.filter(_.psid >= activePsid)
        case None => allStates
      }
    }

  /** Compute the [[com.digitalasset.canton.ledger.participant.state.SynchronizerIndex]] for each
    * lsid in lsids.
    */
  private def getSynchronizerIndexes(
      lsids: Seq[SynchronizerId],
      pruneUptoInclusive: Offset,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, LedgerPruningError, Map[
    SynchronizerId,
    Option[SynchronizerIndex],
  ]] =
    for {
      pruningCandidatePersistentStates <- EitherT
        .right[LedgerPruningError](lsids.parFilterA { lsid =>
          participantNodePersistentState.value.ledgerApiStore
            .lastSynchronizerOffsetBeforeOrAt(lsid, pruneUptoInclusive)
            .map(_.isDefined)
        })
      _ <- EitherT.cond[FutureUnlessShutdown](
        pruningCandidatePersistentStates.nonEmpty,
        (),
        LedgerPruningNothingToPrune: LedgerPruningError,
      )
      res = lsids.map(
        (
            lsid =>
              lsid -> participantNodePersistentState.value.ledgerApiStore
                .cleanSynchronizerIndex(lsid)
        )
      )
    } yield res.toMap

  // Make sure that we do not prune an offset whose publication time has not been elapsed since the max deduplication duration.
  private def firstUnsafeOffsetPublicationTime()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[UnsafeOffset]] = {
    val (dedupStartLowerBound, maxDedupDuration) =
      participantNodePersistentState.value.settingsStore.settings.maxDeduplicationDuration match {
        case None =>
          // If we don't know the max dedup duration, use the earliest possible timestamp to be on the safe side
          CantonTimestamp.MinValue -> "unknown"
        case Some(maxDedupDuration) =>
          // Take the highest publication time of a published event as the baseline for converting the duration,
          // because the `CommandDeduplicator` will not use a lower timestamp, even if the participant clock
          // jumps backwards during fail-over.
          val publicationTimeLowerBound =
            participantNodePersistentState.value.ledgerApiStore
              .ledgerEndCache()
              .map(_.lastPublicationTime)
              .getOrElse(CantonTimestamp.MinValue)
          logger.debug(
            s"Publication time lower bound is $publicationTimeLowerBound with max deduplication duration of $maxDedupDuration"
          )
          // Subtract on `java.time.Instant` instead of CantonTimestamp so that we don't error on an underflow
          CantonTimestamp
            .fromInstant(publicationTimeLowerBound.toInstant.minus(maxDedupDuration.unwrap))
            .getOrElse(CantonTimestamp.MinValue) ->
            show"${maxDedupDuration.duration}"
      }
    participantNodePersistentState.value.ledgerApiStore
      .firstSynchronizerOffsetAfterOrAtPublicationTime(dedupStartLowerBound)
      .map { firstSynchronizerOffsetAfterOrAtDedupStartLowerBound =>
        val result = firstSynchronizerOffsetAfterOrAtDedupStartLowerBound.map(synchronizerOffset =>
          UnsafeOffset(
            offset = synchronizerOffset.offset,
            synchronizerId = synchronizerOffset.synchronizerId,
            recordTime = CantonTimestamp(synchronizerOffset.recordTime),
            cause = s"max deduplication duration of $maxDedupDuration",
          )
        )
        errorLoggingContext.debug(
          s"First unsafe pruning offset for deduplication (computed with lower bound $dedupStartLowerBound) $result"
        )
        result
      }
  }
}

object FirstUnsafeOffsetComputation {
  private def firstUnsafeReassignmentEventFor(
      persistent: LogicalSyncPersistentState,
      ledgerApiStore: LedgerApiStore,
  )(implicit
      executionContext: ExecutionContext,
      errorLoggingContext: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Option[UnsafeOffset]] = {
    implicit val tc: TraceContext = errorLoggingContext.traceContext
    val synchronizerId = persistent.lsid

    for {
      earliestIncompleteReassignmentO <- EitherT
        .right(
          persistent.reassignmentStore.findEarliestIncomplete()
        )

      unsafeOffsetO <- earliestIncompleteReassignmentO.flatTraverse {
        case (
              earliestIncompleteReassignmentGlobalOffset,
              earliestIncompleteReassignmentId,
              targetSynchronizerId,
            ) =>
          for {
            unsafeOffsetForReassignments <- EitherT[
              FutureUnlessShutdown,
              LedgerPruningError,
              SynchronizerOffset,
            ](
              ledgerApiStore
                .synchronizerOffset(earliestIncompleteReassignmentGlobalOffset)
                .map(
                  _.toRight(
                    Pruning.LedgerPruningInternalError(
                      s"incomplete reassignment from $earliestIncompleteReassignmentGlobalOffset not found on $synchronizerId"
                    )
                  )
                )
            )
            unsafeOffsetEarliestIncompleteReassignmentO = Option(
              UnsafeOffset(
                unsafeOffsetForReassignments.offset,
                unsafeOffsetForReassignments.synchronizerId,
                CantonTimestamp(unsafeOffsetForReassignments.recordTime),
                s"incomplete reassignment from $synchronizerId to $targetSynchronizerId (reassignmentId $earliestIncompleteReassignmentId)",
              )
            )

          } yield unsafeOffsetEarliestIncompleteReassignmentO
      }
    } yield {
      errorLoggingContext.debug(
        s"First unsafe pruning offset from reassignment store for logical synchronizer $synchronizerId at $unsafeOffsetO"
      )
      unsafeOffsetO
    }
  }

  /** Determines the first offset that is unsafe to prune on the basis of logical synchronizer
    * stores, like in-flight submission store or ledger api store.
    */
  private def firstUnsafeLogicalOffset(
      persistent: LogicalSyncPersistentState,
      synchronizerIndex: Option[SynchronizerIndex],
      ledgerApiStore: LedgerApiStore,
      inFlightSubmissionStore: InFlightSubmissionStore,
      pruneUptoInclusive: Offset,
      safeToPruneCommitmentState: Option[SafeToPruneCommitmentState],
  )(implicit
      executionContext: ExecutionContext,
      errorLoggingContext: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Option[UnsafeOffset]] = {
    implicit val tc: TraceContext = errorLoggingContext.traceContext
    val synchronizerId = persistent.lsid

    for {

      upToTimestampInclusive <- EitherT
        .fromOptionF[FutureUnlessShutdown, LedgerPruningError, CantonTimestamp](
          ledgerApiStore
            // we pass the ledger end because we want the absolute latest timestamp up to where it's safe to prune,
            // therefore we always perform the computation to the limit
            .lastRecordTimeBeforeOrAtSynchronizerOffset(
              persistent.lsid,
              ledgerApiStore.ledgerEndCache().map(_.lastOffset).getOrElse(pruneUptoInclusive),
            ),
          Pruning.LedgerPruningOffsetUnsafeSynchronizer(synchronizerId),
        )

      safeCommitmentTick <- EitherT
        .fromOptionF[FutureUnlessShutdown, LedgerPruningError, CantonTimestamp](
          persistent.acsCommitmentStore
            .noOutstandingCommitments(
              upToTimestampInclusive,
              safeToPruneCommitmentState,
            ),
          Pruning.LedgerPruningOffsetUnsafeSynchronizer(synchronizerId),
        )

      earliestInFlight <- EitherT.right(inFlightSubmissionStore.lookupEarliest(synchronizerId))

      unsafeTimestamps = NonEmpty(
        List,
        safeCommitmentTick -> "ACS background reconciliation",
      ) ++ synchronizerIndex
        .flatMap(_.sequencerIndex)
        .map(_ -> "Synchronizer index crash recovery")
        ++ earliestInFlight.map(_ -> "inFlightSubmissionTs")

      _ = errorLoggingContext
        .debug(
          s"Getting safe to prune timestamp for logical synchronizer $synchronizerId with data ${unsafeTimestamps.forgetNE}"
        )

      (firstUnsafeTimestamp, cause) = unsafeTimestamps.minBy1(_._1)

      firstUnsafeOffsetO <- EitherT.right(
        ledgerApiStore.firstSynchronizerOffsetAfterOrAt(
          synchronizerId,
          firstUnsafeTimestamp,
        )
      )
    } yield {
      errorLoggingContext.debug(
        s"First unsafe pruning offset for logical synchronizer $synchronizerId at $firstUnsafeOffsetO from $cause"
      )
      firstUnsafeOffsetO.map(synchronizerOffset =>
        UnsafeOffset(
          offset = synchronizerOffset.offset,
          synchronizerId = synchronizerId,
          recordTime = CantonTimestamp(synchronizerOffset.recordTime),
          cause = cause,
        )
      )
    }
  }

  /** Determines the first offset that is unsafe to prune on the basis of physical synchronizer
    * stores, like request journal store or topology store.
    */
  private def firstUnsafePhysicalOffset(
      persistent: PhysicalSyncPersistentState,
      synchronizerIndex: Option[SynchronizerIndex],
      ledgerApiStore: LedgerApiStore,
  )(implicit
      executionContext: ExecutionContext,
      errorLoggingContext: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Option[UnsafeOffset]] = {
    implicit val tc: TraceContext = errorLoggingContext.traceContext
    val psid = persistent.psid

    for {
      crashRecovery <- EitherT.right {
        persistent.requestJournalStore.crashRecoveryPruningBoundInclusive(synchronizerIndex)
      }

      // Topology event crash recovery requires to not prune above the earliest sequenced timestamp referring to a not yet effective topology transaction,
      // as SequencedEventStore is used to get the trace-context of the originating topology-transaction.
      earliestSequencedTimestampForNonEffectiveTopologyTransactions <-
        synchronizerIndex
          .map(_.recordTime)
          .flatTraverse(recordTime =>
            EitherT.right[LedgerPruningError](
              persistent.topologyStore
                .findEffectiveStateChanges(
                  // as if we would crash at current SynchronizerIndex
                  fromEffectiveInclusive = recordTime,
                  onlyAtEffective = false,
                ) // using the same query as in topology crash recovery
                .map(_.view.map(_.sequencedTime).minOption.map(_.value))
            )
          )
      _ = errorLoggingContext.debug(
        s"Earliest sequenced timestamp for not-yet-effective topology transactions for synchronizer $psid: $earliestSequencedTimestampForNonEffectiveTopologyTransactions"
      )

      unsafeTimestamps = crashRecovery
        .map(_ -> "cleanReplayTs")
        .toList ++ earliestSequencedTimestampForNonEffectiveTopologyTransactions
        .map(_ -> "Topology event crash recovery")
        .toList

      minUnsafe = unsafeTimestamps.minByOption(_._1)

      _ = errorLoggingContext.debug(
        s"Getting safe to prune timestamp for physical synchronizer $psid with data $unsafeTimestamps"
      )

      result <- EitherT.right {
        minUnsafe.fold(FutureUnlessShutdown.pure(Option.empty[UnsafeOffset])) {
          case (firstUnsafeRecordTime, cause) =>
            ledgerApiStore
              .firstSynchronizerOffsetAfterOrAt(
                psid.logical,
                firstUnsafeRecordTime,
              )
              .map { firstUnsafeOffsetO =>
                errorLoggingContext.debug(
                  s"First unsafe pruning offset for physical synchronizer $psid at $firstUnsafeOffsetO from $cause"
                )
                firstUnsafeOffsetO.map(synchronizerOffset =>
                  UnsafeOffset(
                    offset = synchronizerOffset.offset,
                    synchronizerId = psid.logical,
                    recordTime = CantonTimestamp(synchronizerOffset.recordTime),
                    cause = cause,
                  )
                )
              }
        }
      }
    } yield result
  }

}
