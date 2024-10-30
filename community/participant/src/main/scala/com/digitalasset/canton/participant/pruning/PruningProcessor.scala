// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import cats.syntax.option.*
import cats.syntax.parallel.*
import cats.syntax.traverseFilter.*
import cats.{Eval, Monad}
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{PositiveInt, PositiveLong}
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  Lifecycle,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.Pruning.*
import com.digitalasset.canton.participant.metrics.PruningMetrics
import com.digitalasset.canton.participant.store.EventLogId.DomainEventLogId
import com.digitalasset.canton.participant.store.ParticipantEventLog.ProductionParticipantEventLogId
import com.digitalasset.canton.participant.store.{
  DomainConnectionConfigStore,
  EventLogId,
  ParticipantNodePersistentState,
  SyncDomainPersistentState,
}
import com.digitalasset.canton.participant.sync.{
  LedgerSyncEvent,
  SyncDomainPersistentStateManager,
  TimestampedEvent,
}
import com.digitalasset.canton.participant.{GlobalOffset, LocalOffset, Pruning}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.*
import org.slf4j.event.Level

import scala.concurrent.{ExecutionContext, Future}

/** The pruning processor coordinates the pruning of all participant node stores
  *
  * @param participantNodePersistentState the persistent state of the participant node that is not specific to a domain
  * @param syncDomainPersistentStateManager domain state manager that provides access to domain-local stores for pruning
  * @param maxPruningBatchSize          size to which to break up pruning batches to limit (memory) resource consumption
  * @param metrics                      pruning metrics
  * @param loggerFactory                named logger
  */
class PruningProcessor(
    participantNodePersistentState: Eval[ParticipantNodePersistentState],
    syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
    sortedReconciliationIntervalsProviderFactory: SortedReconciliationIntervalsProviderFactory,
    indexedStringStore: IndexedStringStore,
    maxPruningBatchSize: PositiveInt,
    metrics: PruningMetrics,
    exitOnFatalFailures: Boolean,
    domainConnectionStatus: DomainId => Option[DomainConnectionConfigStore.Status],
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

  /** Logs a warning if there is an unfinished pruning.
    */
  private def reportUnfinishedPruning()(implicit traceContext: TraceContext): Unit =
    FutureUtil.doNotAwait(
      executionQueue
        .execute(
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
        )
        .onShutdown(logger.debug("Pruning aborted due to shutdown")),
      "Unable to retrieve pruning status.",
      level = if (isClosing) Level.INFO else Level.ERROR,
    )

  private def domainEventLogId(domainId: DomainId): Future[DomainEventLogId] =
    EventLogId.forDomain(indexedStringStore)(domainId)

  /** Prune ledger event stream of this participant up to the given global offset inclusively.
    * Returns the global offset of the last pruned event.
    *
    * Safe to call multiple times concurrently.
    */
  def pruneLedgerEvents(
      pruneUpToInclusive: GlobalOffset
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, LedgerPruningError, Unit] = {

    def go: Unit => Future[Either[Unit, Either[LedgerPruningError, Unit]]] = _ => {
      (for {
        pruneUpToNext <- EitherT.right(
          participantNodePersistentState.value.multiDomainEventLog
            .locateOffset(maxPruningBatchSize.value - 1L)
            .value
        ) // minus one since localOffset parameter is "exclusive" of first entry
        offset = pruneUpToNext.fold(pruneUpToInclusive)(_.min(pruneUpToInclusive))
        done = offset == pruneUpToInclusive
        _prunedBatch <- pruneLedgerEventBatch(offset)
      } yield done).transform {
        case Left(e) => Right(Left(e)): Either[Unit, Either[LedgerPruningError, Unit]]
        case Right(true) => Right(Right(()))
        case Right(false) => Left(())
      }.value
    }

    def doPrune(): EitherT[Future, LedgerPruningError, Unit] =
      EitherTUtil.timed(metrics.overall)(
        for {
          _ensuredSafeToPrune <- ensurePruningOffsetIsSafe(pruneUpToInclusive)
          _prunedAllEventBatches <- EitherT(Monad[Future].tailRecM(())(go))
        } yield ()
      )
    executionQueue.executeE(doPrune(), s"prune ledger events upto $pruneUpToInclusive")
  }

  /** Returns an offset of at most `boundInclusive` that is safe to prune and whose timestamp is before or at `beforeOrAt`.
    *
    * @param boundInclusive The caller must choose a bound so that the ledger API server never requests an offset at or below `boundInclusive`.
    *                       Offsets at or below ledger end are typically a safe choice.
    */
  def safeToPrune(beforeOrAt: CantonTimestamp, boundInclusive: GlobalOffset)(implicit
      traceContext: TraceContext
  ): EitherT[Future, LedgerPruningError, Option[GlobalOffset]] = {
    val allDomains = syncDomainPersistentStateManager.getAll.toList
    firstUnsafeOffset(allDomains, beforeOrAt, boundInclusive).semiflatMap {
      case Some(firstUnsafe) => firstOffsetBefore(firstUnsafe.globalOffset)
      case None =>
        // If there are no unsafe offsets, the bound is safe to prune.
        Future.successful(Some(boundInclusive))
    }
  }

  private def firstOffsetBefore(
      globalOffset: GlobalOffset
  )(implicit traceContext: TraceContext): Future[Option[GlobalOffset]] =
    // The MultiDomainEventLog does not allocate global offsets consecutively,
    // so we look up the last offset.
    PositiveLong
      .create(globalOffset.toLong - 1)
      .fold(
        _ => Future.successful(None),
        offset =>
          participantNodePersistentState.value.multiDomainEventLog
            .lastGlobalOffset(upToInclusive = Some(GlobalOffset(offset)))
            .value,
      )

  private def firstUnsafeOffset(
      allDomains: List[(DomainId, SyncDomainPersistentState)],
      beforeOrAt: CantonTimestamp,
      boundInclusive: GlobalOffset,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, LedgerPruningError, Option[UnsafeOffset]] = {

    def globalOffsetFor(
        eventLogId: EventLogId,
        event: TimestampedEvent,
        cause: String,
    ): Future[Option[UnsafeOffset]] =
      // If the unsafe offset has not yet been published,
      // then pruning will not affect the unsafe offset.
      // This relies on the fact that we cannot "unpublish" events from an event log.
      // Crash recovery ensures that events deleted from the multi-domain event log will
      // also be pruned from the domain's event logs before this method is called again.
      participantNodePersistentState.value.multiDomainEventLog
        .globalOffsetFor(eventLogId, event.localOffset)
        .map(_.map { case (globalOffset, _publicationTime) =>
          UnsafeOffset(globalOffset, eventLogId, event.localOffset, cause)
        })

    def firstUnsafeEventFor(
        domainId: DomainId,
        persistent: SyncDomainPersistentState,
    ): EitherT[Future, LedgerPruningError, Option[UnsafeOffset]] =
      for {
        requestCounterCursorPrehead <- EitherT.right(
          persistent.requestJournalStore.preheadClean
        )
        sequencerCounterCursorPrehead <- EitherT.right(
          persistent.sequencerCounterTrackerStore.preheadSequencerCounter
        )
        sortedReconciliationIntervalsProvider <- sortedReconciliationIntervalsProviderFactory
          .get(domainId, sequencerCounterCursorPrehead)
          .leftMap(LedgerPruningInternalError)

        safeCommitmentTick <- EitherT
          .fromOptionF[Future, LedgerPruningError, CantonTimestampSecond](
            AcsCommitmentProcessor.safeToPrune(
              persistent.requestJournalStore,
              requestCounterCursorPrehead,
              sequencerCounterCursorPrehead,
              sortedReconciliationIntervalsProvider,
              persistent.acsCommitmentStore,
              participantNodePersistentState.value.inFlightSubmissionStore,
              domainId,
              checkForOutstandingCommitments = true,
            ),
            Pruning.LedgerPruningOffsetUnsafeDomain(domainId),
          )
        _ = logger.debug(s"Safe commitment tick for domain $domainId at $safeCommitmentTick")

        firstUnsafeEventO <- EitherT.right(
          persistent.eventLog
            .lookupEventRange(None, None, safeCommitmentTick.forgetRefinement.some, None, Some(1))
            .map(_.headOption)
        )

        eventLogId <- EitherT.right(domainEventLogId(domainId))

        firstUnsafeOffsetO <- EitherT.right(firstUnsafeEventO.traverseFilter {
          case (_localOffset, event) =>
            globalOffsetFor(
              eventLogId,
              event,
              s"ACS background reconciliation and crash recovery for domain $domainId",
            )
        })
      } yield {
        logger.debug(s"First unsafe pruning offset for domain $domainId at $firstUnsafeOffsetO")
        firstUnsafeOffsetO
      }

    def firstUnsafeTransferEventFor(
        domainId: DomainId,
        persistent: SyncDomainPersistentState,
    ): EitherT[Future, LedgerPruningError, Option[UnsafeOffset]] =
      for {
        eventLogId <- EitherT.right(domainEventLogId(domainId))

        earliestIncompleteTransferO <- EitherT.right(
          persistent.transferStore.findEarliestIncomplete()
        )

        unsafeOffset <- earliestIncompleteTransferO.fold(
          EitherT.rightT[Future, LedgerPruningError](None: Option[UnsafeOffset])
        ) { earliestIncompleteTransfer =>
          val (
            earliestIncompleteTransferGlobalOffset,
            earliestIncompleteTransferId,
            targetDomainId,
          ) = earliestIncompleteTransfer
          for {
            unsafeEventForTransfers <- participantNodePersistentState.value.multiDomainEventLog
              .lookupOffset(earliestIncompleteTransferGlobalOffset)
              .toRight(
                Pruning.LedgerPruningInternalError(
                  s"incomplete transfer from $earliestIncompleteTransferGlobalOffset not found on $domainId"
                ): LedgerPruningError
              )
            (transferEventLogId, transferLocalOffset, _publicationTimestamp) =
              unsafeEventForTransfers

            unsafeOffsetEarliestIncompleteTransferO = Option(
              UnsafeOffset(
                earliestIncompleteTransferGlobalOffset,
                transferEventLogId,
                transferLocalOffset,
                s"incomplete reassignment from ${earliestIncompleteTransferId.sourceDomain} to $targetDomainId (transferId $earliestIncompleteTransferId)",
              )
            )

          } yield unsafeOffsetEarliestIncompleteTransferO
        }
      } yield {
        logger.debug(s"First unsafe pruning offset for domain $domainId at $unsafeOffset")
        unsafeOffset
      }

    def firstUnsafeOffsetInParticipantEventLog(
        allActiveDomains: List[(DomainId, SyncDomainPersistentState)]
    ): Future[Option[UnsafeOffset]] =
      for {
        unsafeOffsetByDomain <- allActiveDomains.parTraverseFilter {
          case (domainId, _persistentState) =>
            for {
              // We are only worried about ensuring idempotency of concurrent insertions.
              // Any submission that is not yet in-flight will be assigned a higher local offset upon
              // the first insertion anyway, which pruning will not touch.
              inFlightTsO <- participantNodePersistentState.value.inFlightSubmissionStore
                .lookupEarliest(domainId)
              unsafeEventO <- inFlightTsO.traverseFilter { inFlightTs =>
                participantNodePersistentState.value.participantEventLog
                  .firstEventWithAssociatedDomainAtOrAfter(domainId, inFlightTs)
              }
            } yield unsafeEventO
        }
        firstUnsafeLocalEventO = unsafeOffsetByDomain.minByOption(_.localOffset)
        // If the unsafe event has not yet been published,
        // then pruning will not remove this event anyway
        firstUnsafeGlobalEventO <- firstUnsafeLocalEventO.traverseFilter { event =>
          globalOffsetFor(
            ProductionParticipantEventLogId,
            event,
            "in-flight submission tracking at offset",
          )
        }
      } yield firstUnsafeGlobalEventO

    // Make sure that we do not prune an offset whose publication time has not been elapsed since the max deduplication duration.
    def firstUnsafeOffsetPublicationTime: Future[Option[UnsafeOffset]] = {
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
              participantNodePersistentState.value.multiDomainEventLog.publicationTimeLowerBound
            logger.debug(
              s"Publication time lower bound is $publicationTimeLowerBound with max deduplication duration of $maxDedupDuration"
            )
            // Subtract on `java.time.Instant` instead of CantonTimestamp so that we don't error on an underflow
            CantonTimestamp
              .fromInstant(publicationTimeLowerBound.toInstant.minus(maxDedupDuration.unwrap))
              .getOrElse(CantonTimestamp.MinValue) ->
              show"${maxDedupDuration.duration}"
        }
      participantNodePersistentState.value.multiDomainEventLog
        .getOffsetByTimeAtOrAfter(dedupStartLowerBound)
        .map { case (globalOffset, eventLogId, localOffset) =>
          UnsafeOffset(
            globalOffset,
            eventLogId,
            localOffset,
            cause = s"max deduplication duration of $maxDedupDuration",
          )
        }
        .value
    }
    val allActiveDomainsE = {
      // Check that no migration is running concurrently.
      // This is just a sanity check; it does not prevent a migration from being started concurrently with pruning
      import DomainConnectionConfigStore.*
      allDomains.filterA { case (domainId, _state) =>
        domainConnectionStatus(domainId) match {
          case None =>
            Left(LedgerPruningInternalError(s"No domain status for $domainId"))
          case Some(Active) => Right(true)
          case Some(Inactive) => Right(false)
          case Some(migratingStatus) =>
            logger.warn(s"Unable to prune while $domainId is being migrated ($migratingStatus)")
            Left(LedgerPruningNotPossibleDuringHardMigration(domainId, migratingStatus))
        }
      }
    }
    for {
      allActiveDomains <- EitherT.fromEither[Future](allActiveDomainsE)
      affectedDomainsOffsets <- EitherT.right[LedgerPruningError](allActiveDomains.parFilterA {
        case (domainId, _persistent) =>
          domainEventLogId(domainId).flatMap { eventLogId =>
            participantNodePersistentState.value.multiDomainEventLog
              .lastLocalOffsetBeforeOrAt(eventLogId, Option(boundInclusive), beforeOrAt)
              .map(_.isDefined)
          }
      })
      pelAffected <- EitherT.right[LedgerPruningError](
        participantNodePersistentState.value.multiDomainEventLog
          .lastLocalOffsetBeforeOrAt(
            ProductionParticipantEventLogId,
            Option(boundInclusive),
            beforeOrAt,
          )
      )
      _ <- EitherT.cond[Future](
        affectedDomainsOffsets.nonEmpty || pelAffected.isDefined,
        (),
        LedgerPruningNothingToPrune(beforeOrAt, boundInclusive): LedgerPruningError,
      )
      unsafeDomainOffsets <- affectedDomainsOffsets.parTraverseFilter {
        case (domainId, persistent) =>
          firstUnsafeEventFor(domainId, persistent)
      }
      unsafeIncompleteTransferOffsets <- allDomains.parTraverseFilter {
        case (domainId, persistent) =>
          firstUnsafeTransferEventFor(domainId, persistent)
      }
      unsafePelOffset <- EitherT.right(firstUnsafeOffsetInParticipantEventLog(allActiveDomains))
      unsafeDedupOffset <- EitherT.right(firstUnsafeOffsetPublicationTime)
    } yield (unsafeDedupOffset.toList ++ unsafePelOffset.toList ++ unsafeDomainOffsets ++ unsafeIncompleteTransferOffsets)
      .minByOption(_.globalOffset)
  }

  private def pruneLedgerEventBatch(
      pruneUpToInclusiveBatchEnd: GlobalOffset
  )(implicit traceContext: TraceContext): EitherT[Future, LedgerPruningError, Unit] =
    performUnlessClosingEitherT[LedgerPruningError, Unit](
      functionFullName,
      LedgerPruningCancelledDueToShutdown,
    ) {
      logger.info(s"Start pruning up to $pruneUpToInclusiveBatchEnd...")
      val pruningStore = participantNodePersistentState.value.pruningStore
      for {
        _ <- EitherT.right(pruningStore.markPruningStarted(pruneUpToInclusiveBatchEnd))
        _ <- EitherT.right(performPruning(pruneUpToInclusiveBatchEnd))
        _ <- EitherT.right(pruningStore.markPruningDone(pruneUpToInclusiveBatchEnd))
      } yield {
        logger.info(s"Pruned up to $pruneUpToInclusiveBatchEnd")
      }
    }

  private def lookUpDomainAndParticipantPruningCutoffs(
      pruneUpToInclusive: GlobalOffset
  )(implicit traceContext: TraceContext): Future[PruningCutoffs] =
    for {
      offsetData <- participantNodePersistentState.value.multiDomainEventLog
        .lookupOffset(pruneUpToInclusive)
        .getOrElse(
          ErrorUtil.internalError(
            new IllegalArgumentException(s"Cannot find event for global offset $pruneUpToInclusive")
          )
        )
      (_, _, tsForPruningOffset) = offsetData
      domainStates = syncDomainPersistentStateManager.getAll
      result <- participantNodePersistentState.value.multiDomainEventLog
        .lastDomainOffsetsBeforeOrAtGlobalOffset(
          pruneUpToInclusive,
          domainStates.keys.toList,
          ProductionParticipantEventLogId,
        )
        .map { case (offsetById, participantOffset) =>
          val offsetByState = domainStates.flatMap { case (domainId, state) =>
            offsetById.get(domainId).map { case (lastLocalOffset, lastRequestOffset) =>
              PruningCutoffs
                .DomainOffset(
                  state,
                  lastLocalOffset,
                  lastRequestOffset.map(_.requestCounter),
                  force =
                    domainConnectionStatus(domainId).contains(DomainConnectionConfigStore.Inactive),
                )
            }
          }
          PruningCutoffs(
            pruneUpToInclusive,
            tsForPruningOffset,
            offsetByState.toList,
            participantOffset,
          )
        }
    } yield result

  private def lookUpContractsArchivedBeforeOrAt(
      globalOffset: GlobalOffset
  )(implicit traceContext: TraceContext): Future[Set[LfContractId]] =
    participantNodePersistentState.value.multiDomainEventLog
      .lookupEventRange(Some(globalOffset), None)
      .map { eventsToBePruned =>
        val acceptedTransactions = eventsToBePruned.collect {
          case (
                _offset,
                TimestampedEvent(at: LedgerSyncEvent.TransactionAccepted, _, _, _),
              ) =>
            at.transaction
        }
        LfTransactionUtil.consumedContractIds(acceptedTransactions)
      }

  private def ensurePruningOffsetIsSafe(
      globalOffset: GlobalOffset
  )(implicit traceContext: TraceContext): EitherT[Future, LedgerPruningError, Unit] = {

    val domains = syncDomainPersistentStateManager.getAll.toList
    for {
      firstUnsafeOffsetO <- firstUnsafeOffset(domains, CantonTimestamp.MaxValue, globalOffset)
      _ <- firstUnsafeOffsetO match {
        case None => EitherT.pure[Future, LedgerPruningError](())
        case Some(unsafe) if unsafe.globalOffset > globalOffset =>
          EitherT.pure[Future, LedgerPruningError](())
        case Some(unsafe) =>
          EitherT(
            firstOffsetBefore(unsafe.globalOffset).map { lastSafeOffsetO =>
              val err = Pruning.LedgerPruningOffsetUnsafeToPrune(
                globalOffset,
                unsafe.eventLogId,
                unsafe.localOffset,
                unsafe.cause,
                lastSafeOffsetO,
              )
              Either.left[LedgerPruningError, Unit](err)
            }
          )
      }
    } yield ()
  }

  private[pruning] def performPruning(
      upToInclusive: GlobalOffset
  )(implicit traceContext: TraceContext): Future[Unit] =
    for {
      cutoffs <- lookUpDomainAndParticipantPruningCutoffs(upToInclusive)
      archivedContracts <- lookUpContractsArchivedBeforeOrAt(upToInclusive)

      _ <- cutoffs.domainOffsets.parTraverse(pruneDomain(archivedContracts))

      // Prune event logs after domains, because the events are used to determine the contracts to be pruned from the contract store.
      _ <- pruneEventLogs(cutoffs)
    } yield ()

  /** Prune a domain persistent state.
    *
    * @param archived  Contracts which have (by some external logic) been deemed safe to delete
    */
  private def pruneDomain(
      archived: Iterable[LfContractId]
  )(domainOffset: PruningCutoffs.DomainOffset)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    val PruningCutoffs.DomainOffset(state, lastLocalOffset, lastRequestCounter, forcePrune) =
      domainOffset

    logger.info(
      show"Pruning ${state.domainId.item} up to local offset $lastLocalOffset and request counter $lastRequestCounter"
    )
    logger.debug("Pruning contract store...")

    for {
      // We must prune the contract store even if the event log is empty, because there is not necessarily an archival event
      // for divulged contracts or transferred-away contracts.
      _ <- state.contractStore.deleteIgnoringUnknown(archived)

      // Here we make use of the convention that the local offset of an event coincides with the underlying request counter.
      _ <- lastRequestCounter.fold(Future.unit)(state.contractStore.deleteDivulged)

      _pruneByTimeF <- lastLocalOffset
        .fold(OptionT.none[Future, TimestampedEvent])(state.eventLog.eventAt)
        .foldF {
          logger.info(
            show"No events founds for ${state.domainId.item} up to local offset $lastLocalOffset."
          )
          Future.successful(())
        } { event =>
          // At this point, the timestamp is guaranteed to be before or at the timestamp of the request journal's head
          // Thus, it's safe to prune the associated data at or before the timestamp.
          val beforeTs = event.timestamp

          logger.debug("Pruning sequenced event store...")
          // we don't prune stores that are pruned by the PruneObserver regularly anyway
          for {
            _ <- state.sequencedEventStore.prune(beforeTs)

            _ = logger.debug("Pruning request journal store...")
            _ <- state.requestJournalStore.prune(beforeTs, forcePrune)

            _ = logger.debug("Pruning acs commitment store...")
            _ <- state.acsCommitmentStore.prune(beforeTs)
            // TODO(#2600) Prune the transfer store
          } yield ()
        }
    } yield ()
  }

  private def pruneEventLogs(
      cutoffs: PruningCutoffs
  )(implicit traceContext: TraceContext): Future[Unit] = {
    logger.info("Pruning event logs...")
    logger.debug("Pruning participant event logs...")
    for {
      _ <- cutoffs.participantOffset.parTraverse(
        participantNodePersistentState.value.participantEventLog.prune(_)
      )

      _ = logger.debug("Pruning domain event logs...")
      _ <- cutoffs.domainOffsets.parTraverse {
        case PruningCutoffs.DomainOffset(state, localOffsetO, _, _) =>
          localOffsetO.fold(Future.unit)(state.eventLog.prune)
      }

      // Need to prune multi domain event log after domain logs, because otherwise crash recover may re-publish pruned events.
      _ = logger.debug("Pruning multi domain event log...")
      _ <- participantNodePersistentState.value.multiDomainEventLog.prune(cutoffs.globalOffset)

      // Need to prune command deduplication after multidomain event log, because otherwise crash recovery may re-insert events.
      _ = logger.debug(
        s"Pruning command dedupliation table at ${cutoffs.globalOffset} with publication time ${cutoffs.publicationTime}..."
      )
      _ <- participantNodePersistentState.value.commandDeduplicationStore.prune(
        cutoffs.globalOffset,
        cutoffs.publicationTime,
      )
    } yield ()
  }

  override protected def onClosed(): Unit = Lifecycle.close(executionQueue)(logger)

}

private[pruning] object PruningProcessor {
  private final case class UnsafeOffset(
      globalOffset: GlobalOffset,
      eventLogId: EventLogId,
      localOffset: LocalOffset,
      cause: String,
  )

  /** PruningCutoffs captures two "formats" of the same pruning cutoff: The global offset and per-domain local offsets (with participant offset).
    * @param globalOffset  cutoff to be applied to the multi domain event log that is read upstream
    * @param publicationTime the publication time of the `globalOffset`
    * @param domainOffsets cutoff as domain-local offsets used for canton-internal per-domain pruning
    * @param participantOffset optional participant-local offset
    */
  final case class PruningCutoffs(
      globalOffset: GlobalOffset,
      publicationTime: CantonTimestamp,
      domainOffsets: List[PruningCutoffs.DomainOffset],
      participantOffset: Option[LocalOffset],
  )

  object PruningCutoffs {

    /** @param state SyncDomainPersistentState of the domain
      * @param lastLocalOffset Last local offset below the given globalOffset
      * @param lastRequestCounter Last request counter below the given globalOffset
      * @param force If domain is inactive/defunct and we get to force prune
      */
    final case class DomainOffset(
        state: SyncDomainPersistentState,
        lastLocalOffset: Option[LocalOffset],
        lastRequestCounter: Option[RequestCounter],
        force: Boolean,
    )
  }
}
