// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.Eval
import cats.syntax.functor.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.store.SequencerCounterTrackerStore
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import io.functionmeta.functionFullName

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.math.Ordering.Implicits.*

private[participant] class PruneObserver(
    requestJournalStore: RequestJournalStore,
    sequencerCounterTrackerStore: SequencerCounterTrackerStore,
    sortedReconciliationIntervalsProvider: SortedReconciliationIntervalsProvider,
    acsCommitmentStore: AcsCommitmentStore,
    acs: ActiveContractStore,
    keyJournal: ContractKeyJournal,
    inFlightSubmissionStore: Eval[InFlightSubmissionStore],
    domainId: DomainId,
    acsPruningInterval: NonNegativeFiniteDuration,
    clock: Clock,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging
    with FlagCloseable {

  /** Stores the participant's local time when we last started a pruning call
    * (or [[com.digitalasset.canton.data.CantonTimestamp.MinValue]] if unknown)
    * and a future that completes when that pruning call has finished.
    *
    * The synchronization around this variable is relatively loose.
    * In case of concurrent calls, the risk is to do unnecessary calls
    * to pruning, which is not a big deal since the pruning operation is idempotent.
    */
  private val lastPrune: AtomicReference[(CantonTimestamp, Future[Unit])] =
    new AtomicReference(CantonTimestamp.MinValue -> Future.unit)

  def observer(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Unit] = {
    val now = clock.now
    val (lastPruneTs, lastPruningF) = lastPrune.get()

    val durationSinceLastPruning: Duration = now - lastPruneTs
    val doPruning: Boolean = durationSinceLastPruning >= acsPruningInterval.unwrap
    if (!doPruning) {
      logger.debug(
        s"Skipping ACS background pruning at commitment tick because the elapsed time $durationSinceLastPruning since last pruning at most the acs pruning interval $acsPruningInterval"
      )
      FutureUnlessShutdown.unit
    } else if (!lastPruningF.isCompleted) {
      logger.warn(
        s"""Background ACS pruning initiated at $lastPruneTs took longer than the configured ACS pruning interval $acsPruningInterval.
           |Pruning at $now is skipped. Consider to increase the setting participants.<participant>.parameters.stores.acs-pruning-interval.
          """.stripMargin
      )
      FutureUnlessShutdown.unit
    } else {
      performUnlessClosingF(functionFullName) {
        for {
          safeToPruneTsO <-
            AcsCommitmentProcessor.safeToPrune(
              requestJournalStore,
              sequencerCounterTrackerStore,
              sortedReconciliationIntervalsProvider,
              acsCommitmentStore,
              inFlightSubmissionStore.value,
              domainId,
              checkForOutstandingCommitments = false,
            )
          _ <- safeToPruneTsO.fold(Future.unit)(prune(_, now))
        } yield ()
      }
    }
  }

  private def prune(pruneTs: CantonTimestampSecond, localTs: CantonTimestamp)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Unit] = {

    val promise = Promise[Unit]()
    val (oldTs, _) = lastPrune.getAndUpdate { case old @ (oldTs, _) =>
      if (oldTs < localTs) localTs -> promise.future else old
    }

    if (oldTs < localTs) {
      // Clean unused entries from the ACS
      val acsF = EitherTUtil
        .logOnError(acs.prune(pruneTs.forgetRefinement), s"Periodic ACS prune at $pruneTs:")
        .value
        // Discard the result of this prune, as it's not needed
        .void
      // clean unused contract key journal entries
      val journalF =
        EitherTUtil
          .logOnError(
            keyJournal.prune(pruneTs.forgetRefinement),
            s"Periodic contract key journal prune at $pruneTs: ",
          )
          .value
          // discard the result of this prune
          .void
      val pruneF = acsF.flatMap(_ => journalF)
      promise.completeWith(pruneF)
      pruneF
    } else {
      /*
      Possible race condition here, another call to prune with later timestamp
      was done in the meantime. Not doing anything.
       */
      Future.unit
    }
  }
}
