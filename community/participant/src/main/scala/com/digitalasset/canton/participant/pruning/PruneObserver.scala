// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import com.digitalasset.canton.data.CantonTimestampSecond
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store._
import com.digitalasset.canton.store.SequencerCounterTrackerStore
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration, PositiveSeconds}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import cats.syntax.functor._

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordering.Implicits._

private[participant] class PruneObserver(
    requestJournalStore: RequestJournalStore,
    sequencerCounterTrackerStore: SequencerCounterTrackerStore,
    reconciliationInterval: PositiveSeconds,
    acsCommitmentStore: AcsCommitmentStore,
    acs: ActiveContractStore,
    keyJournal: ContractKeyJournal,
    inFlightSubmissionStore: InFlightSubmissionStore,
    domainId: DomainId,
    acsPruningInterval: NonNegativeFiniteDuration,
    clock: Clock,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  /*
    The synchronization around this variable is relatively loose.
    In case of concurrent calls, the risk is to do unnecessary calls
    to pruning, which is not a big deal since the pruning operation is idempotent.
   */
  private val lastPrune: AtomicReference[CantonTimestampSecond] = new AtomicReference(
    CantonTimestampSecond.MinValue
  )

  def observer(implicit ec: ExecutionContext, traceContext: TraceContext): Future[Unit] = {
    val now = clock.now
    val durationSinceLastPruning: Duration = now - lastPrune.get().forgetSecond
    val doPruning: Boolean = durationSinceLastPruning > acsPruningInterval.unwrap

    for {
      safeToPruneTsO <-
        if (doPruning)
          AcsCommitmentProcessor.safeToPrune(
            requestJournalStore,
            sequencerCounterTrackerStore,
            reconciliationInterval,
            acsCommitmentStore,
            inFlightSubmissionStore,
            domainId,
            checkForOutstandingCommitments = false,
          )
        else Future.successful(None)

      _ <- safeToPruneTsO.fold(Future.unit)(prune)
    } yield ()
  }

  private def prune(ts: CantonTimestampSecond)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Unit] = {

    val newValue = lastPrune.updateAndGet(_.max(ts))

    if (newValue == ts) {
      // Clean unused entries from the ACS
      val acsF = EitherTUtil
        .logOnError(acs.prune(ts.forgetSecond), s"Periodic ACS prune at $ts:")
        .value
        // Discard the result of this prune, as it's not needed
        .void
      // clean unused contract key journal entries
      val journalF =
        EitherTUtil
          .logOnError(
            keyJournal.prune(ts.forgetSecond),
            s"Periodic contract key journal prune at $ts: ",
          )
          .value
          // discard the result of this prune
          .void
      acsF.flatMap(_ => journalF)
    } else {
      /*
      Possible race condition here, another call to prune with later timestamp
      was done in the meantime. Not doing anything.
       */
      Future.unit
    }
  }
}
