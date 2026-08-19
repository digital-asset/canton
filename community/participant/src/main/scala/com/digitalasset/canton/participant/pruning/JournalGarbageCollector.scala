// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.Eval
import cats.syntax.foldable.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.ledger.participant.state.{AcsChangeFactory, SynchronizerIndex}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.{AcsChangeListener, RecordTime}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, PositiveFiniteDuration}
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureUtil
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Promise}

/** Canton synchronisation journals garbage collectors
  *
  * The difference between the normal (ledger) pruning feature and the journal garbage collector is
  * that the ledger pruning is configured and invoked by the user, whereas the journal garbage
  * collector runs periodically in the background, where the retention period is generally not
  * configurable.
  */
private[participant] class JournalGarbageCollector(
    requestJournalStore: RequestJournalStore,
    synchronizerIndexF: () => Option[SynchronizerIndex],
    sortedReconciliationIntervalsProvider: SortedReconciliationIntervalsProvider,
    acsCommitmentStore: AcsCommitmentStore,
    acs: ActiveContractStore,
    submissionTrackerStore: SubmissionTrackerStore,
    inFlightSubmissionStore: Eval[InFlightSubmissionStore],
    psid: PhysicalSynchronizerId,
    journalGarbageCollectionDelay: NonNegativeFiniteDuration,
    disableLegacyAcsCommitmentProcessor: Boolean,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends JournalGarbageCollector.Scheduler {

  def observer(
      traceContext: TraceContext
  ): Unit = flush()(traceContext)

  override protected def run()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    synchronizeWithClosing(functionFullName) {
      for {
        safeToPruneTsO <-
          PruningProcessor.latestSafeToPruneTick(
            requestJournalStore,
            synchronizerIndexF(),
            sortedReconciliationIntervalsProvider,
            acsCommitmentStore,
            inFlightSubmissionStore.value,
            psid.logical,
            disableLegacyAcsCommitmentProcessor,
          )
        _ <- safeToPruneTsO.traverse_ { ts =>
          val maxDelay = (ts - CantonTimestampSecond.MinValue).toSeconds
          val cappedJournalGarbageCollectionDelay =
            journalGarbageCollectionDelay.duration.toSeconds.min(maxDelay)

          prune(ts.minusSeconds(cappedJournalGarbageCollectionDelay))
        }
      } yield ()
    }

  private def prune(pruneTs: CantonTimestampSecond)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    logger.debug(s"Starting periodic background pruning of journals up to $pruneTs")
    val acsDescription = s"Periodic ACS prune at $pruneTs"
    // Clean unused entries from the ACS
    val acsF = synchronizeWithClosing(acsDescription)(acs.prune(pruneTs.forgetRefinement))
    val submissionTrackerStoreDescription =
      s"Periodic submission tracker store prune at $pruneTs"
    // Clean unused entries from the submission tracker store
    val submissionTrackerStoreF = synchronizeWithClosing(submissionTrackerStoreDescription)(
      submissionTrackerStore.prune(pruneTs.forgetRefinement)
    )
    Seq(acsF, submissionTrackerStoreF).sequence_
  }
}

private[participant] object JournalGarbageCollector {
  private[pruning] abstract class Scheduler
      extends NamedLogging
      with FlagCloseable
      with HasCloseContext {

    /** Manage internal state of the collector
      *
      * @param requested
      *   if true, then the acs commitment processor completed a commitment period and suggested to
      *   kick off pruning
      * @param running
      *   if set, then a prune is currently running and the promise will be completed once it is
      *   done
      */
    private case class State(requested: Boolean, running: Option[Promise[Unit]])

    private val state: AtomicReference[State] = new AtomicReference(
      State(requested = false, running = None)
    )

    protected def run()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]
    protected implicit def executionContext: ExecutionContext

    private[pruning] def flush()(implicit traceContext: TraceContext): Unit =
      // set request flag and kick off pruning if flag was not already set
      if (!state.getAndUpdate(_.copy(requested = true)).requested)
        doFlush()(traceContext)

    private def doFlush()(implicit traceContext: TraceContext): Unit =
      // if we are not closing and not running, then we can start a new prune
      if (!isClosing) {
        val currentState = state.getAndUpdate {
          // start new process if idle and not blocked
          case State(true, None) => State(requested = false, Some(Promise()))
          // not enabled or already running, do nothing
          case x => x
        }
        if (currentState.running.isEmpty) {
          // we are enabled and not running, so start a new prune
          val runningF = run().onShutdown(()).thereafter { _ =>
            // once we've completed, see if we need to restart the next iteration immediately
            val current = state.getAndUpdate(_.copy(running = None))
            current.running.foreach(_.success(()))
            if (current.requested) {
              doFlush()(traceContext)
            }
          }
          FutureUtil.doNotAwait(
            runningF,
            "Periodic background journal pruning failed",
            logPassiveInstanceAtInfo = true,
            closeContext = Some(closeContext),
          )
        }
      }

  }

  class RateLimitedCollector @VisibleForTesting private[canton] (
      minimumGap: PositiveFiniteDuration,
      flush: TraceContext => Unit,
  ) extends AcsChangeListener {

    private val minimumGapMicros: Long = minimumGap.toScala.toMicros

    private val lastTrigger: AtomicReference[CantonTimestamp] = new AtomicReference(
      CantonTimestamp.MinValue
    )

    override def publish(toc: RecordTime, acsChangeFactoryO: Option[AcsChangeFactory])(implicit
        traceContext: TraceContext,
        closeContext: CloseContext,
    ): Unit = maybeTrigger(toc.timestamp)

    override def publishForUpgradeTime(
        upgradeTime: CantonTimestamp
    )(implicit traceContext: TraceContext, closeContext: CloseContext): Unit =
      maybeTrigger(upgradeTime)

    private def maybeTrigger(
        timestamp: CantonTimestamp
    )(implicit traceContext: TraceContext): Unit = {
      val previous = lastTrigger.getAndUpdate { last =>
        require(
          last <= timestamp,
          s"Timestamps must be non-decreasing: last = $last, next = $timestamp",
        )
        if (timestamp.toMicros - last.toMicros >= minimumGapMicros) {
          timestamp
        } else last
      }
      if (timestamp.toMicros - previous.toMicros >= minimumGapMicros) {
        flush(traceContext)
      }
    }
  }

  object RateLimitedCollector {
    def create(
        minimumGap: PositiveFiniteDuration,
        collector: JournalGarbageCollector,
    ): RateLimitedCollector =
      new RateLimitedCollector(minimumGap, implicit traceContext => collector.flush())
  }
}
