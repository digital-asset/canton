// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.SynchronizerSuccessor
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.participant.admin.party.PartyReplicationTopologyWorkflow.FreezingToOnboardingStatus
import com.digitalasset.canton.participant.protocol.party.FreezingToOnboardingOperation.PendingFreezingToOnboardingStore
import com.digitalasset.canton.participant.protocol.party.OnboardingClearanceOperation.PendingOnboardingClearanceStore
import com.digitalasset.canton.participant.protocol.party.{
  FreezingToOnboardingOperation,
  OnboardingClearanceOperation,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyTransactionProcessingSubscriber,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{PartyToParticipant, TopologyChangeOp}
import com.digitalasset.canton.topology.{ParticipantId, PartyId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.retry.Success.either
import com.digitalasset.canton.util.retry.{Backoff, ErrorKind, ExceptionRetryPolicy}
import com.google.common.annotations.VisibleForTesting
import org.slf4j.event.Level

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

/** Moves this participant's entry for a party from Freezing to Onboarding once it has imported the
  * party's ACS, as part of HyPR (Hybrid Party Replication). The onboarding clearance then moves the
  * entry on to Active: the move emits the `Onboarding` event that starts it.
  *
  * The party ACS import records a pending operation per party. Once connected to the synchronizer,
  * the participant catches up with the synchronizer and then proposes the move. It proposes it
  * again until its entry is no longer Freezing, because a proposal can be rejected when sequenced,
  * for example if another participant changes the party's mapping first.
  *
  * The pending operation is deleted once the entry is Onboarding or Active, also when that was
  * sequenced before this participant was restored from a backup. If the entry is removed instead,
  * the pending operation and the onboarding clearance record of the import are deleted.
  *
  * While a synchronizer upgrade is announced, the move is retried silently.
  *
  * Note: Non-final class required for mocking.
  */
class FreezingToOnboardingScheduler(
    participantId: ParticipantId,
    workflowContext: PartyReplicationTopologyWorkflow.TopologyWorkflowContext,
    pendingMovesStore: PendingFreezingToOnboardingStore,
    pendingClearancesStore: PendingOnboardingClearanceStore,
    clock: Clock,
    protected val loggerFactory: NamedLoggerFactory,
    override protected val timeouts: ProcessingTimeout,
    retryInitialDelay: FiniteDuration = OnboardingClearanceScheduler.DefaultRetryInitialDelay,
    retryMaxDelay: FiniteDuration = OnboardingClearanceScheduler.DefaultRetryMaxDelay,
)(implicit ec: ExecutionContext)
    extends TopologyTransactionProcessingSubscriber
    with FlagCloseable
    with NamedLogging {
  import FreezingToOnboardingScheduler.*

  private def psid: PhysicalSynchronizerId = workflowContext.psid

  @VisibleForTesting
  private[party] val moves: TrieMap[PartyId, Move] = TrieMap.empty

  /** Starts the move for every pending operation on this synchronizer. */
  private[participant] def requestPendingMoves()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    pendingMovesStore
      .getAll(FreezingToOnboardingOperation.operationName, Some(psid.logical))
      .map(_.foreach { pending =>
        FreezingToOnboardingOperation.partyIdFromKey(pending.key) match {
          case Right(partyId) => requestMoveInBackground(partyId)
          case Left(error) =>
            // Unreachable: the key is written from a party ID only
            ErrorUtil.invalidState(s"Failed to parse the party ID from ${pending.key}: $error")
        }
      })

  /** Starts the move for the party in the background, unless it is already running. */
  private[participant] def requestMoveInBackground(partyId: PartyId)(implicit
      traceContext: TraceContext
  ): Unit = {
    val move = new Move
    if (moves.putIfAbsent(partyId, move).isEmpty) {
      logger.info(s"Starting to move party $partyId from Freezing to Onboarding.")
      val retryPolicy = Backoff(
        logger = logger,
        hasSynchronizeWithClosing = this,
        maxRetries = Int.MaxValue,
        initialDelay = retryInitialDelay,
        maxDelay = retryMaxDelay,
        operationName = s"freezing-to-onboarding-$partyId",
      )
      retryPolicy
        .unlessShutdown(attemptMove(partyId, move).value, MoveRetryPolicy)
        .unwrap
        .onComplete { outcome =>
          moves.remove(partyId, move).discard
          outcome match {
            case Failure(exception) =>
              logger.warn(
                s"Aborted moving party $partyId from Freezing to Onboarding. To resume, reconnect synchronizer $psid or restart the participant.",
                exception,
              )
            case Success(_) =>
          }
        }
    } else
      logger.debug(s"The move of party $partyId from Freezing to Onboarding is already running.")
  }

  private def attemptMove(partyId: PartyId, move: Move)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, MoveAttemptError, Unit] =
    for {
      context <- EitherT.fromEither[FutureUnlessShutdown](
        workflowContext.synchronizerContext.toRight(
          MoveAttemptError.SynchronizerNotAvailable: MoveAttemptError
        )
      )
      _ <- EitherT.right[MoveAttemptError](awaitCaughtUp(move, context))
      snapshot <- EitherT.right[MoveAttemptError](
        context.topologyClient.currentSnapshotApproximation
      )
      lsuO <- EitherT.right[MoveAttemptError](snapshot.announcedLsu())
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        lsuO
          .map { case (successor, _) =>
            // The announcement rejects proposals sequenced after it, so propose again afterwards
            move.proposedAt.set(None)
            MoveAttemptError.LsuAnnounced(successor): MoveAttemptError
          }
          .toLeft(())
      )
      status <- currentStatus(partyId, move, context)
      _ <- status match {
        case None =>
          logger.info(
            s"Stopped moving party $partyId from Freezing to Onboarding: its pending operation no longer exists."
          )
          EitherT.rightT[FutureUnlessShutdown, MoveAttemptError](())
        case Some(FreezingToOnboardingStatus.Proposed(serial)) =>
          move.proposedAt.set(Some(serial))
          EitherT.leftT[FutureUnlessShutdown, Unit](
            MoveAttemptError.AwaitingMove(serial): MoveAttemptError
          )
        case Some(FreezingToOnboardingStatus.Moved) =>
          logger.info(s"Moved party $partyId from Freezing to Onboarding.")
          EitherT.right[MoveAttemptError](deleteMove(partyId))
        case Some(FreezingToOnboardingStatus.Aborted(state)) =>
          logger.warn(
            s"This participant's entry for party $partyId is ${state.fold("removed")(_.toString)} " +
              "while it was Freezing after the party's ACS import. The contracts that the import added " +
              "remain: restore the participant from the backup taken before the import, or purge only " +
              "these contracts. The import skipped the contracts that this participant already held, " +
              "e.g. for other parties it hosts."
          )
          // The removal of a Freezing entry emits no Revoked event, so nothing else deletes the
          // clearance record. Deleting it first lets the next connect repeat an interrupted abort.
          EitherT.right[MoveAttemptError](
            deleteClearance(partyId).flatMap(_ => deleteMove(partyId))
          )
      }
    } yield ()

  /** The status of the move, or empty if its pending operation no longer exists. */
  private def currentStatus(
      partyId: PartyId,
      move: Move,
      context: PartyReplicationTopologyWorkflow.SynchronizerTopologyContext,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, MoveAttemptError, Option[FreezingToOnboardingStatus]] =
    for {
      recordExists <- EitherT.right[MoveAttemptError](
        pendingMovesStore
          .get(
            psid.logical,
            FreezingToOnboardingOperation.operationKey(partyId),
            FreezingToOnboardingOperation.operationName,
          )
          .isDefined
      )
      status <- (recordExists, move.ended.get()) match {
        case (false, _) =>
          EitherT.rightT[FutureUnlessShutdown, MoveAttemptError](
            Option.empty[FreezingToOnboardingStatus]
          )
        case (true, Some(ended)) =>
          EitherT.rightT[FutureUnlessShutdown, MoveAttemptError](
            Option[FreezingToOnboardingStatus](ended)
          )
        case (true, None) =>
          workflowContext.workflow
            .moveFreezingToOnboarding(partyId, move.proposedAt.get(), context)
            .bimap(
              MoveAttemptError.WorkflowError(_): MoveAttemptError,
              Option[FreezingToOnboardingStatus](_),
            )
      }
    } yield status

  /** Waits until this participant has processed the topology transactions sequenced up to the
    * current time, so that it does not judge its entry from a stale topology state, e.g. while it
    * processes the events since a backup it was restored from. Does so once per move.
    *
    * The current time is taken from the participant's clock, as the synchronizer sequences the
    * events the participant has yet to process before that time. The time tracker's latest time
    * comes from these events too, and so can a time proof: the tracker takes the next one it
    * receives, also one requested before a reconnect.
    *
    * Best effort only: if the participant's clock is behind the synchronizer's, some of these
    * events are sequenced after its current time. A move proposed from the stale state then carries
    * a stale serial, and the synchronizer rejects it.
    */
  private def awaitCaughtUp(
      move: Move,
      context: PartyReplicationTopologyWorkflow.SynchronizerTopologyContext,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    if (move.caughtUp.get()) FutureUnlessShutdown.unit
    else {
      val now = clock.now
      // Gets a time at or after `now` sequenced, also if the synchronizer is idle
      context.timeTracker.requestTick(now, immediately = true).discard
      context.topologyClient
        .awaitSequencedTimestamp(SequencedTime(now))
        .getOrElse(FutureUnlessShutdown.unit)
        .map(_ => move.caughtUp.set(true))
    }

  private def deleteMove(partyId: PartyId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    pendingMovesStore.delete(
      psid.logical,
      FreezingToOnboardingOperation.operationKey(partyId),
      FreezingToOnboardingOperation.operationName,
    )

  private def deleteClearance(partyId: PartyId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    pendingClearancesStore.delete(
      psid.logical,
      OnboardingClearanceOperation.operationKey(partyId),
      OnboardingClearanceOperation.operationName,
    )

  /** Records for each running move whether a transaction has ended it, so that the move stops even
    * if the participant is Freezing again by its next attempt.
    */
  override def observed(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sequencerCounter: SequencerCounter,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    if (moves.nonEmpty)
      transactions.iterator.filterNot(_.isProposal).foreach { signedTx =>
        signedTx.transaction.mapping.select[PartyToParticipant].foreach { ptp =>
          moves.get(ptp.partyId).foreach { move =>
            val state = Option
              .when(signedTx.transaction.operation == TopologyChangeOp.Replace)(
                ptp.participants.find(_.participantId == participantId).map(_.state)
              )
              .flatten
            FreezingToOnboardingStatus
              .endedBy(state)
              .foreach(ended => move.ended.compareAndSet(None, Some(ended)).discard)
          }
        }
      }
    FutureUnlessShutdown.unit
  }
}

object FreezingToOnboardingScheduler {

  /** The state of a running move. */
  private[party] final class Move {
    val caughtUp = new AtomicBoolean(false)
    val proposedAt = new AtomicReference[Option[PositiveInt]](None)
    val ended = new AtomicReference[Option[FreezingToOnboardingStatus.Ended]](None)
  }

  sealed trait MoveAttemptError extends Product with Serializable
  object MoveAttemptError {
    case object SynchronizerNotAvailable extends MoveAttemptError
    final case class LsuAnnounced(successor: SynchronizerSuccessor) extends MoveAttemptError
    final case class AwaitingMove(serial: PositiveInt) extends MoveAttemptError
    final case class WorkflowError(message: String) extends MoveAttemptError
  }

  /** Logs the expected waits at DEBUG only, rather than at WARN after many retries: for an
    * announced upgrade, for a proposed move to become effective, and for the synchronizer, which is
    * no longer available while the participant disconnects from it, before the scheduler is closed.
    */
  private val MoveRetryPolicy: ExceptionRetryPolicy = new ExceptionRetryPolicy {
    override protected def determineExceptionErrorKind(
        exception: Throwable,
        logger: TracedLogger,
    )(implicit tc: TraceContext): ErrorKind =
      ErrorKind.UnknownErrorKind

    override def retryLogLevel(outcome: Try[Any]): Option[Level] = outcome match {
      case Success(
            UnlessShutdown.Outcome(
              Left(
                _: MoveAttemptError.LsuAnnounced | _: MoveAttemptError.AwaitingMove |
                MoveAttemptError.SynchronizerNotAvailable
              )
            )
          ) =>
        Some(Level.DEBUG)
      case _ => super.retryLogLevel(outcome)
    }
  }
}
