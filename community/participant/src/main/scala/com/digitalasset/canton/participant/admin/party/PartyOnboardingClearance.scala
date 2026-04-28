// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerSuccessor}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.participant.admin.data.{
  FlagNotSet,
  FlagSet,
  PartyOnboardingFlagStatus,
}
import com.digitalasset.canton.participant.admin.party.PartyReplicationTopologyWorkflow.AuthorizeClearanceError
import com.digitalasset.canton.participant.protocol.party.OnboardingClearanceOperation
import com.digitalasset.canton.participant.protocol.party.OnboardingClearanceOperation.PendingOnboardingClearanceStore
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyTransactionProcessingSubscriber,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{PartyToParticipant, TopologyChangeOp}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry.Success.either
import com.digitalasset.canton.util.retry.{Backoff, ErrorKind, ExceptionRetryPolicy}
import com.google.common.annotations.VisibleForTesting
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** Holds all necessary information to perform an onboarding clearance.
  */
private[admin] final case class OnboardingClearanceTask(
    partyId: PartyId,
    earliestClearanceTime: CantonTimestamp,
    onboardingEffectiveAt: EffectiveTime,
)

/** Schedules and executes party onboarding clearances.
  *
  * This implementation is thread-safe, idempotent, and shutdown-safe. It ensures that for any given
  * party on a specific participant and synchronizer, only one clearance task is scheduled at a
  * time.
  *
  * The clearance process is passive:
  *   1. A task is submitted and held in a map.
  *   1. A time-based trigger is scheduled to *propose* the clearance transaction.
  *   1. The `observed` method is responsible for detecting the effective transaction and removing
  *      the task.
  *   1. On node restart, synchronizer reconnect, or passive to active HA transition, the
  *      `ConnectedSynchronizer` is responsible for re-submitting pending clearances to this
  *      scheduler to ensure durability.
  *   1. Upon component shutdown (`FlagCloseable`), any pending background triggers or retry loops
  *      are safely aborted or ignored.
  *
  * Note: Non-final class required for mocking.
  *
  * @param timeouts
  *   Processing timeouts used for the closing context.
  */
class OnboardingClearanceScheduler(
    participantId: ParticipantId,
    workflowContext: PartyReplicationTopologyWorkflow.TopologyWorkflowContext,
    protected val loggerFactory: NamedLoggerFactory,
    pendingClearancesStore: PendingOnboardingClearanceStore,
    override protected val timeouts: ProcessingTimeout,
    retryInitialDelay: FiniteDuration = OnboardingClearanceScheduler.DefaultRetryInitialDelay,
    retryMaxDelay: FiniteDuration = OnboardingClearanceScheduler.DefaultRetryMaxDelay,
)(implicit ec: ExecutionContext)
    extends TopologyTransactionProcessingSubscriber
    with FlagCloseable
    with NamedLogging {

  private def psid: PhysicalSynchronizerId = workflowContext.psid
  private def topologyWorkflow: PartyReplicationTopologyWorkflow = workflowContext.workflow

  private def topologyContext
      : Either[String, PartyReplicationTopologyWorkflow.SynchronizerTopologyContext] =
    workflowContext.synchronizerContext.toRight(s"Synchronizer connection is not yet available.")

  @VisibleForTesting
  private[party] val pendingClearances: TrieMap[PartyId, OnboardingClearanceTask] = TrieMap.empty

  @VisibleForTesting
  private[party] val inFlightBackgroundRequests: TrieMap[PartyId, Unit] = TrieMap.empty

  // Ensures we only log the LSU warning once for the entire scheduler, across all pending parties and methods
  // Once LSU is annouced and the topology is frozen, hence submitting onboarding flag clearance would simply result in an error
  private val lsuAnnouncementLogged = new AtomicBoolean(false)

  /** Ensures that the start and cancellation of an LSU is logged exactly once. */
  private def logLsuAnnouncedOnce(
      lsuO: Option[(SynchronizerSuccessor, EffectiveTime)]
  )(implicit traceContext: TraceContext): Unit =
    lsuO match {
      case Some((successor, upgradeTime)) =>
        if (lsuAnnouncementLogged.compareAndSet(false, true)) {
          logger.info(
            s"Onboarding flag clearances will be deferred or retried silently in the background because of announced LSU for $psid (successor: $successor, upgrade time: $upgradeTime)."
          )
        }
      case None =>
        if (lsuAnnouncementLogged.compareAndSet(true, false)) {
          logger.info(
            s"Resuming onboarding flag clearances because previously announced LSU for $psid was cancelled."
          )
        }
    }

  /** Safely checks the persistent store for Protocol Version 35 or higher. If no record exists, it
    * aborts the operation and removes the task from the pending map.
    */
  private def ensureClearanceRecordExists(
      partyId: PartyId
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, OnboardingClearanceScheduler.ClearanceAttemptError, Unit] = {
    import com.digitalasset.canton.version.ProtocolVersion

    if (psid.protocolVersion >= ProtocolVersion.v35) {
      EitherT(
        pendingClearancesStore
          .get(
            psid.logical,
            operationKey = OnboardingClearanceOperation.operationKey(partyId),
            operationName = OnboardingClearanceOperation.operationName,
          )
          .value
          .map {
            case None =>
              logger.info(
                s"Aborting onboarding flag clearance because no pending onboarding clearance record found for $partyId."
              )
              // Actively remove the task as requested
              pendingClearances.remove(partyId).discard
              Left(OnboardingClearanceScheduler.ClearanceAttemptError.NoPendingClearanceRecord)
            case Some(_) =>
              Right(())
          }
      )
    } else {
      EitherT.rightT[FutureUnlessShutdown, OnboardingClearanceScheduler.ClearanceAttemptError](())
    }
  }

  /** Attempts to clear the party's onboarding flag.
    *
    * This method is intended to back synchronous API endpoints like
    * ([[com.digitalasset.canton.participant.admin.grpc.GrpcPartyManagementService.clearPartyOnboardingFlag]])
    * and is designed to be safely polled repeatedly.
    *
    * If the flag cannot be cleared immediately, a deferred task is scheduled in the background.
    *
    * LSU behavior and polling: If an LSU is currently announced, this method employs a "Graceful
    * Wait" strategy. It bypasses standard authorization checks and immediately returns
    * `FlagSet(safeTime)` (where `safeTime` is the LSU post-upgrade time). This satisfies the
    * synchronous polling contract without throwing errors, deferring the actual clearance attempt
    * to the background. Because the method re-evaluates the LSU state on every call, continuous
    * polling handles LSU cancellations seamlessly. If an LSU is cancelled, a subsequent poll will
    * detect the absence of the LSU, authorize the clearance normally, and automatically overwrite
    * the distant background task with a new, timely execution schedule.
    *
    * @param partyId
    *   The party whose onboarding flag to clear.
    * @param onboardingEffectiveAt
    *   Effective time of the original transaction that set the `onboarding = true` flag.
    * @return
    *   A `FutureUnlessShutdown` yielding:
    *   - `Right(FlagNotSet)` if the flag is or was already cleared.
    *   - `Right(FlagSet(safeTime))` if the flag is set and clearance is scheduled for `safeTime`.
    *   - `Left(String)` if a precondition fails or an authorization error occurs.
    *
    * @note
    *   Preconditions:
    *   1. Connection: `getConnectedSynchronizer()` must return `Some(...)`.
    *   1. Psid: Connected synchronizer's `psid` must match this scheduler's `psid`.
    *   1. Timestamp: `onboardingEffectiveAt` must be the *exact* effective time of the original
    *      `onboarding = true` transaction.
    *   1. Idempotency: Relies on `schedule()` to ensure only one task is scheduled per party. Safe
    *      to be called repeatedly (e.g., during crash recovery) as subsequent calls for the same
    *      party will be ignored if a task is already pending.
    *   1. Persistence (PV35+): For protocol versions 35 and higher, a pending clearance record for
    *      the party must exist in the store. If no record is found, the operation is aborted and
    *      the pending task is actively removed.
    */
  private[participant] def requestClearance(
      partyId: PartyId,
      onboardingEffectiveAt: EffectiveTime,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, PartyOnboardingFlagStatus] =
    for {
      context <- EitherT.fromEither[FutureUnlessShutdown](topologyContext)

      snapshot <- EitherT.liftF(context.topologyClient.currentSnapshotApproximation)
      lsuO <- EitherT.liftF(snapshot.announcedLsu())

      _ = logLsuAnnouncedOnce(lsuO)

      outcome <- lsuO match {
        case Some((_, upgradeTime)) =>
          val safeTime = upgradeTime.immediateSuccessor.value

          logger.debug(
            s"Deferring synchronous clearance for $partyId to the background loop until safe time $safeTime due to active LSU."
          )

          // Graceful Wait: Fulfill the synchronous API polling contract by returning FlagSet.
          // Using the immediateSuccessor of the upgradeTime as the safe time ensures the background timer
          // rests until the upgrade is actually expected to happen, saving resources.
          EitherT.rightT[FutureUnlessShutdown, String](
            FlagSet(safeTime): PartyOnboardingFlagStatus
          )
        case None =>
          for {
            _ <- ensureClearanceRecordExists(partyId).leftMap(_.message)
            res <- topologyWorkflow
              .authorizeClearingOnboardingFlag(
                partyId,
                participantId,
                onboardingEffectiveAt,
                context,
              )
              .leftMap(_.message)
          } yield res
      }
    } yield {
      handleClearanceSuccess(partyId, onboardingEffectiveAt, outcome)
      outcome
    }

  /** Attempts to clear the party's onboarding flag asynchronously in the background. Returns `Unit`
    * immediately. Retries transient failures "forever" without blocking the caller.
    *
    * This method is idempotent. It uses an internal lock to ensure only one background retry loop
    * is active per party at any given time.
    *
    * This method is invoked explicitly in internal contexts:
    *   - During synchronizer reconnect
    *     ([[com.digitalasset.canton.participant.sync.ConnectedSynchronizer.start]]) to recover any
    *     pending clearances.
    *   - Upon processing a `PartyToParticipant` topology transaction where a participant hosts a
    *     party with `onboarding = true`
    *     ([[com.digitalasset.canton.participant.protocol.ParticipantTopologyTerminateProcessing.terminate]]).
    *
    * LSU behavior - "Silent Retry": Unlike the synchronous API, this method does not use a
    * "Graceful Wait". If an LSU is announced, it intentionally returns an internal error to trigger
    * the `Backoff` retry policy. This creates an active background polling loop that "silently"
    * retries (log spam is suppressed) until the LSU is cancelled or completes, ensuring early
    * cancellations are caught promptly.
    *
    * @param partyId
    *   The party whose onboarding flag to clear.
    * @param onboardingEffectiveAt
    *   The exact effective time of the specific `PartyToParticipant` transaction that initially set
    *   `onboarding = true`.
    *
    * @note
    *   Preconditions:
    *   1. Connection: `getConnectedSynchronizer()` must return `Some(...)`.
    *   1. Psid: Connected synchronizer's `psid` must match this scheduler's `psid`.
    *   1. Timestamp: `onboardingEffectiveAt` must be the *exact* effective time of the original
    *      `onboarding = true` transaction.
    *   1. Idempotency: Relies on `schedule()` to ensure only one task is scheduled per party. Safe
    *      to be called repeatedly (e.g., during crash recovery) as subsequent calls for the same
    *      party will be ignored if a task is already pending.
    *   1. Persistence (PV35+): For protocol versions 35 and higher, a pending clearance record for
    *      the party must exist in the store. If no record is found, the operation is aborted and
    *      the pending task is actively removed.
    */
  private[participant] def requestClearanceInBackground(
      partyId: PartyId,
      onboardingEffectiveAt: EffectiveTime,
  )(implicit traceContext: TraceContext): Unit =
    // Idempotency: Only proceed if there is not already an active loop for this party
    if (inFlightBackgroundRequests.putIfAbsent(partyId, ()).isEmpty) {

      logger.info(s"Starting background clearance attempt for party $partyId.")

      val retryPolicy = Backoff(
        logger = logger,
        hasSynchronizeWithClosing = this,
        maxRetries = Int.MaxValue,
        initialDelay = retryInitialDelay,
        maxDelay = retryMaxDelay,
        operationName = s"background-clearance-attempt-$partyId",
      )

      val backgroundTask = retryPolicy.unlessShutdown(
        executeClearanceAttempt(partyId, onboardingEffectiveAt).value,
        OnboardingClearanceScheduler.ClearanceExceptionRetryPolicy,
      )

      // Handle completion and state cleanup
      backgroundTask.unwrap.onComplete { outcome =>
        // Always release the lock once the loop exits, regardless of success or failure
        inFlightBackgroundRequests.remove(partyId).discard

        outcome match {
          case Failure(exception) =>
            logger.warn(
              s"Aborted background clearance retries for $partyId. " +
                s"The onboarding flag is still set. " +
                s"To make the party fully available, manually clear the flag using the participant admin API, reconnect synchronizer $psid, or restart the participant.",
              exception,
            )
          case _ =>
          // Success or terminal error is handled gracefully inside executeClearanceAttempt
        }
      }
    } else {
      // Safely ignore the redundant request
      logger.debug(
        s"A background clearance attempt for $partyId is already running. Ignoring duplicate request."
      )
    }

  /** Core execution logic used by both the initial background request and the delayed trigger.
    *
    * Evaluates to an `EitherT` mapped to a typed `ClearanceAttemptError`. This allows the custom
    * `ClearanceExceptionRetryPolicy` to accurately control the `Backoff` loop's logging and retry
    * behavior based on the specific failure reason.
    *
    * Note: This method intentionally diverges from `requestClearance` when handling an announced
    * LSU. Because the synchronous API needs a "Graceful Wait" (returning `FlagSet`) and the
    * asynchronous loop needs a "Silent Retry" (returning an error), they require two slightly
    * different outcomes from the same LSU condition. Returning
    * `Left(ClearanceAttemptError.LsuAnnounced)` here forces the `Backoff` engine to actively poll
    * in the background, ensuring we catch early LSU cancellations.
    */
  private def executeClearanceAttempt(
      partyId: PartyId,
      onboardingEffectiveAt: EffectiveTime,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, OnboardingClearanceScheduler.ClearanceAttemptError, Unit] = {

    val attempt = for {
      context <- EitherT
        .fromEither[FutureUnlessShutdown](topologyContext)
        .leftMap(err =>
          OnboardingClearanceScheduler.ClearanceAttemptError
            .SynchronizerNotAvailable(err): OnboardingClearanceScheduler.ClearanceAttemptError
        )

      snapshot <- EitherT.liftF(
        context.topologyClient.currentSnapshotApproximation
      )
      lsuO <- EitherT.liftF(snapshot.announcedLsu())

      _ = logLsuAnnouncedOnce(lsuO)

      _ <- lsuO match {
        case Some((successor, upgradeTime)) =>
          // Silent Retry: This triggers the active polling of the Backoff retry policy,
          // ensuring we catch an early cancellation of the LSU.
          EitherT.leftT[FutureUnlessShutdown, Unit](
            OnboardingClearanceScheduler.ClearanceAttemptError.LsuAnnounced(
              successor,
              upgradeTime,
            ): OnboardingClearanceScheduler.ClearanceAttemptError
          )
        case None =>
          // Check the store before allowing the workflow to proceed
          ensureClearanceRecordExists(partyId)
      }

      outcome <- topologyWorkflow
        .authorizeClearingOnboardingFlag(
          partyId,
          participantId,
          onboardingEffectiveAt,
          context,
        )
        .leftMap(err =>
          OnboardingClearanceScheduler.ClearanceAttemptError
            .WorkflowError(err): OnboardingClearanceScheduler.ClearanceAttemptError
        )
    } yield outcome

    attempt
      .map { outcome =>
        // Success: Handle outcome and map to Right(()) to break the retry loop
        handleClearanceSuccess(partyId, onboardingEffectiveAt, outcome)
      }
      .recover {
        case err @ OnboardingClearanceScheduler.ClearanceAttemptError.WorkflowError(
              AuthorizeClearanceError.PartyNotHosted(_)
            ) =>
          logger.info(
            s"Aborting clearance for $partyId: Party is no longer hosted. (${err.message})"
          )
          // Terminal error: Intentionally break the retry loop by recovering to Right(())
          ()
        case OnboardingClearanceScheduler.ClearanceAttemptError.NoPendingClearanceRecord =>
          // Terminal error: Safely break the loop, task was already aborted and removed in `ensureClearanceRecordExists`
          ()
      }
      .leftMap {
        case err @ OnboardingClearanceScheduler.ClearanceAttemptError.LsuAnnounced(_, _) =>
          // Return the pure marker so our ExceptionRetryPolicy catches it and silences the loop log
          err
        case err =>
          // This gracefully catches SynchronizerNotAvailable, general WorkflowErrors, etc.
          logger.debug(s"Authorization failed with transient error, retrying: ${err.message}")
          err // Transient error: Trigger retry normally
      }
  }

  /** Centralized handling of successful clearance outcomes. */
  private def handleClearanceSuccess(
      partyId: PartyId,
      onboardingEffectiveAt: EffectiveTime,
      outcome: PartyOnboardingFlagStatus,
  )(implicit traceContext: TraceContext): Unit =
    outcome match {
      case FlagNotSet =>
        logger.info(
          s"No onboarding flag clearance. Flag is not set for party $partyId on participant $participantId (physical synchronizer: $psid)."
        )
      case FlagSet(safeToClear) =>
        logger.info(
          s"Deferring onboarding flag clearanceTask until $safeToClear for party $partyId (participant: $participantId, physical synchronizer: $psid)."
        )
        schedule(OnboardingClearanceTask(partyId, safeToClear, onboardingEffectiveAt))
    }

  private def schedule(task: OnboardingClearanceTask)(implicit traceContext: TraceContext): Unit = {
    // We use `put` instead of `putIfAbsent` to atomically fetch the previous state while updating.
    val previous = pendingClearances.put(task.partyId, task)
    previous match {
      case None =>
        logger.info(s"Scheduled a new onboarding clearance task for party ${task.partyId}.")
        scheduleTrigger(task)
      case Some(existing) if existing.earliestClearanceTime != task.earliestClearanceTime =>
        logger.info(
          s"Updated existing onboarding clearance task for party ${task.partyId} with new safe time ${task.earliestClearanceTime}."
        )
        scheduleTrigger(task)
      case Some(_) =>
        logger.debug(
          s"An existing onboarding clearance task was found for party ${task.partyId} with the same safe time. Not scheduling a new trigger."
        )
    }
  }

  /** Schedules a time-based trigger to attempt the clearance. If the component begins shutting down
    * before the tick arrives, the trigger is safely ignored.
    */
  private def scheduleTrigger(
      task: OnboardingClearanceTask
  )(implicit traceContext: TraceContext): Unit =
    topologyContext match {
      case Left(error) =>
        logger.info(
          s"Failed scheduling clearance trigger for party ${task.partyId} because $error. " +
            s"If the flag does not clear automatically, manually clear it using the participant admin API, reconnect synchronizer $psid, or restart the participant."
        )

      case Right(context) =>
        // The trigger relies strictly on the earliestClearanceTime provided by the task.
        val triggerTime = task.earliestClearanceTime

        val tickFuture =
          context.timeTracker.awaitTick(triggerTime).getOrElse {
            logger.info(
              s"Time $triggerTime for onboarding clearance task for party ${task.partyId} is already in the past. " +
                s"Triggering immediately."
            )
            Future.unit
          }

        tickFuture.onComplete {
          case Success(_) =>
            if (isClosing) {
              logger.info(s"Skipping clearance trigger for party ${task.partyId} due to shutdown.")
            } else {
              // Time has come, attempt to trigger the clearance
              triggerClearanceAttempt(task)
            }
          case Failure(e) =>
            logger.warn(
              s"Failed to await tick for onboarding clearance task (party: ${task.partyId}). " +
                s"If the flag does not clear automatically, manually clear it using the participant admin API, reconnect synchronizer $psid, or restart the participant.",
              e,
            )
        }
    }

  /** This method is called once the `earliestClearanceTime` for a task has been reached. It
    * attempts to propose the clearance transaction, utilizing an infinite background retry loop to
    * survive transient disruptions. If the component shuts down during the retries, the outcome
    * processing is safely bypassed.
    *
    * It does *not* remove the task; removal is handled by `observed`.
    */
  private def triggerClearanceAttempt(task: OnboardingClearanceTask)(implicit
      traceContext: TraceContext
  ): Unit =
    // Check if the task is still pending and ensure it has not been updated with a new time.
    // If the time does not match, this trigger is stale (e.g., LSU was cancelled and time was updated).
    pendingClearances.get(task.partyId) match {
      case Some(currentTask) if currentTask.earliestClearanceTime == task.earliestClearanceTime =>
        // Ensure the background reconnect hook has not already started a loop for this party
        if (inFlightBackgroundRequests.putIfAbsent(task.partyId, ()).isEmpty) {
          logger.info(
            s"Safe time for party ${task.partyId} reached. Proposing topology transaction to clear onboarding flag in the background."
          )

          val retryPolicy = Backoff(
            logger = logger,
            hasSynchronizeWithClosing = this,
            maxRetries = Int.MaxValue,
            initialDelay = retryInitialDelay,
            maxDelay = retryMaxDelay,
            operationName = s"trigger-clearance-attempt-${task.partyId}",
          )

          val retryTask = retryPolicy.unlessShutdown(
            executeClearanceAttempt(task.partyId, task.onboardingEffectiveAt).value,
            OnboardingClearanceScheduler.ClearanceExceptionRetryPolicy,
          )

          retryTask.unwrap.onComplete { outcome =>
            // Always release the lock once the loop exits
            inFlightBackgroundRequests.remove(task.partyId).discard

            outcome match {
              case Failure(exception) =>
                logger.warn(
                  s"Aborted scheduled clearance retries for ${task.partyId}. " +
                    s"The onboarding flag is still set. " +
                    s"To make the party fully available, manually clear the flag using the participant admin API, reconnect synchronizer $psid, or restart the participant.",
                  exception,
                )
              case _ =>
            }
          }
        } else {
          logger.debug(
            s"Clearance trigger for party ${task.partyId} fired, but a background loop is already actively processing it. Yielding."
          )
        }

      case Some(currentTask) =>
        logger.info(
          s"Clearance trigger for party ${task.partyId} fired, but task was updated to a new time (${currentTask.earliestClearanceTime}). Ignoring this stale trigger."
        )

      case None =>
        logger.info(
          s"Onboarding flag clearance trigger for party ${task.partyId} fired, but task was already removed."
        )
    }

  /** This must be called whenever a topology transaction is committed. It inspects transactions as
    * they become effective and removes any matching tasks from `pendingClearances`.
    *
    * This method must be **idempotent**, as it may be re-called with the same transactions during
    * crash recovery. The current implementation is safe because removing items from a `TrieMap`
    * (`--=`) is an idempotent operation.
    *
    * Heavy operations like the removal of pending onboarding flag clearance operations from the
    * persistence are deferred to other components intentionally (keep this code path light-weight).
    */
  override def observed(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sequencerCounter: SequencerCounter,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.delegate {
      if (pendingClearances.nonEmpty) {
        // Find all parties that are either cleared or removed
        val partiesToRemove = transactions.iterator
          .filterNot(_.isProposal)
          .flatMap { signedTx =>
            val op = signedTx.transaction.operation
            signedTx.transaction.mapping.select[PartyToParticipant].flatMap { ptp =>
              val hosting = ptp.participants.find(_.participantId == this.participantId)

              // Case 1: Clearance (Replace op, participant is host, onboarding is false)
              val isClearance = op == TopologyChangeOp.Replace && hosting.exists(!_.onboarding)

              // Case 2: Offboarding (Remove op, participant was a host)
              val isOffboarding = op == TopologyChangeOp.Remove && hosting.isDefined

              // Case 3: Eviction (Replace op, participant is no longer a host)
              val isEviction = op == TopologyChangeOp.Replace && hosting.isEmpty

              Option.when(
                pendingClearances
                  .contains(ptp.partyId) && (isClearance || isOffboarding || isEviction)
              )(ptp.partyId)
            }
          }
          .toSet

        partiesToRemove.foreach { party =>
          logger.info(
            s"Detected effective clearance or offboarding for party $party (at $effectiveTimestamp). Removing pending clearance task."
          )
        }

        pendingClearances --= partiesToRemove
      }

      FutureUnlessShutdown.unit
    }
}

object OnboardingClearanceScheduler {
  // Retry defaults are chosen to avoid spamming the topology
  val DefaultRetryInitialDelay: FiniteDuration = 10.seconds
  val DefaultRetryMaxDelay: FiniteDuration = 60.seconds

  sealed trait ClearanceAttemptError {
    def message: String
  }
  object ClearanceAttemptError {
    final case class LsuAnnounced(successor: SynchronizerSuccessor, upgradeTime: EffectiveTime)
        extends ClearanceAttemptError {
      override def message: String =
        s"Synchronizer upgrade announced for $successor at $upgradeTime"
    }
    final case object NoPendingClearanceRecord extends ClearanceAttemptError {
      override def message: String = "No pending clearance record found"
    }
    final case class WorkflowError(error: AuthorizeClearanceError) extends ClearanceAttemptError {
      override def message: String = error.message
    }
    final case class SynchronizerNotAvailable(reason: String) extends ClearanceAttemptError {
      override def message: String = reason
    }
  }

  /** Custom retry policy that delegates to the default, but suppresses log spam for normal LSU
    * background operations.
    */
  val ClearanceExceptionRetryPolicy: ExceptionRetryPolicy = new ExceptionRetryPolicy {
    override protected def determineExceptionErrorKind(
        exception: Throwable,
        logger: TracedLogger,
    )(implicit tc: TraceContext): ErrorKind =
      ErrorKind.UnknownErrorKind

    override def retryLogLevel(outcome: Try[Any]): Option[Level] = outcome match {
      case Success(Left(ClearanceAttemptError.LsuAnnounced(_, _))) =>
        // By returning TRACE/DEBUG here, we instruct Canton's RetryWithDelay engine
        // to not emit the "Retrying after X delay" INFO/WARN messages for this expected condition.
        Some(Level.DEBUG)
      case _ =>
        super.retryLogLevel(outcome)
    }
  }
}
