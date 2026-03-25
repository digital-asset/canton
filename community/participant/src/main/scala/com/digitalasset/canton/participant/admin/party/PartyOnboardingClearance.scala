// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.data.{
  FlagNotSet,
  FlagSet,
  PartyOnboardingFlagStatus,
}
import com.digitalasset.canton.participant.sync.ConnectedSynchronizer
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
import com.digitalasset.canton.util.retry.{AllExceptionRetryPolicy, Backoff}
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Holds all necessary information to perform an onboarding clearance.
  */
private[admin] final case class OnboardingClearanceTask(
    partyId: PartyId,
    earliestClearanceTime: CantonTimestamp,
    onboardingEffectiveAt: EffectiveTime,
)

/** Schedules and executes party onboarding clearance tasks.
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
  *   1. On node restart or synchronizer reconnection, the `ConnectedSynchronizer` is responsible
  *      for re-submitting pending clearances to this scheduler to ensure durability.
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
    psid: PhysicalSynchronizerId,
    getConnectedSynchronizer: () => Option[ConnectedSynchronizer],
    protected val loggerFactory: NamedLoggerFactory,
    topologyWorkflow: PartyReplicationTopologyWorkflow,
    override protected val timeouts: ProcessingTimeout,
    retryInitialDelay: FiniteDuration = 1.second,
    retryMaxDelay: FiniteDuration = 10.seconds,
)(implicit ec: ExecutionContext)
    extends TopologyTransactionProcessingSubscriber
    with FlagCloseable
    with NamedLogging {

  @VisibleForTesting
  private[party] val pendingClearances: TrieMap[PartyId, OnboardingClearanceTask] = TrieMap.empty

  /** Helper to consistently extract and validate the active synchronizer, eliminating nested
    * checks. This explicitly enforces Preconditions 1 & 2 defined in `requestClearance`.
    */
  private def activeSynchronizer(contextMsg: String): Either[String, ConnectedSynchronizer] =
    getConnectedSynchronizer()
      .toRight(s"Synchronizer connection is not ready (absent): $contextMsg")
      .flatMap(sync =>
        Either.cond(
          sync.psid == psid,
          sync,
          s"Psid mismatch: Expected $psid, got ${sync.psid} for $contextMsg",
        )
      )

  /** Attempts to clear the party's onboarding flag.
    *
    * If the flag is not cleared immediately, schedules a deferred `OnboardingClearanceTask` via
    * `schedule()`. This method is invoked explicitly in the following contexts:
    *   - When calling the clear party onboarding flag endpoint
    *     ([[com.digitalasset.canton.participant.admin.grpc.GrpcPartyManagementService.clearPartyOnboardingFlag]]).
    *   - During synchronizer reconnect
    *     ([[com.digitalasset.canton.participant.sync.ConnectedSynchronizer.start]]) to recover any
    *     pending clearances.
    *   - Upon processing a PartyToParticipant topology transaction with a participant hosting with
    *     its onboarding flag set to true.
    *     ([[com.digitalasset.canton.participant.protocol.ParticipantTopologyTerminateProcessing.terminate]]).
    *
    * @param partyId
    *   The party whose onboarding flag to clear.
    * @param onboardingEffectiveAt
    *   Effective time of the original transaction that set the `onboarding = true` flag.
    * @param maxInitialRetries
    *   The number of times to retry the initial authorization attempt in case of transient errors.
    *   The appropriate value depends on the calling context:
    *   - `0` for the gRPC endpoint: Synchronous, interactive call. Fails fast so the client can
    *     manually retry.
    *   - `> 0` (e.g., 3) for `ConnectedSynchronizer`: Asynchronous, background recovery. Because
    *     falling back to a manual recovery (by calling the endpoint) should be the last resort.
    *   - `1` for `ParticipantTopologyTerminateProcessing`: Synchronous step in the event pipeline.
    *     Needs slight resilience against minor blips, but should not hang the pipeline
    *     indefinitely.
    * @return
    *   A `FutureUnlessShutdown` containing:
    *   - `Right(FlagNotSet)` if flag is/was already clear.
    *   - `Right(FlagSet(safeTime))` if flag is set; task scheduled for `safeTime`.
    *   - `Left(String)` on precondition violation or check error.
    * @note
    *   Preconditions:
    *   1. Connection: `getConnectedSynchronizer()` must return `Some(...)`.
    *   1. Psid: Connected synchronizer's `psid` must match this scheduler's `psid`.
    *   1. Timestamp: `onboardingEffectiveAt` must be the *exact* effective time of the original
    *      `onboarding = true` transaction.
    *   1. Idempotency: Relies on `schedule()` to ensure only one task is scheduled per party. Safe
    *      to be called repeatedly (e.g., during crash recovery) as subsequent calls for the same
    *      party will be ignored if a task is already pending.
    */
  private[participant] def requestClearance(
      partyId: PartyId,
      onboardingEffectiveAt: EffectiveTime,
      maxInitialRetries: NonNegativeInt = NonNegativeInt.zero,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, PartyOnboardingFlagStatus] =
    for {
      sync <- EitherT.fromEither[FutureUnlessShutdown](
        activeSynchronizer(
          s"Onboarding flag clearance request for $psid (party: $partyId, participant: $participantId)."
        )
      )

      // Attempt to clear the onboarding flag as initial step
      outcome <-
        if (maxInitialRetries.unwrap > 0) {
          val retryPolicy = Backoff(
            logger = logger,
            hasSynchronizeWithClosing = sync,
            maxRetries = maxInitialRetries.value,
            initialDelay = retryInitialDelay,
            maxDelay = retryMaxDelay,
            operationName = s"initial-clearance-attempt-$partyId",
          )

          // Because of `import Success.either`, a Left(...) will automatically trigger a retry!
          val retryTask = retryPolicy.unlessShutdown(
            topologyWorkflow
              .authorizeClearingOnboardingFlag(
                partyId,
                participantId,
                onboardingEffectiveAt,
                sync,
              )
              .value,
            AllExceptionRetryPolicy,
          )

          EitherT(retryTask)
        } else {
          // Execution without retries (suitable when the client code already contains some retry mechanism)
          topologyWorkflow.authorizeClearingOnboardingFlag(
            partyId,
            participantId,
            onboardingEffectiveAt,
            sync,
          )
        }
    } yield {
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
      outcome
    }

  private def schedule(task: OnboardingClearanceTask)(implicit traceContext: TraceContext): Unit =
    // Only schedule the trigger if the current invocation *put* the task
    if (pendingClearances.putIfAbsent(task.partyId, task).isEmpty) {
      logger.info(s"Scheduled a new onboarding clearance task for party ${task.partyId}.")
      scheduleTrigger(task)
    } else {
      logger.info(
        s"An existing onboarding clearance task was found for party ${task.partyId}. Not scheduling a new one."
      )
    }

  /** Schedules a time-based trigger to attempt the clearance. If the component begins shutting down
    * before the tick arrives, the trigger is safely ignored.
    */
  private def scheduleTrigger(
      task: OnboardingClearanceTask
  )(implicit traceContext: TraceContext): Unit =
    activeSynchronizer(s"schedule trigger for party ${task.partyId}") match {
      case Left(error) =>
        logger.error(s"Cannot schedule trigger for party ${task.partyId}: $error")

      case Right(sync) =>
        val triggerTime = task.earliestClearanceTime.immediateSuccessor

        val tickFuture = sync.ephemeral.timeTracker.awaitTick(triggerTime).getOrElse {
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
              s"Failed to await tick for onboarding clearance task (party: ${task.partyId}).",
              e,
            )
        }
    }

  /** This method is called once the `earliestClearanceTime` for a task has been reached. It
    * attempts to propose the clearance transaction, utilizing a background retry policy (up to 3
    * times) to survive transient disruptions. If the component shuts down during the retries, the
    * outcome processing is safely bypassed.
    *
    * It does *not* remove the task; removal is handled by `observed`.
    */
  private def triggerClearanceAttempt(task: OnboardingClearanceTask)(implicit
      traceContext: TraceContext
  ): Unit =
    // Check if the task is still pending. It might have been cleared by `observed`
    // between when `schedule` was called and when this trigger fired.
    if (pendingClearances.contains(task.partyId)) {
      activeSynchronizer(s"trigger clearance for party ${task.partyId}") match {
        case Left(error) =>
          logger.error(
            s"Cannot trigger clearance for party ${task.partyId}: $error Task will remain pending for manual retry."
          )

        case Right(sync) =>
          logger.info(
            s"Safe time for party ${task.partyId} reached. Proposing topology transaction to clear onboarding flag."
          )

          // Because of `import Success.either`, a Left(...) will automatically trigger a retry!
          val retryPolicy = Backoff(
            logger = logger,
            hasSynchronizeWithClosing = sync,
            maxRetries = 3,
            initialDelay = retryInitialDelay,
            maxDelay = retryMaxDelay,
            operationName = s"trigger-clearance-attempt-${task.partyId}",
          )

          val retryTask = retryPolicy.unlessShutdown(
            topologyWorkflow
              .authorizeClearingOnboardingFlag(
                task.partyId,
                participantId,
                task.onboardingEffectiveAt,
                sync,
              )
              .value,
            AllExceptionRetryPolicy,
          )

          // Handle the ultimate outcome after all retries are exhausted (or succeed)
          retryTask.unwrap.onComplete { result =>
            if (isClosing) {
              logger.debug(
                s"Skipping outcome processing for party ${task.partyId} due to shutdown."
              )
            } else {
              logger.debug(s"handle-clearance-outcome-${task.partyId}")
              result match {

                case Success(UnlessShutdown.Outcome(Right(FlagNotSet))) =>
                  logger.info(
                    s"Onboarding flag for party ${task.partyId} was already clear or cleared immediately upon attempt."
                  )
                case Success(UnlessShutdown.Outcome(Right(FlagSet(_)))) =>
                  logger.info(
                    s"Onboarding flag clearance attempt for party ${task.partyId} submitted. Waiting for it to become effective."
                  )
                case Success(UnlessShutdown.Outcome(Left(error))) =>
                  logger.error(
                    s"Onboarding flag clearance attempt for party ${task.partyId} failed after exhausting retries: $error. Task will remain pending for manual retry."
                  )
                case Success(UnlessShutdown.AbortedDueToShutdown) =>
                  logger.info(
                    s"Onboarding flag clearance attempt for ${task.partyId} was aborted due to shutdown."
                  )
                case Failure(e) =>
                  logger.error(
                    s"Onboarding flag clearance attempt for ${task.partyId} failed with an exception.",
                    e,
                  )
              }
            }
          }
      }
    } else {
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

              Option.when(isClearance || isOffboarding)(ptp.partyId)
            }
          }
          .toSet

        partiesToRemove.foreach { party =>
          if (pendingClearances.contains(party)) {
            logger.info(
              s"Detected effective clearance or offboarding for party $party (at $effectiveTimestamp). Removing pending clearance task."
            )
          }
        }

        pendingClearances --= partiesToRemove
      }

      FutureUnlessShutdown.unit
    }
}
