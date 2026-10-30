// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.*
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.digitalasset.canton.config.{
  NonNegativeFiniteDuration,
  ProcessingTimeout,
  ReassignmentsConfig,
}
import com.digitalasset.canton.crypto.PseudoRandom
import com.digitalasset.canton.data.{CantonTimestamp, ReassignmentSubmitterMetadata}
import com.digitalasset.canton.error.CantonBaseError.isStatusErrorCode
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentValidation.NoReassignmentData
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessorError.AutomaticAssignmentError
import com.digitalasset.canton.participant.store.ReassignmentStore.ReassignmentCompleted
import com.digitalasset.canton.platform.apiserver.services.command.SubmissionAccounting
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.ReassignmentTag.Target
import com.digitalasset.canton.util.retry.{Backoff, NoExceptionRetryPolicy, Success}
import com.digitalasset.canton.{LedgerSubmissionId, LfPartyId}
import com.google.rpc.Code
import com.google.rpc.status.Status
import org.slf4j.event.Level

import java.time.Duration as JDuration
import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

private[participant] object AutomaticAssignment {

  /** Returns a duration drawn uniformly at random in [delay, 2 * delay), or zero if delay is zero.
    */
  private[reassignment] def randomizedDelay(delay: NonNegativeFiniteDuration): JDuration = {
    val delayJ = delay.asJava
    if (delayJ.toMillis > 0) delayJ.plusMillis(PseudoRandom.randomLong(delayJ.toMillis))
    else delayJ
  }

  def perform(
      id: ReassignmentId,
      targetSynchronizer: Target[PhysicalSynchronizerId],
      targetStaticSynchronizerParameters: Target[StaticSynchronizerParameters],
      reassignmentCoordination: ReassignmentCoordination,
      stakeholders: Set[LfPartyId],
      unassignmentSubmitterMetadata: ReassignmentSubmitterMetadata,
      participantId: ParticipantId,
      targetTimestamp: Target[CantonTimestamp],
      unassignmentTs: CantonTimestamp,
      reassignmentsConfig: ReassignmentsConfig,
      clock: Clock,
  )(implicit
      ec: ExecutionContext,
      elc: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = {
    val logger = elc
    implicit val traceContext: TraceContext = elc.traceContext

    def hostedStakeholders(
        snapshot: Target[TopologySnapshot]
    ): FutureUnlessShutdown[Set[LfPartyId]] =
      snapshot.unwrap
        .hostedOn(stakeholders, participantId)
        .map(partiesWithAttributes =>
          partiesWithAttributes.collect {
            case (partyId, attributes)
                if attributes.permission == ParticipantPermission.Submission =>
              partyId
          }.toSet
        )

    def performAutoAssignmentOnce: EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
      for {
        targetTopology <- reassignmentCoordination
          .getTargetApproximateSnapshot(targetSynchronizer)
        possibleSubmittingParties <- EitherT.right(hostedStakeholders(targetTopology))
        assignmentSubmitter <- EitherT.fromOption[FutureUnlessShutdown](
          possibleSubmittingParties.headOption,
          AutomaticAssignmentError("No possible submitting party for automatic assignment"),
        )
        submissionResult <- reassignmentCoordination
          .assign(
            targetSynchronizer,
            ReassignmentSubmitterMetadata(
              assignmentSubmitter,
              participantId,
              unassignmentSubmitterMetadata.commandId,
              submissionId = Some(LedgerSubmissionId.assertFromString(UUID.randomUUID().toString)),
              unassignmentSubmitterMetadata.userId,
              workflowId = None,
            ),
            id,
            targetTopology,
            SubmissionAccounting.participantAdmin(participantId.adminParty.toLf),
          )
          .mapK(FutureUnlessShutdown.outcomeK)
        AssignmentProcessingSteps.SubmissionResult(completionF) = submissionResult
        status <- EitherT.right(completionF).mapK(FutureUnlessShutdown.outcomeK)
        _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
          status.code == Code.OK_VALUE,
          AssignmentFailed(status): ReassignmentProcessorError,
        )
      } yield ()

    final case class AssignmentFailed(status: Status) extends ReassignmentProcessorError {
      def message = s"Assignment of $id failed due to: $status"
    }

    def performAutoAssignmentRepeatedly
        : EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = {
      implicit val StopRetry: Success[Either[ReassignmentProcessorError, Unit]] = Success {
        case Left(e: AssignmentFailed) => !isStatusErrorCode(MediatorError.Timeout, e.status)
        case _ => true
      }

      case object SyncWithClosing extends FlagCloseable with HasCloseContext {
        override val timeouts: ProcessingTimeout = ProcessingTimeout()
        override val logger: TracedLogger = elc.logger
      }

      val retry = Backoff(
        elc.logger,
        hasSynchronizeWithClosing = SyncWithClosing,
        maxRetries = 5,
        initialDelay = 1.second,
        maxDelay = 10.seconds,
        operationName = s"automatic assignment of $id",
      )
      EitherT(retry.unlessShutdown(performAutoAssignmentOnce.value, NoExceptionRetryPolicy))
    }

    def triggerAutoAssignment(
        targetSnapshot: Target[TopologySnapshot],
        awaitSubmissionTime: => EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit],
    ): Unit = {

      val autoAssignment = for {
        targetHostedStakeholders <- EitherT.right(hostedStakeholders(targetSnapshot))
        _ <-
          if (targetHostedStakeholders.nonEmpty) {
            for {
              _ <- awaitSubmissionTime

              _ <- EitherTUtil.leftSubflatMap(performAutoAssignmentRepeatedly) {
                // Filter out submission errors occurring because the reassignment is already completed
                case NoReassignmentData(_, ReassignmentCompleted(_, _)) =>
                  Either.unit
                // Filter out the case that the participant has disconnected from the target synchronizer in the meantime.
                case UnknownPhysicalSynchronizer(synchronizer, _)
                    if synchronizer == targetSynchronizer.unwrap =>
                  Either.unit
                case SynchronizerNotReady(synchronizer, _)
                    if synchronizer == targetSynchronizer.unwrap =>
                  Either.unit
                // Filter out the case that the target synchronizer is closing right now
                case other => Left(other)
              }
            } yield ()
          } else EitherT.pure[FutureUnlessShutdown, ReassignmentProcessorError](())
      } yield ()

      EitherTUtil.doNotAwaitUS(autoAssignment, "Automatic assignment failed", Level.INFO)
    }

    def triggerAfterExclusivityTimeout
        : EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = for {
      // targetTimestamp is provided by the unassignment submitter and may be ahead of the target
      // topology observed locally, so wait for it before taking the snapshot
      _ <- reassignmentCoordination.awaitTimestamp(
        targetSynchronizer,
        targetStaticSynchronizerParameters,
        targetTimestamp,
        FutureUnlessShutdown.unit,
      )
      targetIps <- reassignmentCoordination
        .cryptoSnapshot(
          targetSynchronizer,
          targetStaticSynchronizerParameters,
          targetTimestamp,
        )

      targetSnapshot = targetIps.map(_.ipsSnapshot)

      targetSynchronizerParameters <- EitherT(
        targetSnapshot
          .traverse(
            _.findDynamicSynchronizerParameters()
              .map(_.leftMap(SynchronizerNotReady(targetSynchronizer.unwrap, _)))
          )
          .map(_.sequence)
      ).leftWiden[ReassignmentProcessorError]
    } yield {

      if (targetSynchronizerParameters.unwrap.automaticAssignmentEnabled)
        triggerAutoAssignment(
          targetSnapshot,
          for {
            exclusivityLimit <- EitherT
              .fromEither[FutureUnlessShutdown](
                targetSynchronizerParameters.unwrap
                  .assignmentExclusivityLimitFor(targetTimestamp.unwrap)
                  .map(Target(_))
                  .leftMap(ReassignmentParametersError(targetSynchronizer.unwrap, _))
              )
              .leftWiden[ReassignmentProcessorError]
            _ = logger.info(
              s"Registering automatic submission of assignment with ID $id at time $exclusivityLimit, where base timestamp is $targetTimestamp"
            )
            _ <- reassignmentCoordination
              .awaitSynchronizerTime(targetSynchronizer, exclusivityLimit)
              .mapK(FutureUnlessShutdown.outcomeK)
              .leftWiden[ReassignmentProcessorError]
            _ <- reassignmentCoordination
              .awaitTimestamp(
                targetSynchronizer,
                targetStaticSynchronizerParameters,
                exclusivityLimit,
                FutureUnlessShutdown.pure(
                  logger.debug(s"Automatic assignment triggered immediately")
                ),
              )
              .leftWiden[ReassignmentProcessorError]
          } yield (),
        )
      else ()
    }

    def triggerAfterDelay: EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
      reassignmentCoordination
        .getTargetApproximateSnapshot(targetSynchronizer)
        .leftWiden[ReassignmentProcessorError]
        .map { targetSnapshot =>
          triggerAutoAssignment(
            targetSnapshot, {
              val delay = randomizedDelay(reassignmentsConfig.automaticAssignmentDelay)
              val submissionTime = unassignmentTs.plus(delay)
              logger.info(
                s"Scheduling automatic assignment of $id at $submissionTime ($delay after the unassignment at $unassignmentTs)"
              )
              EitherT.right[ReassignmentProcessorError](
                clock.scheduleAt(_ => (), s"automatic assignment of $id", submissionTime)
              )
            },
          )
        }

    if (ReassignmentValidation.isAssignmentExclusivityEnforced(targetSynchronizer.unwrap))
      triggerAfterExclusivityTimeout
    else if (reassignmentsConfig.enableAutomaticAssignment) triggerAfterDelay
    else EitherT.pure[FutureUnlessShutdown, ReassignmentProcessorError](())
  }
}
