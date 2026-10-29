// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import cats.syntax.parallel.*
import com.digitalasset.canton.LfSubmissionId
import com.digitalasset.canton.data.ReassignmentSubmitterMetadata
import com.digitalasset.canton.error.TransactionRoutingError
import com.digitalasset.canton.error.TransactionRoutingError.AutomaticReassignmentForTransactionFailure
import com.digitalasset.canton.ledger.participant.state.{
  RoutingSynchronizerState,
  SubmitterInfo,
  SynchronizerRank,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.sync.ConnectedSynchronizersLookup
import com.digitalasset.canton.platform.apiserver.services.command.SubmissionAccounting
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

private[routing] class ContractsReassigner(
    connectedSynchronizers: ConnectedSynchronizersLookup,
    submittingParticipant: ParticipantId,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {
  def reassign(
      synchronizerRankTarget: SynchronizerRank,
      submitterInfo: SubmitterInfo,
      synchronizerState: RoutingSynchronizerState,
      accounting: SubmissionAccounting,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, Unit] =
    if (synchronizerRankTarget.reassignments.nonEmpty) {
      val targetPsid = synchronizerRankTarget.synchronizerId
      logger.info(
        s"Automatic transaction reassignment to synchronizer $targetPsid"
      )

      for {
        targetTopology <- EitherT.fromEither[FutureUnlessShutdown](
          synchronizerState.getTopologySnapshotFor(targetPsid)
        )

        // This should not fire because LAPI ensures that submission id is non-empty:
        // if empty, a UUID is generated
        submissionId <- EitherT.fromEither(
          submitterInfo.submissionId.toRight(
            TransactionRoutingError.MalformedInputErrors.InvalidSubmissionId
              .Error(submissionId = "", issue = "Submission id must not be empty")
          )
        )

        /*
          The submission id of the automatic reassignments is:
            submissionId-(a|u)random
          where
            submissionId is the submissionId of the command
            a|u: a assignment, u for unassignment
            random is a UUID without the - (the same uuid is used for the unassignment and the assignment)
          To ensure that the whole string can fit into a submission id (255 characters), we check that the provided
          submission id as length at most 221 = 255 - 32 - 1 - 1.
         */
        _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
          submissionId.length <= 221,
          TransactionRoutingError.MalformedInputErrors.InvalidSubmissionId.Error(
            submissionId = submissionId,
            issue =
              "The length of the submission id of a command requiring automatic reassignments should be at most 221 characters.",
          ),
        )

        // The source topology is read from a map, not recomputed. Each batch only submits its
        // reassignment.
        _ <- synchronizerRankTarget.reassignments.toSeq
          .parTraverse_ { case ((lfParty, sourceSynchronizerId, _), cids) =>
            val randomSuffix = UUID.randomUUID().toString.replace("-", "")

            def reassignmentSubmitterMetadata(isUnassigned: Boolean) = {
              val kind = if (isUnassigned) "u" else "a"

              ReassignmentSubmitterMetadata(
                submitter = lfParty,
                submittingParticipant = submittingParticipant,
                commandId = submitterInfo.commandId,
                submissionId = Some(
                  LfSubmissionId.assertFromString(
                    s"$submissionId-$kind$randomSuffix"
                  )
                ),
                userId = submitterInfo.userId,
                workflowId = None,
              )
            }

            for {
              sourceTopology <- EitherT.fromEither[FutureUnlessShutdown](
                synchronizerState.getTopologySnapshotFor(sourceSynchronizerId)
              )

              _ <- perform(
                Source(sourceSynchronizerId),
                Source(sourceTopology),
                Target(targetPsid),
                Target(targetTopology),
                Source(reassignmentSubmitterMetadata(isUnassigned = true)),
                Target(reassignmentSubmitterMetadata(isUnassigned = false)),
                cids.toSeq,
                accounting,
              )
                .mapK(FutureUnlessShutdown.outcomeK)
            } yield ()
          }
      } yield ()
    } else {
      EitherT.pure[FutureUnlessShutdown, TransactionRoutingError](())
    }

  private def perform(
      sourceSynchronizerId: Source[PhysicalSynchronizerId],
      sourceTopology: Source[TopologySnapshot],
      targetSynchronizerId: Target[PhysicalSynchronizerId],
      targetTopology: Target[TopologySnapshot],
      submitterUnassignmentMetadata: Source[ReassignmentSubmitterMetadata],
      submitterAssignmentMetadata: Target[ReassignmentSubmitterMetadata],
      contractIds: Seq[LfContractId],
      accounting: SubmissionAccounting,
  )(implicit traceContext: TraceContext): EitherT[Future, TransactionRoutingError, Unit] = {
    val reassignment = for {
      sourceSynchronizer <- EitherT.fromEither[Future](
        connectedSynchronizers
          .get(sourceSynchronizerId.unwrap)
          .toRight("Not connected to the source synchronizer")
      )

      targetSynchronizer <- EitherT.fromEither[Future](
        connectedSynchronizers
          .get(targetSynchronizerId.unwrap)
          .toRight("Not connected to the target synchronizer")
      )

      _unit <- EitherT
        .cond[Future](
          sourceSynchronizer.ready,
          (),
          "The source synchronizer is not ready for submissions",
        )

      unassignmentResult <- sourceSynchronizer
        .submitUnassignments(
          submitterUnassignmentMetadata.value,
          contractIds,
          targetSynchronizerId,
          sourceTopology,
          overrideSourceValidationPkgIds = Map.empty,
          overrideTargetValidationPkgIds = Map.empty,
          accounting,
        )
        .mapK(FutureUnlessShutdown.outcomeK)
        .semiflatMap(Predef.identity)
        .leftMap(_.toString)
        .onShutdown(Left("Application is shutting down"))
      unassignmentStatus <- EitherT.right[String](unassignmentResult.unassignmentCompletionF)
      _unassignmentApprove <- EitherT.cond[Future](
        unassignmentStatus.code == com.google.rpc.Code.OK_VALUE,
        (),
        s"The unassignment for ${unassignmentResult.reassignmentId} failed with status $unassignmentStatus",
      )

      _unit <- EitherT
        .cond[Future](
          targetSynchronizer.ready,
          (),
          "The target synchronizer is not ready for submission",
        )

      assignmentResult <- targetSynchronizer
        .submitAssignments(
          submitterAssignmentMetadata.value,
          unassignmentResult.reassignmentId,
          targetTopology,
          accounting,
        )
        .leftMap[String](err => s"Assignment failed with error $err")
        .flatMap { s =>
          EitherT(s.map(Right(_)).onShutdown(Left("Application is shutting down")))
        }

      assignmentStatus <- EitherT.right[String](assignmentResult.assignmentCompletionF)
      _assignmentApprove <- EitherT.cond[Future](
        assignmentStatus.code == com.google.rpc.Code.OK_VALUE,
        (),
        s"The assignment for ${unassignmentResult.reassignmentId} failed with verdict $assignmentStatus",
      )
    } yield ()

    reassignment.leftMap[TransactionRoutingError](str =>
      AutomaticReassignmentForTransactionFailure.Failed(str)
    )
  }
}
