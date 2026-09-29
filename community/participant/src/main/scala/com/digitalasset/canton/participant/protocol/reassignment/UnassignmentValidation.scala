// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.*
import cats.syntax.functor.*
import com.digitalasset.canton.data.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.participant.metrics.ReassignmentMetrics
import com.digitalasset.canton.participant.protocol.ProcessingSteps
import com.digitalasset.canton.participant.protocol.conflictdetection.ActivenessResult
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentValidation.{
  CommonUnassignmentValidator,
  ReassigningParticipantUnassignmentValidator,
  ReassigningParticipantValidation,
  ValidationErrorOr,
}
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentValidationResult.ReassigningParticipantValidationResult
import com.digitalasset.canton.participant.protocol.validation.AuthenticationValidator
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ContractValidator
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}

import scala.concurrent.ExecutionContext

private[reassignment] class UnassignmentValidation(
    participantId: ParticipantId,
    contractValidator: ContractValidator,
    getTopologyAtTs: GetTopologyAtTimestamp,
    reassignmentMetrics: ReassignmentMetrics,
)(implicit val ec: ExecutionContext, traceContext: TraceContext) {

  def perform(
      parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree],
      activenessF: FutureUnlessShutdown[ActivenessResult],
  ): ValidationErrorOr[UnassignmentValidationResult] = {
    val isReassigningParticipant =
      parsedRequest.fullViewTree.isReassigningParticipant(participantId)

    for {
      commonValidationResult <- new CommonUnassignmentValidator(activenessF, contractValidator)
        .performValidation(
          parsedRequest
        )
      hostedConfirmingParties <- EitherT.right[ReassignmentProcessorError](
        parsedRequest.snapshot.ipsSnapshot
          .canConfirm(participantId, parsedRequest.fullViewTree.confirmingParties)
      )
      reassignmentValidation <-
        if (isReassigningParticipant)
          new ReassigningParticipantUnassignmentValidator(
            participantId,
            contractValidator,
            getTopologyAtTs,
            reassignmentMetrics,
          ).performValidations(
            parsedRequest
          )
        else
          EitherT.right[ReassignmentProcessorError](
            FutureUnlessShutdown.pure(
              ReassigningParticipantValidation(
                assignmentExclusivity = None,
                reassigningParticipantValidationResult = UnassignmentValidationResult
                  .ReassigningParticipantValidationResult(EitherT.pure(()), Nil),
              )
            )
          )
    } yield UnassignmentValidationResult(
      unassignmentData =
        UnassignmentData(parsedRequest.fullViewTree, parsedRequest.requestTimestamp),
      rootHash = parsedRequest.rootHash,
      hostedConfirmingParties = hostedConfirmingParties,
      isReassigningParticipant = isReassigningParticipant,
      assignmentExclusivity = reassignmentValidation.assignmentExclusivity,
      commonValidationResult = commonValidationResult,
      reassigningParticipantValidationResult =
        reassignmentValidation.reassigningParticipantValidationResult,
    )
  }
}

private[reassignment] object UnassignmentValidation {

  type ValidationErrorOr[A] = EitherT[
    FutureUnlessShutdown,
    ReassignmentProcessorError,
    A,
  ]

  private def validationErrorFromUnitEither[A](f: EitherT[FutureUnlessShutdown, A, Unit])(implicit
      ec: ExecutionContext
  ): ValidationErrorOr[Option[A]] = EitherT.right(f.value.map(e => e.swap.toOption))

  class CommonUnassignmentValidator(
      val activenessF: FutureUnlessShutdown[ActivenessResult],
      val contractValidator: ContractValidator,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ) {
    private def checkSubmitterCheckResult(
        parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree]
    ): ValidationErrorOr[Option[ReassignmentValidationError]] = {
      val fullTree = parsedRequest.fullViewTree

      validationErrorFromUnitEither(
        ReassignmentValidation
          .checkSubmitter(
            ReassignmentRef(fullTree.contracts.contractIds.toSet),
            topologySnapshot = Source(parsedRequest.snapshot.ipsSnapshot),
            submitter = fullTree.submitter,
            participantId = fullTree.submitterMetadata.submittingParticipant,
            stakeholders = fullTree.contracts.stakeholders.all,
          )
      )
    }

    def performValidation(
        parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree]
    ): ValidationErrorOr[UnassignmentValidationResult.CommonValidationResult] = for {
      activenessResult <- EitherT.right(activenessF)
      participantSignatureVerificationResult <- EitherT.right(
        AuthenticationValidator.verifyViewSignature(parsedRequest)
      )
      contractAuthenticationResultF = for {
        _ <- ReassignmentValidation.authenticateContractsAgainstSource(
          contractValidator,
          parsedRequest.fullViewTree,
        )
        _ <- EitherT.fromEither(
          ReassignmentValidation.checkStakeholders(parsedRequest.fullViewTree)
        )
      } yield ()
      packageVettingErrors <- checkSourcePackagesVetted(
        parsedRequest.fullViewTree,
        Source(parsedRequest.snapshot.ipsSnapshot),
      )
      submitterCheckResult <- checkSubmitterCheckResult(parsedRequest)
      // check multi-synchronizer flag is enabled on the source synchronizer
      multiSynchronizerCheckResult <- validationErrorFromUnitEither(
        ReassignmentValidation
          .checkMultiSynchronizerEnabled(
            topologySnapshot = parsedRequest.snapshot.ipsSnapshot,
            stakeholders = parsedRequest.fullViewTree.stakeholders,
            psid = parsedRequest.fullViewTree.sourceSynchronizer.unwrap,
          )
      )
    } yield UnassignmentValidationResult.CommonValidationResult(
      activenessResult,
      participantSignatureVerificationResult,
      contractAuthenticationResultF,
      packageVettingErrors,
      submitterCheckResult,
      multiSynchronizerCheckResult,
    )

    // check the package of the template is vetted
    private def checkSourcePackagesVetted(
        fullTree: FullUnassignmentTree,
        sourceTopology: Source[TopologySnapshot],
    ): ValidationErrorOr[Option[ReassignmentValidationError]] =
      validationErrorFromUnitEither(
        ReassignmentValidation
          .checkPackagesVetted(
            stakeholders = fullTree.contracts.stakeholders,
            contractIds = fullTree.contracts.contractIds.toSet,
            packageIds = fullTree.contracts.sourcePackageIds.unwrap,
            topologySnapshot = sourceTopology.unwrap,
            synchronizerId = fullTree.sourceSynchronizer.unwrap,
          )
      )
  }

  final class ReassigningParticipantUnassignmentValidator(
      val participantId: ParticipantId,
      val contractValidator: ContractValidator,
      val getTopologyAtTs: GetTopologyAtTimestamp,
      val reassignmentMetrics: ReassignmentMetrics,
  )(implicit val executionContext: ExecutionContext, val traceContext: TraceContext) {

    private def checkAssignmentExclusivity(
        fullTree: FullUnassignmentTree,
        targetTopology: Target[TopologySnapshot],
    ): ValidationErrorOr[Option[Target[CantonTimestamp]]] =
      ProcessingSteps
        .getAssignmentExclusivity(targetTopology, fullTree.targetTimestamp)
        .map(Option(_))
        .leftMap[ReassignmentProcessorError](
          ReassignmentParametersError(fullTree.targetSynchronizer.unwrap, _)
        )

    // check the package of the template is vetted
    private def checkTargetPackagesVetted(
        fullTree: FullUnassignmentTree,
        targetTopology: Target[TopologySnapshot],
    ): ValidationErrorOr[Option[ReassignmentValidationError]] =
      validationErrorFromUnitEither(
        ReassignmentValidation.checkPackagesVetted(
          stakeholders = fullTree.contracts.stakeholders,
          contractIds = fullTree.contracts.contractIds.toSet,
          packageIds = fullTree.contracts.targetPackageIds.unwrap,
          topologySnapshot = targetTopology.unwrap,
          synchronizerId = fullTree.targetSynchronizer.unwrap,
        )
      )

    // check the declared reassigning participants are included in the computed ones and sufficient
    // check all stakeholders are hosted on active participants
    private def checkReassigningParticipants(
        parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree],
        targetTopology: Target[TopologySnapshot],
    ): ValidationErrorOr[Option[ReassignmentValidationError]] =
      EitherT
        .right(
          new ReassigningParticipantsComputation(
            parsedRequest.fullViewTree.contracts.stakeholders,
            Source(parsedRequest.snapshot.ipsSnapshot),
            targetTopology,
          ).checkSufficient(
            parsedRequest.fullViewTree.reassigningParticipants,
            ReassignmentRef(parsedRequest.fullViewTree.contracts.contractIds.toSet),
          ).value
        )
        .map(_.swap.toOption)

    private def computeReassigningParticipantValidationResult(
        parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree],
        targetTopology: Target[TopologySnapshot],
    ): ValidationErrorOr[ReassigningParticipantValidationResult] =
      for {
        participantsErrors <- checkReassigningParticipants(parsedRequest, targetTopology)
        packageVettingErrors <- checkTargetPackagesVetted(
          parsedRequest.fullViewTree,
          targetTopology,
        )
        // check multi-synchronizer flag is enabled on the target synchronizer
        multiSynchronizerCheckResult <- validationErrorFromUnitEither(
          ReassignmentValidation
            .checkMultiSynchronizerEnabled(
              topologySnapshot = targetTopology.unwrap,
              stakeholders = parsedRequest.fullViewTree.stakeholders,
              psid = parsedRequest.fullViewTree.targetSynchronizer.unwrap,
            )
        )
      } yield {
        val contractAuthenticationResultF =
          ReassignmentValidation.authenticateContractsAgainstTarget(
            contractValidator,
            parsedRequest.fullViewTree,
          )
        ReassigningParticipantValidationResult(
          contractAuthenticationResultF,
          participantsErrors.toList ++ packageVettingErrors.toList ++ multiSynchronizerCheckResult.toList,
        )
      }

    private def recordLocalTargetTsLag(
        fullViewTree: FullUnassignmentTree,
        targetTopology: Target[TopologySnapshot],
    ): Unit = {
      val lag = fullViewTree.targetTimestamp.unwrap - targetTopology.unwrap.timestamp
      reassignmentMetrics.localTargetTimestampLag.update(math.max(0L, lag.toMillis))(
        ReassignmentMetrics.synchronizers(
          fullViewTree.sourceSynchronizer.map(_.logical),
          fullViewTree.targetSynchronizer.map(_.logical),
        )
      )
    }

    def performValidations(
        parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree]
    ): ValidationErrorOr[ReassigningParticipantValidation] = {
      val fullViewTree = parsedRequest.fullViewTree

      getTopologyAtTs
        .getTargetApproximateSnapshot(fullViewTree.targetSynchronizer)
        .biflatMap(
          unknownTarget =>
            // Return a validation error rather than a processing error to not halt processing
            EitherT.pure[FutureUnlessShutdown, ReassignmentProcessorError](
              ReassigningParticipantValidation(
                assignmentExclusivity = None,
                reassigningParticipantValidationResult = ReassigningParticipantValidationResult(
                  contractAuthenticationResultF = EitherT.pure(()),
                  errors = Seq(unknownTarget),
                ),
              )
            ),
          targetTopology => {
            recordLocalTargetTsLag(fullViewTree, targetTopology)
            for {
              assignmentExclusivity <- checkAssignmentExclusivity(fullViewTree, targetTopology)
              reassigningParticipantValidationResult <-
                computeReassigningParticipantValidationResult(parsedRequest, targetTopology)
            } yield ReassigningParticipantValidation(
              assignmentExclusivity,
              reassigningParticipantValidationResult,
            )
          },
        )
    }

  }

  private[reassignment] final case class ReassigningParticipantValidation(
      assignmentExclusivity: Option[Target[CantonTimestamp]],
      reassigningParticipantValidationResult: UnassignmentValidationResult.ReassigningParticipantValidationResult,
  )
}
