// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.*
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.data.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.participant.protocol.ProcessingSteps
import com.digitalasset.canton.participant.protocol.conflictdetection.ActivenessResult
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentValidationError.ReassigningParticipantsMismatch
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentValidation.{
  CommonUnassignmentValidator,
  ReassigningParticipantUnassignmentValidator,
  ReassigningParticipantValidation,
  ValidationErrorOr,
}
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentValidationError.PackageIdUnknownOrUnvetted
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentValidationResult.ReassigningParticipantValidationResult
import com.digitalasset.canton.participant.protocol.submission.UsableSynchronizers
import com.digitalasset.canton.participant.protocol.validation.AuthenticationValidator
import com.digitalasset.canton.protocol.{LfContractId, Stakeholders}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ContractValidator
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}

import scala.concurrent.ExecutionContext

private[reassignment] class UnassignmentValidation(
    participantId: ParticipantId,
    contractValidator: ContractValidator,
    getTopologyAtTs: GetTopologyAtTimestamp,
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

      EitherT.right(
        ReassignmentValidation
          .checkSubmitter(
            ReassignmentRef(fullTree.contracts.contractIds.toSet),
            topologySnapshot = Source(parsedRequest.snapshot.ipsSnapshot),
            submitter = fullTree.submitter,
            participantId = fullTree.submitterMetadata.submittingParticipant,
            stakeholders = fullTree.contracts.stakeholders.all,
          )
          .value
          .map(_.swap.toOption)
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
      submitterCheckResult <- checkSubmitterCheckResult(parsedRequest)
      // check multi-synchronizer flag is enabled on the source synchronizer
      multiSynchronizerCheckResult <- EitherT.right(
        ReassignmentValidation
          .checkMultiSynchronizerEnabled(
            topologySnapshot = parsedRequest.snapshot.ipsSnapshot,
            stakeholders = parsedRequest.fullViewTree.stakeholders,
            psid = parsedRequest.fullViewTree.sourceSynchronizer.unwrap,
          )
          .value
          .map(_.swap.toOption)
      )
    } yield UnassignmentValidationResult.CommonValidationResult(
      activenessResult,
      participantSignatureVerificationResult,
      contractAuthenticationResultF,
      submitterCheckResult,
      multiSynchronizerCheckResult,
    )
  }

  final class ReassigningParticipantUnassignmentValidator(
      val participantId: ParticipantId,
      val contractValidator: ContractValidator,
      val getTopologyAtTs: GetTopologyAtTimestamp,
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

    private def checkPackagesVetted(
        stakeholders: Stakeholders,
        contractIds: Set[LfContractId],
        packageIds: Set[LfPackageId],
        topologySnapshot: TopologySnapshot,
        synchronizerId: PhysicalSynchronizerId,
    ): ValidationErrorOr[Option[ReassignmentValidationError]] =
      EitherT.right(
        UsableSynchronizers
          .checkPackagesVetted(
            synchronizerId,
            topologySnapshot,
            stakeholders.all.map(_ -> packageIds).toMap,
            topologySnapshot.referenceTime,
          )
          .value
          .map(
            _.swap.toOption.map(u =>
              PackageIdUnknownOrUnvetted(contractIds, u.unknownTo, synchronizerId)
            )
          )
      )

    // check the package of the template is vetted
    private def checkTargetPackagesVetted(
        fullTree: FullUnassignmentTree,
        targetTopology: Target[TopologySnapshot],
    ): ValidationErrorOr[Option[ReassignmentValidationError]] =
      checkPackagesVetted(
        stakeholders = fullTree.contracts.stakeholders,
        // TODO(#29199): Use target package IDs
        contractIds = fullTree.contracts.contractIds.toSet,
        packageIds = fullTree.contracts.packageIds,
        topologySnapshot = targetTopology.unwrap,
        synchronizerId = fullTree.targetSynchronizer.unwrap,
      )

    // check the reassigning participants from the request match the computed reassigning participants
    // check all stakeholders are hosted on active participants
    // check the recipients from the request match the computed recipients
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
          ).compute.value
        )
        .map {
          case Right(contractReassigningParticipants) =>
            val fullViewTree = parsedRequest.fullViewTree
            val requestReassigningParticipants = fullViewTree.reassigningParticipants
            Option.when(contractReassigningParticipants != requestReassigningParticipants)(
              ReassigningParticipantsMismatch(
                reassignmentRef =
                  ReassignmentRef.ContractIdRef(fullViewTree.contracts.contractIds.toSet),
                expected = contractReassigningParticipants,
                declared = requestReassigningParticipants,
              )
            )
          case Left(rve) =>
            Some(rve)
        }

    private def computeReassigningParticipantValidationResult(
        parsedRequest: ParsedReassignmentRequest[FullUnassignmentTree],
        targetTopology: Target[TopologySnapshot],
    ): ValidationErrorOr[ReassigningParticipantValidationResult] =
      for {
        participantsErrors <- checkReassigningParticipants(parsedRequest, targetTopology)
        vettingErrors <- checkTargetPackagesVetted(parsedRequest.fullViewTree, targetTopology)
        // check multi-synchronizer flag is enabled on the target synchronizer
        multiSynchronizerCheckResult <- EitherT.right(
          ReassignmentValidation
            .checkMultiSynchronizerEnabled(
              topologySnapshot = targetTopology.unwrap,
              stakeholders = parsedRequest.fullViewTree.stakeholders,
              psid = parsedRequest.fullViewTree.targetSynchronizer.unwrap,
            )
            .value
            .map(_.swap.toOption)
        )
      } yield {
        val contractAuthenticationResultF =
          ReassignmentValidation.authenticateContractsAgainstTarget(
            contractValidator,
            parsedRequest.fullViewTree,
          )
        ReassigningParticipantValidationResult(
          contractAuthenticationResultF,
          participantsErrors.toList ++ vettingErrors.toList ++ multiSynchronizerCheckResult.toList,
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
          targetTopology =>
            for {
              assignmentExclusivity <- checkAssignmentExclusivity(fullViewTree, targetTopology)
              reassigningParticipantValidationResult <-
                computeReassigningParticipantValidationResult(parsedRequest, targetTopology)
            } yield ReassigningParticipantValidation(
              assignmentExclusivity,
              reassigningParticipantValidationResult,
            ),
        )
    }

  }

  private[reassignment] final case class ReassigningParticipantValidation(
      assignmentExclusivity: Option[Target[CantonTimestamp]],
      reassigningParticipantValidationResult: UnassignmentValidationResult.ReassigningParticipantValidationResult,
  )
}
