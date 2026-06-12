// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.{CantonTimestamp, ViewParticipantData, ViewPosition}
import com.digitalasset.canton.error.TransactionError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.ProcessingSteps
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.MalformedPayload
import com.digitalasset.canton.participant.protocol.validation.ExternalCallConsistencyChecker.{
  ExternalCallOccurrence,
  Inconsistency,
}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{LfPartyId, checked}
import com.digitalasset.daml.lf.data.Bytes

import scala.concurrent.ExecutionContext

private final case class ExternalCallInconsistencyDetails(
    key: DAMLe.ExternalCallKey,
    outputs: Set[Bytes],
)

private final case class ViewWithHostedParties(
    viewPosition: ViewPosition,
    validationResult: ViewValidationResult,
    hostedConfirmingParties: Set[LfPartyId],
    hasHostedExternalCallResults: Boolean,
)

class TransactionConfirmationResponsesFactory(
    participantId: ParticipantId,
    synchronizerId: PhysicalSynchronizerId,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  import com.digitalasset.canton.util.ShowUtil.*

  private val protocolVersion = synchronizerId.protocolVersion
  private val maxExternalCallDisagreementDetailsLength = 1024

  private def externalCallResultMismatch(
      error: ModelConformanceChecker.Error
  ): Option[DAMLe.ExternalCallResultMismatch] = error match {
    case ModelConformanceChecker.DAMLeError(
          mismatch: DAMLe.ExternalCallResultMismatch,
          _,
        ) =>
      Some(mismatch)
    case _ => None
  }

  private def externalCallRecordedResultDisagreement(
      error: ModelConformanceChecker.Error
  ): Option[DAMLe.ExternalCallRecordedResultDisagreement] = error match {
    case ModelConformanceChecker.DAMLeError(
          disagreement: DAMLe.ExternalCallRecordedResultDisagreement,
          _,
        ) =>
      Some(disagreement)
    case _ => None
  }

  private def routeExternalCallInconsistencies[A](
      orderedFindings: Seq[A],
      viewsWithHostedParties: Seq[ViewWithHostedParties],
  )(
      inconsistencyDetails: (
          A,
          ViewParticipantData.ViewExternalCallResult,
      ) => Option[ExternalCallInconsistencyDetails]
  ): Map[LfPartyId, Seq[(A, Inconsistency)]] =
    if (orderedFindings.isEmpty) Map.empty
    else {
      val orderedViews =
        viewsWithHostedParties.sortBy(_.viewPosition)(ViewPosition.orderViewPosition.toOrdering)

      val routed = orderedFindings.flatMap { finding =>
        val occurrencesByPartyAndDetails = orderedViews
          .flatMap { viewWithHostedParties =>
            val viewParticipantData =
              viewWithHostedParties.validationResult.view.viewParticipantData
            if (!viewParticipantData.supportsExternalCallResults)
              Seq.empty
            else {
              viewParticipantData.externalCallResults.toSeq.flatMap { result =>
                inconsistencyDetails(finding, result).toList.flatMap { details =>
                  val occurrence = ExternalCallOccurrence(
                    viewWithHostedParties.viewPosition,
                    result.exerciseIndex,
                    result.callIndex,
                  )
                  result.checkingParties
                    .intersect(viewWithHostedParties.hostedConfirmingParties)
                    .toSeq
                    .map(party => (party, details, occurrence))
                }
              }
            }
          }
          .groupMap { case (party, details, _) => party -> details } { case (_, _, occurrence) =>
            occurrence
          }

        occurrencesByPartyAndDetails.toSeq.map { case ((party, details), occurrences) =>
          party -> (finding -> Inconsistency(
            details.key,
            details.outputs,
            occurrences.toSet,
          ))
        }
      }

      routed.groupMap(_._1)(_._2)
    }

  private def localExternalCallMismatchInconsistencies(
      mismatches: Seq[DAMLe.ExternalCallResultMismatch],
      viewsWithHostedParties: Seq[ViewWithHostedParties],
  ): Map[LfPartyId, Seq[(DAMLe.ExternalCallResultMismatch, Inconsistency)]] = {
    val orderedMismatches =
      mismatches.distinct.sortBy(mismatch =>
        (
          mismatch.extensionId,
          mismatch.functionId,
          mismatch.config.toHexString,
          mismatch.input.toHexString,
          mismatch.computedOutput.toHexString,
          mismatch.recordedOutput.toHexString,
        )
      )

    routeExternalCallInconsistencies(orderedMismatches, viewsWithHostedParties) {
      case (mismatch, result) =>
        val call = result.result
        Option.when(
          call.extensionId == mismatch.extensionId &&
            call.functionId == mismatch.functionId &&
            call.config == mismatch.config &&
            call.input == mismatch.input
        )(
          ExternalCallInconsistencyDetails(
            DAMLe.ExternalCallKey(
              mismatch.extensionId,
              mismatch.functionId,
              mismatch.config.toHexString,
              mismatch.input.toHexString,
            ),
            Set(mismatch.recordedOutput, mismatch.computedOutput),
          )
        )
    }
  }

  private def recordedExternalCallDisagreementInconsistencies(
      disagreements: Seq[DAMLe.ExternalCallRecordedResultDisagreement],
      viewsWithHostedParties: Seq[ViewWithHostedParties],
  ): Map[
    LfPartyId,
    Seq[
      (
          DAMLe.ExternalCallRecordedResultDisagreement,
          Inconsistency,
      )
    ],
  ] = {
    val orderedDisagreements =
      disagreements.distinct.sortBy(disagreement =>
        (
          disagreement.key.extensionId,
          disagreement.key.functionId,
          disagreement.key.config,
          disagreement.key.input,
          disagreement.outputs.toSeq.map(_.toHexString).sorted.mkString(","),
        )
      )

    routeExternalCallInconsistencies(orderedDisagreements, viewsWithHostedParties) {
      case (resultDisagreement, result) =>
        val call = result.result
        Option.when(
          call.extensionId == resultDisagreement.key.extensionId &&
            call.functionId == resultDisagreement.key.functionId &&
            call.config.toHexString == resultDisagreement.key.config &&
            call.input.toHexString == resultDisagreement.key.input &&
            resultDisagreement.outputs(call.output)
        )(
          ExternalCallInconsistencyDetails(
            resultDisagreement.key,
            resultDisagreement.outputs,
          )
        )
    }
  }

  private final class ExternalCallRoutingContext(
      resultMismatches: Seq[DAMLe.ExternalCallResultMismatch],
      recordedResultDisagreements: Seq[DAMLe.ExternalCallRecordedResultDisagreement],
      viewsWithHostedParties: Seq[ViewWithHostedParties],
      viewValidationResults: Map[ViewPosition, ViewValidationResult],
  ) {
    lazy val hasAnyHostedExternalCallResults: Boolean =
      viewsWithHostedParties.exists(_.hasHostedExternalCallResults)

    private lazy val localMismatchInconsistencies =
      localExternalCallMismatchInconsistencies(
        resultMismatches,
        viewsWithHostedParties,
      )

    private lazy val recordedReplayDisagreementInconsistencies =
      recordedExternalCallDisagreementInconsistencies(
        recordedResultDisagreements,
        viewsWithHostedParties,
      )

    private lazy val routableExternalCallMismatches =
      localMismatchInconsistencies.valuesIterator
        .flatMap(_.iterator)
        .map(_._1)
        .toSet

    private lazy val routableRecordedExternalCallDisagreements =
      recordedReplayDisagreementInconsistencies.valuesIterator
        .flatMap(_.iterator)
        .map(_._1)
        .toSet

    private lazy val recordedConsistencyResult =
      if (hasAnyHostedExternalCallResults) {
        val allHostedConfirmingParties =
          viewsWithHostedParties.flatMap(_.hostedConfirmingParties).toSet
        ExternalCallConsistencyChecker.check(
          viewValidationResults,
          allHostedConfirmingParties,
        )
      } else ExternalCallConsistencyChecker.Result.empty

    def isRoutableModelConformanceError(error: ModelConformanceChecker.Error): Boolean =
      externalCallResultMismatch(error).exists(routableExternalCallMismatches) ||
        externalCallRecordedResultDisagreement(error).exists(
          routableRecordedExternalCallDisagreements
        )

    def inconsistenciesForView(
        viewPosition: ViewPosition,
        hostedConfirmingParties: Set[LfPartyId],
    ): Seq[(LfPartyId, Inconsistency)] = {
      val recordedResultInconsistencies =
        recordedConsistencyResult.inconsistencies.toSeq.flatMap {
          case (party, inconsistencies) if hostedConfirmingParties(party) =>
            inconsistencies
              .find(_.occurrences.exists(_.viewPosition == viewPosition))
              .map(party -> _)
          case _ => None
        }

      val partiesWithRecordedResultInconsistencies =
        recordedResultInconsistencies.map(_._1).toSet

      val recordedReplayInconsistencies =
        recordedReplayDisagreementInconsistencies.toSeq.flatMap {
          case (party, disagreementInconsistencies)
              if hostedConfirmingParties(party) &&
                !partiesWithRecordedResultInconsistencies(party) =>
            disagreementInconsistencies
              .find(_._2.occurrences.exists(_.viewPosition == viewPosition))
              .map { case (_, inconsistency) => party -> inconsistency }
          case _ => None
        }

      val partiesWithRecordedInconsistencies =
        (recordedResultInconsistencies ++ recordedReplayInconsistencies).map(_._1).toSet

      val localMismatchInconsistenciesForView =
        localMismatchInconsistencies.toSeq.flatMap {
          case (party, mismatchInconsistencies)
              if hostedConfirmingParties(party) && !partiesWithRecordedInconsistencies(party) =>
            mismatchInconsistencies
              .find(_._2.occurrences.exists(_.viewPosition == viewPosition))
              .map { case (_, inconsistency) => party -> inconsistency }
          case _ => None
        }

      recordedResultInconsistencies ++
        recordedReplayInconsistencies ++
        localMismatchInconsistenciesForView
    }
  }

  private def responseForView(
      viewPosition: ViewPosition,
      localVerdict: LocalVerdict,
      parties: Set[LfPartyId],
  ): ConfirmationResponse =
    checked(
      ConfirmationResponse
        .tryCreate(
          Some(viewPosition),
          localVerdict,
          parties,
        )
    )

  private def ordinaryLocalVerdictAndParties(
      requestId: RequestId,
      transactionValidationResult: TransactionValidationResult,
      internalConsistencyResultE: Either[
        InternalConsistencyChecker.ErrorWithInternalConsistencyCheck,
        Unit,
      ],
      modelConformanceRejections: Seq[LocalVerdict],
      viewWithHostedParties: ViewWithHostedParties,
      consistencyVerdict: Option[LocalVerdict],
  )(implicit
      traceContext: TraceContext
  ): Option[(LocalVerdict, Set[LfPartyId])] = {
    val viewPosition = viewWithHostedParties.viewPosition
    val hostedConfirmingParties = viewWithHostedParties.hostedConfirmingParties

    // Rejections due to a failed internal consistency check
    val internalConsistencyRejections =
      internalConsistencyResultE.swap.toOption.map(cause =>
        logged(
          requestId,
          LocalRejectError.MalformedRejects.ModelConformance.Reject(cause.toString),
        ).toLocalReject(protocolVersion)
      )

    // Rejections due to a failed authentication check
    val authenticationRejections =
      transactionValidationResult.authenticationResult
        .get(viewPosition)
        .map(err =>
          logged(
            requestId,
            LocalRejectError.MalformedRejects.MalformedRequest.Reject(err.message),
          ).toLocalReject(protocolVersion)
        )

    // Rejections due to a transaction detected as a replay
    val replayRejections =
      transactionValidationResult.replayCheckResult
        .map(err =>
          logged(
            requestId,
            // Conceptually, a normal LocalReject for the admin party should suffice for rejecting replays.
            // However, we nevertheless use a `Malformed` rejection here so that the rejection preference sorting
            // ensures that this rejection or something at least as strong will make it to the mediator.
            LocalRejectError.MalformedRejects.MalformedRequest.Reject(
              err.format(viewPosition)
            ),
          ).toLocalReject(protocolVersion)
        )

    // Rejections due to a failed authorization check
    val authorizationRejections =
      transactionValidationResult.authorizationResult
        .get(viewPosition)
        .map(cause =>
          logged(
            requestId,
            LocalRejectError.MalformedRejects.MalformedRequest.Reject(cause),
          ).toLocalReject(protocolVersion)
        )

    // Rejections due to a failed time validation
    val timeValidationRejections =
      transactionValidationResult.timeValidationResultE.swap.toOption
        .map {
          case TimeValidator.LedgerTimeRecordTimeDeltaTooLargeError(
                ledgerTime,
                recordTime,
                maxDelta,
              ) =>
            LocalRejectError.TimeRejects.LedgerTime.Reject(
              s"ledgerTime=$ledgerTime, recordTime=$recordTime, maxDelta=$maxDelta"
            )
          case TimeValidator.PreparationTimeRecordTimeDeltaTooLargeError(
                preparationTime,
                recordTime,
                maxDelta,
              ) =>
            LocalRejectError.TimeRejects.PreparationTime.Reject(
              s"preparationTime=$preparationTime, recordTime=$recordTime, maxDelta=$maxDelta"
            )
          case TimeValidator.ExternallySignedRecordTimeExceedsMaximum(
                recordTime: CantonTimestamp,
                maxRecordTime: CantonTimestamp,
              ) =>
            LocalRejectError.TimeRejects.MaxRecordTimeExceeded.Reject(
              s"recordTime=$recordTime, maxRecordTime=$maxRecordTime"
            )
        }
        .map(logged(requestId, _))
        .map(_.toLocalReject(protocolVersion))

    val contractConsistencyRejections =
      transactionValidationResult.contractConsistencyResultE.swap.toOption.map(err =>
        logged(
          requestId,
          LocalRejectError.MalformedRejects.MalformedRequest.Reject(err.toString),
        ).toLocalReject(protocolVersion)
      )

    val localVerdicts: Seq[LocalVerdict] =
      consistencyVerdict.toList ++ timeValidationRejections ++ contractConsistencyRejections ++
        authenticationRejections ++ authorizationRejections ++
        modelConformanceRejections ++ internalConsistencyRejections ++
        replayRejections

    localVerdicts
      .collectFirst[(LocalVerdict, Set[LfPartyId])] {
        case malformed: LocalReject if malformed.isMalformed => malformed -> Set.empty
        case localReject: LocalReject if hostedConfirmingParties.nonEmpty =>
          localReject -> hostedConfirmingParties
      }
      .orElse(
        Option.when(hostedConfirmingParties.nonEmpty)(
          LocalApprove(protocolVersion) -> hostedConfirmingParties
        )
      )
  }

  /** Takes a `transactionValidationResult` and computes the
    * [[protocol.messages.ConfirmationResponses]], to be sent to the mediator.
    */
  def createConfirmationResponses(
      requestId: RequestId,
      malformedPayloads: Seq[MalformedPayload],
      transactionValidationResult: TransactionValidationResult,
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[ConfirmationResponses]] = {

    def hostedConfirmingPartiesOfView(
        viewValidationResult: ViewValidationResult
    ): FutureUnlessShutdown[Set[LfPartyId]] = {
      val confirmingParties =
        viewValidationResult.view.viewCommonData.viewConfirmationParameters.confirmers
      topologySnapshot.canConfirm(participantId, confirmingParties)
    }

    def verdictsForView(
        viewValidationResult: ViewValidationResult,
        hostedConfirmingParties: Set[LfPartyId],
    )(implicit
        traceContext: TraceContext
    ): Option[LocalVerdict] = {
      val viewHash = viewValidationResult.view.unwrap.viewHash

      val ViewActivenessResult(
        inactive,
        alreadyLocked,
        existing,
      ) =
        viewValidationResult.activenessResult

      if (inactive.nonEmpty)
        logger.info(
          show"View $viewHash of request $requestId rejected due to inactive contract(s) $inactive"
        )
      if (alreadyLocked.nonEmpty)
        logger.info(
          show"View $viewHash of request $requestId rejected due to contention on contract(s) $alreadyLocked"
        )

      if (hostedConfirmingParties.isEmpty) {
        // The participant does not host a confirming party.
        // Therefore, no rejection needs to be computed.
        None
      } else if (existing.nonEmpty) {
        // The transaction would recreate existing contracts. Reject.
        Some(
          logged(
            requestId,
            LocalRejectError.MalformedRejects.CreatesExistingContracts
              .Reject(existing.toSeq.map(_.coid)),
          ).toLocalReject(protocolVersion)
        )
      } else {
        def stakeholderOfUsedContractIsHostedConfirmingParty(coid: LfContractId): Boolean =
          viewValidationResult.view.viewParticipantData.coreInputs
            .get(coid)
            .exists(_.stakeholders.intersect(hostedConfirmingParties).nonEmpty)

        // All informees are stakeholders of created contracts in the core of the view.
        // It therefore suffices to deal with input contracts by stakeholder.
        val createdAbsolute =
          viewValidationResult.view.viewParticipantData.createdCore
            .map(_.contract.contractId)
            .toSet
        val lockedForActivation = alreadyLocked intersect createdAbsolute

        val inactiveInputs = inactive.filter(stakeholderOfUsedContractIsHostedConfirmingParty)
        val lockedInputs = alreadyLocked.filter(stakeholderOfUsedContractIsHostedConfirmingParty)

        if (inactiveInputs.nonEmpty) {
          // The transaction uses an inactive contract. Reject.
          Some(
            logged(
              requestId,
              LocalRejectError.ConsistencyRejections.InactiveContracts
                .Reject(inactiveInputs.toSeq.map(_.coid)),
            ).toLocalReject(protocolVersion)
          )
        } else if (lockedInputs.nonEmpty | lockedForActivation.nonEmpty) {
          // The transaction would create / use a locked contract. Reject.
          val allLocked = lockedForActivation ++ lockedInputs
          Some(
            logged(
              requestId,
              LocalRejectError.ConsistencyRejections.LockedContracts
                .Reject(allLocked.toSeq.map(_.coid)),
            ).toLocalReject(protocolVersion)
          )
        } else {
          // Everything ok from the perspective of conflict detection.
          None
        }
      }
    }

    def responsesForWellFormedPayloads(
        transactionValidationResult: TransactionValidationResult
    ): FutureUnlessShutdown[Option[ConfirmationResponses]] = {
      for {
        modelConformanceResultE <- transactionValidationResult.modelConformanceResultET.value

        internalConsistencyResultE <- transactionValidationResult.internalConsistencyResultET.value

        modelConformanceErrors =
          modelConformanceResultE.swap.toSeq.flatMap(_.nonAbortErrors)

        externalCallResultMismatches = modelConformanceErrors.flatMap(externalCallResultMismatch)

        externalCallRecordedResultDisagreements =
          modelConformanceErrors.flatMap(externalCallRecordedResultDisagreement)

        viewsWithHostedParties <- transactionValidationResult.viewValidationResults.toSeq
          .parTraverse { case (viewPosition, viewValidationResult) =>
            for {
              hostedConfirmingParties <-
                hostedConfirmingPartiesOfView(viewValidationResult)

              viewParticipantData = viewValidationResult.view.viewParticipantData
              viewHasHostedExternalCallResults =
                viewParticipantData.supportsExternalCallResults &&
                  hostedConfirmingParties.nonEmpty &&
                  viewParticipantData.externalCallResults.nonEmpty
            } yield ViewWithHostedParties(
              viewPosition = viewPosition,
              validationResult = viewValidationResult,
              hostedConfirmingParties = hostedConfirmingParties,
              hasHostedExternalCallResults = viewHasHostedExternalCallResults,
            )
          }

        externalCallRouting = new ExternalCallRoutingContext(
          externalCallResultMismatches,
          externalCallRecordedResultDisagreements,
          viewsWithHostedParties,
          transactionValidationResult.viewValidationResults,
        )

        // Rejections due to a failed model conformance check
        // Aborts are logged by the Engine callback when the abort happens
        modelConformanceRejections =
          modelConformanceErrors.flatMap { case cause =>
            Option.unless(externalCallRouting.isRoutableModelConformanceError(cause))(
              logged(
                requestId,
                LocalRejectError.MalformedRejects.ModelConformance.Reject(cause.toString),
              ).toLocalReject(protocolVersion)
            )
          }

        responses = viewsWithHostedParties.flatMap { viewWithHostedParties =>
          val viewPosition = viewWithHostedParties.viewPosition
          val viewValidationResult = viewWithHostedParties.validationResult
          val hostedConfirmingParties = viewWithHostedParties.hostedConfirmingParties

          // Approve if the consistency check succeeded, reject otherwise.
          val consistencyVerdict =
            verdictsForView(viewValidationResult, hostedConfirmingParties)

          val localVerdictAndPartiesO =
            ordinaryLocalVerdictAndParties(
              requestId,
              transactionValidationResult,
              internalConsistencyResultE,
              modelConformanceRejections,
              viewWithHostedParties,
              consistencyVerdict,
            )

          localVerdictAndPartiesO match {
            case Some((malformed: LocalReject, _)) if malformed.isMalformed =>
              Seq(responseForView(viewPosition, malformed, Set.empty))
            case Some((localVerdict, parties))
                if !externalCallRouting.hasAnyHostedExternalCallResults =>
              Seq(responseForView(viewPosition, localVerdict, parties))
            case None => Seq.empty
            case Some((localVerdict, _)) =>
              val externalCallInconsistencies =
                externalCallRouting.inconsistenciesForView(viewPosition, hostedConfirmingParties)
              val inconsistentParties = externalCallInconsistencies.map(_._1).toSet

              val externalCallResponses =
                externalCallInconsistencies.groupMap(_._2)(_._1).toSeq.map {
                  case (inconsistency, parties) =>
                    val reject = logged(
                      requestId,
                      LocalRejectError.ConsistencyRejections.ExternalCallResultDisagreement
                        .Reject(
                          inconsistency.description
                            .limit(maxExternalCallDisagreementDetailsLength)
                            .toString
                        ),
                    ).toLocalReject(protocolVersion)
                    checked(
                      ConfirmationResponse
                        .tryCreate(
                          Some(viewPosition),
                          reject,
                          parties.toSet,
                        )
                    )
                }

              val generalParties = hostedConfirmingParties -- inconsistentParties

              externalCallResponses ++
                Option
                  .when(generalParties.nonEmpty)(
                    responseForView(viewPosition, localVerdict, generalParties)
                  )
                  .toList
          }
        }
      } yield {
        checked(
          NonEmpty
            .from(responses)
            .map(
              ConfirmationResponses
                .tryCreate(
                  requestId,
                  transactionValidationResult.updateId.toRootHash,
                  synchronizerId,
                  participantId,
                  _,
                  protocolVersion,
                )
            )
        )
      }
    }

    if (malformedPayloads.nonEmpty) {
      FutureUnlessShutdown.pure(
        ProcessingSteps.constructResponsesForMalformedPayloads(
          requestId = requestId,
          rootHash = transactionValidationResult.updateId.toRootHash,
          malformedPayloads = malformedPayloads,
          synchronizerId = synchronizerId,
          participantId = participantId,
          protocolVersion = protocolVersion,
        )
      )
    } else {
      responsesForWellFormedPayloads(transactionValidationResult)
    }
  }

  private def logged[T <: TransactionError](requestId: RequestId, err: T)(implicit
      traceContext: TraceContext
  ): T = {
    err.logWithContext(Map("requestId" -> requestId.toString))
    err
  }

}
