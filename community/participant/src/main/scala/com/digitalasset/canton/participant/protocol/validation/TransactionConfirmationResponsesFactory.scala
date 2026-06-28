// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.{CantonTimestamp, ViewPosition}
import com.digitalasset.canton.error.TransactionError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.ProcessingSteps
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.MalformedPayload
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{LfPartyId, checked}

import scala.concurrent.ExecutionContext

private[validation] final case class ViewWithHostedParties(
    viewPosition: ViewPosition,
    validationResult: ViewValidationResult,
    hostedConfirmingParties: Set[LfPartyId],
)

class TransactionConfirmationResponsesFactory(
    participantId: ParticipantId,
    synchronizerId: PhysicalSynchronizerId,
    externalCallValidator: ExternalCallValidator,
    externalCallValidationParallelism: PositiveInt,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  import com.digitalasset.canton.util.ShowUtil.*

  private val protocolVersion = synchronizerId.protocolVersion
  private val externalCallRouter =
    new ExternalCallResponseRouter(
      externalCallValidator,
      externalCallValidationParallelism,
      loggerFactory,
    )

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

  private def responsesForView(
      requestId: RequestId,
      viewWithHostedParties: ViewWithHostedParties,
      localVerdictAndPartiesO: Option[(LocalVerdict, Set[LfPartyId])],
      externalCallRouting: ExternalCallRoutingContext,
      localValidationRoutes: ExternalCallValidationRoutes,
  )(implicit traceContext: TraceContext): Seq[ConfirmationResponse] = {
    val viewPosition = viewWithHostedParties.viewPosition
    val hostedConfirmingParties = viewWithHostedParties.hostedConfirmingParties

    localVerdictAndPartiesO match {
      case Some((malformed: LocalReject, _)) if malformed.isMalformed =>
        Seq(responseForView(viewPosition, malformed, Set.empty))
      case None => Seq.empty
      case Some((localVerdict, parties)) =>
        val recordedExternalCallInconsistencies =
          externalCallRouting.inconsistenciesForView(viewPosition, hostedConfirmingParties)

        val localValidationInconsistencies =
          localVerdict match {
            case _: LocalApprove =>
              localValidationRoutes.rejectsForView(viewPosition, hostedConfirmingParties)
            case _ =>
              Seq.empty
          }

        val externalCallInconsistencies =
          recordedExternalCallInconsistencies ++ localValidationInconsistencies

        val rejectedParties = externalCallInconsistencies.map(_._1).toSet

        val externalCallAbstains =
          localVerdict match {
            case _: LocalApprove =>
              localValidationRoutes.abstainsForView(
                viewPosition,
                hostedConfirmingParties,
                rejectedParties,
              )
            case _ =>
              Seq.empty
          }

        val externalCallResponses =
          externalCallInconsistencies
            .groupMap(_._2)(_._1)
            .toSeq
            .sortBy(_._1)(ExternalCallConsistencyChecker.orderInconsistency)
            .map { case (inconsistency, parties) =>
              val reject = logged(
                requestId,
                LocalRejectError.ConsistencyRejections.ExternalCallResultDisagreement
                  .Reject(
                    inconsistency.description
                      .limit(ExternalCallResponseRouter.maxExternalCallDisagreementDetailsLength)
                      .toString
                  ),
              ).toLocalReject(protocolVersion)
              responseForView(
                viewPosition,
                reject,
                parties.toSet,
              )
            }

        val externalCallAbstainResponses =
          externalCallAbstains
            .groupMap(_._2)(_._1)
            .toSeq
            .sortBy { case (reason, _) => reason }
            .map { case (reason, parties) =>
              responseForView(
                viewPosition,
                LocalAbstainError.CannotPerformAllValidations
                  .Abstain(
                    reason
                      .limit(ExternalCallResponseRouter.maxExternalCallDisagreementDetailsLength)
                      .toString
                  )
                  .toLocalAbstain(protocolVersion),
                parties.toSet,
              )
            }

        val abstainedParties = externalCallAbstains.map(_._1).toSet
        val remainingParties = parties -- rejectedParties -- abstainedParties

        externalCallResponses ++
          externalCallAbstainResponses ++
          Option
            .when(remainingParties.nonEmpty)(
              responseForView(viewPosition, localVerdict, remainingParties)
            )
            .toList
    }
  }

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
    ): FutureUnlessShutdown[Option[ConfirmationResponses]] =
      for {
        modelConformanceResultE <- transactionValidationResult.modelConformanceResultET.value

        internalConsistencyResultE <- transactionValidationResult.internalConsistencyResultET.value

        modelConformanceErrors =
          modelConformanceResultE.swap.toSeq.flatMap(_.nonAbortErrors)

        externalCallRecordedResultDisagreements =
          modelConformanceErrors.flatMap(
            ExternalCallResponseRouter.externalCallRecordedResultDisagreement
          )

        viewsWithHostedParties <- transactionValidationResult.viewValidationResults.toSeq
          .parTraverse { case (viewPosition, viewValidationResult) =>
            for {
              hostedConfirmingParties <-
                hostedConfirmingPartiesOfView(viewValidationResult)
            } yield ViewWithHostedParties(
              viewPosition = viewPosition,
              validationResult = viewValidationResult,
              hostedConfirmingParties = hostedConfirmingParties,
            )
          }

        externalCallRouting = new ExternalCallRoutingContext(
          externalCallRecordedResultDisagreements,
          viewsWithHostedParties,
          transactionValidationResult.viewValidationResults,
        )

        _ = externalCallRouter.reportVisibleRecordedDisagreementAlarms(
          requestId,
          externalCallRouting.recordedConsistencyResult,
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

        ordinaryViewResults = viewsWithHostedParties.map { viewWithHostedParties =>
          val hostedConfirmingParties = viewWithHostedParties.hostedConfirmingParties
          val consistencyVerdict =
            verdictsForView(viewWithHostedParties.validationResult, hostedConfirmingParties)
          val ordinaryVerdictAndPartiesO =
            ordinaryLocalVerdictAndParties(
              requestId,
              transactionValidationResult,
              internalConsistencyResultE,
              modelConformanceRejections,
              viewWithHostedParties,
              consistencyVerdict,
            )
          viewWithHostedParties -> ordinaryVerdictAndPartiesO
        }

        approvingViewPositions =
          ordinaryViewResults.collect {
            case (viewWithHostedParties, Some((_: LocalApprove, parties))) if parties.nonEmpty =>
              viewWithHostedParties.viewPosition -> parties
          }.toMap

        localValidationOccurrences =
          externalCallRouter.validationOccurrences(
            viewsWithHostedParties,
            approvingViewPositions,
            externalCallRouting,
          )

        localValidationRoutes <-
          if (localValidationOccurrences.isEmpty)
            FutureUnlessShutdown.pure(ExternalCallValidationRoutes(Map.empty, Map.empty))
          else externalCallRouter.validateExternalCalls(localValidationOccurrences)

        responses = ordinaryViewResults.flatMap {
          case (viewWithHostedParties, localVerdictAndPartiesO) =>
            responsesForView(
              requestId,
              viewWithHostedParties,
              localVerdictAndPartiesO,
              externalCallRouting,
              localValidationRoutes,
            )
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

    if (malformedPayloads.nonEmpty) {
      externalCallRouter.reportVisibleRecordedDisagreementAlarms(
        requestId,
        ExternalCallConsistencyChecker.check(
          transactionValidationResult.viewValidationResults,
          Set.empty,
        ),
      )
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
