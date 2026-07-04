// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.syntax.parallel.*
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
import com.digitalasset.nonempty.NonEmpty

import scala.concurrent.ExecutionContext

class TransactionConfirmationResponsesFactory(
    participantId: ParticipantId,
    synchronizerId: PhysicalSynchronizerId,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  import com.digitalasset.canton.util.ShowUtil.*

  private val protocolVersion = synchronizerId.protocolVersion

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

    /** Converts the local verdict of a view into confirmation responses, splitting it by party
      * where precomputed external-call outcomes ([[ExternalCallResponseRouter.Result]]) apply:
      * hosted confirming parties that check a disagreeing external-call result reject the view with
      * the disagreement, parties whose checked results could not be re-validated abstain (on views
      * that would otherwise be approved), and the remaining parties respond with the view's own
      * verdict. For a request without external calls, `externalCallResult` is empty and the view's
      * verdict is returned unsplit.
      */
    def responsesForVerdict(
        viewPosition: ViewPosition,
        hostedConfirmingParties: Set[LfPartyId],
        localVerdictAndPartiesO: Option[(LocalVerdict, Set[LfPartyId])],
        externalCallResult: ExternalCallResponseRouter.Result,
    ): Seq[ConfirmationResponse] =
      localVerdictAndPartiesO match {
        case None => Seq.empty

        case Some((malformed: LocalReject, parties)) if malformed.isMalformed =>
          // A malformed rejection covers the whole view, so no external-call verdicts are routed
          // for it. `parties` is empty by construction of localVerdictAndPartiesO, meeting the
          // ConfirmationResponse invariant for malformed verdicts, so tryCreate cannot throw.
          Seq(checked(ConfirmationResponse.tryCreate(Some(viewPosition), malformed, parties)))

        case Some((localVerdict, parties)) =>
          // Parties that reject because they check disagreeing external-call results (recorded
          // across views, or within the replay data of a reinterpretation).
          val disagreementRejects =
            externalCallResult.inconsistenciesForView(viewPosition, hostedConfirmingParties)

          // Parties that reject because re-validating a recorded result produced a different
          // output. Only relevant if the view would otherwise be approved. Disagreement
          // rejections take precedence, so parties already rejecting above are excluded and at
          // most one external-call rejection is emitted per party and view.
          val validationRejects = localVerdict match {
            case _: LocalApprove =>
              externalCallResult.validationRoutes.rejectsForView(
                viewPosition,
                hostedConfirmingParties -- disagreementRejects.map(_._1).toSet,
              )
            case _ => Seq.empty
          }

          val externalCallRejects = disagreementRejects ++ validationRejects
          val rejectedParties = externalCallRejects.map(_._1).toSet

          // Parties that abstain because a recorded result they check could not be re-validated.
          // Rejections take precedence, so rejecting parties are excluded.
          val externalCallAbstains = localVerdict match {
            case _: LocalApprove =>
              externalCallResult.validationRoutes.abstainsForView(
                viewPosition,
                hostedConfirmingParties -- rejectedParties,
              )
            case _ => Seq.empty
          }

          val externalCallRejectResponses =
            externalCallRejects
              .groupMap(_._2)(_._1)
              .toSeq
              .sortBy(_._1)(ExternalCallConsistencyChecker.orderInconsistency)
              .map { case (inconsistency, parties) =>
                val reject = logged(
                  requestId,
                  LocalRejectError.ConsistencyRejections.ExternalCallResultDisagreement
                    .Reject(inconsistency.description),
                ).toLocalReject(protocolVersion)
                // The parties of a groupMap group are nonEmpty, meeting the
                // ConfirmationResponse invariant for non-malformed verdicts.
                checked(ConfirmationResponse.tryCreate(Some(viewPosition), reject, parties.toSet))
              }

          val externalCallAbstainResponses =
            externalCallAbstains
              .groupMap(_._2)(_._1)
              .toSeq
              .sortBy { case (reason, _) => reason }
              .map { case (reason, parties) =>
                // The parties of a groupMap group are nonEmpty, meeting the
                // ConfirmationResponse invariant for non-malformed verdicts.
                checked(
                  ConfirmationResponse.tryCreate(
                    Some(viewPosition),
                    LocalAbstainError.CannotPerformAllValidations
                      .Abstain(reason)
                      .toLocalAbstain(protocolVersion),
                    parties.toSet,
                  )
                )
              }

          val abstainedParties = externalCallAbstains.map(_._1).toSet
          val remainingParties = parties -- rejectedParties -- abstainedParties

          externalCallRejectResponses ++
            externalCallAbstainResponses ++
            Option
              .when(remainingParties.nonEmpty)(
                // remainingParties is guarded nonEmpty, meeting the ConfirmationResponse
                // invariant for non-malformed verdicts.
                checked(
                  ConfirmationResponse
                    .tryCreate(Some(viewPosition), localVerdict, remainingParties)
                )
              )
              .toList
      }

    def responsesForWellFormedPayloads(
        transactionValidationResult: TransactionValidationResult
    ): FutureUnlessShutdown[Option[ConfirmationResponses]] = {
      for {
        modelConformanceResultE <- transactionValidationResult.modelConformanceResultET.value

        internalConsistencyResultE <- transactionValidationResult.internalConsistencyResultET.value

        externalCallResult <- transactionValidationResult.externalCallValidationResultF

        // Rejections due to a failed model conformance check
        // Aborts are logged by the Engine callback when the abort happens
        modelConformanceRejections =
          modelConformanceResultE.swap.toSeq.flatMap(error =>
            error.nonAbortErrors.flatMap(cause =>
              // An external-call replay disagreement that is routed to a hosted checking party is
              // not turned into (nor logged as) a malformed model-conformance rejection of the
              // whole view: the routed parties instead reject the affected views with a dedicated,
              // equally logged ExternalCallResultDisagreement rejection (see responsesForVerdict).
              // A routed error is never silent: its visible-disagreement alarm has fired in the
              // external-call check, and each affected view logs its rejection when emitting it --
              // unless the view is rejected as malformed for another reason, in which case that
              // stronger rejection wins the view (the alarm remains on record).
              Option.unless(externalCallResult.isRoutableModelConformanceError(cause))(
                logged(
                  requestId,
                  LocalRejectError.MalformedRejects.ModelConformance.Reject(cause.toString),
                ).toLocalReject(protocolVersion)
              )
            )
          )

        responses <- transactionValidationResult.viewValidationResults.toSeq
          .parFlatTraverse { case (viewPosition, viewValidationResult) =>
            for {
              hostedConfirmingParties <-
                hostedConfirmingPartiesOfView(viewValidationResult)

            } yield {

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

              // Approve if the consistency check succeeded, reject otherwise.
              val consistencyVerdicts =
                verdictsForView(viewValidationResult, hostedConfirmingParties)

              val localVerdicts: Seq[LocalVerdict] =
                consistencyVerdicts.toList ++ timeValidationRejections ++ contractConsistencyRejections ++
                  authenticationRejections ++ authorizationRejections ++
                  modelConformanceRejections ++ internalConsistencyRejections ++
                  replayRejections

              val localVerdictAndPartiesO = localVerdicts
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

              responsesForVerdict(
                viewPosition,
                hostedConfirmingParties,
                localVerdictAndPartiesO,
                externalCallResult,
              )
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
