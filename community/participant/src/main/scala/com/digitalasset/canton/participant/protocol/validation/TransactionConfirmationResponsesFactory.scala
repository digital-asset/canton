// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.{CantonTimestamp, ViewPosition}
import com.digitalasset.canton.error.TransactionError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.ProcessingSteps
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.MalformedPayload
import com.digitalasset.canton.participant.protocol.validation.ExternalCallConsistencyChecker.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{LfPartyId, checked}

import scala.concurrent.ExecutionContext

class TransactionConfirmationResponsesFactory(
    participantId: ParticipantId,
    synchronizerId: PhysicalSynchronizerId,
    protected val loggerFactory: NamedLoggerFactory,
    externalCallConsistencyChecker: ExternalCallConsistencyChecker = new ExternalCallConsistencyChecker(),
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

    def responsesForWellformedPayloads(
        transactionValidationResult: TransactionValidationResult
    ): FutureUnlessShutdown[Option[ConfirmationResponses]] = {
      for {
        modelConformanceResultE <- transactionValidationResult.modelConformanceResultET.value

        // Rejections due to a failed model conformance check
        // Aborts are logged by the Engine callback when the abort happens
        modelConformanceRejections =
          modelConformanceResultE.swap.toSeq.flatMap(error =>
            error.nonAbortErrors.map(cause =>
              logged(
                requestId,
                LocalRejectError.MalformedRejects.ModelConformance.Reject(cause.toString),
              ).toLocalReject(protocolVersion)
            )
          )

        // Step 1: Compute hosted confirming parties for all views
        viewsWithHostedParties: Seq[(ViewPosition, ViewValidationResult, Set[LfPartyId])] <-
          transactionValidationResult.viewValidationResults.toSeq
            .parTraverse { case (viewPosition, viewValidationResult) =>
              hostedConfirmingPartiesOfView(viewValidationResult).map { hostedParties =>
                (viewPosition, viewValidationResult, hostedParties)
              }
            }

        // Step 2: Collect all external calls and check consistency per party
        allHostedConfirmingParties = viewsWithHostedParties.flatMap(_._3).toSet
        allExternalCalls = externalCallConsistencyChecker.collectExternalCalls(
          transactionValidationResult.viewValidationResults
        )
        externalCallConsistencyResults = externalCallConsistencyChecker.checkConsistency(
          allExternalCalls,
          allHostedConfirmingParties,
        )

        // Log external call inconsistencies
        _ = externalCallConsistencyResults.results.foreach {
          case (party, Inconsistent(key, outputs, viewPositions)) =>
            logger.warn(
              s"Request $requestId: Party $party sees inconsistent external call results for ${key.functionId}: outputs=$outputs in views $viewPositions"
            )
          case _ => // consistent, no logging needed
        }

        // Step 3: Generate verdicts for each view, considering per-party external call consistency
        responses <- viewsWithHostedParties
          .parTraverse { case (viewPosition, viewValidationResult, hostedConfirmingParties) =>
            FutureUnlessShutdown.pure {
              // Rejections due to a failed internal consistency check
              val internalConsistencyRejections =
                transactionValidationResult.internalConsistencyResultE.swap.toOption.map(cause =>
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

              // Determine general verdict (applies to all parties unless overridden by external call inconsistency)
              val generalVerdictO: Option[(LocalVerdict, Boolean)] = localVerdicts
                .collectFirst[(LocalVerdict, Boolean)] {
                  case malformed: LocalReject if malformed.isMalformed => (malformed, true)
                  case localReject: LocalReject => (localReject, false)
                }
                .orElse(Some((LocalApprove(protocolVersion), false)))

              // Partition parties based on external call consistency results
              val responses = partitionByExternalCallConsistency(
                hostedConfirmingParties,
                externalCallConsistencyResults,
                generalVerdictO,
                viewPosition,
                requestId,
              )

              responses
            }
          }
          .map(_.flatten)
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

    /** Partitions parties based on their external call consistency results.
      *
      * Parties with inconsistent external call results get a malformed rejection,
      * while parties with consistent results get the general verdict.
      *
      * @param hostedConfirmingParties All hosted confirming parties for this view
      * @param consistencyResults Per-party external call consistency results
      * @param generalVerdictO The general verdict (and whether it's malformed) to apply to consistent parties
      * @param viewPosition The position of the view being processed
      * @param requestId The request ID for logging
      * @return Confirmation responses partitioned by verdict
      */
    def partitionByExternalCallConsistency(
        hostedConfirmingParties: Set[LfPartyId],
        consistencyResults: ExternalCallConsistencyResults,
        generalVerdictO: Option[(LocalVerdict, Boolean)],
        viewPosition: com.digitalasset.canton.data.ViewPosition,
        requestId: RequestId,
    )(implicit traceContext: TraceContext): Seq[ConfirmationResponse] = {
      if (hostedConfirmingParties.isEmpty) {
        Seq.empty
      } else {
        // Separate parties by their external call consistency result
        val (inconsistentParties, consistentParties) = hostedConfirmingParties.partition { party =>
          consistencyResults.results.get(party).exists {
            case _: Inconsistent => true
            case Consistent => false
          }
        }

        val responses = Seq.newBuilder[ConfirmationResponse]

        // Handle parties with external call inconsistency - they get a malformed rejection
        if (inconsistentParties.nonEmpty) {
          // Group by the specific inconsistency to create minimal number of responses
          val groupedByInconsistency = inconsistentParties
            .map(p => (p, consistencyResults.results(p)))
            .groupBy(_._2)

          groupedByInconsistency.foreach {
            case (Inconsistent(key, outputs, _), _) =>
              val reject = logged(
                requestId,
                LocalRejectError.MalformedRejects.ExternalCallInconsistency.Reject(
                  s"Function ${key.functionId} returned different outputs: ${outputs.mkString(", ")}"
                ),
              ).toLocalReject(protocolVersion)
              responses += checked(
                ConfirmationResponse.tryCreate(
                  Some(viewPosition),
                  reject,
                  Set.empty, // Malformed rejections use empty confirming parties
                )
              )
            case _ => // Consistent parties handled below
          }
        }

        // Handle consistent parties with the general verdict
        generalVerdictO match {
          case Some((generalVerdict, isMalformed)) =>
            val shouldEmitGeneralVerdict = if (isMalformed) {
              // For malformed general verdicts, we emit with empty parties but only once
              // Skip if we already emitted malformed for inconsistent parties
              consistentParties.nonEmpty || inconsistentParties.isEmpty
            } else {
              true
            }

            if (shouldEmitGeneralVerdict) {
              val partiesToUse = if (isMalformed) Set.empty[LfPartyId] else consistentParties
              if (partiesToUse.nonEmpty || isMalformed) {
                responses += checked(
                  ConfirmationResponse.tryCreate(
                    Some(viewPosition),
                    generalVerdict,
                    partiesToUse,
                  )
                )
              }
            }
          case None => // No general verdict to emit
        }

        responses.result()
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
      responsesForWellformedPayloads(transactionValidationResult)
    }
  }

  private def logged[T <: TransactionError](requestId: RequestId, err: T)(implicit
      traceContext: TraceContext
  ): T = {
    err.logWithContext(Map("requestId" -> requestId.toString))
    err
  }

}
