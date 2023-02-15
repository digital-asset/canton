// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.syntax.parallel.*
import com.digitalasset.canton.data.ConfirmingParty
import com.digitalasset.canton.error.TransactionError
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.MalformedPayload
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.{ConfirmationPolicy, LfContractId, LfGlobalKey, RequestId}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, checked}

import scala.concurrent.{ExecutionContext, Future}

class ConfirmationResponseFactory(
    participantId: ParticipantId,
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  import com.digitalasset.canton.util.ShowUtil.*

  private val verdictProtocolVersion =
    LocalVerdict.protocolVersionRepresentativeFor(protocolVersion)

  def createConfirmationResponses(
      requestId: RequestId,
      malformedPayloads: Seq[MalformedPayload],
      transactionValidationResult: TransactionValidationResult,
      topologySnapshot: TopologySnapshot,
  )(implicit traceContext: TraceContext, ec: ExecutionContext): Future[Seq[MediatorResponse]] = {

    def logged[T <: TransactionError](err: T): T = {
      err.logWithContext(Map("requestId" -> requestId.toString))
      err
    }

    def malformed(msg: Malformed): Seq[MediatorResponse] = {
      val malformedViewHashes = malformedPayloads.map(_.viewHash)
      val parsedViewsHashes = transactionValidationResult.viewValidationResults.keys.toSeq

      (malformedViewHashes ++ parsedViewsHashes).map { viewHash =>
        checked(
          MediatorResponse
            .tryCreate(
              requestId,
              participantId,
              Some(viewHash),
              logged(msg),
              None,
              Set.empty,
              domainId,
              protocolVersion,
            )
        )
      }
    }

    def hostedConfirmingPartiesOfView(
        viewValidationResult: ViewValidationResult,
        confirmationPolicy: ConfirmationPolicy,
    ): Future[Set[LfPartyId]] = {
      viewValidationResult.view.viewCommonData.informees.toList
        .parTraverseFilter {
          case ConfirmingParty(party, _) =>
            topologySnapshot
              .canConfirm(participantId, party, confirmationPolicy.requiredTrustLevel)
              .map(if (_) Some(party) else None)
          case _ => Future.successful(None)
        }
        .map(_.toSet)
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
        duplicateKeys,
        inconsistentKeys,
        alreadyLockedKeys,
      ) =
        viewValidationResult.activenessResult

      if (inactive.nonEmpty)
        logger.debug(
          show"View $viewHash of request $requestId rejected due to inactive contract(s) $inactive"
        )
      if (alreadyLocked.nonEmpty)
        logger.debug(
          show"View $viewHash of request $requestId rejected due to contention on contract(s) $alreadyLocked"
        )
      if (duplicateKeys.nonEmpty)
        logger.debug(
          show"View $viewHash of request $requestId rejected due to duplicate keys $duplicateKeys"
        )
      if (inconsistentKeys.nonEmpty)
        logger.debug(
          show"View $viewHash of request $requestId rejected due to inconsistent keys $inconsistentKeys"
        )
      if (alreadyLockedKeys.nonEmpty)
        logger.debug(
          show"View $viewHash of request $requestId rejected due to contention on key(s) $alreadyLockedKeys"
        )

      if (hostedConfirmingParties.isEmpty) {
        // The participant does not host a confirming party.
        // Therefore, no rejection needs to be computed.
        None
      } else if (existing.nonEmpty) {
        // The transaction would recreate existing contracts. Reject.
        Some(
          logged(
            LocalReject.MalformedRejects.CreatesExistingContracts
              .Reject(existing.toSeq.map(_.coid))(verdictProtocolVersion)
          )
        )
      } else {
        def stakeholderOfUsedContractIsHostedConfirmingParty(coid: LfContractId): Boolean =
          viewValidationResult.view.viewParticipantData.coreInputs
            .get(coid)
            .exists(_.stakeholders.intersect(hostedConfirmingParties).nonEmpty)

        def extractKeysWithMaintainerBeingHostedConfirmingParty(
            keysWithMaintainers: Map[LfGlobalKey, Set[LfPartyId]]
        ): Seq[LfGlobalKey] = keysWithMaintainers.collect {
          case (key, maintainers) if maintainers.intersect(hostedConfirmingParties).nonEmpty => key
        }.toSeq

        // All informees are stakeholders of created contracts in the core of the view.
        // It therefore suffices to deal with input contracts by stakeholder.
        val createdAbsolute =
          viewValidationResult.view.viewParticipantData.createdCore
            .map(_.contract.contractId)
            .toSet
        val lockedForActivation = alreadyLocked intersect createdAbsolute

        val inactiveInputs = inactive.filter(stakeholderOfUsedContractIsHostedConfirmingParty)
        val lockedInputs = alreadyLocked.filter(stakeholderOfUsedContractIsHostedConfirmingParty)
        val lockedKeys = extractKeysWithMaintainerBeingHostedConfirmingParty(alreadyLockedKeys)
        val duplicateKeysForParty = extractKeysWithMaintainerBeingHostedConfirmingParty(
          duplicateKeys
        )
        val inconsistentKeysForParty = extractKeysWithMaintainerBeingHostedConfirmingParty(
          inconsistentKeys
        )

        if (inactiveInputs.nonEmpty) {
          // The transaction uses an inactive contract. Reject.
          Some(
            LocalReject.ConsistencyRejections.InactiveContracts
              .Reject(inactiveInputs.toSeq.map(_.coid))(verdictProtocolVersion)
          )
        } else if (duplicateKeysForParty.nonEmpty) {
          // The transaction would assign several contracts to the same key. Reject.
          Some(
            LocalReject.ConsistencyRejections.DuplicateKey
              .Reject(duplicateKeysForParty.map(_.toString()))(verdictProtocolVersion)
          )
        } else if (inconsistentKeysForParty.nonEmpty) {
          // The key lookups / creations / exercise by key have inconsistent key resolutions. Reject.
          Some(
            LocalReject.ConsistencyRejections.InconsistentKey
              .Reject(inconsistentKeysForParty.map(_.toString()))(verdictProtocolVersion)
          )
        } else if (lockedInputs.nonEmpty | lockedForActivation.nonEmpty) {
          // The transaction would create / use a locked contract. Reject.
          val allLocked = lockedForActivation ++ lockedInputs
          Some(
            LocalReject.ConsistencyRejections.LockedContracts
              .Reject(allLocked.toSeq.map(_.coid))(verdictProtocolVersion)
          )
        } else if (lockedKeys.nonEmpty) {
          // The transaction would resolve a locked key. Reject.
          Some(
            LocalReject.ConsistencyRejections.LockedKeys
              .Reject(lockedKeys.map(_.toString()))(verdictProtocolVersion)
          )
        } else {
          // Everything ok from the perspective of conflict detection.
          None
        }
      }
    }

    def responsesForWellformedPayloads(
        transactionValidationResult: TransactionValidationResult,
        confirmationPolicy: ConfirmationPolicy,
    ): Future[Seq[MediatorResponse]] =
      transactionValidationResult.viewValidationResults.toSeq.parTraverseFilter {
        case (viewHash, viewValidationResult) =>
          for {
            hostedConfirmingParties <- hostedConfirmingPartiesOfView(
              viewValidationResult,
              confirmationPolicy,
            )
          } yield {

            // Rejections due to a failed model conformance check
            val modelConformanceRejections =
              transactionValidationResult.modelConformanceResult.swap.toOption.map(cause =>
                logged(
                  LocalReject.MalformedRejects.ModelConformance.Reject(cause.toString)(
                    verdictProtocolVersion
                  )
                )
              )

            // Rejections due to a failed authentication check
            val authenticationRejections =
              transactionValidationResult.authenticationResult
                .get(viewHash)
                .map(err =>
                  logged(
                    LocalReject.MalformedRejects.MalformedRequest.Reject(err.toString)(
                      verdictProtocolVersion
                    )
                  )
                )

            // Rejections due to a failed authorization check
            val authorizationRejections =
              transactionValidationResult.authorizationResult
                .get(viewHash)
                .map(cause =>
                  logged(
                    LocalReject.MalformedRejects.MalformedRequest.Reject(cause)(
                      verdictProtocolVersion
                    )
                  )
                )

            // Rejections due to a failed time validation
            val timeValidationRejections =
              transactionValidationResult.timeValidationResult.swap.toOption.map {
                case TimeValidator.LedgerTimeRecordTimeDeltaTooLargeError(
                      ledgerTime,
                      recordTime,
                      maxDelta,
                    ) =>
                  LocalReject.TimeRejects.LedgerTime.Reject(
                    s"ledgerTime=$ledgerTime, recordTime=$recordTime, maxDelta=$maxDelta"
                  )(verdictProtocolVersion)
                case TimeValidator.SubmissionTimeRecordTimeDeltaTooLargeError(
                      submissionTime,
                      recordTime,
                      maxDelta,
                    ) =>
                  LocalReject.TimeRejects.SubmissionTime.Reject(
                    s"submissionTime=$submissionTime, recordTime=$recordTime, maxDelta=$maxDelta"
                  )(verdictProtocolVersion)
              }

            // Approve if the consistency check succeeded, reject otherwise.
            val consistencyVerdicts = verdictsForView(viewValidationResult, hostedConfirmingParties)

            val localVerdicts: Seq[LocalVerdict] =
              consistencyVerdicts.toList ++ timeValidationRejections ++
                authenticationRejections ++ authorizationRejections ++ modelConformanceRejections

            val localVerdictAndPartiesO = localVerdicts
              .collectFirst[(LocalVerdict, Set[LfPartyId])] {
                case malformed: Malformed => malformed -> Set.empty
                case localReject: LocalReject if hostedConfirmingParties.nonEmpty =>
                  localReject -> hostedConfirmingParties
              }
              .orElse(
                Option.when(hostedConfirmingParties.nonEmpty)(
                  LocalApprove(protocolVersion) -> hostedConfirmingParties
                )
              )

            localVerdictAndPartiesO.map { case (localVerdict, parties) =>
              checked(
                MediatorResponse
                  .tryCreate(
                    requestId,
                    participantId,
                    Some(viewHash),
                    localVerdict,
                    Some(transactionValidationResult.transactionId.toRootHash),
                    parties,
                    domainId,
                    protocolVersion,
                  )
              )
            }
          }
      }

    if (malformedPayloads.nonEmpty)
      Future.successful(
        malformed(
          LocalReject.MalformedRejects.Payloads
            .Reject(malformedPayloads.toString)(verdictProtocolVersion)
        )
      )
    else {
      val confirmationPolicies = transactionValidationResult.confirmationPolicies
      if (confirmationPolicies.sizeCompare(1) != 0)
        Future.successful(
          malformed(
            LocalReject.MalformedRejects.MultipleConfirmationPolicies
              .Reject(confirmationPolicies.toString)(verdictProtocolVersion)
          )
        )
      else
        responsesForWellformedPayloads(transactionValidationResult, confirmationPolicies.head1)
    }
  }
}
