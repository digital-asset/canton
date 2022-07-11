// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.syntax.functor._
import cats.syntax.traverse._
import cats.syntax.traverseFilter._
import com.digitalasset.canton.data.ConfirmingParty
import com.digitalasset.canton.error.TransactionError
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.MalformedPayload
import com.digitalasset.canton.protocol.messages.LocalReject.Malformed
import com.digitalasset.canton.protocol.messages._
import com.digitalasset.canton.protocol.{ConfirmationPolicy, LfContractId, RequestId, ViewHash}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, checked}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

class ConfirmationResponseFactory(
    participantId: ParticipantId,
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  import com.digitalasset.canton.util.ShowUtil._

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
        .traverseFilter {
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
        confirmationPolicy: ConfirmationPolicy,
    )(implicit
        traceContext: TraceContext,
        ec: ExecutionContext,
    ): Future[Map[LocalVerdict, Set[LfPartyId]]] = {
      val viewHash = viewValidationResult.view.unwrap.viewHash

      val hostedConfirmingPartiesF =
        hostedConfirmingPartiesOfView(viewValidationResult, confirmationPolicy)

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

      hostedConfirmingPartiesF.map { hostedConfirmingParties =>
        if (hostedConfirmingParties.isEmpty)
          Map.empty[LocalVerdict, Set[LfPartyId]]
        else if (existing.nonEmpty)
          Map(
            logged(
              LocalReject.ConsistencyRejections.CreatesExistingContracts
                .Reject(existing.toSeq.map(_.coid))
            ) -> hostedConfirmingParties
          )
        else {
          val verdicts = mutable.Map.empty[LocalVerdict, Set[LfPartyId]]

          def addVerdictForParty(party: LfPartyId, verdict: LocalVerdict): Unit = {
            val oldParties = verdicts.getOrElse(verdict, Set.empty[LfPartyId])
            val _ = verdicts += verdict -> (oldParties + party)
          }

          def stakeholderOfUsedContract(party: LfPartyId)(coid: LfContractId): Boolean =
            viewValidationResult.view.viewParticipantData.coreInputs
              .get(coid)
              .exists(_.stakeholders.contains(party))

          // All informees are stakeholders of created contracts in the core of the view.
          // It therefore suffices to deal with input contracts by stakeholder.
          val createdAbsolute =
            viewValidationResult.view.viewParticipantData.createdCore
              .map(_.contract.contractId)
              .toSet
          val lockedForActivation = alreadyLocked intersect createdAbsolute

          hostedConfirmingParties.foreach { party =>
            val inactiveInputs = inactive.filter(stakeholderOfUsedContract(party))
            val lockedInputs = alreadyLocked.filter(stakeholderOfUsedContract(party))
            val lockedKeys = alreadyLockedKeys.collect {
              case (key, maintainers) if maintainers.contains(party) => key
            }.toSeq
            val duplicateKeysForParty = duplicateKeys.collect {
              case (key, maintainers) if maintainers.contains(party) => key
            }.toSeq
            val inconsistentKeysForParty = inconsistentKeys.collect {
              case (key, maintainers) if maintainers.contains(party) => key
            }.toSeq
            if (inactiveInputs.nonEmpty)
              addVerdictForParty(
                party,
                LocalReject.ConsistencyRejections.InactiveContracts
                  .Reject(inactiveInputs.toSeq.map(_.coid)),
              )
            else if (duplicateKeysForParty.nonEmpty)
              addVerdictForParty(
                party,
                LocalReject.ConsistencyRejections.DuplicateKey
                  .Reject(duplicateKeysForParty.map(_.toString())),
              )
            else if (inconsistentKeysForParty.nonEmpty)
              addVerdictForParty(
                party,
                LocalReject.ConsistencyRejections.InconsistentKey
                  .Reject(inconsistentKeysForParty.map(_.toString())),
              )
            else if (lockedInputs.nonEmpty | lockedForActivation.nonEmpty) {
              val allLocked = lockedForActivation ++ lockedInputs
              addVerdictForParty(
                party,
                LocalReject.ConsistencyRejections.LockedContracts
                  .Reject(allLocked.toSeq.map(_.coid)),
              )
            } else if (lockedKeys.nonEmpty)
              addVerdictForParty(
                party,
                LocalReject.ConsistencyRejections.LockedKeys.Reject(lockedKeys.map(_.toString())),
              )
            else
              addVerdictForParty(party, LocalApprove)
          }
          verdicts.toMap
        }
      }
    }

    def wellformedPayloads(
        transactionValidationResult: TransactionValidationResult,
        confirmationPolicy: ConfirmationPolicy,
    ): Future[Seq[MediatorResponse]] = {
      val localVerdictsF: Future[Map[ViewHash, Map[LocalVerdict, Set[LfPartyId]]]] =
        transactionValidationResult.modelConformanceResult match {

          case Left(cause) =>
            val verdict = logged(
              LocalReject.MalformedRejects.ModelConformance.Reject(cause.toString)
            )
            Future.successful(transactionValidationResult.viewValidationResults.fmap {
              _viewValidationResult =>
                Map[LocalVerdict, Set[LfPartyId]](verdict -> Set.empty)
            })
          case Right(_) =>
            transactionValidationResult.timeValidationResult.fold(
              err =>
                transactionValidationResult.viewValidationResults.toList
                  .traverse { case (k, viewValidationResult) =>
                    val partiesF =
                      hostedConfirmingPartiesOfView(viewValidationResult, confirmationPolicy)
                    partiesF.map { parties =>
                      val ret =
                        if (parties.isEmpty) Map.empty[LocalVerdict, Set[LfPartyId]]
                        else {
                          // Note, this is logged in the TimeValidator
                          val rej = err match {
                            case TimeValidator.LedgerTimeRecordTimeDeltaTooLargeError(
                                  ledgerTime,
                                  recordTime,
                                  maxDelta,
                                ) =>
                              LocalReject.TimeRejects.LedgerTime.Reject(
                                s"ledgerTime=$ledgerTime, recordTime=$recordTime, maxDelta=$maxDelta"
                              )
                            case TimeValidator.SubmissionTimeRecordTimeDeltaTooLargeError(
                                  submissionTime,
                                  recordTime,
                                  maxDelta,
                                ) =>
                              LocalReject.TimeRejects.SubmissionTime.Reject(
                                s"submissionTime=$submissionTime, recordTime=$recordTime, maxDelta=$maxDelta"
                              )
                          }
                          Map[LocalVerdict, Set[LfPartyId]](rej -> parties)
                        }

                      (k, ret)
                    }
                  }
                  .map(_.toMap),
              { case () =>
                transactionValidationResult.viewValidationResults.toList
                  .traverse { case (k, v) =>
                    verdictsForView(v, confirmationPolicy).map(r => (k, r))
                  }
                  .map(_.toMap)
              },
            )
        }

      val rootHash = Some(transactionValidationResult.transactionId.toRootHash)
      localVerdictsF.map { localVerdicts =>
        localVerdicts.toSeq.flatMap { case (viewHash, localVerdicts) =>
          localVerdicts.toSeq.map { case (localVerdict, parties) =>
            checked(
              MediatorResponse
                .tryCreate(
                  requestId,
                  participantId,
                  Some(viewHash),
                  localVerdict,
                  rootHash,
                  parties,
                  domainId,
                  protocolVersion,
                )
            )
          }
        }
      }
    }

    if (malformedPayloads.nonEmpty)
      Future.successful(
        malformed(LocalReject.MalformedRejects.Payloads.Reject(malformedPayloads.toString))
      )
    else {
      val confirmationPolicies = transactionValidationResult.confirmationPolicies
      if (confirmationPolicies.sizeCompare(1) != 0)
        Future.successful(
          malformed(
            LocalReject.MalformedRejects.MultipleConfirmationPolicies
              .Reject(confirmationPolicies.toString)
          )
        )
      else
        wellformedPayloads(transactionValidationResult, confirmationPolicies.head1)
    }
  }
}
