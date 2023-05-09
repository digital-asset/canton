// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.EitherT
import cats.syntax.alternative.*
import cats.syntax.bifunctor.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.lf.engine.Error as LfError
import com.daml.lf.interpretation.Error as LfInterpretationError
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.DomainSnapshotSyncCryptoApi
import com.digitalasset.canton.data.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.transfer.TransferInValidation.*
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.*
import com.digitalasset.canton.participant.protocol.{ProcessingSteps, SingleDomainCausalTracker}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.condUnitET
import com.digitalasset.canton.util.EitherUtil.condUnitE
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.Transfer.TargetProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future}

private[transfer] class TransferInValidation(
    domainId: TargetDomainId,
    participantId: ParticipantId,
    engine: DAMLe,
    transferCoordination: TransferCoordination,
    causalityTracking: Boolean,
    targetProtocolVersion: TargetProtocolVersion,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging {

  def checkStakeholders(
      transferInRequest: FullTransferInTree
  )(implicit traceContext: TraceContext): EitherT[Future, TransferProcessorError, Unit] = {
    val transferId = transferInRequest.transferOutResultEvent.transferId

    val declaredContractStakeholders = transferInRequest.contract.metadata.stakeholders
    val declaredViewStakeholders = transferInRequest.stakeholders

    for {
      metadata <- engine
        .contractMetadata(
          transferInRequest.contract.contractInstance,
          declaredContractStakeholders,
        )
        .leftMap {
          case LfError.Interpretation(
                e @ LfError.Interpretation.DamlException(
                  LfInterpretationError.FailedAuthorization(_, _)
                ),
                _,
              ) =>
            StakeholdersMismatch(
              Some(transferId),
              declaredViewStakeholders = declaredViewStakeholders,
              declaredContractStakeholders = Some(declaredContractStakeholders),
              expectedStakeholders = Left(e.message),
            )
          case error => MetadataNotFound(error)
        }
      recomputedStakeholders = metadata.stakeholders
      _ <- condUnitET[Future](
        declaredViewStakeholders == recomputedStakeholders && declaredViewStakeholders == declaredContractStakeholders,
        StakeholdersMismatch(
          Some(transferId),
          declaredViewStakeholders = declaredViewStakeholders,
          declaredContractStakeholders = Some(declaredContractStakeholders),
          expectedStakeholders = Right(recomputedStakeholders),
        ),
      ).leftWiden[TransferProcessorError]
    } yield ()
  }

  // The transferring participant must send on the causal state at the time of the transfer-out.
  // This state is sent to all participants hosting a party that the transferring participant confirms for.
  def checkCausalityState(causalityLookup: SingleDomainCausalTracker)(
      targetCrypto: DomainSnapshotSyncCryptoApi,
      confirmFor: Set[LfPartyId],
      transferringParticipant: Boolean,
      transferId: TransferId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, List[(CausalityMessage, Recipients)]] = {
    if (transferringParticipant && causalityTracking) {
      val clocksF = causalityLookup.awaitAndFetchTransferOut(transferId, confirmFor)
      for {
        clocks <- EitherT.liftF(clocksF)
        confirmForClocks <- {
          val (noInfoFor, clocksList) =
            confirmFor.toList.map(p => clocks.get(p).toRight(left = p)).separate

          val either = if (noInfoFor.isEmpty) {
            Right(clocksList)
          } else {
            logger.error(
              s"Transferring participant is missing causality information for ${noInfoFor}."
            )
            Left(CausalityInformationMissing(transferId, missingFor = noInfoFor.toSet))
          }
          EitherT.fromEither[Future](either)
        }

        recipients <- EitherT.liftF {
          confirmForClocks.parTraverse { clock =>
            val hostedBy =
              targetCrypto.ipsSnapshot.activeParticipantsOf(clock.partyId).map(_.keySet)
            hostedBy.map(ptps => (clock, ptps))
          }
        }

        causalityMessages = {
          recipients.flatMap { case (clock, hostedBy) =>
            val msg = CausalityMessage(domainId, targetProtocolVersion.v, transferId, clock)
            logger.debug(
              s"Sending causality message for $transferId with clock $clock to $hostedBy"
            )
            Recipients.ofSet(hostedBy).map(msg -> _).toList
          }
        }
      } yield causalityMessages
    } else EitherT.rightT[Future, TransferProcessorError](List.empty)
  }

  @VisibleForTesting
  private[transfer] def validateTransferInRequest(
      tsIn: CantonTimestamp,
      transferInRequest: FullTransferInTree,
      transferDataO: Option[TransferData],
      targetCrypto: DomainSnapshotSyncCryptoApi,
      transferringParticipant: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, Option[TransferInValidationResult]] = {
    val txOutResultEvent = transferInRequest.transferOutResultEvent.result

    val transferId = transferInRequest.transferOutResultEvent.transferId

    def checkSubmitterIsStakeholder: Either[TransferProcessorError, Unit] =
      condUnitE(
        transferInRequest.stakeholders.contains(transferInRequest.submitter),
        SubmittingPartyMustBeStakeholderIn(
          transferId,
          transferInRequest.submitter,
          transferInRequest.stakeholders,
        ),
      )

    val targetIps = targetCrypto.ipsSnapshot

    transferDataO match {
      case Some(transferData) =>
        val sourceDomain = transferData.transferOutRequest.sourceDomain
        val transferOutTimestamp = transferData.transferOutTimestamp
        for {
          _ready <- {
            logger.info(
              s"Waiting for topology state at ${transferOutTimestamp} on transfer-out domain $sourceDomain ..."
            )
            EitherT(
              transferCoordination
                .awaitTransferOutTimestamp(sourceDomain, transferOutTimestamp)
                .sequence
            )
          }

          sourceCrypto <- transferCoordination.cryptoSnapshot(
            sourceDomain.unwrap,
            transferOutTimestamp,
          )
          // TODO(M40): Check the signatures of the mediator and the sequencer

          _ <- condUnitET[Future](
            txOutResultEvent.content.timestamp <= transferData.transferOutDecisionTime,
            ResultTimestampExceedsDecisionTime(
              transferId,
              timestamp = txOutResultEvent.content.timestamp,
              decisionTime = transferData.transferOutDecisionTime,
            ),
          )

          // TODO(M40): Validate the shipped transfer-out result w.r.t. stakeholders
          // TODO(M40): Validate that the transfer-out result received matches the transfer-out result in transferData

          _ <- condUnitET[Future](
            transferInRequest.contract == transferData.contract,
            ContractDataMismatch(transferId),
          )
          _ <- EitherT.fromEither[Future](checkSubmitterIsStakeholder)
          transferOutSubmitter = transferData.transferOutRequest.submitter
          targetTimeProof = transferData.transferOutRequest.targetTimeProof.timestamp

          // TODO(M40): Check that transferData.transferOutRequest.targetTimeProof.timestamp is in the past
          cryptoSnapshot <- transferCoordination
            .cryptoSnapshot(transferData.targetDomain.unwrap, targetTimeProof)

          exclusivityLimit <- ProcessingSteps
            .getTransferInExclusivity(
              cryptoSnapshot.ipsSnapshot,
              targetTimeProof,
            )
            .leftMap[TransferProcessorError](TransferParametersError(domainId.unwrap, _))

          _ <- condUnitET[Future](
            tsIn >= exclusivityLimit
              || transferOutSubmitter == transferInRequest.submitter,
            NonInitiatorSubmitsBeforeExclusivityTimeout(
              transferId,
              transferInRequest.submitter,
              currentTimestamp = tsIn,
              timeout = exclusivityLimit,
            ),
          )
          _ <- condUnitET[Future](
            transferData.creatingTransactionId == transferInRequest.creatingTransactionId,
            CreatingTransactionIdMismatch(
              transferId,
              transferInRequest.creatingTransactionId,
              transferData.creatingTransactionId,
            ),
          )
          sourceIps = sourceCrypto.ipsSnapshot
          confirmingParties <- EitherT.right(
            transferInRequest.stakeholders.toList.parTraverseFilter { stakeholder =>
              for {
                source <- sourceIps.canConfirm(participantId, stakeholder)
                target <- targetIps.canConfirm(participantId, stakeholder)
              } yield if (source && target) Some(stakeholder) else None
            }
          )

        } yield Some(TransferInValidationResult(confirmingParties.toSet))
      case None =>
        for {
          _ <- EitherT.fromEither[Future](checkSubmitterIsStakeholder)
          res <-
            if (transferringParticipant) {
              val targetIps = targetCrypto.ipsSnapshot
              val confirmingPartiesF = transferInRequest.stakeholders.toList
                .parTraverseFilter { stakeholder =>
                  targetIps
                    .canConfirm(participantId, stakeholder)
                    .map(if (_) Some(stakeholder) else None)
                }
                .map(_.toSet)
              EitherT(confirmingPartiesF.map { confirmingParties =>
                Right(Some(TransferInValidationResult(confirmingParties))): Either[
                  TransferProcessorError,
                  Option[TransferInValidationResult],
                ]
              })
            } else EitherT.rightT[Future, TransferProcessorError](None)
        } yield res
    }
  }
}

object TransferInValidation {
  final case class TransferInValidationResult(confirmingParties: Set[LfPartyId])

  private[transfer] sealed trait TransferInValidationError extends TransferProcessorError

  final case class NoTransferData(
      transferId: TransferId,
      lookupError: TransferStore.TransferLookupError,
  ) extends TransferInValidationError {
    override def message: String =
      s"Cannot find transfer data for transfer `$transferId`: ${lookupError.cause}"
  }

  final case class TransferOutIncomplete(transferId: TransferId, participant: ParticipantId)
      extends TransferInValidationError {
    override def message: String =
      s"Cannot transfer-in `$transferId` because transfer-out is incomplete"
  }

  final case class PartyNotHosted(
      transferId: TransferId,
      party: LfPartyId,
      participant: ParticipantId,
  ) extends TransferInValidationError {
    override def message: String =
      s"Cannot transfer-in `$transferId` because $party is not hosted on $participant"
  }

  final case class NoParticipantForReceivingParty(transferId: TransferId, party: LfPartyId)
      extends TransferInValidationError {
    override def message: String = s"Cannot transfer-in `$transferId` because $party is not active"
  }

  final case class UnexpectedDomain(
      transferId: TransferId,
      targetDomain: DomainId,
      receivedOn: DomainId,
  ) extends TransferInValidationError {
    override def message: String =
      s"Cannot transfer-in `$transferId`: expecting domain `$targetDomain` but received on `$receivedOn`"
  }

  final case class ResultTimestampExceedsDecisionTime(
      transferId: TransferId,
      timestamp: CantonTimestamp,
      decisionTime: CantonTimestamp,
  ) extends TransferInValidationError {
    override def message: String =
      s"Cannot transfer-in `$transferId`: result time $timestamp exceeds decision time $decisionTime"
  }

  final case class NonInitiatorSubmitsBeforeExclusivityTimeout(
      transferId: TransferId,
      submitter: LfPartyId,
      currentTimestamp: CantonTimestamp,
      timeout: CantonTimestamp,
  ) extends TransferInValidationError {
    override def message: String =
      s"Cannot transfer-in `$transferId`: only submitter can initiate before exclusivity timeout $timeout"
  }

  final case class ContractDataMismatch(transferId: TransferId) extends TransferInValidationError {
    override def message: String = s"Cannot transfer-in `$transferId`: contract data mismatch"
  }

  final case class CreatingTransactionIdMismatch(
      transferId: TransferId,
      transferInTransactionId: TransactionId,
      localTransactionId: TransactionId,
  ) extends TransferInValidationError {
    override def message: String =
      s"Cannot transfer-in `$transferId`: creating transaction id mismatch"
  }

}
