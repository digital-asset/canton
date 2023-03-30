// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.*
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import cats.{Applicative, MonoidK}
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.*
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantAttributes
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.FutureInstances.*

import scala.concurrent.{ExecutionContext, Future}

object TransferOutRequestValidation {

  import com.digitalasset.canton.util.ShowUtil.*

  /** Holds information about what (admin) parties and participants need to be involved in
    * performing a transfer-out of a certain contract.
    * @param adminParties The admin parties for each transfer-out participant i.e. hosting a signatory
    *                          with confirmation rights on both the source and target domains.
    * @param participants All participants hosting at least one stakeholder (i.e., including observers
    *                          not only signatories), regardless of their permission.
    */
  private[transfer] sealed abstract case class AdminPartiesAndParticipants(
      adminParties: Set[LfPartyId],
      participants: Set[ParticipantId],
  )

  private[protocol] def adminPartiesWithSubmitterCheck(
      participantId: ParticipantId,
      contractId: LfContractId,
      submitter: LfPartyId,
      stakeholders: Set[LfPartyId],
      sourceIps: TopologySnapshot,
      targetIps: TopologySnapshot,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, TransferProcessorError, AdminPartiesAndParticipants] = {
    for {
      canSubmit <- EitherT.right(
        sourceIps.hostedOn(submitter, participantId).map(_.exists(_.permission == Submission))
      )
      _ <- EitherTUtil.condUnitET[Future](
        canSubmit,
        NoSubmissionPermissionOut(contractId, submitter, participantId),
      )
      adminPartiesAndRecipients <- TransferOutRequestValidation.adminPartiesWithoutSubmitterCheck(
        contractId,
        submitter,
        stakeholders,
        sourceIps,
        targetIps,
        logger,
      )
    } yield {
      adminPartiesAndRecipients
    }
  }

  private[transfer] def adminPartiesWithoutSubmitterCheck(
      contractId: LfContractId,
      submitter: LfPartyId,
      stakeholders: Set[LfPartyId],
      sourceIps: TopologySnapshot,
      targetIps: TopologySnapshot,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, TransferProcessorError, AdminPartiesAndParticipants] = {

    val stakeholdersWithParticipantPermissionsF =
      PartyParticipantPermissions(stakeholders, sourceIps, targetIps)

    for {
      stakeholdersWithParticipantPermissions <- EitherT.right(
        stakeholdersWithParticipantPermissionsF
      )
      _ <- EitherT.cond[Future](
        stakeholders.contains(submitter),
        (),
        SubmittingPartyMustBeStakeholderOut(contractId, submitter, stakeholders),
      )
      transferOutParticipants <- EitherT.fromEither[Future](
        transferOutParticipants(stakeholdersWithParticipantPermissions)
      )
      transferOutAdminParties <- EitherT(
        transferOutParticipants.toList
          .parTraverse(adminPartyFor(sourceIps, logger))
          .map(
            _.sequence.toEither
              .bimap(
                errors => AdminPartyPermissionErrors(stringOfNec(errors)),
                _.toSet,
              )
          )
      ).leftWiden[TransferProcessorError]
    } yield {
      val participantsPermissionsSourceDomain =
        stakeholdersWithParticipantPermissions.permissions.values
          .map(_._1)

      val participants = participantsPermissionsSourceDomain.foldLeft(Set.empty[ParticipantId]) {
        case (acc, permissions) => acc ++ permissions.all
      }

      new AdminPartiesAndParticipants(transferOutAdminParties, participants) {}
    }
  }

  private def adminPartyFor(sourceIps: TopologySnapshot, logger: TracedLogger)(
      participant: ParticipantId
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[ValidatedNec[String, LfPartyId]] = {
    val adminParty = participant.adminParty.toLf
    confirmingAdminParticipants(sourceIps, adminParty, logger).map { adminParticipants =>
      Validated.condNec(
        adminParticipants.get(participant).exists(_.permission.canConfirm),
        adminParty,
        s"Transfer-out participant $participant cannot confirm on behalf of its admin party.",
      )
    }
  }

  def checkAdminParticipantCanConfirm(sourceIps: TopologySnapshot, logger: TracedLogger)(
      adminParty: LfPartyId
  )(implicit traceContext: TraceContext, ec: ExecutionContext): Future[ValidatedNec[String, Unit]] =
    confirmingAdminParticipants(sourceIps, adminParty, logger).map { adminParticipants =>
      Validated.condNec(
        adminParticipants.exists { case (participant, _) =>
          participant.adminParty.toLf == adminParty
        },
        (),
        s"Admin party $adminParty not hosted on its transfer-out participant with confirmation permission.",
      )
    }

  private def confirmingAdminParticipants(
      ips: TopologySnapshot,
      adminParty: LfPartyId,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[Map[ParticipantId, ParticipantAttributes]] = {
    ips.activeParticipantsOf(adminParty).map(_.filter(_._2.permission.canConfirm)).map {
      adminParticipants =>
        if (adminParticipants.sizeCompare(1) != 0)
          logger.warn(
            s"Admin party $adminParty hosted with confirmation permission on ${adminParticipants.keySet.toString}."
          )
        adminParticipants
    }
  }

  /** Computes the transfer-out participants for the transfers and checks the following hosting requirements for each party:
    * <ul>
    * <li>If the party is hosted on a participant with [[identity.ParticipantPermission$.Submission$]] permission,
    * then at least one such participant must also have [[identity.ParticipantPermission$.Submission$]] permission
    * for that party on the target domain.
    * This ensures that the party can initiate the transfer-in if needed and continue to use the contract on the
    * target domain, unless the permissions change in between.
    * </li>
    * <li>The party must be hosted on a participant that has [[identity.ParticipantPermission$.Confirmation$]] permission
    * on both domains for this party.
    * </li>
    * </ul>
    */
  private def transferOutParticipants(
      participantsByParty: PartyParticipantPermissions
  ): Either[TransferProcessorError, Set[ParticipantId]] = {

    participantsByParty.permissions.toList
      .traverse { case (party, (source, target)) =>
        val submissionPermissionCheck = Validated.condNec(
          source.submitters.isEmpty || source.submitters.exists(target.submitters.contains),
          (),
          show"For party $party, no participant with submission permission on source domain (at ${participantsByParty.sourceTs}) has submission permission on target domain (at ${participantsByParty.targetTs}).",
        )

        val transferOutParticipants = source.confirmers.intersect(target.confirmers)
        val confirmersOverlap =
          Validated.condNec(
            transferOutParticipants.nonEmpty,
            transferOutParticipants,
            show"No participant of the party $party has confirmation permission on both domains at respective timestamps ${participantsByParty.sourceTs} and ${participantsByParty.targetTs}.",
          )

        Applicative[ValidatedNec[String, *]]
          .productR(submissionPermissionCheck)(confirmersOverlap)
      }
      .bimap(errors => PermissionErrors(stringOfNec(errors)), MonoidK[Set].algebra.combineAll)
      .toEither
  }

  private def stringOfNec[A](chain: NonEmptyChain[String]): String = chain.toList.mkString(", ")

  sealed trait TransferOutProcessorError extends TransferProcessorError

  final case class UnexpectedDomain(transferId: TransferId, receivedOn: DomainId)
      extends TransferOutProcessorError {
    override def message: String =
      s"Cannot transfer-out `$transferId`: received transfer on $receivedOn"
  }

  final case class TargetDomainIsSourceDomain(domain: DomainId, contractId: LfContractId)
      extends TransferOutProcessorError {
    override def message: String =
      s"Cannot transfer-out contract `$contractId`: source and target domains are the same"
  }

  final case class UnknownContract(contractId: LfContractId) extends TransferOutProcessorError {
    override def message: String = s"Cannot transfer-out contract `$contractId`: unknown contract"
  }

  final case class InvalidResult(
      transferId: TransferId,
      result: DeliveredTransferOutResult.InvalidTransferOutResult,
  ) extends TransferOutProcessorError {
    override def message: String = s"Cannot transfer-out `$transferId`: invalid result"
  }

  final case class AutomaticTransferInError(message: String) extends TransferOutProcessorError

  final case class PermissionErrors(message: String) extends TransferOutProcessorError

  final case class AdminPartyPermissionErrors(message: String) extends TransferOutProcessorError

  final case class StakeholderHostingErrors(message: String) extends TransferOutProcessorError

  final case class AdminPartiesMismatch(
      contractId: LfContractId,
      expected: Set[LfPartyId],
      declared: Set[LfPartyId],
  ) extends TransferOutProcessorError {
    override def message: String =
      s"Cannot transfer-out contract `$contractId`: admin parties mismatch"
  }

  final case class RecipientsMismatch(
      contractId: LfContractId,
      expected: Option[Recipients],
      declared: Recipients,
  ) extends TransferOutProcessorError {
    override def message: String =
      s"Cannot transfer-out contract `$contractId`: recipients mismatch"
  }

  final case class ContractStoreFailed(transferId: TransferId, error: ContractStoreError)
      extends TransferProcessorError {
    override def message: String =
      s"Cannot transfer-out `$transferId`: internal contract store error"
  }

}
