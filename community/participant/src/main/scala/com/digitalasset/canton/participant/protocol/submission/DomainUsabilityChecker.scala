// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import com.daml.lf.transaction.TransactionVersion
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.protocol.submission.DomainUsabilityChecker.*
import com.digitalasset.canton.protocol.PackageInfoService
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.{DamlLfVersionToProtocolVersions, ProtocolVersion}
import com.digitalasset.canton.{LfPackageId, LfPartyId}

import scala.concurrent.{ExecutionContext, Future}

private[submission] sealed trait DomainUsabilityChecker[E <: DomainNotUsedReason] {
  def isUsable: EitherT[Future, E, Unit]
}

class DomainUsabilityCheckerFull(
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    snapshot: TopologySnapshot,
    requiredPackagesByParty: Map[LfPartyId, Set[LfPackageId]],
    packageInfoService: PackageInfoService,
    localParticipantId: ParticipantId,
    transactionVersion: TransactionVersion,
)(implicit
    ec: ExecutionContext,
    traceContext: TraceContext,
) extends DomainUsabilityChecker[DomainNotUsedReason] {
  override def isUsable: EitherT[Future, DomainNotUsedReason, Unit] = {
    val parties: Set[LfPartyId] = requiredPackagesByParty.keys.toSet

    val vetting = new DomainUsabilityCheckerVetting[DomainNotUsedReason](
      domainId,
      snapshot,
      requiredPackagesByParty,
      packageInfoService,
      localParticipantId,
    )

    val partiesConnected =
      new DomainUsabilityCheckerPartiesConnected[DomainNotUsedReason](domainId, snapshot, parties)

    val protocolVersionChecker = new DomainUsabilityCheckProtocolVersion[DomainNotUsedReason](
      domainId = domainId,
      protocolVersion = protocolVersion,
      transactionVersion = transactionVersion,
    )

    val checkers: Seq[DomainUsabilityChecker[DomainNotUsedReason]] =
      Seq(vetting, partiesConnected, protocolVersionChecker)

    checkers.parTraverse_(_.isUsable)
  }
}

/** The following is checked:
  *
  * - Every party in `parties` is hosted by an active participant on domain `domainId`
  */
class DomainUsabilityCheckerPartiesConnected[E >: MissingActiveParticipant <: DomainNotUsedReason](
    domainId: DomainId,
    snapshot: TopologySnapshot,
    parties: Set[LfPartyId],
)(implicit
    ec: ExecutionContext
) extends DomainUsabilityChecker[E] {
  override def isUsable: EitherT[Future, E, Unit] = snapshot
    .allHaveActiveParticipants(parties, _.isActive)
    .leftMap(MissingActiveParticipant(domainId, _))
}

/** The following is checked:
  *
  * - For every (`party`, `pkgs`) in `requiredPackagesByParty`
  *
  * - For every participant `P` hosting `party`
  *
  * - All packages `pkgs` are vetted by `P` on domain `domainId`
  *
  * Note: in order to avoid false errors, it is important that the set of packages needed
  * for the parties hosted locally covers the set of packages needed for all the parties.
  *
  * This is guaranteed in the following situations:
  *
  * - Phase 1:
  * Because the submitting participant hosts one of the authorizers, which sees the whole
  * transaction. Hence, they need all the packages necessary for the transaction.
  *
  * - Phase 3:
  * The participant receives a projection for the parties it hosts. Hence, the packages
  * needed for these parties will be sufficient to re-interpret the whole projection.
  */
class DomainUsabilityCheckerVetting[E >: UnknownPackage <: DomainNotUsedReason](
    domainId: DomainId,
    snapshot: TopologySnapshot,
    requiredPackagesByParty: Map[LfPartyId, Set[LfPackageId]],
    packageInfoService: PackageInfoService,
    localParticipantId: ParticipantId,
)(implicit
    ec: ExecutionContext,
    traceContext: TraceContext,
) extends DomainUsabilityChecker[E] {
  def isUsable: EitherT[Future, E, Unit] = EitherT(
    for {
      requiredPerParticipant <- requiredPackagesByParticipant
      unknownPackages <- requiredPerParticipant.toList
        .parFlatTraverse { case (participant, packages) =>
          unknownPackages(participant, packages)
        }
    } yield Either.cond(
      unknownPackages.isEmpty,
      (),
      UnknownPackage(domainId, unknownPackages.toSet),
    )
  )

  private def unknownPackages(
      participantId: ParticipantId,
      required: Set[LfPackageId],
  ): Future[List[PackageUnknownTo]] =
    snapshot.findUnvettedPackagesOrDependencies(participantId, required).value.flatMap {
      case Right(notVetted) =>
        notVetted.toList.parTraverse(packageId =>
          packageInfoService.getDescription(packageId).map { descriptionOpt =>
            val description = descriptionOpt
              .map(_.sourceDescription.unwrap)
              .getOrElse("package does not exist on local node")

            PackageUnknownTo(packageId, description, participantId)
          }
        )
      case Left(missingPackageId) =>
        Future.successful(
          List(
            PackageUnknownTo(
              missingPackageId,
              "package missing on local participant",
              localParticipantId,
            )
          )
        )
    }

  private def requiredPackagesByParticipant: Future[Map[ParticipantId, Set[LfPackageId]]] = {
    requiredPackagesByParty.toList.foldM(Map.empty[ParticipantId, Set[LfPackageId]]) {
      case (acc, (party, packages)) =>
        for {
          // fetch all participants of this party
          participants <- snapshot.activeParticipantsOf(party)
        } yield {
          // add the required packages for this party to the set of required packages of this participant
          participants.foldLeft(acc) { case (res, (participantId, _)) =>
            res.updated(participantId, res.getOrElse(participantId, Set()).union(packages))
          }
        }
    }
  }
}

class DomainUsabilityCheckProtocolVersion[
    E >: DomainNotSupportingMinimumProtocolVersion <: DomainNotUsedReason
](
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    transactionVersion: TransactionVersion,
)(implicit
    ec: ExecutionContext
) extends DomainUsabilityChecker[E] {
  override def isUsable: EitherT[Future, E, Unit] = {
    val minimumPVForTransaction =
      DamlLfVersionToProtocolVersions.getMinimumSupportedProtocolVersion(
        transactionVersion
      )

    EitherTUtil.condUnitET(
      protocolVersion >= minimumPVForTransaction,
      DomainNotSupportingMinimumProtocolVersion(
        domainId,
        protocolVersion,
        minimumPVForTransaction,
        transactionVersion,
      ),
    )
  }
}

object DomainUsabilityChecker {
  sealed trait DomainNotUsedReason extends PrettyPrinting {
    def domainId: DomainId
  }

  final case class MissingActiveParticipant(domainId: DomainId, parties: Set[LfPartyId])
      extends DomainNotUsedReason {
    override def pretty: Pretty[MissingActiveParticipant] =
      prettyOfString(err =>
        s"Parties ${err.parties} don't have an active participant on domain ${err.domainId}"
      )
  }

  final case class UnknownPackage(domainId: DomainId, unknownTo: Set[PackageUnknownTo])
      extends DomainNotUsedReason {
    override def pretty: Pretty[UnknownPackage] =
      prettyOfString(err =>
        show"Some packages are not known to all informees on domain ${err.domainId}.\n${err.unknownTo}"
      )
  }

  final case class PackageUnknownTo(
      packageId: LfPackageId,
      description: String,
      participantId: ParticipantId,
  ) extends PrettyPrinting {
    override def pretty: Pretty[PackageUnknownTo] = prettyOfString { put =>
      show"Participant $participantId has not vetted ${put.description.doubleQuoted} (${put.packageId})"
    }
  }

  final case class DomainNotSupportingMinimumProtocolVersion(
      domainId: DomainId,
      currentPV: ProtocolVersion,
      requiredPV: ProtocolVersion,
      lfVersion: TransactionVersion,
  ) extends DomainNotUsedReason {

    override def pretty: Pretty[DomainNotSupportingMinimumProtocolVersion] = prettyOfString { _ =>
      show"The transaction uses a specific LF version $lfVersion that is supported starting protocol version: $requiredPV." +
        show"Currently the Domain $domainId is using $currentPV"
    }
  }
}
