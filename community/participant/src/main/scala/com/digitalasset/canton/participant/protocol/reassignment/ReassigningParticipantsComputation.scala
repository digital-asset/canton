// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.ReassignmentRef
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentValidationError.StakeholderHostingErrors.{
  stakeholderNotHostedOnSynchronizer,
  stakeholdersNoReassigningParticipant,
}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentValidationError.{
  NonReassigningParticipantsDeclared,
  StakeholderHostingErrors,
}
import com.digitalasset.canton.protocol.Stakeholders
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.SingletonTraverse.syntax.SingletonTraverseOps
import com.digitalasset.canton.util.collection.MapsUtil
import com.digitalasset.canton.util.{ReassignmentTag, SingletonTraverse}
import com.digitalasset.nonempty.NonEmpty

import scala.concurrent.ExecutionContext

private[protocol] class ReassigningParticipantsComputation(
    stakeholders: Stakeholders,
    sourceTopology: Source[TopologySnapshot],
    targetTopology: Target[TopologySnapshot],
)(implicit
    traceContext: TraceContext,
    ec: ExecutionContext,
) {

  /** Compute the list of reassigning participant.
    *
    * Returns an error if:
    *   - one stakeholder is not hosted on some reassigning participant
    *   - one signatory does not have enough signatory reassigning participants to meet the
    *     thresholds defined on both source and target synchronizer
    *
    * This is invoked only during the processing of the unassignment request. The data is then
    * persisted by the reassigning participants and added to the assignment request.
    */
  def compute: EitherT[FutureUnlessShutdown, ReassignmentValidationError, Set[ParticipantId]] =
    for {
      sourceStakeholdersInfo <- getStakeholdersPartyInfo(sourceTopology)
        .leftWiden[ReassignmentValidationError]
      targetStakeholdersInfo <- getStakeholdersPartyInfo(targetTopology)
        .leftWiden[ReassignmentValidationError]

      reassigningParticipants <- EitherT.fromEither[FutureUnlessShutdown](
        computeReassigningParticipants(sourceStakeholdersInfo, targetStakeholdersInfo)
      )

      _ <- EitherT
        .fromEither[FutureUnlessShutdown](
          Seq(sourceStakeholdersInfo, targetStakeholdersInfo)
            .traverse_(checkSignatoryReassigningParticipants(_, reassigningParticipants))
        )
        .leftWiden[ReassignmentValidationError]

    } yield reassigningParticipants.values.toSet.flatten

  /** Check that `declared` is included in the computed reassigning participants and satisfies the
    * conditions listed on [[compute]].
    *
    * @param declared
    *   the reassigning participants declared in the unassignment request
    * @param reassignmentRef
    *   used only to identify the reassignment in the [[NonReassigningParticipantsDeclared]] error
    * @return
    *   unit if `declared` is sufficient, the first failing check otherwise
    */
  def checkSufficient(
      declared: Set[ParticipantId],
      reassignmentRef: ReassignmentRef,
  ): EitherT[FutureUnlessShutdown, ReassignmentValidationError, Unit] =
    for {
      sourceStakeholdersInfo <- getStakeholdersPartyInfo(sourceTopology)
        .leftWiden[ReassignmentValidationError]
      targetStakeholdersInfo <- getStakeholdersPartyInfo(targetTopology)
        .leftWiden[ReassignmentValidationError]

      computed <- EitherT.fromEither[FutureUnlessShutdown](
        computeReassigningParticipants(sourceStakeholdersInfo, targetStakeholdersInfo)
      )
      computedParticipants = computed.values.toSet.flatten

      _ <- EitherT.cond[FutureUnlessShutdown](
        declared.subsetOf(computedParticipants),
        (),
        NonReassigningParticipantsDeclared(
          reassignmentRef,
          targetTimestamp = targetTopology.map(_.timestamp),
          reassigningParticipants = computedParticipants,
          declared = declared,
        ): ReassignmentValidationError,
      )

      // `declared` must be sufficient on its own, so the hosting and threshold checks run on it
      restricted = computed.flatMap { case (party, participants) =>
        NonEmpty.from(participants.intersect(declared)).map(party -> _)
      }

      _ <- EitherT.fromEither[FutureUnlessShutdown](checkAllStakeholdersHosted(restricted))

      _ <- EitherT
        .fromEither[FutureUnlessShutdown](
          Seq(sourceStakeholdersInfo, targetStakeholdersInfo)
            .traverse_(checkSignatoryReassigningParticipants(_, restricted))
        )
        .leftWiden[ReassignmentValidationError]

    } yield ()

  /** Check that every stakeholder is hosted on at least one of the given reassigning participants.
    */
  private def checkAllStakeholdersHosted(
      reassigningParticipants: Map[LfPartyId, NonEmpty[Set[ParticipantId]]]
  ): Either[ReassignmentValidationError, Unit] = {
    val reassigningParticipantsMissingFor = stakeholders.all.diff(reassigningParticipants.keySet)
    Either.cond(
      reassigningParticipantsMissingFor.isEmpty,
      (),
      stakeholdersNoReassigningParticipant(reassigningParticipantsMissingFor),
    )
  }

  /** Check that all signatories are hosted on sufficiently many signatory reassigning participants.
    */
  private def checkSignatoryReassigningParticipants(
      permissions: ReassignmentTag[Map[LfPartyId, PartyInfo]],
      reassigningParticipants: Map[LfPartyId, NonEmpty[Set[ParticipantId]]],
  ): Either[StakeholderHostingErrors, Unit] =
    stakeholders.signatories.toSeq.traverse_ { signatory =>
      for {
        partyInfo <- permissions.unwrap
          .get(signatory)
          .toRight(stakeholderNotHostedOnSynchronizer(Set(signatory), permissions))

        confirmingParticipants = partyInfo.participants.collect {
          case (participantId, attributes) if attributes.canConfirm => participantId
        }.toSet

        signatoryReassigningParticipants = confirmingParticipants.intersect(
          reassigningParticipants.get(signatory).fold(Set.empty[ParticipantId])(_.forgetNE)
        )

        _ <- Either.cond(
          signatoryReassigningParticipants.sizeIs >= partyInfo.threshold.unwrap,
          (),
          StakeholderHostingErrors.missingSignatoryReassigningParticipants(
            signatory,
            synchronizer = permissions.kind,
            threshold = partyInfo.threshold,
            signatoryReassigningParticipants = signatoryReassigningParticipants.size,
          ),
        )
      } yield ()
    }

  /** Compute the reassigning participants. Fails if one stakeholder is not hosted on any
    * reassigning participant.
    *
    * @return
    *   for each stakeholder, the reassigning participants hosting it.
    */
  private def computeReassigningParticipants(
      permissionsSource: Source[Map[LfPartyId, PartyInfo]],
      permissionsTarget: Target[Map[LfPartyId, PartyInfo]],
  ): Either[ReassignmentValidationError, Map[LfPartyId, NonEmpty[Set[ParticipantId]]]] = {

    def hostingParticipants(
        permissions: ReassignmentTag[Map[LfPartyId, PartyInfo]]
    ): Map[LfPartyId, Set[ParticipantId]] = permissions.unwrap.map { case (party, partyInfo) =>
      (party, partyInfo.participants.keySet)
    }

    val reassigningParticipants = MapsUtil
      .intersectValues(
        hostingParticipants(permissionsSource),
        hostingParticipants(permissionsTarget),
      )
      .flatMap { case (party, participants) => NonEmpty.from(participants).map(party -> _) }

    checkAllStakeholdersHosted(reassigningParticipants).map(_ => reassigningParticipants)
  }

  // Returns the list of participants hosting at least one of the stakeholders.
  // Fails if one stakeholder is unknown.
  private def getStakeholdersPartyInfo[T[X] <: ReassignmentTag[X]: SingletonTraverse](
      topologySnapshot: T[TopologySnapshot]
  ): EitherT[FutureUnlessShutdown, StakeholderHostingErrors, T[Map[LfPartyId, PartyInfo]]] =
    EitherT(
      topologySnapshot
        .traverseSingleton((_, topology) =>
          topology.activeParticipantsOfPartiesWithInfo(stakeholders.all.toSeq).map { permissions =>
            val unknownParties = stakeholders.all.diff(permissions.keySet)
            val partiesWithoutParticipants = permissions.filter { case (_, partyInfo) =>
              partyInfo.participants.isEmpty
            }

            val missingParties = unknownParties.union(partiesWithoutParticipants.keySet)

            if (missingParties.isEmpty)
              permissions.asRight
            else
              stakeholderNotHostedOnSynchronizer(missingParties, topologySnapshot).asLeft
          }
        )
        .map(_.sequence)
    )
}
