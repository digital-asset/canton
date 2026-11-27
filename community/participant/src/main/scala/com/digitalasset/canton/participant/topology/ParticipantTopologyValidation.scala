// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import cats.implicits.catsSyntaxSemigroup
import cats.syntax.either.*
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLogging}
import com.digitalasset.canton.participant.protocol.reassignment.IncompleteReassignmentData
import com.digitalasset.canton.participant.store.memory.PackageMetadataView
import com.digitalasset.canton.participant.store.{
  AcsInspection,
  DamlPackageStore,
  ReassignmentStore,
}
import com.digitalasset.canton.platform.store.backend.LedgerEnd
import com.digitalasset.canton.protocol.ReassignmentId
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.topology.TopologyManagerError.ParticipantTopologyManagerError.*
import com.digitalasset.canton.topology.transaction.{HostingParticipant, ParticipantPermission}
import com.digitalasset.canton.topology.{
  ForceFlag,
  ForceFlags,
  ParticipantId,
  PartyId,
  SynchronizerId,
  TopologyManagerError,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.nonempty.NonEmpty

import scala.concurrent.ExecutionContext
import scala.math.Ordered.orderingToOrdered

import PackageMetadata.Implicits.packageMetadataSemigroup

trait ParticipantTopologyValidation extends NamedLogging {
  def validatePackageVetting(
      currentlyVettedPackages: Set[LfPackageId],
      nextPackageIds: Set[LfPackageId],
      packageMetadataView: PackageMetadataView,
      dryRunSnapshot: PackageMetadata,
      forceFlags: ForceFlags,
      disableUpgradeValidation: Boolean,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] = {
    val toBeAdded = nextPackageIds -- currentlyVettedPackages
    val packageMetadataSnapshot = packageMetadataView.getSnapshot |+| dryRunSnapshot
    for {
      _ <-
        checkPackageDependencies(
          toBeAdded,
          packageMetadataView.packageStore,
          dryRunSnapshot,
          forceFlags,
        )
      _ <- EitherT.fromEither[FutureUnlessShutdown] {
        if (
          disableUpgradeValidation || forceFlags.permits(ForceFlag.AllowVetIncompatibleUpgrades)
        ) {
          logger.info(
            show"Skipping upgrade validation for newly-added packages $toBeAdded because force flag ${ForceFlag.AllowVetIncompatibleUpgrades.toString} is set"
          )
          Right(())
        } else
          packageMetadataView.packageUpgradeValidator.validateUpgrade(
            toBeAdded,
            nextPackageIds,
            packageMetadataSnapshot.packages,
          )(LoggingContextWithTrace(loggerFactory))
      }
    } yield ()
  }

  def checkInsufficientParticipantPermissionForSignatoryParty(
      party: PartyId,
      forceFlags: ForceFlags,
      acsInspections: Map[SynchronizerId, AcsInspection],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
    MonadUtil.sequentialTraverse_(acsInspections) { case (synchronizerId, acsInspection) =>
      EitherT(
        acsInspection
          .isSignatoryOnActiveContracts(party)
          .map {
            case true
                if !forceFlags
                  .permits(ForceFlag.AllowInsufficientParticipantPermissionForSignatoryParty) =>
              Left(
                InsufficientParticipantPermissionForSignatoryParty.Reject(party, synchronizerId)
              )
            case true =>
              logger.info(
                s"Allow changing the permission of $party to a pure observer with active contracts on $synchronizerId, " +
                  s"where it is a signatory, because the force flag ${ForceFlag.AllowInsufficientParticipantPermissionForSignatoryParty} is set."
              )
              Either.unit
            case false =>
              Either.unit
          }
      )
    }

  def checkCannotDisablePartyWithActiveContracts(
      party: PartyId,
      forceFlags: ForceFlags,
      acsInspections: Map[SynchronizerId, AcsInspection],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
    MonadUtil.sequentialTraverse_(acsInspections) { case (synchronizerId, acsInspection) =>
      EitherT(
        acsInspection
          .hasActiveContracts(party)
          .map {
            case true if !forceFlags.permits(ForceFlag.DisablePartyWithActiveContracts) =>
              Left(
                DisablePartyWithActiveContractsRequiresForce.Reject(party, synchronizerId)
              )
            case true =>
              logger.debug(
                s"Allow to disable $party with active contracts on $synchronizerId because force flag ${ForceFlag.DisablePartyWithActiveContracts} is set."
              )
              Either.unit
            case false =>
              Either.unit
          }
      )
    }

  /** Checks that changing the party to participant mapping does not result in stuck assignments.
    * List all incomplete reassignments for the party and checks for each of them:
    *   - If the party is a signatory, there will still be enough signatory-assigning participants
    *     after the mapping change.
    *   - Otherwise, there is another participant still hosting the party.
    */
  def checkInsufficientSignatoryAssigningParticipantsForParty(
      party: PartyId,
      currentThreshold: PositiveInt,
      nextThresholdO: Option[PositiveInt],
      nextHostingParticipants: Seq[HostingParticipant],
      forceFlags: ForceFlags,
      reassignmentStores: Map[SynchronizerId, ReassignmentStore],
      ledgerEnd: () => Option[LedgerEnd],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] = {
    // The participants that this check expects to confirm assignments under the next mapping:
    // - Onboarding entries are left out: they do not confirm until their onboarding completes.
    // - Nominated and Freezing entries are included: they do not confirm yet, but can only move on
    //   towards Active, where they confirm, or be removed.
    // HostingParticipant.canConfirm tells whether an entry confirms now and accepts Active only.
    val nextConfirmingParticipants = nextHostingParticipants
      .filter(p => p.permission >= ParticipantPermission.Confirmation && !p.onboarding)
      .map(_.participantId)
      .toSet

    def rejectUnlessForced(synchronizerId: SynchronizerId, reassignmentId: ReassignmentId)(
        error: => TopologyManagerError
    ): Either[TopologyManagerError, Unit] =
      if (forceFlags.permits(ForceFlag.AllowInsufficientSignatoryAssigningParticipantsForParty)) {
        logger.debug(
          s"Allow to change party to participant mapping for $party with incomplete reassignments, such as $reassignmentId, on $synchronizerId because force flag ${ForceFlag.AllowInsufficientSignatoryAssigningParticipantsForParty} is set."
        )
        Either.unit
      } else Left(error)

    def checkAsSignatory(
        synchronizerId: SynchronizerId,
        reassigningParticipants: Map[ReassignmentId, Set[ParticipantId]],
    ): Either[TopologyManagerError, Unit] =
      nextThresholdO match {
        // if the next threshold is not defined, it means we are deactivating the party
        case None =>
          reassigningParticipants.keys.headOption.fold(Either.unit[TopologyManagerError])(
            reassignmentId =>
              rejectUnlessForced(synchronizerId, reassignmentId)(
                InsufficientSignatoryAssigningParticipantsForParty
                  .RejectRemovingParty(party, synchronizerId, reassignmentId)
              )
          )
        case Some(nextThreshold) =>
          // find the first reassignment that has less reassigning participants than the threshold
          reassigningParticipants.view
            .mapValues(_.intersect(nextConfirmingParticipants))
            .find(_._2.sizeIs < nextThreshold.value)
            .fold(Either.unit[TopologyManagerError]) {
              case (reassignmentId, signatoryAssigningParticipants) =>
                rejectUnlessForced(synchronizerId, reassignmentId)(
                  if (nextThreshold > currentThreshold)
                    InsufficientSignatoryAssigningParticipantsForParty.RejectThresholdIncrease(
                      party,
                      synchronizerId,
                      reassignmentId,
                      nextThreshold,
                      signatoryAssigningParticipants,
                    )
                  else
                    InsufficientSignatoryAssigningParticipantsForParty
                      .RejectNotEnoughSignatoryAssigningParticipants(
                        party,
                        synchronizerId,
                        reassignmentId,
                        currentThreshold,
                        signatoryAssigningParticipants,
                      )
                )
            }
      }

    // The party is not a signatory of the reassigned contracts: it lacks a host, not
    // signatory-assigning participants.
    def checkAsObserver(
        synchronizerId: SynchronizerId,
        reassignmentIds: Seq[ReassignmentId],
    ): Either[TopologyManagerError, Unit] =
      reassignmentIds.headOption
        .filter(_ => nextHostingParticipants.isEmpty)
        .fold(Either.unit[TopologyManagerError])(reassignmentId =>
          rejectUnlessForced(synchronizerId, reassignmentId)(
            nextThresholdO match {
              case None =>
                InsufficientSignatoryAssigningParticipantsForParty
                  .RejectRemovingParty(party, synchronizerId, reassignmentId)
              case Some(_) =>
                InsufficientSignatoryAssigningParticipantsForParty
                  .RejectNoHostingParticipant(party, synchronizerId, reassignmentId)
            }
          )
        )

    MonadUtil.sequentialTraverse_(reassignmentStores) { case (synchronizerId, reassignmentStore) =>
      EitherT(
        reassignmentStore
          .findIncomplete(
            validAt = ledgerEnd().map(_.lastOffset).getOrElse(Offset.firstOffset),
            stakeholders = NonEmpty.from(Set(party.toLf)),
            limit = NonNegativeInt.maxValue,
          )
          .map { incompleteReassignments =>
            // Assignments are confirmed by the signatories' participants; observers need only be
            // hosted on one participant.
            val (asSignatory, asObserver) = incompleteReassignments
              .collect {
                case IncompleteReassignmentData(
                      reassignmentId,
                      Some(reassigningParticipants),
                      Some(signatories),
                      _,
                      _,
                    ) =>
                  (reassignmentId, reassigningParticipants, signatories.contains(party.toLf))
              }
              .partition { case (_, _, isSignatory) => isSignatory }

            for {
              _ <- checkAsSignatory(
                synchronizerId,
                asSignatory.map { case (reassignmentId, reassigningParticipants, _) =>
                  reassignmentId -> reassigningParticipants
                }.toMap,
              )
              _ <- checkAsObserver(
                synchronizerId,
                asObserver.map { case (reassignmentId, _, _) => reassignmentId },
              )
            } yield ()
          }
      )
    }
  }

  /** @param packageStore
    *   used for security relevant validations, as packageMetadataSnapshot gets stale when packages
    *   are deleted
    * @param dryRunSnapshot
    *   additional packages that should also be considered available
    */
  private def checkPackageDependencies(
      toBeAdded: Set[LfPackageId],
      packageStore: DamlPackageStore,
      dryRunSnapshot: PackageMetadata,
      forceFlags: ForceFlags,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
    EitherT(for {
      // Using the package store instead of packageMetadataSnapshot,
      // as packageMetadataSnapshot could become stale at time of writing.
      tooBeAddedInPackageStore <- packageStore.filterExisting(toBeAdded)
    } yield {
      val toBeAddedInDryRunSnapshot = toBeAdded.filter(dryRunSnapshot.packages.contains)
      val knownToBeAdded = tooBeAddedInPackageStore ++ toBeAddedInDryRunSnapshot

      val unknownToBeAdded = toBeAdded -- knownToBeAdded
      if (unknownToBeAdded.nonEmpty && !forceFlags.permits(ForceFlag.AllowUnknownPackage))
        Left(CannotVetDueToMissingPackages.Missing(unknownToBeAdded))
      else Right(())
    })
}
