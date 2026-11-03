// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import cats.implicits.{toFlatMapOps, toFunctorOps}
import cats.{FlatMap, Traverse}
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.ledger.participant.state.ReassignmentCommand.{Assign, Unassign}
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ReassignmentTag
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.nonempty.NonEmpty

import scala.annotation.tailrec

sealed trait ReassignmentCommandsBatch

object ReassignmentCommandsBatch {

  final case class Unassignments(
      source: Source[SynchronizerId],
      target: Target[SynchronizerId],
      contractIds: NonEmpty[Seq[LfContractId]],
      overrideSourceValidationPkgIds: Source[Map[LfContractId, LfPackageId]],
      overrideTargetValidationPkgIds: Target[Map[LfContractId, LfPackageId]],
  ) extends ReassignmentCommandsBatch {
    def hasPkgValidationOverrides: Boolean =
      overrideSourceValidationPkgIds.unwrap.nonEmpty || overrideTargetValidationPkgIds.unwrap.nonEmpty
    def extend(
        contractId: LfContractId,
        overrideSourceValidationPkgId: Source[Option[LfPackageId]],
        overrideTargetValidationPkgId: Target[Option[LfPackageId]],
    ): Unassignments = {

      def combine[T[X] <: ReassignmentTag[X]: FlatMap](
          pkgIdsT: T[Map[LfContractId, LfPackageId]],
          pkgIdT: T[Option[LfPackageId]],
      ): T[Map[LfContractId, LfPackageId]] =
        for {
          pkgIds <- pkgIdsT
          pkgId <- pkgIdT
        } yield pkgId.fold(pkgIds)(pkgId => pkgIds.updated(contractId, pkgId))

      this.copy(
        contractIds = contractId +: contractIds,
        overrideSourceValidationPkgIds =
          combine(overrideSourceValidationPkgIds, overrideSourceValidationPkgId),
        overrideTargetValidationPkgIds =
          combine(overrideTargetValidationPkgIds, overrideTargetValidationPkgId),
      )
    }

  }

  final case class Assignments(
      target: Target[SynchronizerId],
      reassignmentId: ReassignmentId,
  ) extends ReassignmentCommandsBatch

  abstract class InvalidBatch(val error: String)
  case object NoCommands extends InvalidBatch("no commands")
  case object MixedAssignWithOtherCommands extends InvalidBatch("mixed assign with other commands")
  case object DifferingSynchronizers extends InvalidBatch("differing synchronizers")

  def create(
      commands: Seq[ReassignmentCommand]
  ): Either[InvalidBatch, ReassignmentCommandsBatch] =
    commands match {
      case Nil => Left(NoCommands)
      case Seq(assign: Assign) =>
        Right(
          Assignments(
            target = assign.targetSynchronizer,
            reassignmentId = assign.reassignmentId,
          )
        )
      case (head: Unassign) +: tail =>
        validateUnassigns(
          Unassignments(
            source = head.sourceSynchronizer,
            target = head.targetSynchronizer,
            contractIds = NonEmpty.mk(Seq, head.contractId),
            overrideSourceValidationPkgIds =
              overridePackageIdMap(head, _.overrideSourceValidationPkgId),
            overrideTargetValidationPkgIds =
              overridePackageIdMap(head, _.overrideTargetValidationPkgId),
          ),
          tail,
        )
      case _ => Left(MixedAssignWithOtherCommands)
    }

  private def overridePackageIdMap[T[X] <: ReassignmentTag[X]: Traverse](
      unassign: Unassign,
      overridePackageId: Unassign => T[Option[LfPackageId]],
  ): T[Map[LfContractId, LfPackageId]] =
    overridePackageId(unassign).map {
      _.fold(Map.empty[LfContractId, LfPackageId])(rpId => Map(unassign.contractId -> rpId))
    }

  @tailrec
  private def validateUnassigns(
      soFar: Unassignments,
      rest: Seq[ReassignmentCommand],
  ): Either[InvalidBatch, Unassignments] = rest match {
    case Nil => Right(soFar.copy(contractIds = soFar.contractIds.reverse))
    case (head: Unassign) +: tail =>
      if (head.sourceSynchronizer == soFar.source && head.targetSynchronizer == soFar.target)
        validateUnassigns(
          soFar.extend(
            head.contractId,
            head.overrideSourceValidationPkgId,
            head.overrideTargetValidationPkgId,
          ),
          tail,
        )
      else
        Left(DifferingSynchronizers)
    case _ => Left(MixedAssignWithOtherCommands)
  }
}
