// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.ReassignmentProcessorError
import com.digitalasset.canton.protocol.{ContractInstance, LfContractId}
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag
import com.digitalasset.canton.{LfPackageId, LfPackageName}
import com.digitalasset.daml.lf.data.Ref.PackageId

import scala.concurrent.ExecutionContext

object ValidationPackage {

  def choose[T[X] <: ReassignmentTag[X]](
      contract: ContractInstance,
      overrideValidationPackageIds: T[Map[LfContractId, LfPackageId]],
      snapshot: T[TopologySnapshot],
      packageMetadata: PackageMetadata,
      validatedContractPackageIds: Map[LfContractId, Set[LfPackageId]],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, LfPackageId] =
    overrideValidationPackageIds.unwrap.get(contract.contractId) match {
      case Some(packageId) =>
        EitherT.rightT[FutureUnlessShutdown, ReassignmentProcessorError](packageId)

      case None =>
        val packageName = contract.inst.packageName
        for {
          participants <- EitherT.right(
            snapshot.unwrap
              .activeParticipantsOfParties(contract.stakeholders.toSeq)
              .map(_.values.flatten.toSet)
          )
          vettedPackages <- EitherT.right(snapshot.unwrap.vettedPackages(participants))
          validatedPackages = validatedContractPackageIds.getOrElse(contract.contractId, Set.empty)
          packageIdO = findHighestCommonPackage(
            packageName,
            participants,
            packageMetadata,
            vettedPackages,
            validatedPackages,
            snapshot.unwrap.timestamp,
          )
          packageId <- EitherT.fromOption[FutureUnlessShutdown](
            packageIdO,
            ReassignmentProcessingSteps.NoCommonPackage(
              contract.contractId,
              packageName,
              participants,
            ): ReassignmentProcessorError,
          )
        } yield packageId
    }

  /** compute the highest versions packageId vetted by all participants */
  private def findHighestCommonPackage(
      packageName: LfPackageName,
      participants: Set[ParticipantId],
      packageMetadata: PackageMetadata,
      vettedPackages: Map[ParticipantId, Set[VettedPackage]],
      validatedPackages: Set[LfPackageId],
      timestamp: CantonTimestamp,
  ): Option[LfPackageId] = {

    val packageNamePackageIds: Set[PackageId] =
      packageMetadata.packageNameMap
        .get(packageName)
        .map(_.allPackageIdsForName.forgetNE)
        .getOrElse(Set.empty)

    val vettedPackage = participants.toSeq
      .map(
        vettedPackages
          .getOrElse(_, Set.empty)
          .collect {
            case vettedPackage if vettedPackage.validAt(timestamp) =>
              vettedPackage.packageId
          }
      )
      .foldLeft(packageNamePackageIds)(_ intersect _)

    val vettedValidatedPackages = vettedPackage intersect validatedPackages

    val candidates =
      if (vettedValidatedPackages.nonEmpty) vettedValidatedPackages else vettedPackage

    candidates
      .flatMap(id => packageMetadata.packageIdVersionMap.get(id).map(v => (id, v)))
      .maxByOption(_._2)
      .map(_._1)

  }
}
