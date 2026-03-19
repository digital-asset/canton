// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.ledger.api.{
  ListVettedPackagesOpts,
  ParticipantVettedPackages,
  PriorTopologySerial,
  SinglePackageTargetVetting,
  UpdateVettedPackagesForceFlags,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.PackageInUse
import com.digitalasset.canton.participant.topology.{PackageOps, ParticipantTopologyManagerError}
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.topology.{ForceFlags, ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{LfPackageId, config}
import com.digitalasset.daml.lf.data.Ref.PackageId

import scala.concurrent.ExecutionContext

class PackageOpsForTesting(
    val participantId: ParticipantId,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends PackageOps {

  override def synchronizersWithVettedPackageEntry(packageIds: Set[PackageId])(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, Map[PackageId, NonEmpty[
    Set[PhysicalSynchronizerId]
  ]]] =
    EitherT.rightT(Map.empty)

  override def checkPackageUnused(packageId: PackageId)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, PackageInUse, Unit] =
    EitherT.rightT(())

  override def revokeVettingForPackages(
      packages: List[LfPackageId],
      darDescriptor: PackageService.DarDescription,
      psid: PhysicalSynchronizerId,
      forceFlags: ForceFlags,
      waitToBecomeEffective: Option[config.NonNegativeFiniteDuration],
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, RpcError, Unit] =
    EitherT.rightT(())

  override def vetPackages(
      packages: Seq[PackageId],
      synchronizeVetting: PackageVettingSynchronization,
      waitToBecomeEffective: Option[config.NonNegativeFiniteDuration],
      psid: PhysicalSynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] =
    EitherT.rightT(())

  override def getVettedPackages(
      opts: ListVettedPackagesOpts
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Seq[
    ParticipantVettedPackages
  ]] =
    EitherT.rightT(Seq())

  override def updateVettedPackages(
      targetStates: Seq[SinglePackageTargetVetting[PackageId]],
      psid: PhysicalSynchronizerId,
      synchronizeVetting: PackageVettingSynchronization,
      waitToBecomeEffective: Option[config.NonNegativeFiniteDuration],
      dryRunSnapshot: Option[PackageMetadata],
      expectedTopologySerial: Option[PriorTopologySerial],
      updateForceFlags: Option[UpdateVettedPackagesForceFlags] = None,
  )(implicit
      tc: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ParticipantTopologyManagerError,
    (Option[ParticipantVettedPackages], Option[ParticipantVettedPackages]),
  ] =
    EitherT.rightT((None, None))
}
