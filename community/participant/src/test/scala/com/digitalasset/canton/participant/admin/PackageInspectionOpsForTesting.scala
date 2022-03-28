// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.{
  PackageInUse,
  PackageVetted,
}
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.transaction.{
  TopologyChangeOp,
  TopologyElementId,
  TopologyStateUpdate,
  TopologyStateUpdateElement,
  TopologyTransaction,
  VettedPackages,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

class PackageInspectionOpsForTesting(
    val participantId: ParticipantId,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends PackageInspectionOps {

  override def packageVetted(pkg: PackageId)(implicit
      tc: TraceContext
  ): EitherT[Future, PackageVetted, Unit] =
    EitherT.rightT[Future, PackageVetted](())

  override def packageUnused(packageId: PackageId)(implicit
      tc: TraceContext
  ): EitherT[Future, PackageInUse, Unit] = {
    EitherT.rightT(())
  }

  override def runTx(tx: TopologyTransaction[TopologyChangeOp], force: Boolean)(implicit
      tc: TraceContext
  ): EitherT[Future, ParticipantTopologyManagerError, Unit] = EitherT.rightT(())

  override def genRevokePackagesTx(packages: List[LfPackageId])(implicit
      tc: TraceContext
  ): EitherT[Future, ParticipantTopologyManagerError, TopologyTransaction[TopologyChangeOp]] = {

    val mapping = VettedPackages(participantId, Seq.empty)
    val tx: TopologyTransaction[TopologyChangeOp] = TopologyStateUpdate(
      TopologyChangeOp.Remove,
      TopologyStateUpdateElement(TopologyElementId.generate(), mapping),
    )()

    EitherT.rightT(tx)
  }
}
