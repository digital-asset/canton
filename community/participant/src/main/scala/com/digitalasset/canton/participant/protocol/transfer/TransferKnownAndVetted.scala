// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.{EitherT, OptionT}
import cats.syntax.bifunctor.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.transfer.TransferOutProcessorError.PackageIdUnknownOrUnvetted
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.TransferProcessorError
import com.digitalasset.canton.protocol.{LfContractId, LfTemplateId}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot

import scala.concurrent.ExecutionContext

// TODO(i13202) Adapt and re-use `DomainUsabilityChecker`
private[transfer] sealed abstract case class TransferKnownAndVetted(
    participantIds: Set[ParticipantId],
    topologySnapshot: TopologySnapshot,
    contractId: LfContractId,
    templateId: LfTemplateId,
) {

  private def unknownOrUnvetted(
      participantId: ParticipantId
  )(implicit
      ec: ExecutionContext
  ): OptionT[FutureUnlessShutdown, (ParticipantId, NonEmpty[Set[LfPackageId]])] =
    topologySnapshot
      .findUnvettedPackagesOrDependencies(participantId, Set(templateId.packageId))
      .leftMap(NonEmpty(Set, _))
      .subflatMap(NonEmpty.from(_).toLeft(()))
      .leftMap(participantId -> _)
      .mapK(FutureUnlessShutdown.outcomeK)
      .swap
      .toOption

}

private[transfer] object TransferKnownAndVetted {

  def apply(
      participantIds: Set[ParticipantId],
      topologySnapshot: TopologySnapshot,
      contractId: LfContractId,
      templateId: LfTemplateId,
  )(implicit ec: ExecutionContext): EitherT[FutureUnlessShutdown, TransferProcessorError, Unit] = {
    val validation = new TransferKnownAndVetted(
      participantIds,
      topologySnapshot,
      contractId,
      templateId,
    ) {}
    participantIds.toList
      .parTraverse(validation.unknownOrUnvetted)
      .map(unknownOrUnvetted => PackageIdUnknownOrUnvetted(contractId, unknownOrUnvetted.toMap))
      .toLeft(())
      .leftWiden[TransferProcessorError]
  }

}
