// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.transaction.GrpcConnection
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SequencerId, SynchronizerId}

// See invariants in `checkInvariants` below
final case class ManualLsuRequest private (
    currentPsid: PhysicalSynchronizerId,
    successorPsid: PhysicalSynchronizerId,
    upgradeTime: Option[CantonTimestamp],
    sequencerSuccessors: Map[SequencerId, GrpcConnection],
) {
  def lsid: SynchronizerId = currentPsid.logical
}

object ManualLsuRequest {
  import scala.math.Ordered.orderingToOrdered

  def fromProtoV30(
      request: v30.PerformManualLsuRequest
  ): ParsingResult[ManualLsuRequest] =
    for {
      currentPsid <-
        PhysicalSynchronizerId
          .fromProtoPrimitive(
            request.physicalSynchronizerId,
            "physical_synchronizer_id",
          )

      successorPsid <- PhysicalSynchronizerId
        .fromProtoPrimitive(
          request.successorPhysicalSynchronizerId,
          "successor_physical_synchronizer_id",
        )

      upgradeTimeO <- request.upgradeTime.traverse(CantonTimestamp.fromProtoTimestamp)

      successors <- request.sequencerSuccessors.toSeq.traverse { case (sequencerIdP, connectionP) =>
        for {
          sequencerId <- SequencerId.fromProtoPrimitive(
            sequencerIdP,
            "successor.sequencer_successors.id",
          )
          connection <- GrpcConnection.fromProtoPrimitives(
            connectionP.endpoints,
            connectionP.customTrustCertificates,
          )
        } yield (sequencerId, connection)
      }

      _ <- checkInvariants(
        currentPsid = currentPsid,
        successorPsid = successorPsid,
      ).leftMap(InvariantViolation(None, _))

    } yield ManualLsuRequest(
      currentPsid = currentPsid,
      successorPsid = successorPsid,
      upgradeTime = upgradeTimeO,
      sequencerSuccessors = successors.toMap,
    )

  private def checkInvariants(
      currentPsid: PhysicalSynchronizerId,
      successorPsid: PhysicalSynchronizerId,
  ): Either[String, Unit] = for {
    _ <- Either.cond(
      currentPsid.logical == successorPsid.logical,
      (),
      s"Current and successor physical synchronizer ids must have same logical ids. Found: $currentPsid and $successorPsid",
    )

    _ <- Either.cond(
      currentPsid < successorPsid,
      (),
      s"Current physical synchronizer id must be smaller than the successor. Found: $currentPsid and $successorPsid",
    )
  } yield ()
}
