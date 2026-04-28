// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.sync.LogicalSynchronizerUpgrade.{
  NewConfig,
  SequencerSuccessors,
  SuccessorConnectionConfiguration,
}
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.transaction.GrpcConnection
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SequencerId, SynchronizerId}

// See invariants in `checkInvariants` below
final case class ManualLsuRequest private (
    currentPsid: PhysicalSynchronizerId,
    successorPsid: PhysicalSynchronizerId,
    upgradeTime: Option[CantonTimestamp],
    successorConnectionConfiguration: SuccessorConnectionConfiguration,
) {
  def lsid: SynchronizerId = currentPsid.logical
}

object ManualLsuRequest {
  import scala.math.Ordered.orderingToOrdered

  private def fromProtoV30(
      sequencerSuccessors: v30.PerformManualLsuRequest.SequencerSuccessors
  ): ParsingResult[Seq[(SequencerId, GrpcConnection)]] =
    sequencerSuccessors.successors.toSeq.traverse { case (sequencerIdP, connectionP) =>
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

      successorConnectionConfiguration <- request.successorConnectionConfiguration match {
        case v30.PerformManualLsuRequest.SuccessorConnectionConfiguration.Empty =>
          ProtoDeserializationError.FieldNotSet("successor_connection_configuration").asLeft

        case v30.PerformManualLsuRequest.SuccessorConnectionConfiguration
              .SequencerSuccessors(value) =>
          fromProtoV30(value).map(successors => SequencerSuccessors(successors.toMap))

        case v30.PerformManualLsuRequest.SuccessorConnectionConfiguration.Config(value) =>
          SynchronizerConnectionConfig.fromProtoV30(value).map(NewConfig(_))
      }

      _ <- checkInvariants(
        currentPsid = currentPsid,
        successorPsid = successorPsid,
        successorConnectionConfiguration = successorConnectionConfiguration,
      ).leftMap(InvariantViolation(None, _))

    } yield ManualLsuRequest(
      currentPsid = currentPsid,
      successorPsid = successorPsid,
      upgradeTime = upgradeTimeO,
      successorConnectionConfiguration = successorConnectionConfiguration,
    )

  private def checkInvariants(
      currentPsid: PhysicalSynchronizerId,
      successorPsid: PhysicalSynchronizerId,
      successorConnectionConfiguration: SuccessorConnectionConfiguration,
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

    _ <- successorConnectionConfiguration match {
      case _: SequencerSuccessors => ().asRight
      case NewConfig(config) =>
        Either.cond(
          config.synchronizerId.forall(_ == successorPsid),
          (),
          s"successor_psid ($successorPsid) differs from the one in the new synchronizer config (${config.synchronizerId})",
        )
    }
  } yield ()
}
