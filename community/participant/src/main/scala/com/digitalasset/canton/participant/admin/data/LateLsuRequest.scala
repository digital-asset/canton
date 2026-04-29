// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerSuccessor}
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.sequencing.SequencerConnectionValidation
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.PhysicalSynchronizerId

/** Request for a late LSU (from a decommissioned synchronizer)
  *
  * See invariants in `checkInvariants` below
  */
final case class LateLsuRequest private (
    currentPsid: PhysicalSynchronizerId,
    successorPsid: PhysicalSynchronizerId,
    upgradeTime: CantonTimestamp,
    successorConfig: SynchronizerConnectionConfig,
    successorConnectionValidation: SequencerConnectionValidation,
) {
  def successor: SynchronizerSuccessor = SynchronizerSuccessor(successorPsid, upgradeTime)
}

object LateLsuRequest {
  import scala.math.Ordered.orderingToOrdered

  def fromProtoV30(
      request: v30.PerformLateLsuRequest
  ): ParsingResult[LateLsuRequest] =
    for {
      currentPsid <-
        PhysicalSynchronizerId
          .fromProtoPrimitive(
            request.physicalSynchronizerId,
            "physical_synchronizer_id",
          )

      successor <- request.successor
        .toRight(ProtoDeserializationError.FieldNotSet("successor"))

      successorPsid <- PhysicalSynchronizerId
        .fromProtoPrimitive(
          successor.physicalSynchronizerId,
          "successor.physical_synchronizer_id",
        )

      upgradeTime <- ProtoConverter
        .parseRequired(
          CantonTimestamp.fromProtoTimestamp,
          "successor.announced_upgrade_time",
          successor.announcedUpgradeTime,
        )

      successorConfig <- ProtoConverter
        .parseRequired(
          SynchronizerConnectionConfig.fromProtoV30,
          "successor.config",
          successor.config,
        )

      validation <- SequencerConnectionValidation.fromProtoV30(
        successor.sequencerConnectionValidation
      )

      _ <- checkInvariants(
        currentPsid = currentPsid,
        successorPsid = successorPsid,
      ).leftMap(InvariantViolation(None, _))

    } yield LateLsuRequest(
      currentPsid = currentPsid,
      successorPsid = successorPsid,
      upgradeTime = upgradeTime,
      successorConfig = successorConfig,
      successorConnectionValidation = validation,
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
