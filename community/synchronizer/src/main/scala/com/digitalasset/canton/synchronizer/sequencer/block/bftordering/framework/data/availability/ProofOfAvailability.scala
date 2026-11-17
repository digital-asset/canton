// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability

import com.digitalasset.canton.protocol.BftOrdererLimits
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.ProofOfAvailability as ProtoProofOfAvailability
import com.digitalasset.canton.validation.{ProtoValidation, ValidationParam}
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}

final case class ProofOfAvailability(
    batchId: BatchId,
    acks: Seq[AvailabilityAck],
    epochNumber: EpochNumber,
)

object ProofOfAvailability {
  def fromProto(
      pvv: ProtocolVersionValidation,
      limits: BftOrdererLimits,
      value: ProtoProofOfAvailability,
  ): ParsingResult[ProofOfAvailability] =
    for {
      id <- BatchId.fromProto(value.batchId)
      acks <- ProtoValidation.validateLengthThen(
        value.acks,
        "acks",
        pvv,
        ValidationParam(ProtocolVersion.nextValidationBoundPV -> limits.maxAcks),
      ) { (ack, _) =>
        ProtoConverter.parseRequired(
          AvailabilityAck.fromProto(pvv, ack, _),
          "signature",
          ack.signature,
        )
      }
      epochNumber = EpochNumber(value.epochNumber)
    } yield ProofOfAvailability(id, acks, epochNumber)
}
