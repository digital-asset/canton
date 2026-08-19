// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v32
import com.digitalasset.canton.serialization.ProtoConverter.{ParsingResult, parseLfParticipantId}
import com.digitalasset.canton.validation.ProtoValidation
import com.digitalasset.canton.version.ProtocolVersionValidation

final case class DigestForCounterparticipant(
    digest: Digest.DigestType,
    counterparticipant: LedgerParticipantId,
) extends PrettyPrinting {
  override protected def pretty: Pretty[DigestForCounterparticipant.this.type] = prettyOfClass(
    param("digest", _.digest),
    param("counterparticipant", _.counterparticipant),
  )

  def toProtoV32: v32.DigestForCounterparticipant = v32.DigestForCounterparticipant(
    digest = digest,
    counterparticipant = counterparticipant,
  )
}

object DigestForCounterparticipant {

  def fromProtoV32(
      pvv: ProtocolVersionValidation,
      protoMsg: v32.DigestForCounterparticipant,
  ): ParsingResult[DigestForCounterparticipant] = for {

    counterparticipant <- ProtoValidation.validateThen(
      protoMsg.counterparticipant,
      "counterparticipant",
      pvv,
    )(parseLfParticipantId)
  } yield DigestForCounterparticipant(protoMsg.digest, counterparticipant)

}
