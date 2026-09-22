// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.protocol.v32
import com.digitalasset.canton.serialization.ProtoConverter.{ParsingResult, parseLfParticipantId}
import com.digitalasset.canton.validation.ProtoValidation
import com.digitalasset.canton.version.ProtocolVersionValidation

final case class DigestForCounterparticipant(
    digest: Digest.DigestType,
    counterparticipant: LedgerParticipantId,
) extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[DigestForCounterparticipant] =
    DigestForCounterparticipant

  def toProtoV32: v32.DigestForCounterparticipant = v32.DigestForCounterparticipant(
    digest = digest,
    counterparticipant = counterparticipant,
  )
}

object DigestForCounterparticipant extends PrettyPrintingCompanion[DigestForCounterparticipant] {

  override protected val pretty: Pretty[DigestForCounterparticipant] = prettyOfClass(
    param("digest", _.digest),
    param("counterparticipant", _.counterparticipant),
  )

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
