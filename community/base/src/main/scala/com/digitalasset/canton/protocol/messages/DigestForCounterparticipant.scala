// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.protocol.messages.AcsCommitment.digestValidationParam
import com.digitalasset.canton.protocol.v32
import com.digitalasset.canton.serialization.ProtoConverter.{ParsingResult, parseLfParticipantId}
import com.digitalasset.canton.validation.ProtoValidation
import com.digitalasset.canton.version.ProtocolVersionValidation

final case class DigestForCounterparticipant private (
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

  def create(
      pvv: ProtocolVersionValidation,
      digest: Digest.DigestType,
      counterparticipant: LedgerParticipantId,
  ): Either[String, DigestForCounterparticipant] =
    ProtoValidation
      .validateLength(digest, "digest", pvv, AcsCommitment.digestValidationParam)
      .leftMap(_.message)
      .map(new DigestForCounterparticipant(_, counterparticipant))

  def fromProtoV32(
      pvv: ProtocolVersionValidation,
      protoMsg: v32.DigestForCounterparticipant,
  ): ParsingResult[DigestForCounterparticipant] =
    for {
      _ <- ProtoValidation.validateLength(
        protoMsg.digest,
        "digest",
        pvv,
        digestValidationParam,
      )
      counterparticipant <- ProtoValidation.validateThen(
        protoMsg.counterparticipant,
        "counterparticipant",
        pvv,
      )(parseLfParticipantId)
      // Direct instantiation bypasses `create` limit check because length-validations are done above
    } yield new DigestForCounterparticipant(protoMsg.digest, counterparticipant)
}
