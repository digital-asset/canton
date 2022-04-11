// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either._
import com.digitalasset.canton.{LfPartyId, ProtoDeserializationError}
import com.digitalasset.canton.data.Informee.InvalidInformee
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.HasProtoV0

/** A party that must be informed about the view.
  */
// This class is a reference example of serialization best practices.
// In particular, it demonstrates serializing a trait with different subclasses.
// The design is quite simple. It should be applied whenever possible, but it will not cover all cases.
//
// Please consult the team if you intend to change the design of serialization.
sealed trait Informee
    extends Product
    with Serializable
    with HasProtoV0[v0.Informee]
    with PrettyPrinting {
  def party: LfPartyId

  def weight: Int

  /** Creates the v0-proto version of an informee.
    *
    * Plain informees get weight 0.
    * Confirming parties get their assigned (positive) weight.
    */
  override def toProtoV0: v0.Informee =
    v0.Informee(party = party, weight = weight)

  override def pretty: Pretty[Informee] =
    prettyOfString(inst => inst.party.show + "*" + inst.weight.show)
}

object Informee {
  def tryCreate(party: LfPartyId, weight: Int): Informee =
    if (weight == 0) PlainInformee(party) else ConfirmingParty(party, weight)

  def create(party: LfPartyId, weight: Int): Either[String, Informee] =
    Either.catchOnly[InvalidInformee](tryCreate(party, weight)).leftMap(_.message)

  def fromProtoV0(informeeP: v0.Informee): ParsingResult[Informee] = {
    val v0.Informee(partyString, weight) = informeeP
    for {
      party <- LfPartyId
        .fromString(partyString)
        .leftMap(ProtoDeserializationError.ValueDeserializationError("party", _))
      informee <- Informee
        .create(party, weight)
        .leftMap(err =>
          ProtoDeserializationError.OtherError(s"Unable to deserialize informee data: $err")
        )
    } yield informee
  }

  case class InvalidInformee(message: String) extends RuntimeException(message)
}

/** A party that must confirm the underlying view.
  *
  * @param weight determines the impact of the party on whether the view is approved.
  * @throws com.digitalasset.canton.data.Informee$.InvalidInformee if `weight` is not positive
  */
final case class ConfirmingParty(party: LfPartyId, weight: Int) extends Informee {
  if (weight <= 0)
    throw InvalidInformee(s"Unable to create a confirming party with non-positive weight $weight.")
}

/** An informee that is not a confirming party
  */
final case class PlainInformee(party: LfPartyId) extends Informee {
  override val weight = 0
}
