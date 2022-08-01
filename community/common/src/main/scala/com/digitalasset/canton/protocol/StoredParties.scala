// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.traverse._
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.{
  HasVersionedMessageCompanion,
  HasVersionedWrapper,
  ProtocolVersion,
  VersionedMessage,
}

import scala.collection.immutable.SortedSet

// TODO(#3256) get rid of, or at least simplify this; using an array would also allow us to remove the stakeholders_hash column in the commitment_snapshot table
case class StoredParties(parties: SortedSet[LfPartyId])
    extends HasVersionedWrapper[VersionedMessage[StoredParties]] {

  protected def toProtoV0: v0.StoredParties = v0.StoredParties(parties.toList)
  protected def toProtoVersioned(
      version: ProtocolVersion
  ): VersionedMessage[StoredParties] =
    VersionedMessage(toProtoV0.toByteString, 0)
}

object StoredParties extends HasVersionedMessageCompanion[StoredParties] {
  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersion(v0.StoredParties)(fromProtoV0)
  )

  def fromIterable(parties: Iterable[LfPartyId]): StoredParties = StoredParties(
    SortedSet.from(parties)
  )

  override protected def name: String = "stored parties"

  def fromProtoV0(proto0: v0.StoredParties): ParsingResult[StoredParties] = {
    val v0.StoredParties(partiesP) = proto0
    for {
      parties <- partiesP.traverse(ProtoConverter.parseLfPartyId)
    } yield StoredParties.fromIterable(parties)
  }
}
