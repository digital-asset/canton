// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.traverse._
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.protocol.version.VersionedStoredParties
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.{HasProtoV0, HasVersionedWrapper, HasVersionedWrapperCompanion}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.LfPartyId

import scala.collection.immutable.SortedSet

// TODO(#3256) get rid of, or at least simplify this; using an array would also allow us to remove the stakeholders_hash column in the commitment_snapshot table
case class StoredParties(parties: SortedSet[LfPartyId])
    extends HasProtoV0[v0.StoredParties]
    with HasVersionedWrapper[VersionedStoredParties] {

  override protected def toProtoV0: v0.StoredParties = v0.StoredParties(parties.toList)
  override protected def toProtoVersioned(version: ProtocolVersion): VersionedStoredParties =
    VersionedStoredParties(version = VersionedStoredParties.Version.V0(toProtoV0))
}

object StoredParties extends HasVersionedWrapperCompanion[VersionedStoredParties, StoredParties] {
  def fromIterable(parties: Iterable[LfPartyId]): StoredParties = StoredParties(
    SortedSet.from(parties)
  )

  override protected def ProtoClassCompanion: VersionedStoredParties.type = VersionedStoredParties
  override protected def name: String = "stored parties"

  def fromProtoVersioned(
      proto: VersionedStoredParties
  ): ParsingResult[StoredParties] = {
    val VersionedStoredParties(version) = proto
    version match {
      case VersionedStoredParties.Version.V0(proto0) => fromProtoV0(proto0)
      case VersionedStoredParties.Version.Empty =>
        Left(FieldNotSet("VersionedStoredParties.version"))
    }
  }

  def fromProtoV0(proto0: v0.StoredParties): ParsingResult[StoredParties] = {
    val v0.StoredParties(partiesP) = proto0
    for {
      parties <- partiesP.traverse(ProtoConverter.parseLfPartyId)
    } yield StoredParties.fromIterable(parties)
  }
}
