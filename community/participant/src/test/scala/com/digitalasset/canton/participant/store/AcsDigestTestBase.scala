// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.daml.crypto.MessageDigestPrototype
import com.digitalasset.canton.config.RequireTypes.PositiveLong
import com.digitalasset.canton.crypto.LtHash16Blake3
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  HashedDigest,
  InternedParticipantId,
  LocalPartyFirst,
  PartyAndOrder,
  RawDigest,
  RemotePartyFirst,
}
import com.digitalasset.canton.platform.store.interning.MockStringInterning
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.{BaseTest, InternedPartyId, LfPartyId}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.IdString
import com.google.protobuf.ByteString

import scala.util.ChainingSyntax

trait AcsDigestTestBase extends ChainingSyntax {
  this: BaseTest =>

  protected val rawDigestByteSize = 2048
  protected val sha256Digest = MessageDigestPrototype.Sha256.newDigest
  protected val mockStringInterning = new MockStringInterning()

  protected def genRawDigest(fill: Byte): RawDigest =
    LtHash16Blake3
      .tryCreate(ByteString.copyFrom(Array.fill[Byte](rawDigestByteSize)(fill)))
      .getByteString

  protected def genHashedDigest(rawDigest: RawDigest): HashedDigest =
    sha256Digest
      .digest(rawDigest.toByteArray)
      .pipe(ByteString.copyFrom)

  protected def timestamp(epochSeconds: PositiveLong): CantonTimestamp =
    CantonTimestamp.ofEpochSecond(epochSeconds.unwrap)

  protected def offsetTime(epochSeconds: PositiveLong): (Offset, CantonTimestamp) =
    (Offset.tryFromLong(epochSeconds.unwrap), timestamp(epochSeconds))

  protected def localOrderParty(partyIndex: Int): PartyAndOrder[InternedPartyId] =
    PartyAndOrder[InternedPartyId](internedPartyId(partyIndex), order = LocalPartyFirst)

  protected def remoteOrderParty(partyIndex: Int): PartyAndOrder[InternedPartyId] =
    PartyAndOrder[InternedPartyId](internedPartyId(partyIndex), order = RemotePartyFirst)

  protected def internedPartyId(partyId: LfPartyId): InternedPartyId =
    mockStringInterning.party.internalize(partyId)

  protected def localOrderParty(partyId: LfPartyId): PartyAndOrder[InternedPartyId] =
    PartyAndOrder[InternedPartyId](internedPartyId(partyId), order = LocalPartyFirst)

  protected def remoteOrderParty(partyId: LfPartyId): PartyAndOrder[InternedPartyId] =
    PartyAndOrder[InternedPartyId](internedPartyId(partyId), order = RemotePartyFirst)

  protected def indexedSynchronizer(synchronizerIndex: Int, name: String): IndexedSynchronizer = {
    val synchronizerId: SynchronizerId = SynchronizerId.tryFromString(s"$name::id")
    IndexedSynchronizer.tryCreate(synchronizerId, synchronizerIndex)
  }

  protected def genParticipantDigest(rawDigest: RawDigest): (RawDigest, HashedDigest) =
    (rawDigest, genHashedDigest(rawDigest))

  protected def internedPartyId(partyInt: Int): InternedPartyId =
    mockStringInterning.party.internalize(LfPartyId.assertFromString(s"testParty::$partyInt"))

  protected def internedParticipantId(participantId: Int): InternedParticipantId =
    mockStringInterning.participantId.internalize(
      IdString.ParticipantId.assertFromString(s"testParticipant::$participantId")
    )

  protected def externalizeParticipantId(participantId: InternedParticipantId): Ref.ParticipantId =
    mockStringInterning.participantId.externalize(participantId)
}
