// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.participant.store.AcsDigestStore.CheckpointType.ReconciliationIntervalBoundary
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  Checkpoint,
  InternedParticipantId,
  LocalPartyFirst,
  PartyAndOrder,
  RemotePartyFirst,
}
import com.digitalasset.canton.platform.store.interning.MockStringInterning
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.{BaseTest, InternedPartyId, LfPartyId}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.IdString

trait AcsDigestTestBase extends TestDigestUtils {
  this: BaseTest =>

  protected val mockStringInterning = new MockStringInterning()

  protected def checkpoint(offsetTime: (Offset, CantonTimestamp)): Checkpoint =
    Checkpoint(offsetTime._1, offsetTime._2, ReconciliationIntervalBoundary)

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

  protected def internedPartyId(partyInt: Int): InternedPartyId =
    mockStringInterning.party.internalize(LfPartyId.assertFromString(s"testParty::$partyInt"))

  protected def internedParticipantId(participantId: Int): InternedParticipantId =
    mockStringInterning.participantId.internalize(
      IdString.ParticipantId.assertFromString(s"testParticipant::$participantId")
    )

  protected def externalizeParticipantId(participantId: InternedParticipantId): Ref.ParticipantId =
    mockStringInterning.participantId.externalize(participantId)
}
