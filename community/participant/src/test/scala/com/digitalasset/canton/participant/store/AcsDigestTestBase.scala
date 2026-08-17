// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.Eval
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.participant.store.AcsDigestStore.CheckpointType.ReconciliationIntervalBoundary
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  Checkpoint,
  CheckpointType,
  InternedParticipantId,
}
import com.digitalasset.canton.participant.store.memory.{
  InMemoryAcsCommitmentPeriodStore,
  InMemoryAcsDigestStore,
}
import com.digitalasset.canton.platform.store.interning.{MockStringInterning, StringInterning}
import com.digitalasset.canton.protocol.ExampleTransactionFactory
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.topology.{DefaultTestIdentities, SynchronizerId}
import com.digitalasset.canton.{BaseTest, InternedPartyId, LfPartyId}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.IdString

import scala.concurrent.ExecutionContext

trait AcsDigestTestBase extends TestDigestUtils {
  this: BaseTest =>

  protected val mockStringInterning = new MockStringInterning()

  protected def checkpoint(
      offsetTime: (Offset, CantonTimestamp),
      checkpointType: CheckpointType = ReconciliationIntervalBoundary,
  ): Checkpoint =
    Checkpoint(offsetTime._1, offsetTime._2, checkpointType)

  protected def internedPartyId(partyId: LfPartyId): InternedPartyId =
    mockStringInterning.party.internalize(partyId)

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

  def mkInMemoryDigestStore(
      stringInterning: StringInterning = mockStringInterning
  )(implicit ec: ExecutionContext): InMemoryAcsDigestStore =
    InMemoryAcsDigestStore.create(Eval.now(stringInterning), loggerFactory)

  def mkInMemoryPeriodStore(
      stringInterning: StringInterning = mockStringInterning
  )(implicit ec: ExecutionContext): InMemoryAcsCommitmentPeriodStore =
    new InMemoryAcsCommitmentPeriodStore(
      Eval.now(stringInterning),
      loggerFactory,
      enableConsistencyChecks = true,
    )

  val contractId1 = ExampleTransactionFactory.unsuffixedId(1)
  val contractId2 = ExampleTransactionFactory.unsuffixedId(2)
  val contractId3 = ExampleTransactionFactory.unsuffixedId(3)

  val (party1, party2, party3, party4) = (
    DefaultTestIdentities.party1.toLf,
    DefaultTestIdentities.party2.toLf,
    DefaultTestIdentities.party3.toLf,
    DefaultTestIdentities.party4.toLf,
  )

  val (participant1, participant2, participant3) = (
    DefaultTestIdentities.participant1.toLf,
    DefaultTestIdentities.participant2.toLf,
    DefaultTestIdentities.participant3.toLf,
  )
}
