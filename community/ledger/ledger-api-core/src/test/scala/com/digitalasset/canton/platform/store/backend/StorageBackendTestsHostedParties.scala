// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.ledger.api.ParticipantId
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent.{
  Added,
  ChangedTo,
  Onboarding,
  Revoked,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.{
  AuthorizationEvent,
  AuthorizationLevel,
}
import com.digitalasset.canton.platform.store.backend.PartyStorageBackend.{
  HostedPartyPageBound,
  HostingState,
  PartyHosting,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.nonempty.NonEmpty
import org.scalactic.source
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsHostedParties extends Matchers with StorageBackendSpec {
  this: AnyFlatSpec =>

  import StorageBackendTestValues.*

  private val someParticipantId2 = ParticipantId(Ref.ParticipantId.assertFromString("participant2"))
  private val someParticipantId3 = ParticipantId(Ref.ParticipantId.assertFromString("participant3"))
  private val someSynchronizerId3 = SynchronizerId.tryFromString("x::anothersynchronizer")

  private def recordTime(eventSeqId: Int): Timestamp = Timestamp.assertFromLong(eventSeqId * 1000L)

  private def ptp(
      eventSeqId: Int,
      partyId: Int,
      participantId: Int,
      synchronizerId: Int,
      authorizationEvent: AuthorizationEvent = Added(Submission),
  ): DbDto.EventPartyToParticipant =
    dtoPartyToParticipant(
      offset = offset(eventSeqId.toLong),
      eventSequentialId = eventSeqId.toLong,
      party = interning.party.externalize(partyId),
      participant = ParticipantId(interning.participantId.externalize(participantId)),
      authorizationEvent = authorizationEvent,
      synchronizerId = interning.synchronizerId.externalize(synchronizerId),
      recordTime = recordTime(eventSeqId),
    )

  private def ingestPtp(dtos: DbDto.EventPartyToParticipant*): Unit = {
    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(dtos.toVector, _))
    executeSql(
      updateLedgerEnd(
        offset(dtos.map(_.event_offset).maxOption.getOrElse(1L)),
        ledgerEndSequentialId = dtos.map(_.event_sequential_id).maxOption.getOrElse(0L),
      )
    )
  }

  behavior of "StorageBackend hostedPartyPageBoundary"

  private def pageBoundary(
      validAtEventSeqId: Int,
      limit: Int,
      fromParty: Int,
      fromParticipant: Int,
      fromSynchronizer: Int,
      partyIds: Option[NonEmpty[Set[Int]]],
      participantId: Option[Int],
  ): Option[HostedPartyPageBound] =
    executeSql(
      backend.party.hostedPartyPageBoundary(
        partyIds = partyIds,
        participantId = participantId,
        fromInclusive = HostedPartyPageBound(fromParty, fromParticipant, fromSynchronizer),
        limit = limit,
        validAtEventSeqId = validAtEventSeqId.toLong,
      )
    )

  private def verifyPageBoundary(
      validAtEventSeqId: Int,
      limit: Int,
      fromParty: Int = 0,
      fromParticipant: Int = 0,
      fromSynchronizer: Int = 0,
      partyIds: Option[NonEmpty[Set[Int]]] = None,
      participantId: Option[Int] = None,
  )(
      expectedParty: Int,
      expectedParticipant: Int,
      expectedSynchronizer: Int,
  )(implicit pos: source.Position): Assertion =
    pageBoundary(
      validAtEventSeqId,
      limit,
      fromParty,
      fromParticipant,
      fromSynchronizer,
      partyIds,
      participantId,
    ).value shouldBe HostedPartyPageBound(expectedParty, expectedParticipant, expectedSynchronizer)

  it should "start the page at the inclusive bound" in {
    // These fixtures follow interning order rather than lexicographical order
    val party1 = someParty.interned
    val participant1 = someParticipantId.interned
    val synchronizer1 = someSynchronizerId.interned

    val party2 = someParty3.interned
    val participant2 = someParticipantId3.interned
    val synchronizer2 = someSynchronizerId2.interned

    val party3 = someParty2.interned
    val participant3 = someParticipantId2.interned
    val synchronizer3 = someSynchronizerId3.interned

    // Ingest in an order other than the triplet order, so the results do not
    // accidentally depend on the insertion order.
    val triplets = for {
      party <- Seq(party3, party1, party2)
      participant <- Seq(participant3, participant1, participant2)
      synchronizer <- Seq(synchronizer3, synchronizer1, synchronizer2)
    } yield (party, participant, synchronizer)

    ingestPtp(triplets.zipWithIndex.map { case ((party, participant, synchronizer), index) =>
      ptp(index + 1, party, participant, synchronizer)
    }*)

    forAll(
      Table(
        ("case", "from inclusive", "expected page start"),
        (
          "the minimal bound starts at the first triplet",
          (0, 0, 0),
          (party1, participant1, synchronizer1),
        ),
        (
          "a bound matching a triplet exactly starts at that triplet",
          (party2, participant2, synchronizer2),
          (party2, participant2, synchronizer2),
        ),
        (
          "a bound between synchronizers starts at the next synchronizer",
          (party2, participant2, synchronizer2 + 1),
          (party2, participant2, synchronizer3),
        ),
        (
          "a bound past the last synchronizer starts at the next participant",
          (party2, participant2, synchronizer3 + 1),
          (party2, participant3, synchronizer1),
        ),
        (
          "a bound past the last synchronizer of the last participant starts at the next party",
          (party2, participant3, synchronizer3 + 1),
          (party3, participant1, synchronizer1),
        ),
        (
          "a bound between participants starts at the next participant",
          (party2, participant1 + 1, synchronizer1),
          (party2, participant2, synchronizer1),
        ),
        (
          "a bound past the last participant starts at the next party",
          (party2, participant3 + 1, synchronizer1),
          (party3, participant1, synchronizer1),
        ),
        (
          "a bound between parties starts at the next party",
          (party2 + 1, participant1, synchronizer1),
          (party3, participant1, synchronizer1),
        ),
      )
    ) {
      case (
            description,
            (fromParty, fromParticipant, fromSynchronizer),
            (party, participant, synchronizer),
          ) =>
        clue(description) {
          verifyPageBoundary(
            validAtEventSeqId = 27,
            limit = 1,
            fromParty = fromParty,
            fromParticipant = fromParticipant,
            fromSynchronizer = fromSynchronizer,
          )(
            expectedParty = party,
            expectedParticipant = participant,
            expectedSynchronizer = synchronizer,
          )
        }
    }

    clue("a bound past the final triplet returns no page") {
      pageBoundary(
        validAtEventSeqId = 27,
        limit = 1,
        fromParty = party3,
        fromParticipant = participant3,
        fromSynchronizer = synchronizer3 + 1,
        partyIds = None,
        participantId = None,
      ) shouldBe empty
    }
  }

  it should "return the last triplet as specified by limit" in {
    val party1 = someParty.interned
    val party2 = someParty2.interned
    val party3 = someParty3.interned
    val participant1 = someParticipantId.interned
    val participant2 = someParticipantId2.interned
    val synchronizer1 = someSynchronizerId.interned
    val synchronizer2 = someSynchronizerId2.interned
    ingestPtp(
      ptp(5, party1, participant1, synchronizer1),
      ptp(1, party1, participant2, synchronizer1),
      ptp(2, party1, participant1, synchronizer2),
      ptp(4, party2, participant2, synchronizer1),
      ptp(3, party3, participant1, synchronizer1),
    )

    verifyPageBoundary(validAtEventSeqId = 5, limit = 1)(party1, participant1, synchronizer1)
    verifyPageBoundary(validAtEventSeqId = 5, limit = 2)(party1, participant1, synchronizer2)
    verifyPageBoundary(validAtEventSeqId = 5, limit = 3)(party1, participant2, synchronizer1)
    verifyPageBoundary(validAtEventSeqId = 5, limit = 4)(party2, participant2, synchronizer1)
    verifyPageBoundary(validAtEventSeqId = 5, limit = 5)(party3, participant1, synchronizer1)
    verifyPageBoundary(validAtEventSeqId = 5, limit = 10)(party3, participant1, synchronizer1)
  }

  it should "count each hosting triplet once" in {
    val party1 = someParty.interned
    val party2 = someParty2.interned
    val participant = someParticipantId.interned
    val synchronizer = someSynchronizerId.interned
    ingestPtp(
      ptp(1, party1, participant, synchronizer),
      ptp(2, party1, participant, synchronizer, ChangedTo(Observation)),
      ptp(3, party2, participant, synchronizer),
    )

    verifyPageBoundary(validAtEventSeqId = 3, limit = 1)(party1, participant, synchronizer)
    verifyPageBoundary(validAtEventSeqId = 3, limit = 2)(party2, participant, synchronizer)
    verifyPageBoundary(validAtEventSeqId = 3, limit = 3)(party2, participant, synchronizer)

  }

  it should "count revoked triplets" in {
    val party1 = someParty.interned
    val party2 = someParty2.interned
    val participant = someParticipantId.interned
    val synchronizer = someSynchronizerId.interned
    ingestPtp(
      ptp(1, party1, participant, synchronizer),
      ptp(2, party1, participant, synchronizer, Revoked),
      ptp(3, party2, participant, synchronizer),
    )

    verifyPageBoundary(validAtEventSeqId = 3, limit = 1)(party1, participant, synchronizer)
  }

  it should "ignore events after the snapshot" in {
    val party1 = someParty.interned
    val party2 = someParty2.interned
    val party3 = someParty3.interned
    val participant = someParticipantId.interned
    val synchronizer = someSynchronizerId.interned
    ingestPtp(
      ptp(1, party1, participant, synchronizer),
      ptp(2, party2, participant, synchronizer),
      ptp(3, party3, participant, synchronizer),
    )

    verifyPageBoundary(validAtEventSeqId = 2, limit = 2)(party2, participant, synchronizer)
    pageBoundary(
      validAtEventSeqId = 0,
      limit = 3,
      fromParty = 0,
      fromParticipant = 0,
      fromSynchronizer = 0,
      partyIds = None,
      participantId = None,
    ) shouldBe None
  }

  it should "count only the triplets of the requested parties and participant" in {
    val party1 = someParty.interned
    val party2 = someParty2.interned
    val party3 = someParty3.interned
    val participant1 = someParticipantId.interned
    val participant2 = someParticipantId2.interned
    val synchronizer = someSynchronizerId.interned
    ingestPtp(
      ptp(4, party1, participant1, synchronizer),
      ptp(2, party1, participant2, synchronizer),
      ptp(5, party2, participant1, synchronizer),
      ptp(3, party2, participant2, synchronizer),
      ptp(1, party3, participant1, synchronizer),
    )

    clue("the party filter counts the triplets of the requested parties") {
      verifyPageBoundary(
        validAtEventSeqId = 5,
        limit = 1,
        partyIds = NonEmpty.from(Set(party1, party3)),
      )(
        party1,
        participant1,
        synchronizer,
      )
      verifyPageBoundary(
        validAtEventSeqId = 5,
        limit = 3,
        partyIds = NonEmpty.from(Set(party1, party3)),
      )(
        party3,
        participant1,
        synchronizer,
      )
    }
    clue("the participant filter counts the triplets of the requested participant") {
      verifyPageBoundary(validAtEventSeqId = 5, limit = 1, participantId = Some(participant2))(
        party1,
        participant2,
        synchronizer,
      )
      verifyPageBoundary(validAtEventSeqId = 5, limit = 2, participantId = Some(participant2))(
        party2,
        participant2,
        synchronizer,
      )
    }
    clue("combined filters count only the triplets matching both") {
      verifyPageBoundary(
        validAtEventSeqId = 5,
        limit = 1,
        partyIds = NonEmpty.from(Set(party1, party3)),
        participantId = Some(participant2),
      )(party1, participant2, synchronizer)
      verifyPageBoundary(
        validAtEventSeqId = 5,
        limit = 10,
        partyIds = NonEmpty.from(Set(party1, party3)),
        participantId = Some(participant2),
      )(party1, participant2, synchronizer)
    }
  }

  behavior of "StorageBackend hostedPartyIds"

  private def verifyIds(
      validAtEventSeqId: Int,
      partyIds: Option[NonEmpty[Set[Int]]] = None,
      participantId: Option[Int] = None,
      synchronizerIds: Option[NonEmpty[Set[Int]]] = None,
      fromInclusive: HostedPartyPageBound = HostedPartyPageBound.Min,
      toInclusive: HostedPartyPageBound =
        HostedPartyPageBound(Int.MaxValue, Int.MaxValue, Int.MaxValue),
  )(expectedEventSeqIds: Int*)(implicit pos: source.Position): Assertion =
    executeSql(
      backend.party.hostedPartyIds(
        partyIds = partyIds,
        participantId = participantId,
        synchronizerIds = synchronizerIds,
        fromInclusive = fromInclusive,
        toInclusive = toInclusive,
        validAtEventSeqId = validAtEventSeqId.toLong,
      )
    ).map(_.toInt) shouldBe expectedEventSeqIds.toVector

  it should "return the latest event of each hosting triplet and drop the revoked triplets" in {
    val party1 = someParty.interned
    val party2 = someParty2.interned
    val party3 = someParty3.interned
    val party4 = Ref.Party.assertFromString("party4").interned
    val party5 = Ref.Party.assertFromString("party5").interned
    val participant = someParticipantId.interned
    val synchronizer = someSynchronizerId.interned
    // Ingest in an order other than the event order, so the latest event is not
    // accidentally determined by the insertion order.
    ingestPtp(
      // the only event of party5 is an onboarding
      ptp(9, party5, participant, synchronizer, Onboarding(Observation)),
      // party4 is revoked and re-added
      ptp(8, party4, participant, synchronizer, Added(Observation)),
      ptp(6, party4, participant, synchronizer),
      ptp(7, party4, participant, synchronizer, Revoked),
      // the latest event of party1 is a change
      ptp(2, party1, participant, synchronizer, ChangedTo(Observation)),
      ptp(1, party1, participant, synchronizer),
      // party3 is revoked
      ptp(5, party3, participant, synchronizer, Revoked),
      ptp(4, party3, participant, synchronizer),
      // the only event of party2 is an addition
      ptp(3, party2, participant, synchronizer),
    )

    verifyIds(validAtEventSeqId = 9)(2, 3, 8, 9)
  }

  it should "return the latest event at the snapshot" in {
    val party = someParty.interned
    val participant = someParticipantId.interned
    val synchronizer = someSynchronizerId.interned
    ingestPtp(
      ptp(1, party, participant, synchronizer),
      ptp(2, party, participant, synchronizer, ChangedTo(Observation)),
      ptp(3, party, participant, synchronizer, Revoked),
      ptp(4, party, participant, synchronizer, Added(Submission)),
    )

    verifyIds(validAtEventSeqId = 1)(1)
    verifyIds(validAtEventSeqId = 2)(2)
    verifyIds(validAtEventSeqId = 3)()
    verifyIds(validAtEventSeqId = 4)(4)
    verifyIds(validAtEventSeqId = 0)()
  }

  it should "order the ids by party, then participant, then synchronizer" in {
    val party1 = someParty.interned
    val party2 = someParty2.interned
    val participant1 = someParticipantId.interned
    val participant2 = someParticipantId2.interned
    val synchronizer1 = someSynchronizerId.interned
    val synchronizer2 = someSynchronizerId2.interned

    ingestPtp(
      ptp(1, party2, participant2, synchronizer2),
      ptp(2, party1, participant1, synchronizer1),
      ptp(3, party1, participant1, synchronizer2),
      ptp(4, party1, participant2, synchronizer1),
      ptp(5, party1, participant2, synchronizer2),
      ptp(6, party2, participant1, synchronizer1),
      ptp(7, party2, participant1, synchronizer2),
      ptp(8, party2, participant2, synchronizer1),
    )

    verifyIds(validAtEventSeqId = 8)(2, 3, 4, 5, 6, 7, 8, 1)
  }

  it should "return only the ids of the requested parties, participants and synchronizers" in {
    val party1 = someParty.interned
    val party2 = someParty2.interned
    val participant1 = someParticipantId.interned
    val participant2 = someParticipantId2.interned
    val synchronizer1 = someSynchronizerId.interned
    val synchronizer2 = someSynchronizerId2.interned
    ingestPtp(
      ptp(1, party1, participant1, synchronizer1),
      ptp(2, party1, participant2, synchronizer1),
      ptp(3, party1, participant1, synchronizer2),
      ptp(4, party2, participant1, synchronizer1),
      ptp(5, party2, participant1, synchronizer2),
    )

    verifyIds(validAtEventSeqId = 5)(1, 3, 2, 4, 5)
    verifyIds(validAtEventSeqId = 5, participantId = Some(participant1))(1, 3, 4, 5)
    verifyIds(validAtEventSeqId = 5, partyIds = NonEmpty.from(Set(party2)))(4, 5)
    verifyIds(validAtEventSeqId = 5, synchronizerIds = NonEmpty.from(Set(synchronizer2)))(3, 5)
    verifyIds(
      validAtEventSeqId = 5,
      partyIds = NonEmpty.from(Set(party1)),
      participantId = Some(participant1),
      synchronizerIds = NonEmpty.from(Set(synchronizer1)),
    )(1)
  }

  it should "return only the ids within the bounds" in {
    val party1 = someParty.interned
    val party2 = someParty2.interned
    val party3 = someParty3.interned
    val participant1 = someParticipantId.interned
    val participant2 = someParticipantId2.interned
    val synchronizer1 = someSynchronizerId.interned
    val synchronizer2 = someSynchronizerId2.interned
    ingestPtp(
      ptp(1, party1, participant1, synchronizer1),
      ptp(2, party1, participant1, synchronizer2),
      ptp(3, party1, participant2, synchronizer1),
      ptp(4, party2, participant1, synchronizer1),
      ptp(5, party3, participant1, synchronizer1),
    )

    verifyIds(
      validAtEventSeqId = 5,
      toInclusive = HostedPartyPageBound(party1, participant1, synchronizer1),
    )(1)
    verifyIds(
      validAtEventSeqId = 5,
      fromInclusive = HostedPartyPageBound(party1, participant1, synchronizer1).successor,
      toInclusive = HostedPartyPageBound(party1, participant2, synchronizer1),
    )(2, 3)
    verifyIds(
      validAtEventSeqId = 5,
      fromInclusive = HostedPartyPageBound(party1, participant2, synchronizer1).successor,
      toInclusive = HostedPartyPageBound(party3, participant1, synchronizer1),
    )(4, 5)
    verifyIds(
      validAtEventSeqId = 5,
      fromInclusive = HostedPartyPageBound(party3, participant1, synchronizer1).successor,
      toInclusive = HostedPartyPageBound(party3, participant1, synchronizer1),
    )()
  }

  behavior of "StorageBackend partyHostings"

  private def hosting(
      eventSeqId: Int,
      partyId: Int,
      participantId: Int,
      synchronizerId: Int,
      authorizationLevel: AuthorizationLevel = Submission,
      hostingState: HostingState = HostingState.Active,
  ): PartyHosting =
    PartyHosting(
      partyId = partyId,
      participantId = participantId,
      synchronizerId = synchronizerId,
      offset = offset(eventSeqId.toLong),
      recordTime = recordTime(eventSeqId),
      authorizationLevel = authorizationLevel,
      hostingState = hostingState,
    )

  private def hostings(eventSeqIds: Int*): Vector[PartyHosting] =
    executeSql(backend.party.partyHostings(eventSeqIds.map(_.toLong)))

  it should "return no hostings for no event ids" in {
    val party = someParty.interned
    val participant = someParticipantId.interned
    val synchronizer = someSynchronizerId.interned
    ingestPtp(ptp(1, party, participant, synchronizer))

    hostings() shouldBe empty
  }

  it should "map onboarding and active authorization events to hostings" in {
    val party = someParty.interned
    val participant = someParticipantId.interned
    val synchronizer = someSynchronizerId.interned
    val hostingsByEvent = Table(
      ("event sequential id", "authorization event", "authorization level", "hosting state"),
      (1, Onboarding(Submission), Submission, HostingState.Onboarding),
      (2, Onboarding(Confirmation), Confirmation, HostingState.Onboarding),
      (3, Onboarding(Observation), Observation, HostingState.Onboarding),
      (4, Added(Submission), Submission, HostingState.Active),
      (5, Added(Confirmation), Confirmation, HostingState.Active),
      (6, Added(Observation), Observation, HostingState.Active),
      (7, ChangedTo(Submission), Submission, HostingState.Active),
      (8, ChangedTo(Confirmation), Confirmation, HostingState.Active),
      (9, ChangedTo(Observation), Observation, HostingState.Active),
    )
    ingestPtp(hostingsByEvent.map { case (eventSeqId, event, _, _) =>
      ptp(eventSeqId, party, participant, synchronizer, event)
    }*)

    forAll(hostingsByEvent) { (eventSeqId, _, level, state) =>
      hostings(eventSeqId) shouldBe
        Vector(hosting(eventSeqId, party, participant, synchronizer, level, state))
    }
  }

  it should "return only the hostings of the requested events" in {
    val party1 = someParty.interned
    val party2 = someParty2.interned
    val participant = someParticipantId.interned
    val synchronizer = someSynchronizerId.interned
    ingestPtp(
      ptp(1, party1, participant, synchronizer),
      ptp(2, party1, participant, synchronizer, ChangedTo(Observation)),
      ptp(3, party2, participant, synchronizer),
    )

    hostings(2) shouldBe Vector(hosting(2, party1, participant, synchronizer, Observation))
  }

  it should "return the hostings of all requested events" in {
    val party1 = someParty.interned
    val party2 = someParty2.interned
    val party3 = someParty3.interned
    val participant1 = someParticipantId.interned
    val participant2 = someParticipantId2.interned
    val synchronizer1 = someSynchronizerId.interned
    val synchronizer2 = someSynchronizerId2.interned
    ingestPtp(
      ptp(1, party3, participant1, synchronizer2),
      ptp(2, party3, participant1, synchronizer1),
      ptp(3, party1, participant2, synchronizer1),
      ptp(4, party1, participant1, synchronizer1),
      ptp(5, party2, participant1, synchronizer1),
    )

    hostings(5, 4, 3, 2, 1) should contain theSameElementsAs Vector(
      hosting(1, party3, participant1, synchronizer2),
      hosting(2, party3, participant1, synchronizer1),
      hosting(3, party1, participant2, synchronizer1),
      hosting(4, party1, participant1, synchronizer1),
      hosting(5, party2, participant1, synchronizer1),
    )
  }
}
