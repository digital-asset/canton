// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.hypr

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.crypto.LtHash16Blake3
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.PartyTopologyUtils
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.AcsDigestStore
import com.digitalasset.canton.participant.store.AcsDigestStore.{RawDigest, allCheckpointsFilter}
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ParticipantId, Party, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.Assertions.fail
import org.scalatest.Inspectors.forAll
import org.scalatest.matchers.should.Matchers.*

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

/** Sets and observes the hosting states of a party's `PartyToParticipant` mapping, including the
  * freeze of HyPR (Hybrid Party Replication).
  */
private[multihostedparties] object PartyHostingStateTestHelpers extends PartyTopologyUtils {

  private implicit val traceContext: TraceContext = TraceContext.empty

  /** Proposes, with the party's authorization, the hosting states of the party's entries, keeping
    * or adding `participant` with Observation permission.
    *
    * @param partyParticipant
    *   For a local party, the participant that holds the party's namespace key.
    */
  def proposeAsParty(
      party: Party,
      partyParticipant: ParticipantReference,
      participant: ParticipantReference,
      synchronizerId: PhysicalSynchronizerId,
      hostingStates: Map[ParticipantId, HostingState] = Map.empty,
      requiresPartyToBeOnboarded: Boolean = false,
  ): Unit =
    party.topology.party_to_participant_mappings
      .propose_delta(
        partyParticipant,
        adds = Seq(participant.id -> ParticipantPermission.Observation),
        store = synchronizerId,
        requiresPartyToBeOnboarded = requiresPartyToBeOnboarded,
        participantStates = hostingStates,
      )
      .discard

  /** Proposes, with the authorization of `participant`, the hosting states of the party's entries,
    * keeping or adding `participant` with Observation permission.
    */
  def proposeAsParticipant(
      party: Party,
      participant: ParticipantReference,
      synchronizerId: PhysicalSynchronizerId,
      hostingStates: Map[ParticipantId, HostingState] = Map.empty,
      requiresPartyToBeOnboarded: Boolean = false,
  ): Unit =
    participant.topology.party_to_participant_mappings
      .propose_delta(
        party = party.partyId,
        adds = Seq(participant.id -> ParticipantPermission.Observation),
        store = synchronizerId,
        requiresPartyToBeOnboarded = requiresPartyToBeOnboarded,
        participantStates = hostingStates,
      )
      .discard

  /** Waits until every observer sees the party's mapping with the expected hosting states.
    *
    * @return
    *   The effective time of the mapping.
    */
  def awaitHostingStates(
      party: Party,
      synchronizerId: PhysicalSynchronizerId,
      observers: Seq[ParticipantReference],
      expected: Map[ParticipantId, HostingState],
  ): CantonTimestamp =
    BaseTest.eventually() {
      val mappings = observers.map(
        _.topology.party_to_participant_mappings
          .list(synchronizerId, filterParty = party.filterString)
          .loneElement
      )
      forAll(mappings)(
        _.item.participants.map(host => host.participantId -> host.state).toMap shouldBe expected
      )
      // All observers see the same topology transaction, so its effective time is unique.
      mappings
        .map(mapping => CantonTimestamp.fromInstant(mapping.context.validFrom).value)
        .distinct
        .loneElement
    }

  /** Freezes `party` on `synchronizerId`: the party nominates `targetParticipant`, which then moves
    * itself to Freezing.
    *
    * @param partyParticipant
    *   For a local party, the participant that holds the party's namespace key.
    * @param observers
    *   The participants that must see the freeze, and reach its effective time, before this method
    *   returns. Must include `partyParticipant` and `targetParticipant`, and be connected to
    *   `synchronizerId`.
    * @return
    *   The effective time of the freeze.
    */
  def freezeParty(
      party: Party,
      partyParticipant: ParticipantReference,
      targetParticipant: ParticipantReference,
      synchronizerId: PhysicalSynchronizerId,
      observers: Seq[ParticipantReference],
  ): CantonTimestamp = {
    val hostingStates = currentHostingStates(party, partyParticipant, synchronizerId)

    val nominated = hostingStates + (targetParticipant.id -> HostingState.Nominated)
    proposeAsParty(party, partyParticipant, targetParticipant, synchronizerId, nominated)
    awaitHostingStates(party, synchronizerId, observers, nominated).discard

    val freezing = hostingStates + (targetParticipant.id -> HostingState.Freezing)
    proposeAsParticipant(party, targetParticipant, synchronizerId, freezing)
    awaitEffective(party, synchronizerId, observers, freezing)
  }

  /** Unfreezes `party` on `synchronizerId` by removing the entry of `targetParticipant`, authorized
    * by the party. Parameters as for [[freezeParty]].
    *
    * @return
    *   The effective time of the unfreeze.
    */
  def unfreezeParty(
      party: Party,
      partyParticipant: ParticipantReference,
      targetParticipant: ParticipantReference,
      synchronizerId: PhysicalSynchronizerId,
      observers: Seq[ParticipantReference],
  ): CantonTimestamp = {
    val hostingStates =
      currentHostingStates(party, partyParticipant, synchronizerId) - targetParticipant.id

    party.topology.party_to_participant_mappings
      .propose_delta(
        partyParticipant,
        removes = Seq(targetParticipant.id),
        store = synchronizerId,
        participantStates = hostingStates,
      )
      .discard
    awaitEffective(party, synchronizerId, observers, hostingStates)
  }

  /** Asserts that the participant digests each pair of `participants` compute for each other are
    * equal and not empty, once all of them have checkpointed their digests past `since`. Each
    * participant derives its ACS commitment to a counter-participant from that digest, so equal
    * digests mean matching commitments.
    *
    * @param participants
    *   Each pair must share a contract on `synchronizerId`.
    * @param since
    *   The effective time of the last change that the digests must include. No change to the
    *   contracts shared between the participants may follow it.
    */
  def assertCommitmentsMatch(
      participants: Seq[LocalParticipantReference],
      synchronizerId: PhysicalSynchronizerId,
      since: CantonTimestamp,
  ): Unit =
    BaseTest.eventually() {
      // Self-pings produce events after `since`, so that the digests are checkpointed past it.
      // They change no digest between the participants.
      participants.foreach(participant => participant.health.ping(participant.id))
      forAll(participants)(latestCheckpointRecordTime(_, synchronizerId) should be >= since)

      forAll(participants.combinations(2).toSeq) {
        case Seq(participant, counterParticipant) =>
          val digest = participantDigest(participant, counterParticipant, synchronizerId)
          withClue(s"Digests between ${participant.name} and ${counterParticipant.name}: ") {
            digest shouldBe participantDigest(counterParticipant, participant, synchronizerId)
            LtHash16Blake3.tryCreate(digest) should not be LtHash16Blake3.empty
          }
        case pair => fail(s"Not a pair: $pair")
      }
    }

  /** Asserts that the participant digests `participant` and `counterParticipant` compute for each
    * other differ, once both have checkpointed their digests past `since`: their ACS commitments
    * mismatch.
    */
  def assertCommitmentsMismatch(
      participant: LocalParticipantReference,
      counterParticipant: LocalParticipantReference,
      synchronizerId: PhysicalSynchronizerId,
      since: CantonTimestamp,
  ): Unit =
    BaseTest.eventually() {
      Seq(participant, counterParticipant).foreach(p => p.health.ping(p.id))
      forAll(Seq(participant, counterParticipant))(
        latestCheckpointRecordTime(_, synchronizerId) should be >= since
      )
      participantDigest(participant, counterParticipant, synchronizerId) should not be
        participantDigest(counterParticipant, participant, synchronizerId)
    }

  /** Waits for `future`, which must not be aborted by a shutdown. */
  def awaitUS[A](future: FutureUnlessShutdown[A]): A =
    Await
      .result(future.unwrap, 1.minute)
      .onShutdown(fail("Aborted due to shutdown"))

  private def acsDigestStore(
      participant: LocalParticipantReference,
      synchronizerId: PhysicalSynchronizerId,
  ): AcsDigestStore =
    participant.underlying.value.sync.syncPersistentStateManager
      .acsDigestStore(synchronizerId.logical)
      .value

  private def latestCheckpointRecordTime(
      participant: LocalParticipantReference,
      synchronizerId: PhysicalSynchronizerId,
  ): CantonTimestamp =
    awaitUS(
      acsDigestStore(participant, synchronizerId)
        .latestCheckpointUpTo(Offset.MaxValue, allCheckpointsFilter)
    ).value.recordTime

  /** The latest digest that `participant` computed for `counterParticipant`, from which it derives
    * its commitment to `counterParticipant`.
    */
  private def participantDigest(
      participant: LocalParticipantReference,
      counterParticipant: LocalParticipantReference,
      synchronizerId: PhysicalSynchronizerId,
  ): RawDigest = {
    val ledgerApiStore =
      participant.underlying.value.sync.ledgerApiIndexer.asEval.value.ledgerApiStore
    awaitUS(
      acsDigestStore(participant, synchronizerId).participant
        .lookup(
          ledgerApiStore.stringInterningView.participantId.internalize(counterParticipant.id.toLf),
          Offset.MaxValue,
        )
    ).value.digestUpdate.digestO.value
  }

  private def currentHostingStates(
      party: Party,
      observer: ParticipantReference,
      synchronizerId: PhysicalSynchronizerId,
  ): Map[ParticipantId, HostingState] =
    observer.topology.party_to_participant_mappings
      .list(synchronizerId, filterParty = party.filterString)
      .loneElement
      .item
      .participants
      .map(host => host.participantId -> host.state)
      .toMap

  /** Like [[awaitHostingStates]], and also waits until every observer has reached the effective
    * time, so that the observers' next submissions see the change.
    */
  private def awaitEffective(
      party: Party,
      synchronizerId: PhysicalSynchronizerId,
      observers: Seq[ParticipantReference],
      expected: Map[ParticipantId, HostingState],
  ): CantonTimestamp = {
    val effectiveTime = awaitHostingStates(party, synchronizerId, observers, expected)
    observers.foreach(_.testing.await_synchronizer_time(synchronizerId, effectiveTime))
    effectiveTime
  }
}
