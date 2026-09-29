// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.config.CantonRequireTypes.String185
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.ParticipantId
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent.{
  Added,
  ChangedTo,
  Revoked,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel.{
  Confirmation,
  Submission,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.{
  AuthorizationEvent,
  AuthorizationLevel,
}
import com.digitalasset.canton.ledger.participant.state.index.IndexerPartyDetails
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.{HasExecutionContext, LfPartyId}
import com.digitalasset.daml.lf.data.Ref
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, OptionValues}

import java.util.concurrent.atomic.AtomicLong

private[backend] trait StorageBackendTestsParties
    extends Matchers
    with Inside
    with OptionValues
    with StorageBackendSpec
    with HasExecutionContext { this: AnyFlatSpec =>

  behavior of "StorageBackend (parties)"

  import StorageBackendTestValues.*
  import com.digitalasset.daml.lf.data.Ref.Party.assertFromString as party

  private val currentEvent: AtomicLong = new AtomicLong(0L)
  def nextEventId(): Long = currentEvent.incrementAndGet()
  private val thisParticipantId = ParticipantId(participantId)
  private val otherParticipantId = ParticipantId(
    Ref.ParticipantId.assertFromString("otherParticipant")
  )

  import StorageBackendTestsParties.{Change, LocalityScenario}

  def dtoPTP(
      offset: Offset,
      party: LfPartyId = someParty,
      participantId: ParticipantId = thisParticipantId,
      authorizationEvent: AuthorizationEvent = Added(AuthorizationLevel.Submission),
      synchronizerId: SynchronizerId = someSynchronizerId,
  ): DbDto =
    dtoPartyToParticipant(
      offset = offset,
      eventSequentialId = nextEventId(),
      party = party,
      participant = participantId,
      authorizationEvent = authorizationEvent,
      synchronizerId = synchronizerId,
    )

  private def ledgerEndSequentialIdAt(dtos: Vector[DbDto], ledgerEndOffset: Offset): Long =
    dtos
      .collect {
        case ptp: DbDto.EventPartyToParticipant if ptp.event_offset <= ledgerEndOffset.unwrap =>
          ptp.event_sequential_id
      }
      .maxOption
      .getOrElse(0L)

  // Sets a ledger end whose offset and sequential id are consistent with the given dtos.
  private def updateLedgerEndTo(dtos: Vector[DbDto], ledgerEndOffset: Offset)(
      connection: java.sql.Connection
  ): Unit =
    updateLedgerEnd(
      ledgerEndOffset,
      ledgerEndSequentialId = ledgerEndSequentialIdAt(dtos, ledgerEndOffset),
    )(connection)

  it should "ingest a single party update" in {
    val someOffset = offset(1)
    val dtos = Vector(dtoPTP(someOffset))

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    val partiesBeforeLedgerEndUpdate = executeSql(backend.party.knownParties(None, None, 10))
    executeSql(updateLedgerEndTo(dtos, someOffset))
    val partiesAfterLedgerEndUpdate = executeSql(backend.party.knownParties(None, None, 10))

    // The first query is executed before the ledger end is updated.
    // It should not see the already ingested party allocation.
    partiesBeforeLedgerEndUpdate shouldBe empty

    // The second query should now see the party.
    partiesAfterLedgerEndUpdate should not be empty
  }

  it should "accumulate multiple party records into one response" in {
    val dtos = Vector(
      // singular non-local
      dtoPTP(offset(1), party("aaf"), otherParticipantId),
      // singular local
      dtoPTP(offset(2), party("bbt")),
      // desired values in last record
      dtoPTP(offset(3), party("cct"), otherParticipantId),
      dtoPTP(offset(4), party("cct"), otherParticipantId),
      dtoPTP(offset(5), party("cct")),
      // desired values in last record except of is-local
      dtoPTP(offset(6), party("ddt"), otherParticipantId),
      dtoPTP(offset(7), party("ddt")),
      dtoPTP(offset(8), party("ddt"), otherParticipantId),
      // desired values before ledger end, undesired accept after ledger end
      dtoPTP(offset(15), party("ggf"), otherParticipantId),
      dtoPTP(offset(17), party("ggf")),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    // ledger end deliberately omitting the last test entries
    executeSql(updateLedgerEndTo(dtos, offset(16)))

    def validateEntries(entry: IndexerPartyDetails): Unit =
      entry.isLocal shouldBe entry.party.lastOption.contains('t')

    val allKnownParties = executeSql(backend.party.knownParties(None, None, 10))
    allKnownParties.length shouldBe 5
    allKnownParties.foreach(validateEntries)

    val pageOne = executeSql(backend.party.knownParties(None, None, 4))
    pageOne.length shouldBe 4
    pageOne.foreach(validateEntries)
    pageOne.exists(_.party == "aaf") shouldBe true
    pageOne.exists(_.party == "bbt") shouldBe true
    pageOne.exists(_.party == "cct") shouldBe true
    pageOne.exists(_.party == "ddt") shouldBe true

    val pageTwo =
      executeSql(backend.party.knownParties(Some(LfPartyId.assertFromString("ddt")), None, 10))
    pageTwo.length shouldBe 1
    pageTwo.foreach(validateEntries)
    pageTwo.exists(_.party == "ggf") shouldBe true
  }

  it should "get all parties ordered by id using binary collation" in {
    val dtos = Vector(
      dtoPTP(offset(1), party("a"), otherParticipantId),
      dtoPTP(offset(2), party("a-"), otherParticipantId),
      dtoPTP(offset(3), party("b"), otherParticipantId),
      dtoPTP(offset(4), party("a_"), otherParticipantId),
      dtoPTP(offset(5), party("-a"), otherParticipantId),
      dtoPTP(offset(6), party("_a"), otherParticipantId),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    // ledger end deliberately omitting the last test entries
    executeSql(updateLedgerEndTo(dtos, offset(6)))

    val allKnownParties = executeSql(backend.party.knownParties(None, None, 10))
    allKnownParties.length shouldBe 6

    val filteredParties =
      executeSql(backend.party.knownParties(None, Some(String185.tryCreate("a-")), 10))
    filteredParties.length shouldBe 1

    allKnownParties
      .map(_.party) shouldBe Seq("-a", "_a", "a", "a-", "a_", "b")

    val pageOne = executeSql(backend.party.knownParties(None, None, 3))
    pageOne.length shouldBe 3
    pageOne
      .map(_.party) shouldBe Seq("-a", "_a", "a")

    val pageTwo =
      executeSql(backend.party.knownParties(Some(LfPartyId.assertFromString("a")), None, 10))
    pageTwo.length shouldBe 3
    pageTwo
      .map(_.party) shouldBe Seq("a-", "a_", "b")

    val filteredPageTwo =
      executeSql(
        backend.party
          .knownParties(Some(LfPartyId.assertFromString("a")), Some(String185.tryCreate("a-")), 10)
      )
    filteredPageTwo.length shouldBe 1
  }

  it should "determine party locality across participant and synchronizer dimensions" in {
    val thisP = thisParticipantId
    val otherP = otherParticipantId
    val syncA = someSynchronizerId
    val syncB = someSynchronizerId2
    val added: AuthorizationEvent = Added(Submission)
    // A non-revoking change (permission change) must keep the party local.
    val changedTo: AuthorizationEvent = ChangedTo(Confirmation)
    val revoked: AuthorizationEvent = Revoked

    val scenarios = Seq(
      // --- single participant, single synchronizer: pure add/remove toggling ---
      LocalityScenario(
        party = "s01-local-add",
        expectedLocal = true,
        changes = Seq(Change(thisP, syncA, added)),
      ),
      LocalityScenario(
        party = "s02-local-add-revoke",
        expectedLocal = false,
        changes = Seq(Change(thisP, syncA, added), Change(thisP, syncA, revoked)),
      ),
      LocalityScenario(
        party = "s03-local-add-revoke-add",
        expectedLocal = true,
        changes = Seq(
          Change(thisP, syncA, added),
          Change(thisP, syncA, revoked),
          Change(thisP, syncA, added),
        ),
      ),
      LocalityScenario(
        party = "s04-local-changedto-keeps-local",
        expectedLocal = true,
        changes = Seq(Change(thisP, syncA, added), Change(thisP, syncA, changedTo)),
      ),
      // --- remote participant only: never local, regardless of add/remove ---
      LocalityScenario(
        party = "s05-remote-add",
        expectedLocal = false,
        changes = Seq(Change(otherP, syncA, added)),
      ),
      LocalityScenario(
        party = "s06-remote-add-revoke",
        expectedLocal = false,
        changes = Seq(Change(otherP, syncA, added), Change(otherP, syncA, revoked)),
      ),
      // --- same participant, both synchronizers: locality holds if ANY synchronizer is active ---
      LocalityScenario(
        party = "s07-local-both-syncs-added",
        expectedLocal = true,
        changes = Seq(Change(thisP, syncA, added), Change(thisP, syncB, added)),
      ),
      LocalityScenario(
        party = "s08-local-syncA-revoked-syncB-active",
        expectedLocal = true,
        changes = Seq(
          Change(thisP, syncA, added),
          Change(thisP, syncA, revoked),
          Change(thisP, syncB, added),
        ),
      ),
      LocalityScenario(
        party = "s09-local-syncA-active-syncB-revoked",
        expectedLocal = true,
        changes = Seq(
          Change(thisP, syncA, added),
          Change(thisP, syncB, added),
          Change(thisP, syncB, revoked),
        ),
      ),
      LocalityScenario(
        party = "s10-local-both-syncs-revoked",
        expectedLocal = false,
        changes = Seq(
          Change(thisP, syncA, added),
          Change(thisP, syncA, revoked),
          Change(thisP, syncB, added),
          Change(thisP, syncB, revoked),
        ),
      ),
      // --- both participants on the same synchronizer: locality decided by the local triplet only ---
      LocalityScenario(
        party = "s11-local-active-remote-revoked",
        expectedLocal = true,
        changes = Seq(
          Change(thisP, syncA, added),
          Change(otherP, syncA, added),
          Change(otherP, syncA, revoked),
        ),
      ),
      LocalityScenario(
        party = "s12-local-revoked-remote-active",
        expectedLocal = false,
        changes = Seq(
          Change(thisP, syncA, added),
          Change(thisP, syncA, revoked),
          Change(otherP, syncA, added),
        ),
      ),
      // --- cross participant and synchronizer combinations ---
      LocalityScenario(
        party = "s13-remote-syncA-local-syncB-active",
        expectedLocal = true,
        changes = Seq(Change(otherP, syncA, added), Change(thisP, syncB, added)),
      ),
      LocalityScenario(
        party = "s14-remote-syncA-local-syncB-revoked",
        expectedLocal = false,
        changes = Seq(
          Change(otherP, syncA, added),
          Change(thisP, syncB, added),
          Change(thisP, syncB, revoked),
        ),
      ),
    )

    // Assign strictly increasing offsets across all scenarios so that, within each
    // (party, participant, synchronizer) triplet, the last constructed change is the most recent.
    val offsetCounter = new AtomicLong(0L)
    val dtos = scenarios.view.flatMap { scenario =>
      scenario.changes.map { change =>
        dtoPTP(
          offset = offset(offsetCounter.incrementAndGet()),
          party = party(scenario.party),
          participantId = change.participant,
          authorizationEvent = change.authorizationEvent,
          synchronizerId = change.synchronizer,
        )
      }
    }.toVector

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEndTo(dtos, offset(offsetCounter.get())))

    val knownParties = executeSql(backend.party.knownParties(None, None, 100))
    val localityByParty =
      knownParties.map(details => details.party.toString -> details.isLocal).toMap

    // Every party that has at least one topology event is known, independent of locality.
    knownParties.map(_.party.toString).sorted shouldBe scenarios.map(_.party).sorted

    scenarios.foreach { scenario =>
      withClue(s"party '${scenario.party}': ") {
        localityByParty.get(scenario.party) shouldBe Some(scenario.expectedLocal)
      }
    }
  }

}

private[backend] object StorageBackendTestsParties {

  // A single authorization change of a party on a (participant, synchronizer) pair.
  final case class Change(
      participant: ParticipantId,
      synchronizer: SynchronizerId,
      authorizationEvent: AuthorizationEvent,
  )

  // A party together with the ordered sequence of authorization changes it undergoes and the
  // resulting expected locality.
  final case class LocalityScenario(
      party: String,
      expectedLocal: Boolean,
      changes: Seq[Change],
  )
}
