// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.offpr.multiparty

import com.digitalasset.canton.admin.api.client.data.FlagNotSet
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{EnvironmentDefinition, TestConsoleEnvironment}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.participant.sync.SyncServiceError.{
  SyncServiceAlarm,
  SyncServiceSynchronizerDisconnect,
}
import com.digitalasset.canton.topology.transaction.{
  ParticipantPermission,
  PartyToParticipant,
  TopologyMapping,
}
import com.digitalasset.canton.topology.{Party, PartyId}
import com.digitalasset.canton.version.ProtocolVersion

import java.time.Duration
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

/** Runs with static time; the decision timeout after an activation is 60 seconds. */
final class PipelinedOfflinePartyReplicationIntegrationTest
    extends PipelinedOfflinePartyReplicationIntegrationTestBase {

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.withSetup { implicit env =>
      import env.*
      source = participant1
      target = participant3
    }

  override protected def clearOnboardingFlags(
      parties: Seq[Party],
      targetLedgerEnd: Long,
      to: LocalParticipantReference,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    passDecisionTimeouts()
    eventually() {
      source.health.ping(to)
      parties.foreach(
        to.parties.clear_party_onboarding_flag(_, daId.logical, targetLedgerEnd) shouldBe FlagNotSet
      )
    }
  }

  /** Scenario: four parties, each contract shared by three of them, are replicated step by step.
    * Between the activations, the source creates and archives shared contracts.
    *
    * Goal: each party gets its own activation offset, each contract is in the snapshot of its
    * stakeholder that the target hosts first only, and the target ends up with the source's ACS and
    * then confirms for the parties.
    */
  "Parties replicated step by step get the source's ACS, with shared contracts created and archived between the activations" in {
    implicit env =>
      val parties = Seq("Ivy", "Jade", "Kale", "Lark").map(name =>
        source.parties.enable(name, synchronizeParticipants = Seq(target))
      )
      def p(i: Int): PartyId = parties(i % parties.size)
      // Contract i has the parties i, i+1 and i+2 as stakeholders
      val shared = parties.indices.map(i =>
        IouSyntax.createIou(source)(p(i), p(i + 1), observers = List(p(i + 2)))
      )
      parties.foreach(party => IouSyntax.createIou(source)(party, party).discard)

      proposeOnTarget(parties)
      target.synchronizers.disconnect_all()
      val beforeActivationOffset = source.ledger_api.state.end()
      val activationOffsets = authorizeOnSourceOneByOne(
        parties,
        beforeActivationOffset,
        afterEachActivation = index =>
          // While the target hosts some of the parties only. No timeout: the target is disconnected.
          if (index < parties.size - 1) {
            IouSyntax
              .createIou(source)(
                p(index),
                p(index + 1),
                observers = List(p(index + 3)),
                optTimeout = None,
              )
              .discard
            IouSyntax.archive(source)(shared(index), p(index), optTimeout = None).discard
          },
      )
      partiesSharingAnOffset(activationOffsets) shouldBe empty

      val files = exportAll(parties, beforeActivationOffset)
      shared.zipWithIndex.foreach { case (contract, i) =>
        val firstHosted = Seq(p(i), p(i + 1), p(i + 2)).minBy(activationOffsets.toMap)
        withClue(s"Snapshots with contract $i: ") {
          parties.filter(party =>
            snapshotContractIds(files(party)).contains(contract.id.contractId)
          ) shouldBe Seq(firstHosted)
        }
      }
      clearOnboardingFlags(parties, importAndReconnect(files))
      assertSameAcsOnSourceAndTarget(parties)

      // The parties go on, with the target confirming for them
      parties.foreach { party =>
        source.ledger_api.javaapi.state.acs
          .filter(Iou.COMPANION)(party)
          .filter(_.data.payer == party.toProtoPrimitive)
          .foreach(IouSyntax.archive(source)(_, party).discard)
      }
      parties.indices.foreach(i =>
        IouSyntax.createIou(source)(p(i), p(i + 1), observers = List(p(i + 2))).discard
      )
      assertSameAcsOnSourceAndTarget(parties)
  }

  /** Scenario: the source loads its authorizations of two parties that share a contract in one
    * call, so that both activations are in one topology update.
    *
    * Goal: the parties share an activation offset, and each export leaves out the shared contract,
    * because the target hosts the other party at the activation. A runbook must therefore authorize
    * one party at a time, and can detect the shared offset before exporting.
    */
  "Parties activated in one topology update share an activation offset, and their exports leave out their shared contracts" in {
    implicit env =>
      import env.*
      val parties = Seq("Moss", "Nettle").map(name =>
        source.parties.enable(name, synchronizeParticipants = Seq(target))
      )
      val shared = IouSyntax.createIou(source)(parties(0), parties(1))

      proposeOnTarget(parties)
      target.synchronizers.disconnect_all()
      val beforeActivationOffset = source.ledger_api.state.end()
      val proposals = eventually() {
        val found = source.topology.transactions
          .list(
            store = daId,
            proposals = true,
            filterMappings = Seq(TopologyMapping.Code.PartyToParticipant),
          )
          .result
          .map(_.transaction)
          .filter(
            _.selectMapping[PartyToParticipant].exists(tx => parties.contains(tx.mapping.partyId))
          )
        found should have size 2
        found
      }
      source.topology.transactions.load(
        source.topology.transactions.sign(proposals, store = daId),
        store = daId,
      )

      val activationOffsets =
        parties.map(party => party -> activationOffset(party, beforeActivationOffset))
      partiesSharingAnOffset(activationOffsets).values.toSeq shouldBe Seq(parties)
      val files = exportAll(parties, beforeActivationOffset)
      parties.foreach(party =>
        snapshotContractIds(files(party)) should not contain shared.id.contractId
      )

      // Aborts instead of importing snapshots that both miss the shared contract
      parties.foreach(abortOnSource)
      target.synchronizers.reconnect(daName)
      source.health.ping(target)
  }

  /** Scenario: two parties that share a contract are activated and exported, the batch is aborted,
    * and the replication is retried.
    *
    * Goal: the later party's snapshot relies on the earlier party's for the shared contract, so a
    * failure must abort the whole batch. The retry needs a new `beforeActivationOffset`: the old
    * one still finds the aborted activation, whose snapshot misses contracts created since. With a
    * new one, the target ends up with the source's ACS.
    */
  "Aborting the whole batch and retrying with a new beforeActivationOffset replicates the parties" in {
    implicit env =>
      import env.*
      val parties = Seq("Oat", "Pea").map(name =>
        source.parties.enable(name, synchronizeParticipants = Seq(target))
      )
      val oat = parties(0)
      val pea = parties(1)
      val shared = IouSyntax.createIou(source)(oat, pea)
      IouSyntax.createIou(source)(pea, pea).discard

      proposeOnTarget(parties)
      target.synchronizers.disconnect_all()
      val firstBeforeActivationOffset = source.ledger_api.state.end()
      val firstActivations =
        authorizeOnSourceOneByOne(parties, firstBeforeActivationOffset).toMap
      val firstFiles = exportAll(parties, firstBeforeActivationOffset)
      snapshotContractIds(firstFiles(pea)) should not contain shared.id.contractId
      parties.foreach(abortOnSource)
      target.synchronizers.reconnect(daName)
      // The target proposes again from its view of the mappings, so it must see the abort first
      source.health.ping(target)
      val createdBetweenAttempts = IouSyntax.createIou(source)(oat, pea)

      proposeOnTarget(parties)
      target.synchronizers.disconnect_all()
      val beforeActivationOffset = source.ledger_api.state.end()
      val activations = authorizeOnSourceOneByOne(parties, beforeActivationOffset).toMap
      parties.foreach { party =>
        activationOffset(party, firstBeforeActivationOffset) shouldBe firstActivations(party)
        activations(party) should be > firstActivations(party)
      }
      val files = exportAll(parties, beforeActivationOffset)
      snapshotContractIds(files(oat)) should contain allOf (
        shared.id.contractId,
        createdBetweenAttempts.id.contractId,
      )
      clearOnboardingFlags(parties, importAndReconnect(files))
      assertSameAcsOnSourceAndTarget(parties)
  }

  /** Scenario: the source activates two parties that share a contract, aborts the first party's
    * activation, and then exports the second party. This breaks the rule that a failure aborts the
    * whole batch.
    *
    * Goal: show what happens. The shared contract, created while the target hosted the first party,
    * is in the second party's snapshot, because the target no longer hosts the first party at the
    * second one's activation. The target also replays the contract's creation when it reconnects,
    * as it hosted the first party then. The replay fails the activeness check and is approved,
    * which raises a `SyncServiceAlarm`. With the default `crashAfterFailedValidation = true`, the
    * target stops processing the synchronizer at the replay, again after each reconnect and
    * restart, so it needs a repair. Tests run with `false`, so the target goes on: its ledger API
    * then shows the contract twice, and still once after the contract's archival.
    */
  "Aborting one party before a later party's export gives the target a shared contract twice" in {
    implicit env =>
      import env.*
      val aborted = source.parties.enable("Quince", synchronizeParticipants = Seq(target))
      val exported = source.parties.enable("Thyme", synchronizeParticipants = Seq(target))

      proposeOnTarget(Seq(aborted, exported))
      target.synchronizers.disconnect_all()
      val beforeActivationOffset = source.ledger_api.state.end()
      authorizeOnSourceOneByOne(Seq(aborted), beforeActivationOffset).discard
      val shared = IouSyntax.createIou(source)(aborted, exported, optTimeout = None)
      abortOnSource(aborted)
      eventually()(hostsSeenBy(source, aborted) shouldBe Set(source.id))
      authorizeOnSourceOneByOne(Seq(exported), beforeActivationOffset).discard

      val files = exportAll(Seq(exported), beforeActivationOffset)
      snapshotContractIds(files(exported)) shouldBe Set(shared.id.contractId)
      val targetLedgerEnd = loggerFactory.assertLogsUnorderedOptional(
        {
          val targetLedgerEnd = importAndReconnect(files)
          eventually() {
            target.testing.pcs_search(daName, exactId = shared.id.contractId) should not be empty
          }
          targetLedgerEnd
        },
        (
          LogEntryOptionality.Required,
          (entry: LogEntry) =>
            entry.shouldBeCantonError(
              SyncServiceAlarm,
              _ should include("with failed activeness check is approved"),
            ),
        ),
        (
          LogEntryOptionality.OptionalMany,
          (entry: LogEntry) =>
            entry.errorMessage should include("with failed activeness check is approved"),
        ),
        (
          LogEntryOptionality.Optional,
          (entry: LogEntry) =>
            entry.shouldBeCantonError(
              SyncServiceSynchronizerDisconnect,
              _ should include regex "(?s)fatally disconnected because of.*",
            ),
        ),
      )
      target.synchronizers.list_connected().map(_.synchronizerAlias) should contain(daName)
      clearOnboardingFlags(Seq(exported), targetLedgerEnd)

      withClue("Ledger API: the snapshot's and the replay's create events: ") {
        activeContracts(target, exported).map(_._1) shouldBe Seq.fill(2)(shared.id.contractId)
      }
      IouSyntax.archive(source)(shared, aborted).discard
      source.health.ping(target)
      activeContracts(source, exported) shouldBe empty
      withClue("Ledger API: the archival consumes one of the two create events: ") {
        activeContracts(target, exported).map(_._1) shouldBe Seq(shared.id.contractId)
      }
      target.testing.acs_search(daName, filterStakeholder = Some(exported)) shouldBe empty
  }

  /** Scenario: two parties replicated step by step have contracts that the target's admin party
    * observes, and contracts that the source's admin party observes.
    *
    * Goal: unlike other parties that the target hosts, the target's admin party does not make the
    * export leave out its contracts, so the target gets contracts that it already has. It still
    * ends up with exactly the source's ACS of the parties, each contract once.
    */
  "Parties with contracts that the target's or the source's admin party observes get the source's ACS" in {
    implicit env =>
      import env.*
      val parties = Seq("Umber", "Vetch").map(name =>
        source.parties.enable(name, synchronizeParticipants = Seq(target))
      )
      val observedByTargetAdmin = parties.map { party =>
        IouSyntax.createIou(source)(party, party, observers = List(source.adminParty)).discard
        party -> IouSyntax.createIou(source)(party, party, observers = List(target.adminParty))
      }.toMap
      val shared =
        IouSyntax.createIou(source)(parties(0), parties(1), observers = List(target.adminParty))

      proposeOnTarget(parties)
      target.synchronizers.disconnect_all()
      val beforeActivationOffset = source.ledger_api.state.end()
      authorizeOnSourceOneByOne(parties, beforeActivationOffset).discard
      val files = exportAll(parties, beforeActivationOffset)
      parties.foreach(party =>
        snapshotContractIds(files(party)) should contain(observedByTargetAdmin(party).id.contractId)
      )
      snapshotContractIds(files(parties(0))) should contain(shared.id.contractId)
      clearOnboardingFlags(parties, importAndReconnect(files))
      assertSameAcsOnSourceAndTarget(parties)

      parties.foreach { party =>
        source.ledger_api.javaapi.state.acs
          .filter(Iou.COMPANION)(party)
          .filter(_.data.payer == party.toProtoPrimitive)
          .foreach(IouSyntax.archive(source)(_, party).discard)
      }
      assertSameAcsOnSourceAndTarget(parties)
  }

  /** Scenario: three external parties, each contract shared by two of them, are replicated step by
    * step. Each party signs the authorization of its activation itself, through the source.
    *
    * Goal: as for local parties, each party gets its own activation offset, and the target ends up
    * with the source's ACS and then confirms for the parties.
    */
  "External parties replicated step by step get the source's ACS" in { implicit env =>
    val parties = Seq("Wasabi", "Xigua", "Yarrow").map(name =>
      source.parties.testing.external.enable(name, synchronizeParticipants = Seq(target))
    )
    def p(i: Int): Party = parties(i % parties.size)
    parties.indices.foreach(i => IouSyntax.createIou(source)(p(i), p(i + 1)).discard)

    proposeOnTarget(parties)
    target.synchronizers.disconnect_all()
    val beforeActivationOffset = source.ledger_api.state.end()
    val activationOffsets = authorizeOnSourceOneByOne(
      parties,
      beforeActivationOffset,
      afterEachActivation = index =>
        if (index < parties.size - 1)
          IouSyntax.createIou(source)(p(index), p(index + 1), optTimeout = None).discard,
    )
    partiesSharingAnOffset(activationOffsets) shouldBe empty

    clearOnboardingFlags(parties, importAndReconnect(exportAll(parties, beforeActivationOffset)))
    assertSameAcsOnSourceAndTarget(parties)

    parties.indices.foreach(i => IouSyntax.createIou(source)(p(i), p(i + 1)).discard)
    assertSameAcsOnSourceAndTarget(parties)
  }

  /** Scenario: three parties of one namespace, one identifier a prefix of another's, are replicated
    * step by step, and nobody calls `clear_party_onboarding_flag`.
    *
    * Goal: the target clears every party's onboarding flag by itself after the decision timeout.
    */
  "Without clear calls, the target clears the onboarding flags of all parties, also when one identifier starts with another's" in {
    implicit env =>
      import env.*
      val parties = Seq("Rye", "Rye1", "Sage").map(name =>
        source.parties.enable(name, synchronizeParticipants = Seq(target))
      )
      proposeOnTarget(parties)
      target.synchronizers.disconnect_all()
      val beforeActivationOffset = source.ledger_api.state.end()
      authorizeOnSourceOneByOne(parties, beforeActivationOffset).discard
      importAndReconnect(exportAll(parties, beforeActivationOffset)).discard

      passDecisionTimeouts()
      eventually() {
        source.health.ping(target)
        parties.foreach(party => withClue(s"$party: ")(targetIsOnboarding(party) shouldBe false))
      }
  }

  /** Scenario: two targets replicate Alice at the same time, and nobody calls
    * `clear_party_onboarding_flag`. Both activations take effect at the same simulated time, so
    * both targets' onboarding flag clearances propose at the same time, for the same serial of
    * Alice's mapping, and the synchronizer rejects one of the proposals.
    *
    * Goal: the target whose proposal was rejected proposes again, and both targets end up as Active
    * hosts with the source's ACS.
    *
    * From protocol version 37 on only: before, `propose_delta` does not keep the hosts' states, so
    * the second target's activation would also clear the first target's onboarding flag, which
    * needs the disconnected first target's signature.
    */
  "Two targets that replicate a party at the same time both clear its onboarding flag" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    implicit env =>
      import env.*
      val newHosts = Seq(participant2, participant3)
      IouSyntax.createIou(source)(alice, bob).discard
      IouSyntax.createIou(source)(alice, alice).discard

      val beforeActivationOffset = source.ledger_api.state.end()
      newHosts.zipWithIndex.foreach { case (newHost, index) =>
        // A target proposes on top of the earlier target's activation
        eventually() {
          hostsSeenBy(newHost, alice) shouldBe (source +: newHosts.take(index)).map(_.id).toSet
        }
        newHost.topology.party_to_participant_mappings
          .propose_delta(
            party = alice.partyId,
            adds = Seq(newHost.id -> targetPermission),
            store = daId,
            requiresPartyToBeOnboarded = true,
          )
          .discard
        newHost.synchronizers.disconnect_all()
        alice.topology.party_to_participant_mappings
          .propose_delta(
            source,
            adds = Seq(newHost.id -> targetPermission),
            store = daId,
            requiresPartyToBeOnboarded = true,
          )
          .discard
      }

      newHosts.foreach { newHost =>
        val file = tempDirectory.toTempFile(s"two-targets-${newHost.name}.acs.gz").toString
        source.parties.export_party_acs(
          party = alice,
          synchronizerId = daId.logical,
          targetParticipantId = newHost.id,
          beginOffsetExclusive = beforeActivationOffset,
          exportFilePath = file,
        )
        newHost.parties.import_party_acs(daId, Some(alice), file)
      }
      newHosts.foreach(_.synchronizers.reconnect(daName))

      advanceTimeAndPing(
        environment.simClock.value,
        Duration.ofMinutes(2).plusSeconds(40),
        source,
        newHosts.head,
      )
      eventually() {
        newHosts.foreach(source.health.ping(_))
        newHosts.foreach(newHost =>
          withClue(s"${newHost.name}: ")(isOnboarding(alice, newHost) shouldBe false)
        )
      }
      newHosts.foreach(newHost =>
        withClue(s"Active contracts of Alice on ${newHost.name} vs. the source: ") {
          activeContracts(newHost, alice) shouldBe activeContracts(source, alice)
        }
      )
  }

  /** Scenario: two parties of the source and two of participant2 are replicated to the target in
    * one batch. The target authorizes all four and disconnects once. The parties are activated one
    * at a time, alternating between their sources, and each source exports its parties' snapshots
    * from its own `beginOffsetExclusive`, as each participant has its own ledger offsets. Two
    * contracts are shared across the sources; after the first activation, the sources archive one
    * of them and create another one.
    *
    * Goal: a contract shared across the sources is in the snapshot of its stakeholder that the
    * target hosts first only, and the target ends up with each source's ACS. Offsets of different
    * sources do not compare, so only the order keeps the activations apart: their effective times
    * differ.
    */
  "Parties of two sources replicated in one batch get each source's ACS, also of contracts shared across the sources" in {
    implicit env =>
      import env.*
      val secondSource = participant2
      def enable(name: String, host: LocalParticipantReference, other: LocalParticipantReference) =
        host.parties.enable(name, synchronizeParticipants = Seq(target, other))
      val acacia = enable("Acacia", source, secondSource)
      val bramble = enable("Bramble", source, secondSource)
      val cypress = enable("Cypress", secondSource, source)
      val dahlia = enable("Dahlia", secondSource, source)
      val sourceOf =
        Map(acacia -> source, bramble -> source, cypress -> secondSource, dahlia -> secondSource)
      val order = Seq(acacia, cypress, bramble, dahlia)
      val archivedAcross = IouSyntax.createIou(source)(acacia, cypress)
      val activeAcross = IouSyntax.createIou(secondSource)(dahlia, bramble)
      IouSyntax.createIou(source)(acacia, bramble).discard
      IouSyntax.createIou(secondSource)(cypress, dahlia).discard
      order.foreach(party => IouSyntax.createIou(sourceOf(party))(party, party).discard)

      proposeOnTarget(order)
      target.synchronizers.disconnect_all()
      val beforeActivationOffsets =
        Seq(source, secondSource).map(host => host -> host.ledger_api.state.end()).toMap
      def activate(party: PartyId): Unit =
        authorizeOnSourceOneByOne(
          Seq(party),
          beforeActivationOffsets(sourceOf(party)),
          from = sourceOf(party),
        ).discard
      activate(acacia)
      // While the target hosts Acacia only. No timeout: the target is disconnected.
      IouSyntax.archive(source)(archivedAcross, acacia, optTimeout = None).discard
      val createdAcross = IouSyntax.createIou(secondSource)(cypress, acacia, optTimeout = None)
      order.drop(1).foreach(activate)
      val activationTimes = order.map(party =>
        sourceOf(party).topology.party_to_participant_mappings
          .list(daId, filterParty = party.filterString, filterParticipant = target.id.filterString)
          .loneElement
          .context
          .validFrom
      )
      activationTimes.distinct should have size order.size.toLong

      val files =
        exportAll(Seq(acacia, bramble), beforeActivationOffsets(source)) ++
          exportAll(
            Seq(cypress, dahlia),
            beforeActivationOffsets(secondSource),
            from = secondSource,
          )
      def inSnapshotsOf(contractId: String): Seq[PartyId] =
        order.filter(party => snapshotContractIds(files(party)).contains(contractId))
      withClue("Snapshots with the contracts shared across the sources: ") {
        inSnapshotsOf(archivedAcross.id.contractId) shouldBe Seq(acacia)
        inSnapshotsOf(activeAcross.id.contractId) shouldBe Seq(bramble)
        inSnapshotsOf(createdAcross.id.contractId) shouldBe empty
      }
      clearOnboardingFlags(order, importAndReconnect(files))
      assertSameAcsOnSourceAndTarget(Seq(acacia, bramble))
      assertSameAcsOnSourceAndTarget(Seq(cypress, dahlia), from = secondSource)

      // The parties go on
      IouSyntax.archive(secondSource)(createdAcross, cypress).discard
      IouSyntax.createIou(source)(bramble, cypress).discard
      assertSameAcsOnSourceAndTarget(Seq(acacia, bramble))
      assertSameAcsOnSourceAndTarget(Seq(cypress, dahlia), from = secondSource)
  }

  /** Scenario: two parties of the source that share a contract are replicated in one batch to two
    * targets, participant2 and the target. Each target authorizes both parties and disconnects; the
    * second one does so once it sees the first one's activations, as its authorizations add to
    * them. The source activates the parties for the first target and then for the second, one at a
    * time, and exports one snapshot per party and target from one `beginOffsetExclusive`.
    * participant2 already hosts Bob, with whom one of the parties shares a contract.
    *
    * Goal: each snapshot is specific to its target, as the contract with Bob is left out for
    * participant2 only. Both targets end up with the source's ACS, and clear the onboarding flags
    * although their clearances propose changes of the same mappings at the same time.
    *
    * From protocol version 37 on only: before, `propose_delta` does not keep the hosts' states, so
    * the second target's activation would also clear the first target's onboarding flag, which
    * needs the disconnected first target's signature.
    */
  "Two targets that replicate the same parties in one batch both get the source's ACS" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    implicit env =>
      import env.*
      val targets = Seq(participant2, target)
      val parties = Seq("Elder", "Foxglove").map(
        source.parties.enable(_, synchronizeParticipants = targets)
      )
      val elder = parties(0)
      IouSyntax.createIou(source)(elder, parties(1)).discard
      val withBob = IouSyntax.createIou(source)(elder, bob)
      parties.foreach(party => IouSyntax.createIou(source)(party, party).discard)

      val beforeActivationOffset = source.ledger_api.state.end()
      targets.zipWithIndex.foreach { case (to, index) =>
        eventually() {
          parties.foreach(party =>
            hostsSeenBy(to, party) shouldBe (source +: targets.take(index)).map(_.id).toSet
          )
        }
        proposeOnTarget(parties, to = to)
        to.synchronizers.disconnect_all()
        authorizeOnSourceOneByOne(parties, beforeActivationOffset, to = to).discard
      }
      val files = targets.map(to => to -> exportAll(parties, beforeActivationOffset, to = to)).toMap
      withClue("The contract with Bob in Elder's snapshots for participant2 and the target: ") {
        targets.map(to =>
          snapshotContractIds(files(to)(elder)).contains(withBob.id.contractId)
        ) shouldBe Seq(false, true)
      }
      val targetLedgerEnds =
        targets.map(to => to -> importAndReconnect(files(to), to = to)).toMap
      targets.foreach(to => clearOnboardingFlags(parties, targetLedgerEnds(to), to = to))
      targets.foreach(to => assertSameAcsOnSourceAndTarget(parties, to = to))

      // The parties go on
      IouSyntax.createIou(source)(parties(1), elder).discard
      targets.foreach(to => assertSameAcsOnSourceAndTarget(parties, to = to))
  }

  /** Scenario: two parties of the source with threshold 1 that share a contract are replicated, and
    * each party's activation also raises its threshold to 2, with the source and the target as its
    * confirming hosts. Iris, hosted by the source only, calls IOUs that Gentian issued to her, so
    * that Gentian confirms without submitting: a party with a submitting host and a threshold above
    * 1 cannot submit.
    *
    * Goal: from protocol version 37 on, the threshold does not count the Onboarding target
    * (`PartyInfo.effectiveConfirmationThreshold`), so the source alone confirms for Gentian while
    * the target is disconnected. Before, the onboarding target counts but cannot confirm, so a
    * request would time out; the test does not submit one, as a timed-out call leaves its created
    * contract unreferenced on the source, which fails the ledger API store integrity check. Once
    * the target is Active, a request waits for both hosts' confirmations.
    */
  "Raising the threshold to 2 in the activations needs both hosts once the target is Active" in {
    implicit env =>
      import env.*
      val parties = Seq("Gentian", "Heather").map(
        source.parties.enable(_, synchronizeParticipants = Seq(target))
      )
      val gentian = parties(0)
      val iris = source.parties.enable("Iris", synchronizeParticipants = Seq(target))
      IouSyntax.createIou(source)(gentian, parties(1)).discard
      val ious = Seq.fill(3)(IouSyntax.createIou(source)(gentian, iris))
      def callInBackground(iou: Iou.Contract): Future[Unit] =
        Future(IouSyntax.call(source)(iou, iris, optTimeout = None))(executionContext)

      // The target and the source propose the same mapping, so that their signatures add up
      def proposeThresholdTwo(party: PartyId, proposer: LocalParticipantReference): Unit =
        proposer.topology.party_to_participant_mappings
          .propose(
            party,
            newParticipants = Seq(
              source.id -> ParticipantPermission.Submission,
              target.id -> ParticipantPermission.Confirmation,
            ),
            threshold = PositiveInt.two,
            store = daId,
            participantsRequiringPartyToBeOnboarded = Seq(target.id),
          )
          .discard
      parties.foreach(proposeThresholdTwo(_, target))
      target.synchronizers.disconnect_all()
      val beforeActivationOffset = source.ledger_api.state.end()
      parties.foreach { party =>
        proposeThresholdTwo(party, source)
        activationOffset(party, beforeActivationOffset).discard
      }

      if (testedProtocolVersion >= ProtocolVersion.v37)
        IouSyntax.call(source)(ious(0), iris, optTimeout = None)

      clearOnboardingFlags(parties, importAndReconnect(exportAll(parties, beforeActivationOffset)))
      assertSameAcsOnSourceAndTarget(parties)
      IouSyntax.call(source)(ious(1), iris)

      target.synchronizers.disconnect_all()
      val callWithoutTarget = callInBackground(ious(2))
      // With static time, nothing advances the time to the call's timeout
      always(durationOfSuccess = 5.seconds)(callWithoutTarget.isCompleted shouldBe false)
      target.synchronizers.reconnect(daName)
      callWithoutTarget.futureValue
      assertSameAcsOnSourceAndTarget(parties)
  }
}
