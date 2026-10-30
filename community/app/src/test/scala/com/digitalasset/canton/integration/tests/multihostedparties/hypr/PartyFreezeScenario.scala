// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.hypr

import com.daml.ledger.api.v2.state_service.ParticipantPermission as LapiParticipantPermission
import com.daml.ledger.api.v2.topology_transaction.TopologyEvent.Event as LapiTopologyEvent
import com.daml.ledger.javaapi
import com.digitalasset.base.error.ErrorCode
import com.digitalasset.canton.admin.api.client.data.{FlagSet, PartyOnboardingFlagStatus}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.crypto.{Fingerprint, SigningKeyUsage, SigningKeysWithThreshold}
import com.digitalasset.canton.damltests.java.universal.UniversalContract
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.TransactionRoutingError.TopologyErrors.FrozenParties
import com.digitalasset.canton.error.{CantonBaseError, MediatorError}
import com.digitalasset.canton.integration.tests.multihostedparties.hypr.PartyFreezeScenario.*
import com.digitalasset.canton.integration.tests.multihostedparties.hypr.PartyHostingStateTestHelpers.{
  assertCommitmentsMatch,
  awaitUS,
}
import com.digitalasset.canton.integration.{HasSimClockUtils, TestConsoleEnvironment}
import com.digitalasset.canton.logging.SuppressingLogger
import com.digitalasset.canton.participant.admin.data.ContractImportMode
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.protocol.messages.{
  ConfirmationResponses,
  SignedProtocolMessage,
  TypedSignedProtocolMessageContent,
}
import com.digitalasset.canton.protocol.{RequestId, SynchronizerLimits}
import com.digitalasset.canton.sequencing.protocol.SubmissionRequest
import com.digitalasset.canton.synchronizer.sequencer.{
  ProgrammableSequencer,
  ProgrammableSequencerPolicies,
  SendDecision,
}
import com.digitalasset.canton.time.PositiveFiniteDuration
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  ParticipantPermission,
  PartyToParticipant,
  SignedTopologyTransaction,
  SingleTransactionSignature,
  TopologyChangeOp,
  TopologyTransaction,
}
import com.digitalasset.canton.topology.{
  ForceFlag,
  ForceFlags,
  ParticipantId,
  Party,
  PhysicalSynchronizerId,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, config}
import com.digitalasset.nonempty.NonEmpty
import org.scalatest.Assertions.{fail, withClue}
import org.scalatest.EitherValues.*
import org.scalatest.Inside.inside
import org.scalatest.Inspectors.forAll
import org.scalatest.LoneElement.*
import org.scalatest.OptionValues.*
import org.scalatest.matchers.should.Matchers.*

import java.nio.file.Files
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Promise}
import scala.jdk.CollectionConverters.*

/** Drives a party through HyPR (Hybrid Party Replication) on one synchronizer, and checks after
  * every step what each participant observes.
  *
  * The party first nominates a target participant (Nominated). The target freezes the party
  * (Freezing), reads the party's contracts from the source, imports them while disconnected,
  * reconnects and hosts the party without confirming for it (Onboarding), and confirms once every
  * request in flight at that time is decided (Active). While any entry is Freezing, the party has
  * no hosts on the synchronizer, so no request with the party as a stakeholder can be committed and
  * its contracts do not change.
  *
  * The remote ACS read and the import of its snapshot by a Freezing target do not exist yet.
  * [[readAcs]] and [[importAcs]] stand in for them with the repair service, which needs database
  * persistence.
  *
  * Every write proposes the complete mapping, with each entry's hosting state, and is signed by the
  * given signers only. "Signed by the party" means signed with the party's namespace key: by the
  * participant that holds it for a local party, with the external keys for an external party (see
  * `partySigner`). The scenario keeps the expected mapping and checks it after every write. A
  * participant that starts hosting the party by a direct move into Onboarding or Active first
  * imports the party's current ACS, as its operator must; [[replicate]] imports it as of t_F.
  *
  * @param party
  *   Hosted on `source` only, with threshold 1.
  * @param source
  *   Serves the party's contracts, and loads the party's topology transactions if it is external.
  * @param partySigner
  *   How the party signs its topology transactions.
  * @param counterparty
  *   Hosted on `counterpartyParticipant` only. Its requests need the party's confirmation (see
  *   [[expectStep]]). Both parties must have vetted the `CantonTests` package.
  * @param participants
  *   The participants on which every step is checked. Must include `source` and
  *   `counterpartyParticipant`, and be connected to `synchronizerId`.
  * @param sequencer
  *   The sequencer of `synchronizerId`, used to record the confirmation responses.
  */
final class PartyFreezeScenario private (
    val party: Party,
    source: LocalParticipantReference,
    partySigner: PartySigner,
    counterparty: Party,
    counterpartyParticipant: LocalParticipantReference,
    participants: Seq[LocalParticipantReference],
    synchronizerId: PhysicalSynchronizerId,
    sequencer: ProgrammableSequencer,
    loggerFactory: SuppressingLogger,
    val counterpartysContract: UniversalContract.ContractId,
)(implicit env: TestConsoleEnvironment) {

  private implicit val traceContext: TraceContext = TraceContext.empty

  private var mapping: ExpectedMapping = {
    val current = listMapping(source).value
    ExpectedMapping(
      current.item.participants
        .map(host => host.participantId -> Entry(host.permission, host.state))
        .toMap,
      current.item.threshold,
    )
  }
  // Every write keeps the external party's signing keys
  private val partySigningKeys: Option[SigningKeysWithThreshold] =
    listMapping(source).value.item.partySigningKeysWithThreshold
  private var disconnected: Set[ParticipantId] = Set.empty
  private var pendingSnapshots: Map[ParticipantId, AcsSnapshot] = Map.empty
  private var withAcs: Set[ParticipantId] = Set(source.id)
  private var latestChange: CantonTimestamp = CantonTimestamp.MinValue
  private var eventOffsets: Map[ParticipantId, Long] =
    participants.map(participant => participant.id -> participant.ledger_api.state.end()).toMap

  /** Continues the scenario on the successor of the synchronizer after a logical synchronizer
    * upgrade, which carries over the party's mapping and contracts.
    */
  def continueOn(
      successor: PhysicalSynchronizerId,
      successorSequencer: ProgrammableSequencer,
  ): PartyFreezeScenario = {
    val next = new PartyFreezeScenario(
      party,
      source,
      partySigner,
      counterparty,
      counterpartyParticipant,
      participants,
      successor,
      successorSequencer,
      loggerFactory,
      counterpartysContract,
    )
    next.mapping shouldBe mapping
    next.withAcs = withAcs
    next.pendingSnapshots = pendingSnapshots
    next.eventOffsets = eventOffsets
    next.latestChange = latestChange
    next
  }

  /** The party nominates `targets` in one transaction, signed by the party alone.
    *
    * @return
    *   The effective time of the nomination.
    */
  def nominate(
      targets: Seq[(LocalParticipantReference, ParticipantPermission)],
      threshold: PositiveInt,
      forceFlags: ForceFlags = ForceFlags.none,
  ): CantonTimestamp =
    write(
      signers = Seq(Authorizer.Party),
      mapping.copy(
        entries = mapping.entries ++ targets.map { case (target, permission) =>
          target.id -> Entry(permission, HostingState.Nominated)
        },
        threshold = threshold,
      ),
      forceFlags,
    )

  /** `participant` moves its own entry to `state`, signed by `participant` alone.
    *
    * @return
    *   The effective time of the move.
    */
  def move(
      participant: LocalParticipantReference,
      state: HostingState,
      serial: Option[PositiveInt] = None,
  ): CantonTimestamp =
    write(
      signers = Seq(Authorizer.Participant(participant)),
      mapping.copy(entries =
        mapping.entries.updated(participant.id, entryOf(participant).copy(state = state))
      ),
      serial = serial,
    )

  /** The party moves the entry of `participant` to `state`, signed by the party alone, which a case
    * expects to be rejected.
    */
  def moveSignedByParty(participant: LocalParticipantReference, state: HostingState): Unit =
    write(
      signers = Seq(Authorizer.Party),
      mapping.copy(entries =
        mapping.entries.updated(participant.id, entryOf(participant).copy(state = state))
      ),
    ).discard

  /** Removes the entry of `participant`, signed by the party or by `participant`.
    *
    * @return
    *   The effective time of the removal.
    */
  def remove(
      participant: LocalParticipantReference,
      by: Signer,
      serial: Option[PositiveInt] = None,
      forceFlags: ForceFlags = ForceFlags.none,
  ): CantonTimestamp =
    write(
      signers = Seq(by match {
        case Signer.Party => Authorizer.Party
        case Signer.Participant => Authorizer.Participant(participant)
      }),
      mapping.copy(entries = mapping.entries - participant.id),
      // The removed participant may have imported the party's contracts
      forceFlags.and(ForceFlag.DisablePartyWithActiveContracts),
      serial,
    )

  /** The expected entries of the party's mapping. */
  def entries: Map[ParticipantId, Entry] = mapping.entries

  /** `signer` proposes the mapping with `entries` and `serial`, which a case expects to be
    * rejected, so the expected mapping does not change.
    */
  def proposeRejected(
      signer: LocalParticipantReference,
      entries: Map[ParticipantId, Entry],
      serial: PositiveInt,
  ): Unit =
    propose(
      Authorizer.Participant(signer),
      mapping.copy(entries = entries),
      serial,
      synchronize = true,
    )

  /** The party writes the mapping with `entries` through a wallet that knows only the `v30`
    * mapping, signed by the party alone. Only for an external party.
    *
    * @return
    *   The effective time of the write.
    */
  def writeAsV30Wallet(
      entries: Map[ParticipantId, Entry],
      forceFlags: ForceFlags,
  ): CantonTimestamp =
    write(signers = Seq(Authorizer.V30Wallet), mapping.copy(entries = entries), forceFlags)

  /** As [[writeAsV30Wallet]], for a write that a case expects to be rejected, so the expected
    * mapping does not change.
    */
  def writeAsV30WalletRejected(entries: Map[ParticipantId, Entry], forceFlags: ForceFlags): Unit =
    propose(
      Authorizer.V30Wallet,
      mapping.copy(entries = entries),
      nextSerial,
      synchronize = true,
      forceFlags,
    )

  /** The serial that the next write uses. */
  def nextSerial: PositiveInt =
    PositiveInt.tryCreate(listMapping(source).value.context.serial.unwrap + 1)

  /** `signer` alone proposes `participant`'s entry as `entry`, which needs another signature. Waits
    * until every connected participant has the proposal, which leaves the mapping unchanged.
    */
  def proposeWithoutAuthorization(
      signer: LocalParticipantReference,
      participant: LocalParticipantReference,
      entry: Entry,
  ): Unit = {
    val serial = nextSerial
    val proposed = mapping.entries.updated(participant.id, entry)
    propose(
      Authorizer.Participant(signer),
      ExpectedMapping(proposed, mapping.threshold),
      serial,
      synchronize = false,
    )
    BaseTest.eventually() {
      forAll(connected) { observer =>
        observer.topology.party_to_participant_mappings
          .list(synchronizerId, proposals = true, filterParty = party.filterString)
          .filter(_.item.partyId == party.partyId)
          .map(proposal =>
            proposal.context.serial -> proposal.item.participants
              .map(host => host.participantId -> Entry(host.permission, host.state))
              .toMap
          ) should contain(serial -> proposed)
      }
    }
  }

  /** Purges the party's contracts on `participant`, which no longer has an entry, as the operator
    * of a target removed after its import must.
    */
  def purge(participant: LocalParticipantReference): Unit = {
    val synchronizerAlias = participant.synchronizers
      .list_connected()
      .find(_.physicalSynchronizerId == synchronizerId)
      .value
      .synchronizerAlias
    val contractIds = participant.testing
      .acs_search(synchronizerAlias, filterStakeholder = Some(party.partyId))
      .map(_.contractId)
    participant.synchronizers.disconnect_all()
    disconnected += participant.id
    try participant.repair.purge(synchronizerAlias, contractIds)
    finally {
      participant.synchronizers.reconnect_all()
      disconnected -= participant.id
    }
    withAcs -= participant.id
    participant.testing.acs_search(
      synchronizerAlias,
      filterStakeholder = Some(party.partyId),
    ) shouldBe empty
  }

  /** Adds `participant` in `state`, signed by both the party and `participant`.
    *
    * @return
    *   The effective time of the addition.
    */
  def add(
      participant: LocalParticipantReference,
      permission: ParticipantPermission,
      state: HostingState,
  ): CantonTimestamp =
    write(
      signers = Seq(Authorizer.Participant(participant), Authorizer.Party),
      mapping.copy(entries = mapping.entries.updated(participant.id, Entry(permission, state))),
    )

  /** Lowers the permission of `participant`'s entry, signed by `participant` alone.
    *
    * @return
    *   The effective time of the change.
    */
  def downgrade(
      participant: LocalParticipantReference,
      permission: ParticipantPermission,
  ): CantonTimestamp =
    write(
      signers = Seq(Authorizer.Participant(participant)),
      mapping.copy(entries =
        mapping.entries.updated(participant.id, entryOf(participant).copy(permission = permission))
      ),
    )

  /** The party removes `removed` and nominates `nominee` in one transaction, signed by the party
    * alone.
    *
    * @return
    *   The effective time of the change.
    */
  def replace(
      removed: LocalParticipantReference,
      nominee: LocalParticipantReference,
      permission: ParticipantPermission,
  ): CantonTimestamp =
    write(
      signers = Seq(Authorizer.Party),
      mapping.copy(entries =
        mapping.entries - removed.id + (nominee.id -> Entry(permission, HostingState.Nominated))
      ),
      ForceFlags(ForceFlag.DisablePartyWithActiveContracts),
    )

  /** Onboards `target` with legacy offline party replication, while the party may be frozen: the
    * target and the party add the target as Onboarding while the target is disconnected, the source
    * exports the party's ACS as of the target's onboarding, and the target imports it and
    * reconnects.
    *
    * @return
    *   The target's ledger end before its onboarding, from which [[activate]] looks for it.
    */
  def onboardLegacy(target: LocalParticipantReference, permission: ParticipantPermission): Long = {
    val expected =
      mapping.copy(entries =
        mapping.entries.updated(target.id, Entry(permission, HostingState.Onboarding))
      )
    val serial = nextSerial
    propose(Authorizer.Participant(target), expected, serial, synchronize = false)
    BaseTest.eventually() {
      source.topology.party_to_participant_mappings
        .list(synchronizerId, proposals = true, filterParty = party.filterString)
        .exists(proposal =>
          proposal.item.partyId == party.partyId && proposal.context.serial == serial
        ) shouldBe true
    }
    target.synchronizers.disconnect_all()
    disconnected += target.id
    val beginOffset = source.ledger_api.state.end()
    propose(Authorizer.Party, expected, serial, synchronize = true)
    mapping = expected
    awaitMapping(signers = Seq(Authorizer.Participant(target), Authorizer.Party)).discard

    val file = Files.createTempFile("party-freeze-legacy-acs-snapshot", ".gz")
    file.toFile.deleteOnExit()
    source.parties.export_party_acs(
      party.partyId,
      synchronizerId.logical,
      target.id,
      beginOffsetExclusive = beginOffset,
      exportFilePath = file.toString,
    )
    target.parties.import_party_acs(synchronizerId.logical, Some(party.partyId), file.toString)
    val beforeOnboarding = target.ledger_api.state.end()
    target.synchronizers.reconnect_all()
    disconnected -= target.id
    withAcs += target.id
    pendingSnapshots += target.id -> AcsSnapshot(
      file.toString,
      env.repair.acs.read_from_file(file.toString).map(_.getCreatedEvent.contractId).toSet,
    )
    awaitMapping(signers = Seq(Authorizer.Participant(target), Authorizer.Party)).discard
    checkAcs(target)
    beforeOnboarding
  }

  /** Sets the expected mapping after a write outside of this scenario, signed by `signers`, and
    * waits until every connected participant has reached its effective time.
    */
  def expectWritten(
      entries: Map[ParticipantId, Entry],
      threshold: PositiveInt,
      signers: Seq[LocalParticipantReference],
  ): CantonTimestamp = {
    mapping = ExpectedMapping(entries, threshold)
    awaitMapping(signers.map(Authorizer.Participant.apply))
  }

  /** Raises the permission of `participant`'s entry and sets the mapping's threshold in one
    * transaction, signed by both the party and `participant`.
    *
    * @return
    *   The effective time of the change.
    */
  def upgrade(
      participant: LocalParticipantReference,
      permission: ParticipantPermission,
      threshold: PositiveInt,
  ): CantonTimestamp =
    write(
      signers = Seq(Authorizer.Participant(participant), Authorizer.Party),
      ExpectedMapping(
        mapping.entries.updated(participant.id, entryOf(participant).copy(permission = permission)),
        threshold,
      ),
    )

  /** The party sets the mapping's threshold, signed by the party alone.
    *
    * @return
    *   The effective time of the change.
    */
  def setThreshold(threshold: PositiveInt): CantonTimestamp =
    write(signers = Seq(Authorizer.Party), mapping.copy(threshold = threshold))

  /** Replicates the party to `target`, which must be Nominated: freeze, read the party's ACS from
    * the source, import it on the disconnected target, move to Onboarding after reconnecting, and
    * clear the onboarding flag once the decision deadline has passed.
    *
    * @param afterFreeze
    *   Runs once the target is Freezing.
    * @param afterOnboarding
    *   Runs once the target is Onboarding.
    * @param afterActivation
    *   Runs once the target is Active.
    */
  def replicate(target: LocalParticipantReference)(
      afterFreeze: => Unit,
      afterOnboarding: => Unit,
      afterActivation: => Unit,
  ): Unit = {
    val frozenAt = freeze(target)
    afterFreeze
    importAcs(target, readAcs(frozenAt))
    val beforeOnboarding = onboard(target)
    afterOnboarding
    activate(target, beforeOnboarding)
    afterActivation
  }

  /** `target` moves from Nominated to Freezing.
    *
    * @return
    *   t_F, the effective time of the freeze.
    */
  def freeze(target: LocalParticipantReference): CantonTimestamp =
    move(target, HostingState.Freezing)

  /** Stands in for the remote ACS read: exports the party's ACS on the source as of t_F + 1µs, the
    * earliest time at which the freeze applies.
    *
    * @param frozenAt
    *   t_F, the effective time of the target's freeze.
    */
  def readAcs(frozenAt: CantonTimestamp): AcsSnapshot =
    exportAcs(asOf = frozenAt.immediateSuccessor)

  private def exportAcs(asOf: CantonTimestamp): AcsSnapshot = {
    // A request sequenced after asOf, so that the source's clean index passes asOf. Then the
    // highest offset with a record time at or before asOf covers the ACS as of asOf.
    source.health.ping(source.id)
    val offset = source.parties.find_highest_offset_by_timestamp(
      synchronizerId.logical,
      asOf.toInstant,
      force = true,
    )
    val file = Files.createTempFile("party-freeze-acs-snapshot", ".gz")
    file.toFile.deleteOnExit()
    source.repair.export_acs(
      Set(party.partyId),
      offset,
      file.toString,
      synchronizerId = Some(synchronizerId.logical),
    )
    AcsSnapshot(
      file.toString,
      env.repair.acs.read_from_file(file.toString).map(_.getCreatedEvent.contractId).toSet,
    )
  }

  /** Stands in for the import of a remote ACS snapshot: imports the snapshot in Validation mode
    * while `target` is disconnected, then reconnects `target`.
    */
  def importAcs(target: LocalParticipantReference, snapshot: AcsSnapshot): Unit =
    importAcsWhile(target, snapshot)(whileDisconnected = ())

  /** Like [[importAcs]], and runs `whileDisconnected` while `target` is disconnected. */
  def importAcsWhile(target: LocalParticipantReference, snapshot: AcsSnapshot)(
      whileDisconnected: => Unit
  ): Unit = {
    target.synchronizers.disconnect_all()
    disconnected += target.id
    try {
      whileDisconnected
      target.repair.import_acs(
        synchronizerId.logical,
        snapshot.path,
        contractImportMode = ContractImportMode.Validation,
      )
    } finally {
      target.synchronizers.reconnect_all()
      disconnected -= target.id
    }
    withAcs += target.id
    pendingSnapshots += target.id -> snapshot
  }

  /** `target` moves from Freezing to Onboarding, after importing the party's ACS.
    *
    * @return
    *   The target's ledger end before the move, from which [[activate]] looks for the target's
    *   onboarding.
    */
  def onboard(target: LocalParticipantReference): Long = {
    val beforeOnboarding = target.ledger_api.state.end()
    move(target, HostingState.Onboarding).discard
    beforeOnboarding
  }

  /** Clears the onboarding flag of `target`: once the decision deadline of the requests in flight
    * at its onboarding has passed, `target` moves from Onboarding to Active.
    *
    * @param beforeOnboarding
    *   The target's ledger end before its onboarding.
    */
  def activate(target: LocalParticipantReference, beforeOnboarding: Long): Unit = {
    def clearOnboardingFlag(): PartyOnboardingFlagStatus =
      target.parties.clear_party_onboarding_flag(
        party.partyId,
        synchronizerId.logical,
        beforeOnboarding,
      )
    val safeToClear = inside(clearOnboardingFlag()) { case FlagSet(safeToClear) => safeToClear }
    advanceClockTo(safeToClear.immediateSuccessor)
    // The clearance scheduled for the decision deadline runs before the target has seen a later
    // sequenced event, and then waits for its next retry. The target has seen one now, so asking
    // again clears the flag right away.
    clearOnboardingFlag().discard
    mapping = mapping.copy(entries =
      mapping.entries.updated(target.id, entryOf(target).copy(state = HostingState.Active))
    )
    awaitMapping(signers = Seq(Authorizer.Participant(target))).discard
  }

  /** Advances the simulated clock past the decision deadline of the requests sequenced up to and
    * including `timestamp`, with pings so that every connected participant sees the time pass.
    */
  def advancePastDecisionDeadline(timestamp: CantonTimestamp): Unit = {
    val decisionTimeout =
      source.topology.synchronizer_parameters.latest(synchronizerId).decisionTimeout
    advanceClockTo(timestamp.plus(decisionTimeout.asJava).immediateSuccessor)
  }

  /** Checks what every connected participant observes after a step:
    *   1. each entry's hosting state, permission and the threshold in the mapping (also checked by
    *      every write);
    *   1. the party's hosting view, including the effective confirmation threshold;
    *   1. exactly `events` among the Ledger API topology events of the party on the synchronizer
    *      since the previous step;
    *   1. the outcome of a request by the counterparty that needs the party's confirmation.
    */
  def expectStep(hosting: Hosting, events: Set[Event], request: Request): Unit = {
    checkMapping()
    checkHosting(hosting)
    checkEvents(events)
    checkRequest(request)
  }

  /** The effective time of the latest change of the party's mapping. */
  def latestChangeTime: CantonTimestamp = latestChange

  /** Asserts that the ACS commitments match among the party's Active hosts and the counterparty's
    * participant, which all share the counterparty's contract with the party.
    */
  def assertCommitmentsMatchAmongHosts(): Unit = {
    val hosts = connected.filter(participant =>
      participant == counterpartyParticipant || mapping.entries
        .get(participant.id)
        .exists(_.state == HostingState.Active)
    )
    assertCommitmentsMatch(hosts, synchronizerId, since = latestChange)
  }

  private def connected: Seq[LocalParticipantReference] =
    participants.filterNot(participant => disconnected.contains(participant.id))

  private def entryOf(participant: LocalParticipantReference): Entry =
    mapping.entries.getOrElse(
      participant.id,
      fail(s"${participant.name} has no entry in the mapping of ${party.partyId}"),
    )

  private def listMapping(participant: LocalParticipantReference) =
    participant.topology.party_to_participant_mappings
      .list(synchronizerId, filterParty = party.filterString)
      .find(_.item.partyId == party.partyId)

  /** Proposes `expected` with the next serial, signed by each of `signers` in turn, and waits until
    * every connected participant has reached its effective time.
    */
  private def write(
      signers: Seq[Authorizer],
      expected: ExpectedMapping,
      forceFlags: ForceFlags = ForceFlags.none,
      serial: Option[PositiveInt] = None,
  ): CantonTimestamp = {
    val startsHosting = expected.entries.collect {
      case (participantId, entry)
          if entry.state.canHost && !mapping.entries.get(participantId).exists(_.state.canHost) =>
        participantId
    }.toSet
    participants
      .filter(participant =>
        startsHosting.contains(participant.id) && !withAcs.contains(participant.id)
      )
      .foreach(target =>
        importAcs(target, exportAcs(asOf = source.testing.fetch_synchronizer_time(synchronizerId)))
      )
    val proposedSerial = serial.getOrElse(nextSerial)
    signers.zipWithIndex.foreach { case (signer, index) =>
      // Only the last signature makes the transaction fully authorized and effective
      propose(signer, expected, proposedSerial, synchronize = index == signers.size - 1, forceFlags)
    }
    mapping = expected
    val effectiveTime = awaitMapping(signers)
    participants.filter(participant => startsHosting.contains(participant.id)).foreach(checkAcs)
    effectiveTime
  }

  /** Checks that `target` hosts exactly the contracts of the ACS snapshot it imported last. */
  private def checkAcs(target: LocalParticipantReference): Unit =
    pendingSnapshots.get(target.id).foreach { snapshot =>
      withClue(s"${target.name} hosts the party's contracts of the imported ACS snapshot: ") {
        BaseTest.eventually() {
          target.ledger_api.state.acs
            .of_party(party)
            .map(_.contractId)
            .toSet shouldBe snapshot.contractIds
        }
      }
      pendingSnapshots -= target.id
    }

  /** Proposes the complete mapping `expected`, with each entry's hosting state, signed by `signer`.
    *
    * @param synchronize
    *   Whether to wait until the transaction is effective, which needs it to be fully authorized.
    */
  private def propose(
      signer: Authorizer,
      expected: ExpectedMapping,
      serial: PositiveInt,
      synchronize: Boolean,
      forceFlags: ForceFlags = ForceFlags.none,
  ): Unit = {
    val synchronizeTimeout = Option.when(synchronize)(env.commandTimeouts.bounded)
    def proposeOn(node: LocalParticipantReference): Unit =
      // `propose`, not `propose_delta`: writes also change the threshold and are signed by external
      // parties, which `propose_delta` supports neither. M1 covers `propose_delta`.
      node.topology.party_to_participant_mappings
        .propose(
          party.partyId,
          newParticipants = expected.entries.view.map { case (participantId, entry) =>
            participantId -> entry.permission
          }.toSeq,
          threshold = expected.threshold,
          partySigningKeys = partySigningKeys,
          serial = Some(serial),
          synchronize = synchronizeTimeout,
          store = synchronizerId,
          forceFlags = forceFlags,
          participantStates = expected.entries.view.mapValues(_.state).toMap,
        )
        .discard
    (signer, partySigner) match {
      case (Authorizer.Participant(participant), _) => proposeOn(participant)
      case (Authorizer.Party, PartySigner.Local(participant)) => proposeOn(participant)
      case (Authorizer.Party, PartySigner.External(namespaceKeys)) =>
        loadSignedByParty(
          namespaceKeys,
          expected,
          serial,
          synchronizerId.protocolVersion,
          isProposal = true,
          synchronizeTimeout,
          forceFlags,
        )
      // The wallet does not know that its write may need another signature, so it sends no proposal
      case (Authorizer.V30Wallet, PartySigner.External(namespaceKeys)) =>
        loadSignedByParty(
          namespaceKeys,
          expected,
          serial,
          ProtocolVersion.v36,
          isProposal = false,
          synchronizeTimeout,
          forceFlags,
        )
      case (Authorizer.V30Wallet, PartySigner.Local(_)) =>
        fail("A wallet signs only for an external party")
    }
  }

  /** Loads the mapping `expected` as a transaction for `protocolVersion` on `source`, signed with
    * the external party's `namespaceKeys`.
    */
  private def loadSignedByParty(
      namespaceKeys: NonEmpty[Seq[Fingerprint]],
      expected: ExpectedMapping,
      serial: PositiveInt,
      protocolVersion: ProtocolVersion,
      isProposal: Boolean,
      synchronizeTimeout: Option[config.NonNegativeDuration],
      forceFlags: ForceFlags,
  ): Unit = {
    val transaction = TopologyTransaction
      .create(
        TopologyChangeOp.Replace,
        serial,
        PartyToParticipant
          .create(
            party.partyId,
            expected.threshold,
            expected.entries.toSeq.map { case (participantId, entry) =>
              HostingParticipant(participantId, entry.permission, entry.state)
            },
            partySigningKeys,
          )
          .value,
        protocolVersion,
      )
      .value
    val signatures = namespaceKeys.map(key =>
      SingleTransactionSignature(
        transaction.hash,
        env.global_secret.sign(
          transaction.hash.hash.getCryptographicEvidence,
          key,
          NonEmpty.mk(Set, SigningKeyUsage.Namespace: SigningKeyUsage),
        ),
      )
    )
    source.topology.transactions.load(
      Seq(
        SignedTopologyTransaction.withTopologySignatures(
          transaction,
          signatures,
          isProposal = isProposal,
          synchronizerId.protocolVersion,
        )
      ),
      synchronizerId,
      forceFlags = forceFlags,
      synchronize = synchronizeTimeout,
    )
  }

  /** Waits until every connected participant sees the expected mapping, signed by `signers` only,
    * and has reached its effective time.
    *
    * @return
    *   The effective time of the mapping.
    */
  private def awaitMapping(signers: Seq[Authorizer]): CantonTimestamp = {
    val expectedSigningKeys = signers.flatMap {
      case Authorizer.Participant(participant) => Seq(participant.id.namespace.fingerprint)
      case Authorizer.Party | Authorizer.V30Wallet =>
        partySigner match {
          case PartySigner.Local(_) => Seq(party.partyId.namespace.fingerprint)
          case PartySigner.External(namespaceKeys) => namespaceKeys.forgetNE
        }
    }.toSet
    val effectiveTime = BaseTest.eventually() {
      val effectiveTimes = connected.map { participant =>
        val current = listMapping(participant).value
        withClue(s"Mapping on ${participant.name}: ") {
          current.item.threshold shouldBe mapping.threshold
          current.item.participants
            .map(host => host.participantId -> Entry(host.permission, host.state))
            .toMap shouldBe mapping.entries
          current.context.signedBy.toSet shouldBe expectedSigningKeys
        }
        CantonTimestamp.fromInstant(current.context.validFrom).value
      }
      effectiveTimes.distinct.loneElement
    }
    connected.foreach(_.testing.await_synchronizer_time(synchronizerId, effectiveTime))
    latestChange = effectiveTime
    effectiveTime
  }

  private def checkMapping(): Unit =
    forAll(connected) { participant =>
      val current = listMapping(participant).value.item
      current.threshold shouldBe mapping.threshold
      current.participants
        .map(host => host.participantId -> Entry(host.permission, host.state))
        .toMap shouldBe mapping.entries
    }

  private def checkHosting(expected: Hosting): Unit =
    forAll(connected) { participant =>
      BaseTest.eventually() {
        val snapshot = awaitUS(
          participant.underlying.value.sync.syncCrypto.ips
            .tryForSynchronizer(synchronizerId)
            .currentSnapshotApproximation
        )
        // A party without hosts has no PartyInfo or one without participants
        def summarize(info: Option[PartyInfo]): Option[(PositiveInt, Set[ParticipantId])] =
          info
            .filter(_.participants.nonEmpty)
            .map(info => info.threshold -> info.participants.keySet)
        val default = summarize(
          awaitUS(snapshot.activeParticipantsOfPartiesWithInfo(Seq(party.toLf))).get(party.toLf)
        )
        val ignoringFreeze = summarize(
          awaitUS(snapshot.activeParticipantsOfPartiesWithInfoIgnoringFreeze(Seq(party.toLf)))
            .get(party.toLf)
        )
        withClue(s"Hosting views on ${participant.name}: ") {
          expected match {
            case Hosting.Hosted(threshold, hosts) =>
              default shouldBe Some(threshold -> hosts)
              ignoringFreeze shouldBe default
            case Hosting.Frozen(threshold, hostsIgnoringFreeze) =>
              default shouldBe None
              ignoringFreeze shouldBe Some(threshold -> hostsIgnoringFreeze)
            case Hosting.NotHosted =>
              default shouldBe None
              ignoringFreeze shouldBe None
          }
        }
      }
    }

  private def checkEvents(expected: Set[Event]): Unit =
    connected.foreach { participant =>
      val begin = eventOffsets(participant.id)
      BaseTest.eventually() {
        val end = participant.ledger_api.state.end()
        val observed =
          if (end == begin) Seq.empty
          else
            participant.ledger_api.updates
              .topology_transactions(
                completeAfter = PositiveInt.MaxValue,
                partyIds = Seq(party),
                beginOffsetExclusive = begin,
                endOffsetInclusive = Some(end),
                synchronizerFilter = Some(synchronizerId.logical),
              )
              .flatMap(_.topologyTransaction.events.map(_.event))
        withClue(s"Ledger API topology events on ${participant.name}: ") {
          observed.map(toEvent).toSet shouldBe expected
          // The events carry the permission of the entry
          forAll(observed.collect {
            case LapiTopologyEvent.ParticipantAuthorizationAdded(added) =>
              added.participantId -> added.participantPermission
            case LapiTopologyEvent.ParticipantAuthorizationOnboarding(onboarding) =>
              onboarding.participantId -> onboarding.participantPermission
          }) { case (participantId, permission) =>
            Some(toParticipantPermission(permission)) shouldBe mapping.entries.collectFirst {
              case (id, entry) if id.toLf == participantId => entry.permission
            }
          }
        }
        eventOffsets += participant.id -> end
      }
    }

  private def toEvent(event: LapiTopologyEvent): Event = {
    def participantOf(participantId: String): ParticipantId =
      participants
        .map(_.id)
        .find(_.toLf == participantId)
        .getOrElse(fail(s"Unknown participant $participantId"))
    event match {
      case LapiTopologyEvent.ParticipantAuthorizationAdded(added) =>
        Event.Added(participantOf(added.participantId))
      case LapiTopologyEvent.ParticipantAuthorizationOnboarding(onboarding) =>
        Event.Onboarding(participantOf(onboarding.participantId))
      case LapiTopologyEvent.ParticipantAuthorizationChanged(changed) =>
        Event.ChangedTo(
          participantOf(changed.participantId),
          toParticipantPermission(changed.participantPermission),
        )
      case LapiTopologyEvent.ParticipantAuthorizationRevoked(revoked) =>
        Event.Revoked(participantOf(revoked.participantId))
      case LapiTopologyEvent.Empty => fail("Empty topology event")
    }
  }

  private def checkRequest(expected: Request): Unit =
    probe(expected, withholdResponsesOf = Set.empty)

  /** Checks whether the counterparty's request is approved when only `respondingHosts` of the
    * party's hosts respond to it. The confirmation responses of the party's other hosts are
    * withheld from the mediator until the verdict. The responses of the counterparty's participant
    * are never withheld, as they carry the counterparty's confirmation.
    *
    * @param respondingHosts
    *   The only hosts of the party whose confirmation responses reach the mediator before the
    *   verdict.
    * @param approved
    *   Whether these responses get the request approved, or it times out.
    */
  def expectQuorum(respondingHosts: Set[ParticipantId], approved: Boolean): Unit =
    probe(
      if (approved) Request.ApprovedBy(respondingHosts) else Request.TimesOut,
      withholdResponsesOf = mapping.entries.keySet -- respondingHosts - counterpartyParticipant.id,
    )

  private def probe(expected: Request, withholdResponsesOf: Set[ParticipantId]): Unit = {
    val responses = TrieMap.empty[ParticipantId, Seq[ConfirmationResponses]]
    // The first confirmation response belongs to the probe's request, as nothing else is in flight.
    // It also shows that the request is sequenced.
    val probeRequestId = Promise[RequestId]()
    // A dropped response is never sequenced, so its sender warns that it timed out once it sees an
    // event past the response's max sequencing time. After an approval, that is whenever a later
    // step advances the clock, outside of any log suppression. So the withheld responses of an
    // approved request are held back until the verdict, and the mediator ignores them. Those of a
    // timed-out request are dropped: released after the timeout, they would be too late anyway.
    val verdictReached = Promise[Unit]()
    val heldBack = TrieMap.empty[ParticipantId, Unit]
    sequencer.setPolicy_("record and withhold confirmation responses") { submissionRequest =>
      submissionRequest.sender match {
        case participantId: ParticipantId
            if ProgrammableSequencerPolicies.isConfirmationResponse(submissionRequest) =>
          val confirmationResponses = openConfirmationResponses(participantId, submissionRequest)
          confirmationResponses.foreach(response =>
            probeRequestId.trySuccess(response.requestId).discard
          )
          val forProbe = confirmationResponses.exists(response =>
            probeRequestId.future.value.flatMap(_.toOption).contains(response.requestId)
          )
          // Also withholds a withheld host's resent response, but not its responses to other
          // requests
          if (forProbe && withholdResponsesOf.contains(participantId)) expected match {
            case Request.TimesOut => SendDecision.Drop
            case _ =>
              heldBack.put(participantId, ()).discard
              SendDecision.HoldBack(verdictReached.future)
          }
          else {
            responses
              .updateWith(participantId)(
                _.map(_ ++ confirmationResponses).orElse(Some(confirmationResponses))
              )
              .discard
            SendDecision.Process
          }
        case _ => SendDecision.Process
      }
    }
    try
      expected match {
        case Request.ApprovedBy(hosts) =>
          touchCounterpartysContract()
          // Responses beyond the threshold may arrive after the verdict
          BaseTest.eventually() {
            approversOfParty(responses.toMap) shouldBe hosts
          }
          verdictReached.success(())
          // The released responses must be sequenced before a later step advances the clock past
          // their max sequencing time. A ping is sequenced after them, so once a held-back host has
          // confirmed it, that host has also seen its response sequenced.
          heldBack.keys.foreach(
            source.health.ping(_, synchronizerId = Some(synchronizerId.logical)).discard
          )

        case Request.Rejected =>
          loggerFactory.assertThrowsAndLogs[CommandFailure](
            touchCounterpartysContract(),
            _.shouldBeCantonErrorCode(FrozenParties),
          )

        case Request.RejectedWith(code) =>
          loggerFactory.assertThrowsAndLogs[CommandFailure](
            touchCounterpartysContract(),
            _.shouldBeCantonErrorCode(code),
          )

        case Request.TimesOut =>
          val ledgerEnd = counterpartyParticipant.ledger_api.state.end()
          val completion = loggerFactory.assertLoggedWarningsAndErrorsSeq(
            {
              counterpartyParticipant.ledger_api.javaapi.commands.submit_async(
                Seq(counterparty),
                touchCommands,
                synchronizerId = Some(synchronizerId.logical),
              )
              Await.result(probeRequestId.future, 1.minute).discard
              val confirmationResponseTimeout = source.topology.synchronizer_parameters
                .latest(synchronizerId)
                .confirmationResponseTimeout
              // Past the confirmation response deadline, but before the decision time, so that the
              // mediator's timeout verdict reaches the participants. In one step: the participants
              // publish later requests only after this one is decided, so a request sent in
              // between would wait for the timeout.
              env.environment.simClock.value
                .advance(confirmationResponseTimeout.plusSeconds(1).asJava)
              BaseTest.eventually() {
                // The mediators observe the time through time proofs, which need no confirmation.
                // The sequencer may assign earlier timestamps for a while.
                env.mediators.local
                  .filter(_.is_running)
                  .foreach(_.testing.fetch_synchronizer_time().discard)
                counterpartyParticipant.ledger_api.completions
                  .list(
                    counterparty,
                    atLeastNumCompletions = 1,
                    ledgerEnd,
                    timeout = config.NonNegativeDuration.ofSeconds(1),
                  )
                  .loneElement
              }
            },
            // The participants of the request may warn about the timeout
            forAll(_)(
              _.warningMessage should include regex "Response message for request .* timed out"
            ),
          )
          withClue(s"Completion status ${completion.status}: ") {
            CantonBaseError.isStatusErrorCode(
              MediatorError.Timeout,
              completion.status.value,
            ) shouldBe true
          }
      }
    finally {
      verdictReached.trySuccess(()).discard
      sequencer.resetPolicy()
    }
  }

  /** The participants whose confirmation responses approve on behalf of the party. */
  private def approversOfParty(
      responses: Map[ParticipantId, Seq[ConfirmationResponses]]
  ): Set[ParticipantId] =
    responses.collect {
      case (participantId, confirmationResponses)
          if confirmationResponses.exists(
            _.responses.exists(response =>
              response.localVerdict.isApprove && response.confirmingParties.contains(party.toLf)
            )
          ) =>
        participantId
    }.toSet

  private def openConfirmationResponses(
      sender: ParticipantId,
      submissionRequest: SubmissionRequest,
  ): Seq[ConfirmationResponses] = {
    val pureCrypto = participants.find(_.id == sender).value.crypto.pureCrypto
    submissionRequest.batch.envelopes.flatMap(
      _.toOpenEnvelope(
        pureCrypto,
        SynchronizerLimits.default,
        synchronizerId.protocolVersion,
      ).value.protocolMessage match {
        case SignedProtocolMessage(
              TypedSignedProtocolMessageContent(confirmationResponses: ConfirmationResponses),
              _,
            ) =>
          Seq(confirmationResponses)
        case _ => Seq.empty
      }
    )
  }

  /** The counterparty exercises a nonconsuming choice on its contract with the party as signatory,
    * which the party's hosts must confirm.
    */
  private def touchCounterpartysContract(): Unit =
    counterpartyParticipant.ledger_api.javaapi.commands
      .submit(
        Seq(counterparty),
        touchCommands,
        synchronizerId = Some(synchronizerId.logical),
        // Only the outcome matters, not that a disconnected host observes it
        optTimeout = None,
      )
      .discard

  private def touchCommands: Seq[javaapi.data.Command] =
    counterpartysContract.exerciseTouch(List.empty[String].asJava).commands.asScala.toSeq

  /** Advances the simulated clock to `timestamp` in steps of at most 10 seconds, with a ping from
    * the source to every connected participant after each step, so that each sees the time pass.
    */
  private def advanceClockTo(timestamp: CantonTimestamp): Unit =
    HasSimClockUtils.advanceClockInStepsTo(
      env.environment.simClock.value,
      timestamp,
      maxStep = PositiveFiniteDuration.tryOfSeconds(10),
    ) {
      connected.foreach(participant => source.health.ping(participant.id))
    }
}

object PartyFreezeScenario {

  /** Creates a contract with the party as signatory, on which the counterparty acts, and starts the
    * scenario. Parameters as for [[PartyFreezeScenario]].
    */
  def apply(
      party: Party,
      source: LocalParticipantReference,
      partySigner: PartySigner,
      counterparty: Party,
      counterpartyParticipant: LocalParticipantReference,
      participants: Seq[LocalParticipantReference],
      synchronizerId: PhysicalSynchronizerId,
      sequencer: ProgrammableSequencer,
      loggerFactory: SuppressingLogger,
  )(implicit env: TestConsoleEnvironment): PartyFreezeScenario = {
    // The counterparty's requests on this contract need the party's confirmation. Its
    // nonconsuming choice keeps the party's ACS unchanged, so a participant needs to import that
    // ACS only once.
    val transaction = source.ledger_api.javaapi.commands.submit(
      Seq(party),
      new UniversalContract(
        List.empty[String].asJava,
        List(party.toProtoPrimitive).asJava,
        List(counterparty.toProtoPrimitive).asJava,
        List(counterparty.toProtoPrimitive).asJava,
      ).create.commands.asScala.toSeq,
      synchronizerId = Some(synchronizerId.logical),
    )
    new PartyFreezeScenario(
      party,
      source,
      partySigner,
      counterparty,
      counterpartyParticipant,
      participants,
      synchronizerId,
      sequencer,
      loggerFactory,
      JavaDecodeUtil.decodeAllCreated(UniversalContract.COMPANION)(transaction).loneElement.id,
    )
  }

  /** How the party signs its topology transactions. */
  sealed trait PartySigner extends Product with Serializable
  object PartySigner {

    /** A local party, whose namespace key `participant` holds. */
    final case class Local(participant: LocalParticipantReference) extends PartySigner

    /** An external party: `namespaceKeys` sign with the party's namespace, or with the threshold of
      * its decentralized namespace.
      */
    final case class External(namespaceKeys: NonEmpty[Seq[Fingerprint]]) extends PartySigner
  }

  /** Who signs a removal. */
  sealed trait Signer extends Product with Serializable
  object Signer {

    /** The party. */
    case object Party extends Signer

    /** The participant whose entry is removed. */
    case object Participant extends Signer
  }

  /** A signer of a topology transaction. */
  private sealed trait Authorizer extends Product with Serializable
  private object Authorizer {
    case object Party extends Authorizer
    final case class Participant(participant: LocalParticipantReference) extends Authorizer

    /** The party, through a wallet that knows only the `v30` mapping: Onboarding entries have the
      * onboarding flag, Active entries do not.
      */
    case object V30Wallet extends Authorizer
  }

  /** A file with the party's ACS as of a point in time, and the IDs of its contracts. */
  final case class AcsSnapshot(path: String, contractIds: Set[String])

  /** The expected permission and hosting state of an entry in the party's mapping. */
  final case class Entry(permission: ParticipantPermission, state: HostingState)

  private final case class ExpectedMapping(
      entries: Map[ParticipantId, Entry],
      threshold: PositiveInt,
  )

  /** What the default and the freeze-ignoring hosting views report for the party. */
  sealed trait Hosting extends Product with Serializable
  object Hosting {

    /** The party is not frozen, and both views report `hosts` (the participants of its Onboarding
      * and Active entries) and the effective confirmation `threshold`.
      */
    final case class Hosted(threshold: PositiveInt, hosts: Set[ParticipantId]) extends Hosting

    /** An entry is Freezing: the default view reports no hosts, the freeze-ignoring view reports
      * `hostsIgnoringFreeze` and the effective confirmation `threshold`.
      */
    final case class Frozen(threshold: PositiveInt, hostsIgnoringFreeze: Set[ParticipantId])
        extends Hosting

    /** No entry is Onboarding or Active. */
    case object NotHosted extends Hosting
  }

  /** A Ledger API topology event of the party. */
  sealed trait Event extends Product with Serializable
  object Event {
    final case class Added(participant: ParticipantId) extends Event
    final case class Onboarding(participant: ParticipantId) extends Event
    final case class ChangedTo(participant: ParticipantId, permission: ParticipantPermission)
        extends Event
    final case class Revoked(participant: ParticipantId) extends Event
  }

  /** The outcome of a request that needs the party's confirmation. */
  sealed trait Request extends Product with Serializable
  object Request {

    /** Approved, with approvals from exactly the party's `hosts`. */
    final case class ApprovedBy(hosts: Set[ParticipantId]) extends Request

    /** Rejected by routing with FROZEN_PARTIES, because the party is frozen on the synchronizer. */
    case object Rejected extends Request

    /** Rejected with `code`. */
    final case class RejectedWith(code: ErrorCode) extends Request

    /** Times out, because the party's hosts that can confirm cannot reach its effective
      * confirmation threshold.
      */
    case object TimesOut extends Request
  }

  private def toParticipantPermission(
      permission: LapiParticipantPermission
  ): ParticipantPermission = permission match {
    case LapiParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION =>
      ParticipantPermission.Submission
    case LapiParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION =>
      ParticipantPermission.Confirmation
    case LapiParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION =>
      ParticipantPermission.Observation
    case other => fail(s"Unexpected participant permission $other")
  }
}
