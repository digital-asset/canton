// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.offpr.multiparty

import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.tests.multihostedparties.offpr.OfflinePartyReplicationIntegrationTestBase
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ParticipantId, Party, PhysicalSynchronizerId}

import java.time.Duration
import scala.concurrent.duration.DurationInt

/** Offline party replication of several parties, done step by step for all parties ("pipelined"),
  * as in an operator's runbook:
  *   - the target authorizes all parties and disconnects;
  *   - the source authorizes one party at a time, each once the previous activation is effective;
  *   - the source exports one snapshot per party, all from the same `beginOffsetExclusive`;
  *   - the target imports all snapshots, reconnects and clears the onboarding flags.
  *
  * Each export leaves out the contracts whose other stakeholders the target hosts at the party's
  * activation. The target gets those from an earlier party's snapshot, or as events after that
  * party's activation.
  *
  * The helpers act on `da`, the source and the target unless given others.
  */
trait PipelinedOfflinePartyReplicationIntegrationTestBase
    extends OfflinePartyReplicationIntegrationTestBase {

  /** Calls `clear_party_onboarding_flag` for each party until it returns `FlagNotSet`. */
  protected def clearOnboardingFlags(
      parties: Seq[Party],
      targetLedgerEnd: Long,
      to: LocalParticipantReference = target,
  )(implicit env: TestConsoleEnvironment): Unit

  protected def proposeOnTarget(
      parties: Seq[Party],
      synchronizerId: Option[PhysicalSynchronizerId] = None,
      to: LocalParticipantReference = target,
  )(implicit env: TestConsoleEnvironment): Unit = {
    val store = synchronizerId.getOrElse(env.daId)
    parties.foreach(party =>
      to.topology.party_to_participant_mappings
        .propose_delta(
          party = party.partyId,
          adds = Seq(to.id -> ParticipantPermission.Confirmation),
          store = store,
          requiresPartyToBeOnboarded = true,
        )
        .discard
    )
  }

  /** Each party authorizes its activation through the source, one at a time, each once the previous
    * activation is effective. The source signs for a local party; an external party signs itself.
    *
    * @return
    *   Each party's activation offset on the source
    */
  protected def authorizeOnSourceOneByOne(
      parties: Seq[Party],
      beforeActivationOffset: Long,
      synchronizerId: Option[PhysicalSynchronizerId] = None,
      afterEachActivation: Int => Unit = _ => (),
      from: LocalParticipantReference = source,
      to: LocalParticipantReference = target,
  )(implicit env: TestConsoleEnvironment): Seq[(Party, Long)] = {
    val store = synchronizerId.getOrElse(env.daId)
    parties.zipWithIndex.map { case (party, index) =>
      party.topology.party_to_participant_mappings
        .propose_delta(
          from,
          adds = Seq(to.id -> ParticipantPermission.Confirmation),
          store = store,
          requiresPartyToBeOnboarded = true,
        )
        .discard
      val offset = activationOffset(party, beforeActivationOffset, synchronizerId, from, to)
      afterEachActivation(index)
      party -> offset
    }
  }

  /** Waits for the party's first activation on the target after the offset, as the export does. */
  protected def activationOffset(
      party: Party,
      beforeActivationOffset: Long,
      synchronizerId: Option[PhysicalSynchronizerId] = None,
      from: LocalParticipantReference = source,
      to: LocalParticipantReference = target,
  )(implicit env: TestConsoleEnvironment): Long = {
    val psid = synchronizerId.getOrElse(env.daId)
    from.parties.find_party_max_activation_offset(
      partyId = party.partyId,
      participantId = to.id,
      synchronizerId = psid,
      onboarding = true,
      beginOffsetExclusive = beforeActivationOffset,
      completeAfter = PositiveInt.one,
      timeout = NonNegativeDuration.tryFromDuration(1.minute),
    )
  }

  /** The parties that share their activation offset with another party. */
  protected def partiesSharingAnOffset(
      activationOffsets: Seq[(Party, Long)]
  ): Map[Long, Seq[Party]] =
    activationOffsets.groupBy(_._2).collect {
      case (offset, parties) if parties.sizeIs > 1 => offset -> parties.map(_._1)
    }

  protected def abortOnSource(party: Party)(implicit env: TestConsoleEnvironment): Unit =
    party.topology.party_to_participant_mappings
      .propose_delta(source, removes = Seq(target.id), store = env.daId)
      .discard

  /** @return The snapshot file of each party */
  protected def exportAll(
      parties: Seq[Party],
      beforeActivationOffset: Long,
      synchronizerId: Option[PhysicalSynchronizerId] = None,
      from: LocalParticipantReference = source,
      to: LocalParticipantReference = target,
  )(implicit env: TestConsoleEnvironment): Map[Party, String] = {
    val psid = synchronizerId.getOrElse(env.daId)
    parties.map { party =>
      val file = tempDirectory
        .toTempFile(
          s"pipelined-${party.partyId.uid.identifier.str}-${to.name}-" +
            s"${psid.logical.uid.identifier.str}-$beforeActivationOffset.acs.gz"
        )
        .toString
      from.parties.export_party_acs(
        party = party,
        synchronizerId = psid.logical,
        targetParticipantId = to.id,
        beginOffsetExclusive = beforeActivationOffset,
        exportFilePath = file,
      )
      party -> file
    }.toMap
  }

  protected def snapshotContractIds(file: String)(implicit
      env: TestConsoleEnvironment
  ): Set[String] =
    env.repair.acs.read_from_file(file).map(_.getCreatedEvent.contractId).toSet

  protected def importAll(
      files: Map[Party, String],
      synchronizerId: PhysicalSynchronizerId,
      to: LocalParticipantReference = target,
  ): Unit =
    files.foreach { case (party, file) =>
      to.parties.import_party_acs(synchronizerId, Some(party), file)
    }

  /** Imports the snapshots and reconnects the target.
    *
    * @return
    *   The target's ledger end before the reconnect
    */
  protected def importAndReconnect(
      files: Map[Party, String],
      to: LocalParticipantReference = target,
  )(implicit env: TestConsoleEnvironment): Long = {
    import env.*
    importAll(files, daId, to)
    val targetLedgerEnd = to.ledger_api.state.end()
    to.synchronizers.reconnect(daName)
    targetLedgerEnd
  }

  /** Compares the target's active contracts with the source's, in the ledger API and in the
    * participant's own ACS of each connected synchronizer, as lists so that duplicates show.
    */
  protected def assertSameAcsOnSourceAndTarget(
      parties: Seq[Party],
      from: LocalParticipantReference = source,
      to: LocalParticipantReference = target,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    from.health.ping(to)
    to.health.ping(from)
    parties.foreach { party =>
      withClue(s"Ledger API active contracts of $party on ${to.name} vs. ${from.name}: ") {
        activeContracts(to, party) shouldBe activeContracts(from, party)
      }
      withClue(s"Internal active contracts of $party on ${to.name} vs. ${from.name}: ") {
        internalActiveContracts(to, party) shouldBe internalActiveContracts(from, party)
      }
    }
  }

  /** The contract ids, each with its synchronizer and reassignment counter. */
  protected def activeContracts(
      participant: LocalParticipantReference,
      party: Party,
  ): Seq[(String, Option[String], Long)] =
    participant.ledger_api.state.acs
      .of_party(party)
      .map(contract =>
        (
          contract.contractId,
          contract.synchronizerId.map(_.toProtoPrimitive),
          contract.reassignmentCounter,
        )
      )
      .sorted

  private def internalActiveContracts(
      participant: LocalParticipantReference,
      party: Party,
  ): Seq[(String, String)] =
    participant.synchronizers
      .list_connected()
      .flatMap { connected =>
        participant.testing
          .acs_search(connected.synchronizerAlias, filterStakeholder = Some(party.partyId))
          .map(_.contractId.coid -> connected.synchronizerAlias.unwrap)
      }
      .sorted

  /** With static time: passes the decision timeouts of the activations so far. */
  protected def passDecisionTimeouts()(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    source.health.ping(target)
    advanceTimeAndPing(
      environment.simClock.value,
      Duration.ofMinutes(2).plusSeconds(40),
      source,
      target,
    )
  }

  protected def targetIsOnboarding(party: Party)(implicit env: TestConsoleEnvironment): Boolean =
    isOnboarding(party, target)

  protected def isOnboarding(party: Party, host: LocalParticipantReference)(implicit
      env: TestConsoleEnvironment
  ): Boolean =
    source.topology.party_to_participant_mappings
      .list(env.daId, filterParty = party.toProtoPrimitive)
      .loneElement
      .item
      .participants
      .find(_.participantId == host.id)
      .value
      .onboarding

  /** The party's hosts as `participant` sees them. */
  protected def hostsSeenBy(participant: LocalParticipantReference, party: Party)(implicit
      env: TestConsoleEnvironment
  ): Set[ParticipantId] =
    participant.topology.party_to_participant_mappings
      .list(env.daId, filterParty = party.toProtoPrimitive)
      .loneElement
      .item
      .participants
      .map(_.participantId)
      .toSet
}
