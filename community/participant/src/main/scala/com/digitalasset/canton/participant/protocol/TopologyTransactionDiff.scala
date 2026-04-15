// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent.{
  Added,
  ChangedTo,
  Onboarding,
  Revoked,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.{
  AuthorizationLevel,
  TopologyEvent,
}
import com.digitalasset.canton.protocol.UpdateId
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactions.PositiveSignedTopologyTransactions
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LedgerParticipantId, LfPartyId}

private[protocol] object TopologyTransactionDiff {

  /** Compute a set of topology events from the old state and the current state
    * @param psid
    *   synchronizer on which the topology transactions were sequenced
    * @param oldRelevantState
    *   Previous topology state
    * @param currentRelevantState
    *   Current state, after applying the batch of transactions
    * @param localParticipantId
    *   The local participant that may require initiation of online party replication
    * @return
    *   The set of events, the update_id, and whether a party needs to be replicated to this
    *   participant
    */
  private[protocol] def apply(
      psid: PhysicalSynchronizerId,
      oldRelevantState: PositiveSignedTopologyTransactions,
      currentRelevantState: PositiveSignedTopologyTransactions,
      localParticipantId: ParticipantId,
  ): Option[TopologyTransactionDiff] = {

    val before = partyToParticipant(oldRelevantState, psid.protocolVersion)
    val after = partyToParticipant(currentRelevantState, psid.protocolVersion)

    val added: Set[PartyToParticipantAuthorization] = after.view.collect {
      case ((partyId, participantId), (permission, onboardingAfter))
          // party was added if didn't exist before or was onboarding
          if !onboardingAfter && !before.get(partyId -> participantId).exists {
            case (_, onboardingBefore) => !onboardingBefore
          } =>
        PartyToParticipantAuthorization(partyId, participantId, Added(permission))
    }.toSet
    val onboarding: Set[PartyToParticipantAuthorization] = after.view.collect {
      case ((partyId, participantId), (permission, onboardingAfter))
          if onboardingAfter && !before.contains(partyId -> participantId) =>
        PartyToParticipantAuthorization(partyId, participantId, Onboarding(permission))
    }.toSet
    val changed: Set[TopologyEvent] = after.view.collect {
      case ((partyId, participantId), (permission, _))
          if before.get(partyId -> participantId).exists { case (p, _) => p != permission } =>
        PartyToParticipantAuthorization(partyId, participantId, ChangedTo(permission))
    }.toSet
    val removed: Set[TopologyEvent] = before.view.collect {
      case ((partyId, participantId), _) if !after.contains(partyId -> participantId) =>
        PartyToParticipantAuthorization(partyId, participantId, Revoked)
    }.toSet

    val allEvents: Set[TopologyEvent] = added ++ changed ++ removed ++ onboarding

    val clearingOnboardingLocalParty = after.exists {
      case ((partyId, participantId), (_, onboardingAfter)) =>
        !onboardingAfter &&
        participantId == localParticipantId.toLf &&
        before.get(partyId -> participantId).exists { case (_, onboardingBefore) =>
          onboardingBefore
        }
    }

    val abortingOnboardingLocalParty = before.exists {
      case ((partyId, participantId), (_, onboardingBefore)) =>
        onboardingBefore &&
        participantId == localParticipantId.toLf &&
        !after.contains(partyId -> participantId)
    }

    NonEmpty
      .from(allEvents)
      .map(
        TopologyTransactionDiff(
          _,
          updateId(psid, oldRelevantState, currentRelevantState),
          onboardingLocalParty = onboarding.exists(_.participant == localParticipantId.toLf),
          clearingOnboardingLocalParty = clearingOnboardingLocalParty,
          abortingOnboardingLocalParty = abortingOnboardingLocalParty,
        )
      )
  }

  private[protocol] def updateId(
      synchronizerId: PhysicalSynchronizerId,
      oldRelevantState: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
      currentRelevantState: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
  ): UpdateId = {

    val builder = Hash.build(HashPurpose.TopologyUpdateId, HashAlgorithm.Sha256)
    def addToBuilder(
        stateTransactions: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]]
    ): Unit =
      stateTransactions
        .map(_.hashOfSignatures(synchronizerId.protocolVersion).toHexString)
        .sorted // for not relying on retrieval order
        .foreach(builder.addString)

    builder.addString(synchronizerId.toProtoPrimitive)
    builder.addString("old-relevant-state")
    addToBuilder(oldRelevantState)
    // the same state-tx can be either current or old, but these hashes should be different
    builder.addString("new-relevant-state")
    addToBuilder(currentRelevantState)

    val hash = builder.finish()

    UpdateId(hash)
  }

  /** Given a state of topology transactions, extract per party and participant id party hosting
    * information.
    *
    * Returns a map specifying each party and participant's authorization level along with a flag
    * that specifies whether the party is in the "onboarding" state on the participant. The
    * onboarding flag is only set starting with ProtocolVersion.v35.
    */
  private def partyToParticipant(
      state: PositiveSignedTopologyTransactions,
      pv: ProtocolVersion,
  ): Map[(LfPartyId, LedgerParticipantId), (AuthorizationLevel, Boolean)] = {
    val fromPartyToParticipantMapping = for {
      topologyTransaction <- SignedTopologyTransactions
        .collectOfMapping[TopologyChangeOp.Replace, PartyToParticipant](state)
        .view
      mapping = topologyTransaction.mapping
      participant <- mapping.participants
    } yield {
      val isPartyOnboardingAtLeastAtProtocolVersion35 = pv match {
        case ProtocolVersion.v34 => false
        case ProtocolVersion(_) => participant.onboarding
      }
      (
        mapping.partyId.toLf -> participant.participantId.toLf,
        (toAuthorizationLevel(participant.permission), isPartyOnboardingAtLeastAtProtocolVersion35),
      )
    }
    val forAdminParties = SignedTopologyTransactions
      .collectOfMapping[TopologyChangeOp.Replace, SynchronizerTrustCertificate](state)
      .view
      .map(_.mapping)
      .map(m =>
        (
          m.participantId.adminParty.toLf -> m.participantId.toLf,
          (AuthorizationLevel.Submission, false /* admin not onboarding */ ),
        )
      )
    fromPartyToParticipantMapping
      .++(forAdminParties)
      .toMap
  }

  private val toAuthorizationLevel: ParticipantPermission => AuthorizationLevel = {
    case ParticipantPermission.Submission => AuthorizationLevel.Submission
    case ParticipantPermission.Confirmation => AuthorizationLevel.Confirmation
    case ParticipantPermission.Observation => AuthorizationLevel.Observation
  }
}

private[protocol] final case class TopologyTransactionDiff(
    topologyEvents: NonEmpty[Set[TopologyEvent]],
    updateId: UpdateId,
    onboardingLocalParty: Boolean,
    clearingOnboardingLocalParty: Boolean,
    abortingOnboardingLocalParty: Boolean,
)
