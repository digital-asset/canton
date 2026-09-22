// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party.acsreplication

import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus.{
  AcsReplicationProgress,
  PartyReplicationError,
  PartyReplicationFailed,
  ReplicationParams,
}

/** The ACS replication stage describes the same information as the [[PartyReplicationStatus]], but
  * in a form that describes the "next action" to be taken to advance the ACS replication request on
  * the source and target participants (SP and TP).
  *
  * Stages with a verb in the same mean that ACS replication needs to or is performing an action
  * (e.g. NeedToObtain... or Replicating...) to advance party replication whereas others indicate
  * that ACS replication is waiting for something to happen (NeedSequencerChannelAgreement).
  */
// TODO(#35267) Stop using PartyReplicationStatus and replace it with AcsReplicationStatus
sealed trait AcsReplicationStage extends Product with Serializable

object AcsReplicationStage {
  // Stages are listed in order of occurrence
  /** The first step of the ACS replication. The participant verifies that the PartyToParticipant
    * topology transaction with the mapping has been authorized and is visible on the Ledger API.
    * From that, the topology serial and effective time is obtained. Serial is used for further
    * verifications while the effective time is used for the ACS transfer.
    */
  final case class VerifyingOnboardingTopologyAuthorization(params: ReplicationParams)
      extends AcsReplicationStage

  /** No sequencer channel agreement has been proposed yet.
    *
    * Stage applies to target participant in the following cases
    *   - PartyToParticipant topology transaction has been authorized and effective on TP and SP
    *     (beginning of the party replication)
    */
  final case class NeedsToProposeAcsReplicationSequencerChannel(
      params: ReplicationParams,
      errorMessage: Option[String],
  ) extends AcsReplicationStage

  /** The sequencer channel agreement has been proposed, but the agreement hasn't been reached yet.
    */
  final case class AcsReplicationSequencerChannelAgreementProposed(params: ReplicationParams)
      extends AcsReplicationStage

  /** The sequencer-channel agreement exists and the TP has been authorized to receive the ACS, but
    * the SP and TP still need to request building and connect to the sequencer channel.
    */
  case object NeedToConnectToSequencerChannel extends AcsReplicationStage

  /** The SP or TP is currently disconnected from the sequencer channel
    *
    * @param message
    *   message upon disconnecting used for logging
    */
  final case class NeedToReconnectToDisconnectedSequencerChannel(message: String)
      extends AcsReplicationStage

  /** The party's ACS is being replicated via sequencer channel.
    *
    * @param progress
    *   party replication progress state (persisted and ephemeral, e.g. protocol processor or file
    *   importer)
    */
  final case class ReplicatingPartyAcs(
      params: ReplicationParams,
      progress: AcsReplicationProgress,
  ) extends AcsReplicationStage

  /** ACS replication is in an invalid state and cannot be recovered. If this happens, it's probably
    * a bug.
    * @param error
    *   cause of the invalid state
    */
  final case class IsInInvalidState(error: PartyReplicationError) extends AcsReplicationStage

  /** Helper that determines what ACS replication needs to do next or what it is waiting for to
    * advance ACS replication.
    * @param status
    *   the current ACS replication request status
    * @return
    *   If ACS replication is still in progress and can be advanced, returns the ACS replication
    *   stage.
    */
  def fromPartyReplicationStatus(status: PartyReplicationStatus): Option[AcsReplicationStage] =
    (status match {
      case status @ PartyReplicationStatus(p, agreement, authorizationO, reO, _, _, _, errO) =>
        errO match {
          case None =>
            Option.when(status.isProgressExpected)((p, agreement, authorizationO, reO, None))
          case Some(d: PartyReplicationStatus.Disconnected) =>
            Option.when(status.isProgressExpected)((p, agreement, authorizationO, reO, Some(d)))
          case Some(PartyReplicationFailed(_)) => None
        }
    }).flatMap {
      case (params, _, None, _, _) =>
        Some(AcsReplicationStage.VerifyingOnboardingTopologyAuthorization(params))
      case (
            _,
            _: PartyReplicationStatus.AgreementStatus.Exists,
            Some(_),
            Some(_),
            Some(PartyReplicationStatus.Disconnected(message)),
          ) =>
        Some(AcsReplicationStage.NeedToReconnectToDisconnectedSequencerChannel(message))
      case (params, PartyReplicationStatus.AgreementStatus.NotProposed, Some(_), None, None) =>
        Some(AcsReplicationStage.NeedsToProposeAcsReplicationSequencerChannel(params, None))
      case (params, PartyReplicationStatus.AgreementStatus.Proposed, Some(_), None, None) =>
        Some(AcsReplicationStage.AcsReplicationSequencerChannelAgreementProposed(params))
      case (
            _,
            _: PartyReplicationStatus.AgreementStatus.Exists,
            Some(_),
            None,
            None,
          ) =>
        Some(AcsReplicationStage.NeedToConnectToSequencerChannel)
      case (
            params,
            _: PartyReplicationStatus.AgreementStatus.Exists |
            PartyReplicationStatus.AgreementStatus.Archived,
            Some(_),
            Some(replicationProgress),
            None,
          ) =>
        Some(AcsReplicationStage.ReplicatingPartyAcs(params, replicationProgress))
      case _ =>
        Some(
          AcsReplicationStage.IsInInvalidState(
            PartyReplicationFailed(s"Acs replication is in invalid state: $status")
          )
        )

    }
}
