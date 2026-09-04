// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus.{
  AcsReplicationProgress,
  AgreementStatus,
  Disconnected,
  PartyReplicationError,
  PartyReplicationFailed,
  ReplicationParams,
}

/** The party replication stage describes the same information as the [[PartyReplicationStatus]],
  * but in a form that describes the "next action" to be taken to advance an Online Party
  * Replication (OnPR) request on the source and target participants (SP and TP).
  *
  * Stages with a verb in the same mean that OnPR needs to or is performing an action (e.g.
  * NeedToObtain... or Replicating...) to advance party replication whereas others indicate that
  * OnPR is waiting for something to happen (NeedSequencerChannelAgreement).
  */
sealed trait PartyReplicationStage

object PartyReplicationStage {

  /** No sequencer channel agreement has been proposed yet.
    *
    * Stage applies to target participant in the following cases
    *   - PartyToParticipant topology transaction has been authorized and effective on TP and SP
    *     (beginning of the party replication)
    */
  final case class NeedsToProposePartyReplicationSequencerChannel(
      params: ReplicationParams,
      errorMessage: Option[String],
  ) extends PartyReplicationStage

  /** The sequencer channel agreement has been proposed, but the agreement hasn't been reached yet.
    */
  final case class PartyReplicationSequencerChannelAgreementProposed(params: ReplicationParams)
      extends PartyReplicationStage

  /** The first step of the online party replication. The PartyToParticipant topology transaction
    * with the TP-side onboarding flag needs to be authorized by the party and TP and become visible
    * on the Ledger API on the SP and TP.
    */
  final case class ObtainingOnboardingTopologyAuthorization(params: ReplicationParams)
      extends PartyReplicationStage

  /** The sequencer-channel agreement exists and the PartyToParticipant topology transaction with
    * the TP-side onboarding flag is authorized, but the SP and TP still need to request building
    * and connect to the sequencer channel.
    */
  case object NeedToConnectToSequencerChannel extends PartyReplicationStage

  /** The SP or TP is currently disconnected from the sequencer channel
    *
    * @param message
    *   message upon disconnecting used for logging
    */
  final case class NeedToReconnectToDisconnectedSequencerChannel(message: String)
      extends PartyReplicationStage

  /** The party's ACS is being replicated via file import (stage applies to TP) or sequencer channel
    * (applies to SP export and TP import).
    *
    * @param progress
    *   party replication progress state (persisted and ephemeral, e.g. protocol processor or file
    *   importer)
    */
  final case class ReplicatingPartyAcs(params: ReplicationParams, progress: AcsReplicationProgress)
      extends PartyReplicationStage

  /** The party's ACS and concurrent contract activations are being fed to the indexer for
    * visibility via the Ledger API. This stage ends when all contract activation changes have been
    * indexed and the PartyToParticipant topology transactions with the TP-side onboarding flag
    * cleared is authorized and effective.
    */
  final case class IndexingContractActivationChanges(params: ReplicationParams)
      extends PartyReplicationStage

  /** Party replication is finished except that any of the following pieces of state need to be
    * removed:
    *
    *   - If exists, archive the sequencer channel agreement.
    *   - If possible, delete the activation changes for indexing.
    */
  final case class CleaningUp(params: ReplicationParams) extends PartyReplicationStage

  /** Party replication is in an invalid state and cannot be recovered. If this happens, it's
    * probably a bug.
    * @param error
    *   cause of the invalid state
    */
  final case class IsInInvalidState(error: PartyReplicationError) extends PartyReplicationStage

  /** Helper that determines what OnPR needs to do next or what OnPR is waiting for to advance party
    * replication.
    * @param status
    *   the current OnPR request status
    * @return
    *   If OnPR is still in progress and can be advanced, returns the OnPR stage.
    */
  def fromPartyReplicationStatus(status: PartyReplicationStatus): Option[PartyReplicationStage] =
    (status match {
      case status @ PartyReplicationStatus(p, agreement, auO, reO, inO, _, errO) =>
        errO match {
          case None => Option.when(status.isProgressExpected)((p, agreement, auO, reO, inO, None))
          case Some(d: Disconnected) =>
            Option.when(status.isProgressExpected)((p, agreement, auO, reO, inO, Some(d)))
          case Some(PartyReplicationFailed(_)) => None
        }
    }).flatMap {
      case (params, _, None, _, _, _) =>
        Some(ObtainingOnboardingTopologyAuthorization(params))
      case (
            _,
            _: AgreementStatus.Exists,
            Some(_),
            _,
            _,
            Some(Disconnected(message)),
          ) =>
        Some(NeedToReconnectToDisconnectedSequencerChannel(message))
      // File-based replication only
      case (
            params,
            AgreementStatus.NotNeeded,
            Some(_),
            Some(replicationProgress),
            None,
            None,
          ) =>
        Some(ReplicatingPartyAcs(params, replicationProgress))
      case (params, AgreementStatus.NotProposed, Some(_), _, _, None) =>
        Some(NeedsToProposePartyReplicationSequencerChannel(params, None))
      case (params, AgreementStatus.Proposed, Some(_), _, _, None) =>
        Some(PartyReplicationSequencerChannelAgreementProposed(params))
      case (
            _,
            _: AgreementStatus.Exists,
            Some(_),
            None,
            _,
            None,
          ) =>
        Some(NeedToConnectToSequencerChannel)
      case (params, _, Some(_), Some(replicationProgress), None, None) =>
        Some(ReplicatingPartyAcs(params, replicationProgress))
      case (params, _, Some(_), _, Some(indexingProgress), None) =>
        Some(
          if (!indexingProgress.isIndexingCurrentlyAlmostDone)
            IndexingContractActivationChanges(params)
          else CleaningUp(params)
        )
      case _ =>
        Some(
          IsInInvalidState(
            PartyReplicationFailed(s"Party replication is in invalid state: $status")
          )
        )

    }
}
