// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus.{
  AcsReplicationProgress,
  Disconnected,
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

  /** No agreement has been reached yet on the sequencer channel to be used for party replication.
    * Stage applies to SP and TP only when a sequencer channel is to be used for party replication.
    */
  final case class NeedSequencerChannelAgreement(params: ReplicationParams)
      extends PartyReplicationStage

  /** The sequencer channel agreement exists, but the PartyToParticipant topology transaction with
    * the TP-side onboarding flag needs to be authorized by the party and TP and become visible on
    * the Ledger API on the SP and TP.
    */
  final case object NeedToObtainOnboardingTopologyAuthorization extends PartyReplicationStage

  /** The sequencer-channel agreement exists and the PartyToParticipant topology transaction with
    * the TP-side onboarding flag is authorized, but the SP and TP still need to request building
    * and connect to the sequencer channel.
    */
  final case object NeedToConnectToSequencerChannel extends PartyReplicationStage

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
  final case object IndexingContractActivationChanges extends PartyReplicationStage

  /** Party replication is finished except that any of the following pieces of state need to be
    * removed:
    *
    *   - If exists, archive the sequencer channel agreement.
    *   - If possible, delete the activation changes for indexing.
    */
  final case object CleaningUp extends PartyReplicationStage

  /** Helper that determines what OnPR needs to do next or what OnPR is waiting for to advance party
    * replication.
    * @param status
    *   the current OnPR request status
    * @return
    *   If OnPR is still in progress and can be advanced, returns the OnPR stage.
    */
  def fromPartyReplicationStatus(status: PartyReplicationStatus): Option[PartyReplicationStage] = {
    def ifHaveSequencerChannelAgreement(
        agreementO: Option[PartyReplicationStatus.SequencerChannelAgreement],
        stage: => PartyReplicationStage,
    ): Option[PartyReplicationStage] = agreementO.map(_ => stage)

    (status match {
      case status @ PartyReplicationStatus(p, agO, auO, reO, inO, _, errO) =>
        errO match {
          case None => Option.when(status.isProgressExpected)((p, agO, auO, reO, inO, None))
          case Some(d: Disconnected) =>
            Option.when(status.isProgressExpected)((p, agO, auO, reO, inO, Some(d)))
          case Some(PartyReplicationFailed(_)) => None
        }
    }).flatMap {
      case (params, agreementO, None, None, _, None) =>
        Some(
          ifHaveSequencerChannelAgreement(
            agreementO,
            NeedToObtainOnboardingTopologyAuthorization,
          ).getOrElse(NeedSequencerChannelAgreement(params))
        )
      case (_, agreementO, Some(_authorization), None, _, None) =>
        ifHaveSequencerChannelAgreement(agreementO, NeedToConnectToSequencerChannel)
      case (_, agreementO, _, _, _, Some(Disconnected(message))) =>
        ifHaveSequencerChannelAgreement(
          agreementO,
          NeedToReconnectToDisconnectedSequencerChannel(message),
        )
      case (params, _, _, Some(replicationProgress), None, None) =>
        Some(ReplicatingPartyAcs(params, replicationProgress))
      case (_, _, _, _, Some(indexingProgress), None) =>
        Some(
          if (!indexingProgress.isIndexingCurrentlyAlmostDone) IndexingContractActivationChanges
          else CleaningUp
        )
    }
  }
}
