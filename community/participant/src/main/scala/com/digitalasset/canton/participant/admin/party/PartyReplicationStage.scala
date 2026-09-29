// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus.{
  Disconnected,
  PartyReplicationError,
  PartyReplicationFailed,
  ReplicationMode,
  ReplicationParams,
}
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus.AcsReplicationProgress

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
  // Stages listed in order of occurrence
  /** The first step of the online party replication. The PartyToParticipant topology transaction
    * with the TP-side onboarding flag needs to be authorized by the party and TP and become visible
    * on the Ledger API on the SP and TP.
    */
  final case class ObtainingOnboardingTopologyAuthorization(params: ReplicationParams)
      extends PartyReplicationStage

  final case class NeedsToReplicatePartyAcs(
      params: ReplicationParams
  ) extends PartyReplicationStage

  /** ACS replication via sequencer channel has been triggered but is not running yet because the
    * channel negotiation hasn't been finished yet.
    */
  final case class TriggeredPartyAcsReplication(
      params: ReplicationParams
  ) extends PartyReplicationStage

  /** The party's ACS is being replicated via file import (stage applies to TP) or sequencer channel
    * (applies to SP export and TP import).
    *
    * @param progress
    *   party replication progress state (persisted and ephemeral, e.g. protocol processor or file
    *   importer)
    */
  final case class AcsReplicationInProgress(
      params: ReplicationParams,
      progress: AcsReplicationProgress,
  ) extends PartyReplicationStage

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
      case status @ PartyReplicationStatus(
            p,
            auO,
            reO,
            acsReplicationO,
            inO,
            _,
            replicationMode,
            errO,
          ) =>
        errO match {
          case None =>
            Option.when(status.isProgressExpected)(
              (p, auO, reO, acsReplicationO, inO, replicationMode, None)
            )
          case Some(d: Disconnected) =>
            Option.when(status.isProgressExpected)(
              (p, auO, reO, acsReplicationO, inO, replicationMode, Some(d))
            )
          case Some(PartyReplicationFailed(_)) => None
        }
    }).flatMap {
      case (params, None, None, None, None, _, _) =>
        Some(ObtainingOnboardingTopologyAuthorization(params))
      // File-based replication only
      case (
            params,
            Some(_),
            Some(replicationProgress),
            None,
            None,
            ReplicationMode.File,
            None,
          ) =>
        Some(AcsReplicationInProgress(params, replicationProgress))
      case (params, Some(_), _, None, _, ReplicationMode.SequencerChannel, None) =>
        Some(NeedsToReplicatePartyAcs(params))
      case (
            params,
            Some(_),
            _,
            Some(AcsReplicationStatus(_, _, _, None, false, _)),
            None,
            ReplicationMode.SequencerChannel,
            None,
          ) =>
        Some(TriggeredPartyAcsReplication(params))
      case (
            params,
            Some(_),
            _,
            Some(AcsReplicationStatus(_, _, Some(_), Some(replicationProgress), _, _)),
            None,
            ReplicationMode.SequencerChannel,
            None,
          ) =>
        Some(AcsReplicationInProgress(params, replicationProgress))
      case (params, Some(_), _, _, Some(indexingProgress), _, None) =>
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
