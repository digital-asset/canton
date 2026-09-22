// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party.acsreplication

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.party.{
  PartyReplicationStatus,
  PartyReplicationTopologyWorkflow,
}
import com.digitalasset.canton.participant.sync.ConnectedSynchronizer
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{StoredTopologyTransaction, TimeQuery, TopologyStore}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.{
  PartyToParticipant,
  TopologyChangeOp,
  TopologyMapping,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

/** Verifies correct topology for ACS replication. Specifically that the party is hosted on target
  * participant.
  */
// TODO(#35267) Should only verify that party is hosted on TP. The rest should be done by PartyReplicator and PartyReplicationTopologyWorkflow
class AcsReplicationTopologyWorkflow(
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends FlagCloseable
    with NamedLogging {

  private[party] def partyToParticipantTopologyHead(
      partyId: PartyId,
      topologyStore: TopologyStore[SynchronizerStore],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, StoredTopologyTransaction[Replace, PartyToParticipant]] =
    EitherT(
      partyToParticipantTopologyHeadO(partyId, topologyStore).map(
        _.toRight(
          s"Party $partyId not hosted on synchronizer ${topologyStore.storeId.psid}"
        )
      )
    )

  /** Verifies that party-to-participant topology mapping exists, contains both target and source
    * participant and that the target participant has the onboarding flag set.
    * @param params
    *   party replication parameters
    * @param connectedSynchronizer
    *   active synchronizer, used for topology store access
    * @return
    *   effective time and serial of the onboarding topology transaction or None if not yet
    *   authorized
    */
  private[party] def verifyOnboardingTopology(
      params: PartyReplicationStatus.ReplicationParams,
      connectedSynchronizer: ConnectedSynchronizer,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Option[(EffectiveTime, PositiveInt)]] = {
    val topologyContext =
      PartyReplicationTopologyWorkflow.SynchronizerTopologyContext(connectedSynchronizer)
    val PartyReplicationStatus
      .ReplicationParams(
        requestId,
        partyId,
        _,
        sourceParticipantId,
        targetParticipantId,
        serial,
        _,
      ) = params

    for {
      _ <- EitherT(
        partyToParticipantTopologyHeadO(partyId, topologyContext.topologyStore).map(txO =>
          Either.cond(
            txO.exists(_.mapping.participants.exists(_.participantId == sourceParticipantId)),
            (),
            s"Party $partyId is not hosted by source participant $sourceParticipantId",
          )
        )
      )
      // Only verify the authorized topology once the expected serial has been authorized.
      // It is conceivable that not only the topology transaction with the expected serial has been authorized,
      // but a subsequent serial as well. Therefore, proceed with topology verification if the head serial is larger
      // than or equal (">=") the expected serial.
      partyToParticipantTopologyPartyAddedO <- EitherT.right[String](
        partyToParticipantTopologyHeadO(partyId, topologyContext.topologyStore).map(
          _.filter(_.serial >= serial)
        )
      )
      // Insist that our serial is the latest head state to raise an error if a potentially conflicting
      // topology transaction has been authorized in the meantime.
      _ <- EitherT.cond[FutureUnlessShutdown](
        partyToParticipantTopologyPartyAddedO.forall(serial == _.serial),
        (),
        s"Specified serial $serial does not match the newest serial ${partyToParticipantTopologyPartyAddedO
            .map(_.serial)} when adding $partyId to $targetParticipantId as part of $requestId. Has there been another potentially conflicting party hosting modification?",
      )
      _ <- partyToParticipantTopologyPartyAddedO.fold(
        EitherT.rightT[FutureUnlessShutdown, String](())
      )(verifyAuthorizedTopology(params, _))
    } yield partyToParticipantTopologyPartyAddedO.map { partyToParticipantTopologyPartyAdded =>
      (partyToParticipantTopologyPartyAdded.validFrom, partyToParticipantTopologyPartyAdded.serial)
    }
  }

  /** Verifies that party onboarding has been properly authorized, i.e. that no concurrent topology
    * change conflicts with party replication.
    */
  private def verifyAuthorizedTopology(
      params: PartyReplicationStatus.ReplicationParams,
      partyToParticipantTopologyPartyAdded: StoredTopologyTransaction[
        TopologyChangeOp.Replace,
        PartyToParticipant,
      ],
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val PartyReplicationStatus
      .ReplicationParams(
        requestId,
        partyId,
        _,
        sourceParticipantId,
        targetParticipantId,
        _,
        _,
      ) = params
    for {
      // Check that the SP and TP are now indeed authorized to host the party.
      _ <- EitherT.cond[FutureUnlessShutdown](
        partyToParticipantTopologyPartyAdded.mapping.participants.exists(p =>
          p.participantId == targetParticipantId && p.onboarding
        ),
        (),
        s"Target participant $targetParticipantId not authorized to onboard party $partyId even though just added as part of request $requestId.",
      )
      _ <- EitherT.cond[FutureUnlessShutdown](
        partyToParticipantTopologyPartyAdded.mapping.participants.exists(
          _.participantId == sourceParticipantId
        ),
        (),
        s"Source participant $sourceParticipantId authorization to host party $partyId has been removed, but is necessary for request $requestId.",
      )
    } yield ()
  }

  private def partyToParticipantTopologyHeadO(
      partyId: PartyId,
      topologyStore: TopologyStore[SynchronizerStore],
      proposal: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[StoredTopologyTransaction[Replace, PartyToParticipant]]] =
    // TODO(#25766): add topology client endpoint
    topologyStore
      .inspect(
        proposals = proposal,
        timeQuery = TimeQuery.HeadState,
        asOfExclusiveO = None,
        op = Some(TopologyChangeOp.Replace),
        types = Seq(TopologyMapping.Code.PartyToParticipant),
        idFilter = Some(partyId.uid.identifier.str),
        namespaceFilter = Some(partyId.uid.namespace.filterString),
      )
      .map(
        _.collectOfMapping[PartyToParticipant]
          .collectOfType[TopologyChangeOp.Replace]
          .result
          .headOption
      )

}
