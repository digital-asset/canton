// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party.acsreplication

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.party.PartyReplicationTopologyWorkflow
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationTopologyWorkflow.{
  TopologyTransactionInfo,
  partyToParticipantTopologyHeadO,
}
import com.digitalasset.canton.participant.sync.ConnectedSynchronizer
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{StoredTopologyTransaction, TopologyStore}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.{
  PartyToParticipant,
  TopologyChangeOp,
  TopologyMapping,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.nonempty.NonEmpty

import scala.concurrent.ExecutionContext

/** Verifies correct topology for ACS replication. Specifically that the party is hosted on target
  * and source participant.
  */
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
    *   effective time, onboarding flag and serial of the onboarding topology transaction or None if
    *   not yet authorized
    */
  private[party] def verifyAuthorizedTopology(
      params: AcsReplicationStatus.AcsReplicationParameters,
      connectedSynchronizer: ConnectedSynchronizer,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Option[TopologyTransactionInfo]] = {
    val topologyContext =
      PartyReplicationTopologyWorkflow.SynchronizerTopologyContext(connectedSynchronizer)
    for {
      partyToParticipantTopologyTransactionO <- EitherT(
        partyToParticipantTopologyHeadO(params.partyId, topologyContext.topologyStore).map(txO =>
          Either.cond(
            txO.exists(
              _.mapping.participants.exists(_.participantId == params.sourceParticipantId)
            ),
            txO,
            s"Party ${params.partyId} is not hosted by source participant ${params.sourceParticipantId}",
          )
        )
      )
      _ <- partyToParticipantTopologyTransactionO.fold(
        EitherT.rightT[FutureUnlessShutdown, String](())
      )(verifyPtpTopologyTransaction(params, _))
    } yield partyToParticipantTopologyTransactionO.map { partyToParticipantTopologyPartyAdded =>
      TopologyTransactionInfo(
        partyToParticipantTopologyPartyAdded.validFrom,
        partyToParticipantTopologyPartyAdded.transaction.mapping.participants
          .find(_.participantId == params.targetParticipantId)
          .exists(_.onboarding),
        partyToParticipantTopologyPartyAdded.serial,
      )
    }
  }

  /** Verifies that the party-to-participant topology mapping contains both target and source
    * participant.
    */
  private def verifyPtpTopologyTransaction(
      params: AcsReplicationStatus.AcsReplicationParameters,
      partyToParticipantTopologyTransaction: StoredTopologyTransaction[
        TopologyChangeOp.Replace,
        PartyToParticipant,
      ],
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      // Check that the SP and TP are authorized to host the party.
      _ <- EitherT.cond[FutureUnlessShutdown](
        partyToParticipantTopologyTransaction.mapping.participants.exists(p =>
          p.participantId == params.targetParticipantId
        ),
        (),
        s"Target participant ${params.targetParticipantId} not authorized to host party ${params.partyId} during processing of ACS replication request ${params.requestId}.",
      )
      _ <- EitherT.cond[FutureUnlessShutdown](
        partyToParticipantTopologyTransaction.mapping.participants.exists(
          _.participantId == params.sourceParticipantId
        ),
        (),
        s"Source participant ${params.sourceParticipantId} authorization to host party ${params.partyId} has been removed, but is necessary for ACS replication request ${params.requestId}.",
      )
    } yield ()
}

object AcsReplicationTopologyWorkflow {
  final case class TopologyTransactionInfo(
      validFrom: EffectiveTime,
      isOnboarding: Boolean,
      serial: PositiveInt,
  )

  /** The party's current mapping, or its current proposal if `proposal` is set. Filters by the uid
    * only: with a namespace filter, the store also returns the mappings of other parties of that
    * namespace.
    */
  private[party] def partyToParticipantTopologyHeadO(
      partyId: PartyId,
      topologyStore: TopologyStore[SynchronizerStore],
      proposal: Boolean = false,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Option[StoredTopologyTransaction[Replace, PartyToParticipant]]] =
    // TODO(#25766): add topology client endpoint
    topologyStore
      .findPositiveTransactions(
        asOf = CantonTimestamp.MaxValue,
        asOfInclusive = false,
        isProposal = proposal,
        types = Seq(TopologyMapping.Code.PartyToParticipant),
        filterUid = Some(NonEmpty(Seq, partyId.uid)),
        filterNamespace = None,
      )
      .map(_.collectOfMapping[PartyToParticipant].result.headOption)
}
