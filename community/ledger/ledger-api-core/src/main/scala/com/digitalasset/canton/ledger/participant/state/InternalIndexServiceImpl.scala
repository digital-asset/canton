// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction
import com.daml.logging.entries.LoggingEntries
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.messages.state.AcsRangeInfo
import com.digitalasset.canton.ledger.api.{
  CumulativeFilter,
  EventFormat,
  ParticipantAuthorizationFormat,
  TopologyFormat,
  UpdateFormat,
}
import com.digitalasset.canton.ledger.participant.state.index.IndexService
import com.digitalasset.canton.ledger.participant.state.index.IndexUpdateService.UpdateResponse
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref.Party
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

class InternalIndexServiceImpl(indexService: IndexService) extends InternalIndexService {

  override def activeContracts(
      partyIds: Set[LfPartyId],
      validAt: Option[Offset],
  )(implicit traceContext: TraceContext): Source[GetActiveContractsResponse, NotUsed] =
    indexService
      .getActiveContracts(
        eventFormat = EventFormat(
          filtersByParty =
            partyIds.view.map(_ -> CumulativeFilter.templateWildcardFilter(true)).toMap,
          filtersForAnyParty =
            Option.when(partyIds.isEmpty)(CumulativeFilter.templateWildcardFilter(true)),
          verbose = false,
        ),
        activeAt = validAt,
        rangeInfo = AcsRangeInfo.empty,
      )(loggingContext)

  override def topologyTransactions(
      partyId: LfPartyId,
      fromExclusive: Offset,
  )(implicit traceContext: TraceContext): Source[TopologyTransaction, NotUsed] =
    indexService
      .updates(
        begin = Some(fromExclusive),
        endAt = None,
        updateFormat = UpdateFormat(
          includeTransactions = None,
          includeReassignments = None,
          includeTopologyEvents = Some(
            TopologyFormat(
              participantAuthorizationFormat = Some(
                ParticipantAuthorizationFormat(
                  parties = Some(Set(partyId))
                )
              )
            )
          ),
          includeAcsCommitments = None,
          includeAcsChanges = None,
        ),
        descendingOrder = false,
        skipPruningChecks = false,
      )(loggingContext)
      .collect { case UpdateResponse.ProtoUpdate(update) =>
        update
      }
      .mapConcat(_.update.topologyTransaction)

  override def acsUpdates(
      synchronizerId: SynchronizerId,
      fromExclusive: Option[Offset],
  )(implicit
      traceContext: TraceContext
  ): Source[InternalIndexService.AcsUpdateContainer, NotUsed] = {
    val updateFormat = UpdateFormat(
      includeTransactions = None,
      includeReassignments = None,
      includeTopologyEvents = Some(
        TopologyFormat(
          participantAuthorizationFormat = Some(ParticipantAuthorizationFormat(parties = None))
        )
      ),
      includeAcsCommitments = Some(synchronizerId),
      includeAcsChanges = Some(synchronizerId),
    )

    indexService
      .updates(
        begin = fromExclusive,
        endAt = None,
        updateFormat = updateFormat,
        descendingOrder = false,
        skipPruningChecks = false,
      )(loggingContext)
      .mapConcat(InternalIndexService.AcsUpdateContainer.fromUpdateResponse)
  }

  override def acs(
      synchronizerId: SynchronizerId,
      activeAt: Offset,
      stakeholders1: Set[Party],
      stakeholders2: Set[Party],
  )(implicit
      traceContext: TraceContext
  ): Source[InternalIndexService.ActiveContract, NotUsed] =
    throw new UnsupportedOperationException() // TODO(#33232): Implement

  override def counterParties(
      synchronizerId: SynchronizerId,
      activeAt: Offset,
      party: Option[Party],
  ): Source[LfPartyId, NotUsed] =
    throw new UnsupportedOperationException() // TODO(#33232): Implement

  private def loggingContext(implicit traceContext: TraceContext): LoggingContextWithTrace =
    new LoggingContextWithTrace(LoggingEntries.empty, traceContext)
}
