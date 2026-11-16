// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.{int, long, str}
import cats.implicits.{
  catsSyntaxTuple2Semigroupal,
  catsSyntaxTuple3Semigroupal,
  catsSyntaxTuple6Semigroupal,
}
import com.digitalasset.canton.config.CantonRequireTypes.String185
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.{
  AuthorizationEvent,
  AuthorizationLevel,
}
import com.digitalasset.canton.ledger.participant.state.index.IndexerPartyDetails
import com.digitalasset.canton.platform.Party
import com.digitalasset.canton.platform.store.backend.PartyStorageBackend.{
  HostedPartyPageBound,
  HostingState,
  PartyHosting,
}
import com.digitalasset.canton.platform.store.backend.RowDef.column
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositeSql,
  OptionCompositeSqlOps,
  SqlStringInterpolation,
}
import com.digitalasset.canton.platform.store.backend.{Conversions, PartyStorageBackend, RowDef}
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.nonempty.NonEmpty

import java.sql.Connection

class PartyStorageBackendTemplate(
    queryStrategy: QueryStrategy,
    participantId: Ref.ParticipantId,
    ledgerEndCache: LedgerEndCache,
    stringInterning: StringInterning,
) extends PartyStorageBackend {

  private object RowDefs {
    import CommonRowDefs.*

    private val partyId = column("party_id", int)
    private val participantId = column("participant_id", int)
    private val synchronizerId = column("synchronizer_id", int)

    val indexerPartyDetails: RowDef[IndexerPartyDetails] =
      (
        column("party", str),
        column("is_local", int),
      ).mapN { (party, isLocal) =>
        IndexerPartyDetails(
          party = Party.assertFromString(party),
          isLocal = isLocal > 0,
        )
      }

    val hostedPartyPageBound: RowDef[HostedPartyPageBound] =
      (partyId, participantId, synchronizerId).mapN(HostedPartyPageBound.apply)

    val lastEventSeqId: RowDef[Long] = column("last_event_seq_id", long)

    // Decodes the level and state of a non-revoked authorization event.
    private val authorizationLevelAndHostingState: RowDef[(AuthorizationLevel, HostingState)] =
      (
        column("participant_authorization_event", int),
        column("participant_permission", int),
      ).mapN(Conversions.authorizationEvent).map {
        case AuthorizationEvent.Onboarding(level) => level -> HostingState.Onboarding
        case active: AuthorizationEvent.ActiveAuthorization => active.level -> HostingState.Active
        case AuthorizationEvent.Revoked =>
          throw new IllegalStateException(
            "Revoked hostings should have been filtered out by the query"
          )
      }

    val partyHosting: RowDef[PartyHosting] =
      (
        partyId,
        participantId,
        synchronizerId,
        offset("event_offset"),
        recordTime,
        authorizationLevelAndHostingState,
      ).mapN(PartyHosting.apply)
  }

  private val revokedAuthorizationEvent = Conversions.authorizationEventInt(
    AuthorizationEvent.Revoked
  )

  /** Queries the known parties from the party-to-participant topology events.
    *
    * A party is considered local when, for at least one (party, participant, synchronizer) triplet:
    *   - the participant matches the participant of this node, and
    *   - the most recent event in that triplet is not a revocation.
    *
    * The results are ordered lexicographically by the party id. The ordering is delegated to the
    * database by the party column, which uses binary ("C") collation.
    */
  private def queryParties(
      partyFilter: ComposableQuery.CompositeSql,
      limitClause: ComposableQuery.CompositeSql,
      validAtEventSeqId: Long,
      connection: Connection,
  ): Vector[IndexerPartyDetails] = {
    val eventSeqIdBound = validAtEventSeqId
    // -1 is never a valid interned id, so a not-yet-interned participant matches no rows.
    val participantInternedId =
      stringInterning.participantId.tryInternalize(participantId).getOrElse(-1)
    RowDefs.indexerPartyDetails.queryMultipleRows(columns => SQL"""
        SELECT $columns
        FROM (
          SELECT
            page.party,
            CASE
              WHEN EXISTS (
                SELECT 1
                FROM lapi_events_party_to_participant latest
                WHERE latest.party_id = page.party_id
                  AND latest.participant_id = $participantInternedId
                  AND latest.event_sequential_id <= $eventSeqIdBound
                  AND latest.participant_authorization_event <> $revokedAuthorizationEvent
                  AND latest.event_sequential_id = (
                    SELECT MAX(event.event_sequential_id)
                    FROM lapi_events_party_to_participant event
                    WHERE event.party_id = latest.party_id
                      AND event.participant_id = latest.participant_id
                      AND event.synchronizer_id = latest.synchronizer_id
                      AND event.event_sequential_id <= $eventSeqIdBound
                  )
              )
              THEN 1
              ELSE 0
            END is_local
          FROM (
            SELECT DISTINCT ON (party)
            party, party_id
            FROM lapi_events_party_to_participant
            WHERE event_sequential_id <= $eventSeqIdBound
              $partyFilter
            ORDER BY party
            $limitClause
          ) page
        ) party_locality
        ORDER BY party
      """)(connection)
  }

  override def parties(parties: Seq[Party])(connection: Connection): List[IndexerPartyDetails] = {
    val requestedParties = parties.view.map(p => (p: String)).toSet
    ledgerEndCache() match {
      case Some(ledgerEnd) if requestedParties.nonEmpty =>
        queryParties(
          partyFilter = cSQL"AND party IN ($requestedParties)",
          limitClause = cSQL"",
          validAtEventSeqId = ledgerEnd.lastEventSeqId,
          connection = connection,
        ).toList
      case _ => Nil
    }
  }

  override def knownParties(
      fromExcl: Option[Party],
      filterString: Option[String185],
      maxResults: Int,
      validAtEventSeqId: Long,
  )(
      connection: Connection
  ): List[IndexerPartyDetails] = {
    val fromExclFilter =
      fromExcl.map(from => cSQL"AND party > ${from: String}").foldToEmptyQuery
    val prefixFilter =
      filterString.map(filter => cSQL"AND party LIKE ${filter.str + "%"}").foldToEmptyQuery
    queryParties(
      partyFilter = cSQL"$fromExclFilter $prefixFilter",
      limitClause = QueryStrategy.limitClause(Some(maxResults)),
      validAtEventSeqId = validAtEventSeqId,
      connection = connection,
    ).toList
  }

  private def partyIdsFilter(partyIds: Option[NonEmpty[Set[Int]]]): CompositeSql =
    partyIds.map(ids => cSQL"AND party_id IN (${ids.forgetNE})").foldToEmptyQuery

  private def participantIdFilter(participantId: Option[Int]): CompositeSql =
    participantId.map(id => cSQL"AND participant_id = $id").foldToEmptyQuery

  private def synchronizerIdsFilter(synchronizerIds: Option[NonEmpty[Set[Int]]]): CompositeSql =
    synchronizerIds.map(ids => cSQL"AND synchronizer_id IN (${ids.forgetNE})").foldToEmptyQuery

  private def fromPageBound(bound: HostedPartyPageBound): CompositeSql =
    cSQL"""AND (party_id, participant_id, synchronizer_id) >=
      (${bound.partyId}, ${bound.participantId}, ${bound.synchronizerId})"""

  private def toPageBound(bound: HostedPartyPageBound): CompositeSql =
    cSQL"""AND (party_id, participant_id, synchronizer_id) <=
      (${bound.partyId}, ${bound.participantId}, ${bound.synchronizerId})"""

  override def hostedPartyPageBoundary(
      partyIds: Option[NonEmpty[Set[Int]]],
      participantId: Option[Int],
      fromInclusive: HostedPartyPageBound,
      limit: Int,
      validAtEventSeqId: Long,
  )(connection: Connection): Option[HostedPartyPageBound] =
    RowDefs.hostedPartyPageBound.querySingleOptRow(columns => SQL"""
      SELECT $columns
      FROM (
        SELECT $columns
        FROM lapi_events_party_to_participant
        WHERE event_sequential_id <= $validAtEventSeqId
          ${fromPageBound(fromInclusive)}
          ${participantIdFilter(participantId)}
          ${partyIdsFilter(partyIds)}
        GROUP BY party_id, participant_id, synchronizer_id
        ORDER BY party_id, participant_id, synchronizer_id
        ${QueryStrategy.limitClause(Some(limit))}
      ) page
      ORDER BY party_id DESC, participant_id DESC, synchronizer_id DESC
      LIMIT 1
    """)(connection)

  // The party_id bounds are implied by the triplet bounds, but help cardinality estimation.
  override def hostedPartyIds(
      partyIds: Option[NonEmpty[Set[Int]]],
      participantId: Option[Int],
      synchronizerIds: Option[NonEmpty[Set[Int]]],
      fromInclusive: HostedPartyPageBound,
      toInclusive: HostedPartyPageBound,
      validAtEventSeqId: Long,
  )(connection: Connection): Vector[Long] =
    RowDefs.lastEventSeqId.queryMultipleRows(columns => SQL"""
      SELECT $columns
      FROM (
        SELECT DISTINCT ON (party_id, participant_id, synchronizer_id)
          party_id, participant_id, synchronizer_id,
          event_sequential_id AS last_event_seq_id, participant_authorization_event
        FROM lapi_events_party_to_participant
        WHERE event_sequential_id <= $validAtEventSeqId
          ${participantIdFilter(participantId)}
          ${synchronizerIdsFilter(synchronizerIds)}
          ${partyIdsFilter(partyIds)}
          ${fromPageBound(fromInclusive)}
          ${toPageBound(toInclusive)}
          AND party_id >= ${fromInclusive.partyId}
          AND party_id <= ${toInclusive.partyId}
        ORDER BY party_id, participant_id, synchronizer_id, event_sequential_id DESC
      ) latest
      WHERE latest.participant_authorization_event <> $revokedAuthorizationEvent
      ORDER BY latest.party_id, latest.participant_id, latest.synchronizer_id
    """)(connection)

  override def partyHostings(
      lastEventSeqIds: Iterable[Long]
  )(connection: Connection): Vector[PartyHosting] = {
    val eventSeqIds = lastEventSeqIds.toSet
    if (eventSeqIds.isEmpty) Vector.empty
    else
      RowDefs.partyHosting.queryMultipleRows(columns => SQL"""
        SELECT $columns
        FROM lapi_events_party_to_participant
        WHERE event_sequential_id ${queryStrategy.anyOf(eventSeqIds)}
      """)(connection)
  }
}
