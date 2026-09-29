// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.{int, str}
import anorm.{RowParser, ~}
import com.digitalasset.canton.config.CantonRequireTypes.String185
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent
import com.digitalasset.canton.ledger.participant.state.index.IndexerPartyDetails
import com.digitalasset.canton.platform.Party
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.*
import com.digitalasset.canton.platform.store.backend.{Conversions, PartyStorageBackend}
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.daml.lf.data.Ref

import java.sql.Connection

class PartyStorageBackendTemplate(
    participantId: Ref.ParticipantId,
    ledgerEndCache: LedgerEndCache,
    stringInterning: StringInterning,
) extends PartyStorageBackend {

  private val revokedAuthorizationEvent = Conversions.authorizationEventInt(
    AuthorizationEvent.Revoked
  )

  // Parses a (party id, is-local flag) pair.
  private val partyRowParser: RowParser[(String, Boolean)] =
    str("party") ~ int("is_local") map { case partyId ~ isLocal =>
      (partyId, isLocal > 0)
    }

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
      connection: Connection,
  ): Vector[IndexerPartyDetails] =
    ledgerEndCache() match {
      case None => Vector.empty
      case Some(ledgerEnd) =>
        // -1 is never a valid interned id, so a not-yet-interned participant matches no rows.
        val participantInternedId =
          stringInterning.participantId.tryInternalize(participantId).getOrElse(-1)
        val rows =
          SQL"""
            SELECT
              page.party,
              CASE
                WHEN EXISTS (
                  SELECT 1
                  FROM lapi_events_party_to_participant latest
                  WHERE latest.party_id = page.party_id
                    AND latest.participant_id = $participantInternedId
                    AND latest.event_sequential_id <= ${ledgerEnd.lastEventSeqId}
                    AND latest.participant_authorization_event <> $revokedAuthorizationEvent
                    AND latest.event_sequential_id = (
                      SELECT MAX(event.event_sequential_id)
                      FROM lapi_events_party_to_participant event
                      WHERE event.party_id = latest.party_id
                        AND event.participant_id = latest.participant_id
                        AND event.synchronizer_id = latest.synchronizer_id
                        AND event.event_sequential_id <= ${ledgerEnd.lastEventSeqId}
                    )
                )
                THEN 1
                ELSE 0
              END is_local
            FROM (
              SELECT DISTINCT ON (party)
              party, party_id
              FROM lapi_events_party_to_participant
              WHERE event_sequential_id <= ${ledgerEnd.lastEventSeqId}
                $partyFilter
              ORDER BY party
              $limitClause
            ) page
            ORDER BY page.party
          """.asVectorOf(partyRowParser)(connection)

        rows.map { case (party, isLocal) =>
          IndexerPartyDetails(
            party = Party.assertFromString(party),
            isLocal = isLocal,
          )
        }
    }

  override def parties(parties: Seq[Party])(connection: Connection): List[IndexerPartyDetails] = {
    val requestedParties = parties.view.map(p => (p: String)).toSet
    if (requestedParties.isEmpty) Nil
    else
      queryParties(
        partyFilter = cSQL"AND party IN ($requestedParties)",
        limitClause = cSQL"",
        connection = connection,
      ).toList
  }

  override def knownParties(
      fromExcl: Option[Party],
      filterString: Option[String185],
      maxResults: Int,
  )(
      connection: Connection
  ): List[IndexerPartyDetails] = {
    val fromExclFilter = fromExcl match {
      case Some(from) =>
        val fromStr: String = from
        cSQL"AND party > $fromStr"
      case None => cSQL""
    }
    val prefixFilter = filterString match {
      case Some(filter) =>
        cSQL"AND party LIKE ${filter.str + "%"}"
      case None => cSQL""
    }
    queryParties(
      partyFilter = cSQL"$fromExclFilter $prefixFilter",
      limitClause = QueryStrategy.limitClause(Some(maxResults)),
      connection = connection,
    ).toList
  }

}
