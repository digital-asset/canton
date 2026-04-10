// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.long
import anorm.~
import com.digitalasset.canton.platform.Party
import com.digitalasset.canton.platform.store.backend.PersistentEventType
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositeSql,
  SqlStringInterpolation,
}
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.*
import com.digitalasset.canton.platform.store.backend.common.UpdateStreamingQueries.{
  UpdateIdPageQueryBuilder,
  eventNotDeactivated,
}
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream.{
  IdFilterPageQuery,
  IdPage,
  IdPageBounds,
  IdPageQuery,
  PaginationFromTo,
  PaginationInput,
}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.NameTypeConRef

import java.sql.Connection

sealed trait EventPayloadSourceForUpdatesAcsDelta
object EventPayloadSourceForUpdatesAcsDelta {
  object Activate extends EventPayloadSourceForUpdatesAcsDelta
  object Deactivate extends EventPayloadSourceForUpdatesAcsDelta
}
sealed trait EventPayloadSourceForUpdatesLedgerEffects
object EventPayloadSourceForUpdatesLedgerEffects {
  object Activate extends EventPayloadSourceForUpdatesLedgerEffects
  object Deactivate extends EventPayloadSourceForUpdatesLedgerEffects
  object VariousWitnessed extends EventPayloadSourceForUpdatesLedgerEffects
}

class UpdateStreamingQueries(
    stringInterning: StringInterning,
    queryStrategy: QueryStrategy,
) {

  def activateStakeholderIds(
      witnessO: Option[Party],
      templateIdO: Option[NameTypeConRef],
  ): UpdateIdPageQueryBuilder =
    new UpdateIdPageQueryBuilder(
      eventTypeFilter = idFilter("lapi_events_activate_contract"),
      idPageQueryBuilder = UpdateStreamingQueries.fetchEventIds(
        tableName = "lapi_filter_activate_stakeholder",
        witnessO = witnessO,
        templateIdO = templateIdO,
        stringInterning = stringInterning,
        hasFirstPerSequentialId = true,
      ),
    )

  def activateWitnessesIds(
      witnessO: Option[Party],
      templateIdO: Option[NameTypeConRef],
  ): UpdateIdPageQueryBuilder =
    new UpdateIdPageQueryBuilder(
      eventTypeFilter = idFilter("lapi_events_activate_contract"),
      idPageQueryBuilder = UpdateStreamingQueries.fetchEventIds(
        tableName = "lapi_filter_activate_witness",
        witnessO = witnessO,
        templateIdO = templateIdO,
        stringInterning = stringInterning,
        hasFirstPerSequentialId = true,
      ),
    )

  def deactivateStakeholderIds(
      witnessO: Option[Party],
      templateIdO: Option[NameTypeConRef],
  ): UpdateIdPageQueryBuilder =
    new UpdateIdPageQueryBuilder(
      eventTypeFilter = idFilter("lapi_events_deactivate_contract"),
      idPageQueryBuilder = UpdateStreamingQueries.fetchEventIds(
        tableName = "lapi_filter_deactivate_stakeholder",
        witnessO = witnessO,
        templateIdO = templateIdO,
        stringInterning = stringInterning,
        hasFirstPerSequentialId = true,
      ),
    )

  def deactivateWitnessesIds(
      witnessO: Option[Party],
      templateIdO: Option[NameTypeConRef],
  ): UpdateIdPageQueryBuilder =
    new UpdateIdPageQueryBuilder(
      eventTypeFilter = idFilter("lapi_events_deactivate_contract"),
      idPageQueryBuilder = UpdateStreamingQueries.fetchEventIds(
        tableName = "lapi_filter_deactivate_witness",
        witnessO = witnessO,
        templateIdO = templateIdO,
        stringInterning = stringInterning,
        hasFirstPerSequentialId = true,
      ),
    )

  def variousWitnessIds(
      witnessO: Option[Party],
      templateIdO: Option[NameTypeConRef],
  ): UpdateIdPageQueryBuilder =
    new UpdateIdPageQueryBuilder(
      eventTypeFilter = idFilter("lapi_events_various_witnessed"),
      idPageQueryBuilder = UpdateStreamingQueries.fetchEventIds(
        tableName = "lapi_filter_various_witness",
        witnessO = witnessO,
        templateIdO = templateIdO,
        stringInterning = stringInterning,
        hasFirstPerSequentialId = true,
      ),
    )

  def fetchActiveIds(
      stakeholderO: Option[Ref.Party],
      templateIdO: Option[NameTypeConRef],
      activeAtEventSeqId: Long,
  ): IdFilterPageQuery =
    UpdateStreamingQueries
      .fetchEventIds(
        tableName = "lapi_filter_activate_stakeholder",
        witnessO = stakeholderO,
        templateIdO = templateIdO,
        stringInterning = stringInterning,
        hasFirstPerSequentialId = true,
      )
      .toFiltered(eventNotDeactivated(activeAtEventSeqId))

  def fetchAchsIds(
      stakeholderO: Option[Ref.Party],
      templateIdO: Option[NameTypeConRef],
      activeAtEventSeqId: Long,
  ): IdFilterPageQuery =
    UpdateStreamingQueries
      .fetchEventIds(
        tableName = "lapi_filter_achs_stakeholder",
        witnessO = stakeholderO,
        templateIdO = templateIdO,
        stringInterning = stringInterning,
        hasFirstPerSequentialId = true,
      )
      .toFiltered(eventNotDeactivated(activeAtEventSeqId))

  private def idFilter(tableName: String)(eventTypes: Set[PersistentEventType]): CompositeSql =
    cSQL"""
          EXISTS (
            SELECT 1
            FROM #$tableName data_table
            WHERE
              filters.event_sequential_id = data_table.event_sequential_id
              AND data_table.event_type ${queryStrategy.anyOfSmallInts(eventTypes.map(_.asInt))}
          )"""
}

object UpdateStreamingQueries {

  // TODO(i22416): Rename the arguments of this function, as witnessO and templateIdO are inadequate for party topology events.
  /** @param tableName
    *   one of the filter tables for create, consuming or non-consuming events
    * @param witnessO
    *   the party for which to fetch the event ids, if None the event ids for all the parties should
    *   be fetched
    * @param templateIdO
    *   NOTE: this parameter is not applicable for tree tx stream only oriented filters
    * @param stringInterning
    *   the string interning instance to use for internalizing the party and template id
    * @param hasFirstPerSequentialId
    *   true if the table has the first_per_sequential_id column, false otherwise. If true and
    *   witnessO is None, only a single row per event_sequential_id will be fetched and thus only
    *   unique event ids will be returned
    */
  def fetchEventIds(
      tableName: String,
      witnessO: Option[Ref.Party],
      templateIdO: Option[NameTypeConRef],
      stringInterning: StringInterning,
      hasFirstPerSequentialId: Boolean,
  ): IdPageQueryBuilder =
    filterTableClauses(
      witnessO = witnessO,
      templateIdO = templateIdO,
      stringInterning = stringInterning,
      hasFirstPerSequentialId = hasFirstPerSequentialId,
    ).map(clauses =>
      new IdPageQueryImpl(
        tableName = tableName,
        filterTableClauses = clauses,
      )
    ).getOrElse(new EmptyIdPageQuery)

  private def filterTableClauses(
      witnessO: Option[Ref.Party],
      templateIdO: Option[NameTypeConRef],
      stringInterning: StringInterning,
      hasFirstPerSequentialId: Boolean,
  ): Option[FilterTableClauses] = {
    val partyIdFilterO = witnessO match {
      case Some(witness) =>
        stringInterning.party
          .tryInternalize(witness) match {
          case Some(internedPartyFilter) =>
            // use ordering by party_id even though we are restricting the query to a single party_id
            // to ensure that the correct db index is used
            Some((cSQL"AND filters.party_id = $internedPartyFilter", cSQL"filters.party_id,"))
          case None => None // partyFilter never seen
        }
      case None =>
        // do not filter by party, fetch event for all parties
        Some((cSQL"", cSQL""))
    }

    val templateIdFilterO = templateIdO.map(stringInterning.templateId.tryInternalize) match {
      case Some(None) => None // templateIdFilter never seen
      case internedTemplateIdFilterNested =>
        internedTemplateIdFilterNested.flatten // flatten works for both None, Some(Some(x)) case, Some(None) excluded before
        match {
          case Some(internedTemplateId) =>
            // use ordering by template_id even though we are restricting the query to a single template_id
            // to ensure that the correct db index is used
            Some((cSQL"AND filters.template_id = $internedTemplateId", cSQL"filters.template_id,"))
          case None => Some((cSQL"", cSQL""))
        }
    }

    // if we do not filter by party and the table has first_per_sequential_id column, we only fetch a single row per event_sequential_id
    val firstPerSequentialIdClause = witnessO match {
      case None if hasFirstPerSequentialId =>
        cSQL"AND filters.first_per_sequential_id = true"
      case _ => cSQL""
    }

    for {
      (partyIdFilterClause, partyIdOrderingClause) <- partyIdFilterO
      (templateIdFilterClause, templateIdOrderingClause) <- templateIdFilterO
    } yield FilterTableClauses(
      partyIdFilterClause = partyIdFilterClause,
      partyIdOrderingClause = partyIdOrderingClause,
      templateIdFilterClause = templateIdFilterClause,
      templateIdOrderingClause = templateIdOrderingClause,
      firstPerSequentialIdClause = firstPerSequentialIdClause,
    )
  }

  final case class FilterTableClauses(
      partyIdFilterClause: CompositeSql,
      partyIdOrderingClause: CompositeSql,
      templateIdFilterClause: CompositeSql,
      templateIdOrderingClause: CompositeSql,
      firstPerSequentialIdClause: CompositeSql,
  )

  private def filterTableSelect(
      tableName: String,
      filterTableClauses: FilterTableClauses,
      paginationFromTo: PaginationFromTo,
      limit: Option[Int],
      idFilter: Option[CompositeSql],
  ): CompositeSql = {
    val idBoundsSQL =
      if (paginationFromTo.descending)
        cSQL"${paginationFromTo.toInclusive} <= filters.event_sequential_id AND filters.event_sequential_id < ${paginationFromTo.fromExclusive}"
      else
        cSQL"${paginationFromTo.fromExclusive} < filters.event_sequential_id AND filters.event_sequential_id <= ${paginationFromTo.toInclusive}"
    val idOrderDirectionSQL = if (paginationFromTo.descending) cSQL"DESC" else cSQL"ASC"
    cSQL"""
            SELECT filters.event_sequential_id event_sequential_id
            FROM
              #$tableName filters
            WHERE
              $idBoundsSQL
              ${filterTableClauses.partyIdFilterClause}
              ${filterTableClauses.templateIdFilterClause}
              ${filterTableClauses.firstPerSequentialIdClause}
              ${idFilter.map(f => cSQL"AND $f").getOrElse(cSQL"")}
            ORDER BY
              ${filterTableClauses.partyIdOrderingClause}
              ${filterTableClauses.templateIdOrderingClause}
              filters.event_sequential_id $idOrderDirectionSQL -- deliver in index order
            ${limit.map(l => cSQL"LIMIT $l").getOrElse(cSQL"")}"""
  }

  class UpdateIdPageQueryBuilder(
      eventTypeFilter: Set[PersistentEventType] => CompositeSql,
      idPageQueryBuilder: IdPageQueryBuilder,
  ) extends IdPageQuery {
    override def fetchPage(connection: Connection)(input: PaginationInput): IdPage =
      idPageQueryBuilder.fetchPage(connection)(input)

    def filteredForEventTypes(eventTypes: Set[PersistentEventType]): IdFilterPageQuery =
      idPageQueryBuilder.toFiltered(eventTypeFilter(eventTypes))
  }

  trait IdPageQueryBuilder extends IdPageQuery {
    def toFiltered(idFilter: CompositeSql): IdFilterPageQuery
  }

  class EmptyIdPageQuery extends IdPageQueryBuilder {
    override def fetchPage(connection: Connection)(input: PaginationInput): IdPage =
      IdPage(Vector.empty, lastPage = true)

    override def toFiltered(idFilter: CompositeSql): IdFilterPageQuery = new EmptyIdFilterPageQuery
  }

  class IdPageQueryImpl(
      tableName: String,
      filterTableClauses: FilterTableClauses,
  ) extends IdPageQueryBuilder {
    override def fetchPage(connection: Connection)(input: PaginationInput): IdPage = {
      val sql = filterTableSelect(
        tableName = tableName,
        filterTableClauses = filterTableClauses,
        paginationFromTo = input.fromTo,
        limit = Some(input.limit + 1),
        idFilter =
          None, // disable regardless - this is the case where we reuse the query for a no-ID-filter population case
      )
      val ids = SQL"$sql".asVectorOf(long("event_sequential_id"))(connection)
      val lastPage = ids.sizeIs < input.limit + 1
      IdPage(
        ids = if (lastPage) ids else ids.dropRight(1),
        lastPage = lastPage,
      )
    }

    override def toFiltered(idFilter: CompositeSql): IdFilterPageQuery =
      new IdFilterPageQueryImpl(
        tableName = tableName,
        filterTableClauses = filterTableClauses,
        idFilter = idFilter,
      )
  }

  class EmptyIdFilterPageQuery extends IdFilterPageQuery {
    override def fetchPageBounds(connection: Connection)(
        input: PaginationInput
    ): Option[IdPageBounds] = None
    override def fetchPage(connection: Connection)(fromTo: PaginationFromTo): Vector[Long] =
      Vector.empty
  }

  class IdFilterPageQueryImpl(
      tableName: String,
      filterTableClauses: FilterTableClauses,
      idFilter: CompositeSql,
  ) extends IdFilterPageQuery {
    override def fetchPageBounds(
        connection: Connection
    )(input: PaginationInput): Option[IdPageBounds] = {
      val filterTableSQL = filterTableSelect(
        tableName = tableName,
        filterTableClauses = filterTableClauses,
        paginationFromTo = input.fromTo,
        limit = Some(input.limit + 1),
        idFilter =
          None, // disable regardless - this is the case where we reuse the query for a ID-filter population: the paginated query
      )
      val lastElement = if (input.fromTo.descending) cSQL"min" else cSQL"max"
      SQL"""
              WITH unfiltered_ids AS (
              $filterTableSQL
              )
              SELECT
                $lastElement(unfiltered_ids.event_sequential_id) last_event_sequential_id,
                count(*) page_size
              FROM unfiltered_ids
              """
        .asSingle(long("last_event_sequential_id").? ~ long("page_size") map {
          case lastIdO ~ pageSize =>
            lastIdO.map { lastId =>
              val lastPage = pageSize < input.limit + 1
              IdPageBounds(
                fromTo =
                  if (lastPage)
                    input.fromTo // the whole input range is returned as this is the last page
                  else
                    input.fromTo.copy(
                      toInclusive =
                        // as the page queried for limit+1, the last ID represents the toExclusive now
                        if (input.fromTo.descending) lastId + 1
                        else lastId - 1
                    ),
                lastPage = lastPage,
              )
            }
        })(connection)
    }

    override def fetchPage(connection: Connection)(fromTo: PaginationFromTo): Vector[Long] = {
      val sql = filterTableSelect(
        tableName = tableName,
        filterTableClauses = filterTableClauses,
        paginationFromTo = fromTo,
        limit = None,
        idFilter = Some(idFilter),
      )
      SQL"$sql".asVectorOf(long("event_sequential_id"))(connection)
    }
  }

  /** Checks if an event is not deactivated. It requires that the table with the activations is
    * called "filters" in the query that will be called.
    */
  def eventNotDeactivated(activeAtEventSeqId: Long): CompositeSql =
    cSQL"""
      NOT EXISTS (
        SELECT 1
        FROM lapi_events_deactivate_contract deactivate_evs
        WHERE
          filters.event_sequential_id = deactivate_evs.deactivated_event_sequential_id
          AND deactivate_evs.event_sequential_id <= $activeAtEventSeqId
      )
    """
}
