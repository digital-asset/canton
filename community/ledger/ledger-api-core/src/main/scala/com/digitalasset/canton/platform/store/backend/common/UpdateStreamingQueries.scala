// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.long
import anorm.~
import com.digitalasset.canton.platform.store.backend.PersistentEventType
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositConcatenationOps,
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
import com.digitalasset.canton.platform.store.dao.events.{
  DecomposedContractIdFilter,
  DecomposedFilter,
  DecomposedTemplatePartyFilter,
}
import com.digitalasset.canton.platform.store.interning.StringInterning

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

  def activateIds(
      decomposedFilter: DecomposedFilter
  ): UpdateIdPageQueryBuilder =
    new UpdateIdPageQueryBuilder(
      eventTypeFilter = idFilter("lapi_events_activate_contract"),
      idPageQueryBuilder = decomposedFilter match {
        case filter: DecomposedTemplatePartyFilter =>
          UpdateStreamingQueries.fetchEventIds(
            tableName = "lapi_filter_activate_stakeholder",
            decomposedFilter = filter,
            stringInterning = stringInterning,
            hasFirstPerSequentialId = true,
          )
        case filter: DecomposedContractIdFilter =>
          UpdateStreamingQueries.fetchEventIds(
            tableName = "lapi_events_activate_contract",
            decomposedFilter = filter,
            stringInterning = stringInterning,
            // the event tables do not have the first_per_sequential_id column
            hasFirstPerSequentialId = false,
          )
      },
    )

  def activateWitnessesIds(
      templatePartyFilter: DecomposedTemplatePartyFilter
  ): UpdateIdPageQueryBuilder =
    new UpdateIdPageQueryBuilder(
      eventTypeFilter = idFilter("lapi_events_activate_contract"),
      idPageQueryBuilder = UpdateStreamingQueries.fetchEventIds(
        tableName = "lapi_filter_activate_witness",
        decomposedFilter = templatePartyFilter,
        stringInterning = stringInterning,
        hasFirstPerSequentialId = true,
      ),
    )

  def deactivateIds(
      decomposedFilter: DecomposedFilter
  ): UpdateIdPageQueryBuilder =
    new UpdateIdPageQueryBuilder(
      eventTypeFilter = idFilter("lapi_events_deactivate_contract"),
      idPageQueryBuilder = decomposedFilter match {
        case filter: DecomposedTemplatePartyFilter =>
          UpdateStreamingQueries.fetchEventIds(
            tableName = "lapi_filter_deactivate_stakeholder",
            decomposedFilter = filter,
            stringInterning = stringInterning,
            hasFirstPerSequentialId = true,
          )
        case filter: DecomposedContractIdFilter =>
          UpdateStreamingQueries.fetchEventIds(
            tableName = "lapi_events_deactivate_contract",
            decomposedFilter = filter,
            stringInterning = stringInterning,
            // the event tables do not have the first_per_sequential_id column
            hasFirstPerSequentialId = false,
          )
      },
    )

  def deactivateWitnessesIds(
      templatePartyFilter: DecomposedTemplatePartyFilter
  ): UpdateIdPageQueryBuilder =
    new UpdateIdPageQueryBuilder(
      eventTypeFilter = idFilter("lapi_events_deactivate_contract"),
      idPageQueryBuilder = UpdateStreamingQueries.fetchEventIds(
        tableName = "lapi_filter_deactivate_witness",
        decomposedFilter = templatePartyFilter,
        stringInterning = stringInterning,
        hasFirstPerSequentialId = true,
      ),
    )

  def variousWitnessIds(
      filter: DecomposedTemplatePartyFilter
  ): UpdateIdPageQueryBuilder =
    new UpdateIdPageQueryBuilder(
      eventTypeFilter = idFilter("lapi_events_various_witnessed"),
      UpdateStreamingQueries.fetchEventIds(
        tableName = "lapi_filter_various_witness",
        decomposedFilter = filter,
        stringInterning = stringInterning,
        hasFirstPerSequentialId = true,
      ),
    )

  def fetchActiveIds(
      templatePartyFilter: DecomposedTemplatePartyFilter,
      activeAtEventSeqId: Long,
  ): IdFilterPageQuery =
    UpdateStreamingQueries
      .fetchEventIds(
        tableName = "lapi_filter_activate_stakeholder",
        decomposedFilter = templatePartyFilter,
        stringInterning = stringInterning,
        hasFirstPerSequentialId = true,
      )
      .toFiltered(eventNotDeactivated(activeAtEventSeqId))

  def fetchAchsIds(
      templatePartyFilter: DecomposedTemplatePartyFilter,
      activeAtEventSeqId: Long,
  ): IdFilterPageQuery =
    UpdateStreamingQueries
      .fetchEventIds(
        tableName = "lapi_filter_achs_stakeholder",
        decomposedFilter = templatePartyFilter,
        stringInterning = stringInterning,
        hasFirstPerSequentialId = true,
      )
      .toFiltered(eventNotDeactivated(activeAtEventSeqId))

  private def idFilter(tableName: String)(eventTypes: Set[PersistentEventType]): CompositeSql =
    cSQL"""
      SELECT 1
      FROM #$tableName data_table
      WHERE
        filters.event_sequential_id = data_table.event_sequential_id
        AND data_table.event_type ${queryStrategy.anyOfSmallInts(eventTypes.map(_.asInt))}
      LIMIT 1"""
}

object UpdateStreamingQueries {

  /** @param tableName
    *   one of the filter tables for create, consuming or non-consuming events, or one of the event
    *   tables having the internal_contract_id column indexed together with the event_sequential_id
    *   if a DecomposedContractIdFilter is used
    * @param decomposedFilter
    *   the filter selecting the events. For DecomposedTemplatePartyFilter: if the party is None,
    *   the event ids for all the parties are fetched; the template id is not applicable for tree tx
    *   stream only oriented filters. For DecomposedContractIdFilter: all the event ids of the given
    *   internal contract id are fetched
    * @param stringInterning
    *   the string interning instance to use for internalizing the party and template id
    * @param hasFirstPerSequentialId
    *   true if the table has the first_per_sequential_id column, false otherwise. If true and the
    *   party of a DecomposedTemplatePartyFilter is None, only a single row per event_sequential_id
    *   will be fetched and thus only unique event ids will be returned
    */
  def fetchEventIds(
      tableName: String,
      decomposedFilter: DecomposedFilter,
      stringInterning: StringInterning,
      hasFirstPerSequentialId: Boolean,
  ): IdPageQueryBuilder =
    filterTableClauses(
      decomposedFilter = decomposedFilter,
      stringInterning = stringInterning,
      hasFirstPerSequentialId = hasFirstPerSequentialId,
    ).map(clauses =>
      new IdPageQueryImpl(
        tableName = tableName,
        filterTableClauses = clauses,
      )
    ).getOrElse(new EmptyIdPageQuery)

  /** @param decomposedFilter
    *   the filter to create the filter table clauses for
    * @param stringInterning
    *   the string interning instance to use for internalizing the party and template id
    * @param hasFirstPerSequentialId
    *   true if the table has the first_per_sequential_id column, false otherwise. Only applicable
    *   to [[DecomposedTemplatePartyFilter]]s.
    * @return
    *   None if the filter cannot match any event (the party or template id was never seen)
    */
  private def filterTableClauses(
      decomposedFilter: DecomposedFilter,
      stringInterning: StringInterning,
      hasFirstPerSequentialId: Boolean,
  ): Option[FilterTableClauses] =
    decomposedFilter match {
      case DecomposedContractIdFilter(internalContractId) =>
        Some(FilterTableClauses.contractId(internalContractId))

      case DecomposedTemplatePartyFilter(witnessO, templateIdO) =>
        // None if the party was never seen, Some(None) if we do not filter by party
        val internedPartyIdO: Option[Option[Int]] = witnessO match {
          case Some(witness) => stringInterning.party.tryInternalize(witness).map(Some(_))
          case None => Some(None)
        }
        // None if the template id was never seen, Some(None) if we do not filter by template id
        val internedTemplateIdO: Option[Option[Int]] = templateIdO match {
          case Some(templateId) =>
            stringInterning.templateId.tryInternalize(templateId).map(Some(_))
          case None => Some(None)
        }
        for {
          internedPartyId <- internedPartyIdO
          internedTemplateId <- internedTemplateIdO
        } yield FilterTableClauses.partyTemplate(
          internedPartyIdO = internedPartyId,
          internedTemplateIdO = internedTemplateId,
          // if we do not filter by party and the table has first_per_sequential_id column, we only fetch a single row per event_sequential_id
          firstPerSequentialIdOnly = witnessO.isEmpty && hasFirstPerSequentialId,
        )
    }

  /** Clauses for querying the filter table.
    *
    * @param filterClauses
    *   additional conditions to be appended to the WHERE clause. Either empty or a conjunction
    *   starting with " AND ".
    * @param orderingClauses
    *   additional ordering columns to be prepended to the ORDER BY event_sequential_id clause.
    *   Either empty or a comma-separated list of columns ending with ", ".
    */
  final case class FilterTableClauses(
      filterClauses: CompositeSql,
      orderingClauses: CompositeSql,
  )

  object FilterTableClauses {

    private def fromSeqs(
        filterClauses: Seq[CompositeSql],
        orderingClauses: Seq[CompositeSql],
    ): FilterTableClauses =
      FilterTableClauses(
        filterClauses =
          if (filterClauses.isEmpty) cSQL""
          else filterClauses.mkComposite(" AND ", " AND ", ""),
        orderingClauses =
          if (orderingClauses.isEmpty) cSQL""
          else orderingClauses.mkComposite("", ", ", ", "),
      )

    /** Clauses for filtering by party and/or template id.
      *
      * @param internedPartyIdO
      *   the interned party id to filter by, if None events for all parties are fetched
      * @param internedTemplateIdO
      *   the interned template id to filter by, if None events for all templates are fetched
      * @param firstPerSequentialIdOnly
      *   if true, only a single row per event_sequential_id is fetched
      */
    def partyTemplate(
        internedPartyIdO: Option[Int],
        internedTemplateIdO: Option[Int],
        firstPerSequentialIdOnly: Boolean,
    ): FilterTableClauses = {
      // use ordering by party_id and template_id even though we are restricting the query to a
      // single party_id and template_id to ensure that the correct db index is used
      val party =
        internedPartyIdO.map(partyId => (cSQL"filters.party_id = $partyId", cSQL"filters.party_id"))
      val template = internedTemplateIdO.map(templateId =>
        (cSQL"filters.template_id = $templateId", cSQL"filters.template_id")
      )

      val (filterClauses, orderingClauses) = (party.toList ++ template.toList).unzip

      val firstPerSequentialId =
        Option.when(firstPerSequentialIdOnly)(cSQL"filters.first_per_sequential_id = true")
      fromSeqs(
        filterClauses = filterClauses ++ firstPerSequentialId.toList,
        orderingClauses = orderingClauses,
      )
    }

    /** Clauses for filtering by internal contract id.
      *
      * @param internalContractId
      *   the internal contract id for which all the event ids should be fetched
      */
    def contractId(internalContractId: Long): FilterTableClauses =
      fromSeqs(
        filterClauses = Seq(cSQL"filters.internal_contract_id = $internalContractId"),
        // use ordering by internal_contract_id even though we are restricting the query to a single
        // internal_contract_id to ensure that the correct db index is used
        orderingClauses = Seq(cSQL"filters.internal_contract_id"),
      )
  }

  private def filterTableSelect(
      tableName: String,
      filterTableClauses: FilterTableClauses,
      paginationFromTo: PaginationFromTo,
      limit: Option[Int],
      idFilterO: Option[CompositeSql],
  ): CompositeSql = {
    val idBoundsSQL =
      if (paginationFromTo.descending)
        cSQL"${paginationFromTo.eventSeqIdRange.endInclusive} <= filters.event_sequential_id AND filters.event_sequential_id <= ${paginationFromTo.eventSeqIdRange.startInclusive}"
      else
        cSQL"${paginationFromTo.eventSeqIdRange.startInclusive} <= filters.event_sequential_id AND filters.event_sequential_id <= ${paginationFromTo.eventSeqIdRange.endInclusive}"
    val idOrderDirectionSQL = if (paginationFromTo.descending) cSQL"DESC" else cSQL"ASC"
    val limitSQL = limit.map(l => cSQL"LIMIT $l").getOrElse(cSQL"")
    val whereSQL = cSQL"$idBoundsSQL ${filterTableClauses.filterClauses}"
    val orderBySQL =
      cSQL"${filterTableClauses.orderingClauses} filters.event_sequential_id $idOrderDirectionSQL"
    val baseSQL =
      cSQL"""
            FROM
              #$tableName filters
            WHERE
              $whereSQL
            ORDER BY
              $orderBySQL -- deliver in index order"""
    idFilterO match {
      case None =>
        cSQL"""
            SELECT filters.event_sequential_id event_sequential_id
            $baseSQL
            $limitSQL"""
      case Some(f) =>
        cSQL"""
            WITH ids_with_probe AS (
              SELECT filters.event_sequential_id event_sequential_id, (
                $f
              ) AS probe_result
              $baseSQL
            )
            SELECT event_sequential_id FROM ids_with_probe
            WHERE ids_with_probe.probe_result = 1
            ORDER BY event_sequential_id $idOrderDirectionSQL
            $limitSQL"""
    }
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
        idFilterO =
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
        idFilterO =
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
                    input.fromTo.withEndInclusive(
                      // as the page queried for limit+1, the last ID represents the exclusive end now,
                      // so we subtract one for ascending and increase one for descending to get the inclusive end
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
        idFilterO = Some(idFilter),
      )
      SQL"$sql".asVectorOf(long("event_sequential_id"))(connection)
    }
  }

  /** Checks if an event is not deactivated. It requires that the table with the activations is
    * called "filters" in the query that will be called.
    */
  def eventNotDeactivated(activeAtEventSeqId: Long): CompositeSql =
    // The sub-query returns 0 for the first matching deactivation, or no row at all
    // if there is none. A scalar sub-query without a result row evaluates to NULL, so COALESCE
    // maps that "no deactivation found" case to 1, while a found deactivation keeps the value 0.
    cSQL"""
      SELECT COALESCE((
        SELECT 0
        FROM lapi_events_deactivate_contract deactivate_evs
        WHERE
          filters.event_sequential_id = deactivate_evs.deactivated_event_sequential_id
          AND deactivate_evs.event_sequential_id <= $activeAtEventSeqId
        LIMIT 1
      ), 1)
    """
}
