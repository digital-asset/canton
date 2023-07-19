// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.{array, byteArray, int, long, str}
import anorm.{ResultSetParser, Row, RowParser, SimpleSql, SqlParser, ~}
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.platform.store.backend.ContractStorageBackend
import com.digitalasset.canton.platform.store.backend.ContractStorageBackend.RawContractState
import com.digitalasset.canton.platform.store.backend.Conversions.{
  contractId,
  offset,
  timestampFromMicros,
}
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositeSql,
  SqlStringInterpolation,
}
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlAsVectorOf.*
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.{
  KeyAssigned,
  KeyState,
  KeyUnassigned,
}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.platform.{ContractId, Key, Party}

import java.sql.Connection

class ContractStorageBackendTemplate(
    queryStrategy: QueryStrategy,
    ledgerEndCache: LedgerEndCache,
    stringInterning: StringInterning,
) extends ContractStorageBackend {
  import com.digitalasset.canton.platform.store.backend.Conversions.ArrayColumnToIntArray.*

  override def keyState(key: Key, validAt: Offset)(connection: Connection): KeyState = {
    val resultParser =
      (contractId("contract_id") ~ array[Int]("flat_event_witnesses")).map {
        case cId ~ stakeholders =>
          KeyAssigned(cId, stakeholders.view.map(stringInterning.party.externalize).toSet)
      }.singleOpt

    import com.digitalasset.canton.platform.store.backend.Conversions.HashToStatement
    import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
    SQL"""
         WITH last_contract_key_create AS (
                SELECT participant_events_create.*
                  FROM participant_events_create
                 WHERE create_key_hash = ${key.hash}
                   AND event_offset <= $validAt
                 ORDER BY event_sequential_id DESC
                 FETCH NEXT 1 ROW ONLY
              )
         SELECT contract_id, flat_event_witnesses
           FROM last_contract_key_create -- creation only, as divulged contracts cannot be fetched by key
         WHERE NOT EXISTS
                (SELECT 1
                   FROM participant_events_consuming_exercise
                  WHERE
                    contract_id = last_contract_key_create.contract_id
                    AND event_offset <= $validAt
                )"""
      .as(resultParser)(connection)
      .getOrElse(KeyUnassigned)
  }

  private val fullDetailsContractRowParser
      : RowParser[(Option[ContractId], ContractStorageBackend.RawContractState)] =
    (str("contract_id").? ~ int("template_id").?
      ~ array[Int]("flat_event_witnesses")
      ~ byteArray("create_argument").?
      ~ int("create_argument_compression").?
      ~ int("event_kind")
      ~ timestampFromMicros("ledger_effective_time").?)
      .map {
        case coid ~ internalTemplateId ~ flatEventWitnesses ~ createArgument ~ createArgumentCompression ~ eventKind ~ ledgerEffectiveTime =>
          val cid = coid.toRight("No contract id found").flatMap(ContractId.fromString).toOption
          cid -> RawContractState(
            templateId = internalTemplateId.map(stringInterning.templateId.unsafe.externalize),
            flatEventWitnesses = flatEventWitnesses.view
              .map(stringInterning.party.externalize)
              .toSet,
            createArgument = createArgument,
            createArgumentCompression = createArgumentCompression,
            eventKind = eventKind,
            ledgerEffectiveTime = ledgerEffectiveTime,
          )
      }

  override def contractStates(contractIds: Seq[ContractId], before: Offset)(
      connection: Connection
  ): Map[ContractId, ContractStorageBackend.RawContractState] = if (contractIds.isEmpty) Map.empty
  else {
    // note, below query uses indexes, but the join is run in a loop (using indexes)
    // potential optimization would be to first collect the archivals and then join on the in-memory results
    import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
    SQL"""WITH joined_rows (contract_id, create_event, archive_event, template_id, create_witness, archive_witness,
      create_argument, create_argument_compression, create_ledger_effective_time, archive_ledger_effective_time) AS (SELECT
       c.contract_id,
       c.event_sequential_id,
       a.event_sequential_id,
       c.template_id,
       c.flat_event_witnesses,
       a.flat_event_witnesses,
       c.create_argument,
       c.create_argument_compression,
       c.ledger_effective_time,
       a.ledger_effective_time
     FROM participant_events_create c LEFT JOIN participant_events_consuming_exercise a ON (
      c.contract_id = a.contract_id AND a.event_offset <= $before
     )
     WHERE
       c.contract_id ${queryStrategy.anyOfStrings(contractIds.map(_.coid))}
       AND c.event_offset <= $before
     )
     SELECT contract_id, create_event as event_sequential_id, template_id, create_witness as flat_event_witnesses,
                        create_argument, create_argument_compression, 10 as event_kind, create_ledger_effective_time as ledger_effective_time FROM joined_rows where archive_event is NULL
     UNION ALL
     SELECT contract_id, archive_event as event_sequential_id, template_id, archive_witness as flat_event_witnesses,
               NULL AS create_argument, NULL AS create_argument_compression, 20 as event_kind, archive_ledger_effective_time as ledger_effective_time FROM joined_rows where archive_event is NOT NULL
"""
      .as(fullDetailsContractRowParser.*)(connection)
      .foldLeft(Map.empty[ContractId, ContractStorageBackend.RawContractState]) {
        case (acc, (Some(key), res)) => acc + (key -> res)
        case (acc, _) => acc
      }
  }

  private val contractStateRowParser: RowParser[ContractStorageBackend.RawContractStateEvent] =
    (int("event_kind") ~
      contractId("contract_id") ~
      int("template_id").? ~
      timestampFromMicros("ledger_effective_time").? ~
      byteArray("create_key_value").? ~
      int("create_key_value_compression").? ~
      byteArray("create_argument").? ~
      int("create_argument_compression").? ~
      long("event_sequential_id") ~
      array[Int]("flat_event_witnesses") ~
      offset("event_offset")).map {
      case eventKind ~ contractId ~ internalTemplateId ~ ledgerEffectiveTime ~ createKeyValue ~ createKeyCompression ~ createArgument ~ createArgumentCompression ~ eventSequentialId ~ flatEventWitnesses ~ offset =>
        ContractStorageBackend.RawContractStateEvent(
          eventKind,
          contractId,
          internalTemplateId.map(stringInterning.templateId.externalize),
          ledgerEffectiveTime,
          createKeyValue,
          createKeyCompression,
          createArgument,
          createArgumentCompression,
          flatEventWitnesses.view
            .map(stringInterning.party.externalize)
            .toSet,
          eventSequentialId,
          offset,
        )
    }

  override def contractStateEvents(startExclusive: Long, endInclusive: Long)(
      connection: Connection
  ): Vector[ContractStorageBackend.RawContractStateEvent] = {
    SQL"""
         (SELECT
               10 as event_kind,
               contract_id,
               template_id,
               create_key_value,
               create_key_value_compression,
               create_argument,
               create_argument_compression,
               flat_event_witnesses,
               ledger_effective_time,
               event_sequential_id,
               event_offset
           FROM
               participant_events_create
           WHERE
               event_sequential_id > $startExclusive
               and event_sequential_id <= $endInclusive)
         UNION ALL
         (SELECT
               20 as event_kind,
               contract_id,
               template_id,
               create_key_value,
               create_key_value_compression,
               NULL as create_argument,
               NULL as create_argument_compression,
               flat_event_witnesses,
               ledger_effective_time,
               event_sequential_id,
               event_offset
           FROM
               participant_events_consuming_exercise
           WHERE
               event_sequential_id > $startExclusive
               and event_sequential_id <= $endInclusive)
         ORDER BY event_sequential_id ASC"""
      .asVectorOf(contractStateRowParser)(connection)
  }

  private val contractRowParser: RowParser[ContractStorageBackend.RawContract] =
    (int("template_id")
      ~ byteArray("create_argument")
      ~ int("create_argument_compression").?)
      .map(SqlParser.flatten)
      .map { case (internalTemplateId, createArgument, createArgumentCompression) =>
        new ContractStorageBackend.RawContract(
          stringInterning.templateId.unsafe.externalize(internalTemplateId),
          createArgument,
          createArgumentCompression,
        )
      }

  protected def activeContractSqlLiteral(
      contractId: ContractId,
      treeEventWitnessesClause: CompositeSql,
      resultColumns: List[String],
      coalescedColumns: String,
  ): SimpleSql[Row] = {
    val lastEventSequentialId = ledgerEndCache()._2
    import com.digitalasset.canton.platform.store.backend.Conversions.ContractIdToStatement
    SQL"""  WITH archival_event AS (
               SELECT participant_events_consuming_exercise.*
                 FROM participant_events_consuming_exercise
                WHERE contract_id = $contractId
                  AND event_sequential_id <= $lastEventSequentialId
                  AND $treeEventWitnessesClause  -- only use visible archivals
                FETCH NEXT 1 ROW ONLY
             ),
             create_event AS (
               SELECT contract_id, #${resultColumns.mkString(", ")}
                 FROM participant_events_create
                WHERE contract_id = $contractId
                  AND event_sequential_id <= $lastEventSequentialId
                  AND $treeEventWitnessesClause
                FETCH NEXT 1 ROW ONLY -- limit here to guide planner wrt expected number of results
             ),
             -- no visibility check, as it is used to backfill missing template_id and create_arguments for divulged contracts
             create_event_unrestricted AS (
               SELECT contract_id, #${resultColumns.mkString(", ")}
                 FROM participant_events_create
                WHERE contract_id = $contractId
                  AND event_sequential_id <= $lastEventSequentialId
                FETCH NEXT 1 ROW ONLY -- limit here to guide planner wrt expected number of results
             ),
             divulged_contract AS (
               SELECT divulgence_events.contract_id,
                      -- Note: the divulgence_event.template_id can be NULL
                      -- for certain integrations. For example, the KV integration exploits that
                      -- every participant node knows about all create events. The integration
                      -- therefore only communicates the change in visibility to the IndexDB, but
                      -- does not include a full divulgence event.
                      #$coalescedColumns
                 FROM participant_events_divulgence divulgence_events LEFT OUTER JOIN create_event_unrestricted ON (divulgence_events.contract_id = create_event_unrestricted.contract_id)
                WHERE divulgence_events.contract_id = $contractId -- restrict to aid query planner
                  AND divulgence_events.event_sequential_id <= $lastEventSequentialId
                  AND $treeEventWitnessesClause
                ORDER BY divulgence_events.event_sequential_id
                  -- prudent engineering: make results more stable by preferring earlier divulgence events
                  -- Results might still change due to pruning.
                FETCH NEXT 1 ROW ONLY
             ),
             create_and_divulged_contracts AS (
               (SELECT * FROM create_event)   -- prefer create over divulgence events
               UNION ALL
               (SELECT * FROM divulged_contract)
             )
        SELECT contract_id, #${resultColumns.mkString(", ")}
          FROM create_and_divulged_contracts
         WHERE NOT EXISTS (SELECT 1 FROM archival_event)
         FETCH NEXT 1 ROW ONLY"""
  }

  private def activeContract[T](
      resultSetParser: ResultSetParser[Option[T]],
      resultColumns: List[String],
  )(
      readers: Set[Party],
      contractId: ContractId,
  )(connection: Connection): Option[T] = {
    val internedReaders =
      readers.view.map(stringInterning.party.tryInternalize).flatMap(_.toList).toSet
    if (internedReaders.isEmpty) {
      None
    } else {
      val treeEventWitnessesClause: CompositeSql =
        queryStrategy.arrayIntersectionNonEmptyClause(
          columnName = "tree_event_witnesses",
          internedParties = internedReaders,
        )
      val coalescedColumns: String = resultColumns
        .map(columnName =>
          s"COALESCE(divulgence_events.$columnName, create_event_unrestricted.$columnName) as $columnName"
        )
        .mkString(", ")
      activeContractSqlLiteral(
        contractId,
        treeEventWitnessesClause,
        resultColumns,
        coalescedColumns,
      )
        .as(resultSetParser)(connection)
    }
  }

  private val contractWithoutValueRowParser: RowParser[Int] =
    int("template_id")

  override def activeContractWithArgument(
      readers: Set[Party],
      contractId: ContractId,
  )(connection: Connection): Option[ContractStorageBackend.RawContract] = {
    activeContract(
      resultSetParser = contractRowParser.singleOpt,
      resultColumns = List("template_id", "create_argument", "create_argument_compression"),
    )(
      readers = readers,
      contractId = contractId,
    )(connection)
  }

  override def activeContractWithoutArgument(
      readers: Set[Party],
      contractId: ContractId,
  )(connection: Connection): Option[String] =
    activeContract(
      resultSetParser = contractWithoutValueRowParser.singleOpt,
      resultColumns = List("template_id"),
    )(
      readers = readers,
      contractId = contractId,
    )(connection).map(stringInterning.templateId.unsafe.externalize)
}
