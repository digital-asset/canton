// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  RawArchivedEvent,
  RawRemoveEvent,
  RawThinCreatedEvent,
  TransactionProperties,
}
import com.digitalasset.canton.platform.store.backend.PersistentEventType
import com.digitalasset.canton.platform.store.backend.PersistentParticipantEventUpdateCategory.{
  AcsImportTxStyle,
  ParticipantEventUpdateCategory,
  PurgeTxStyle,
}
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositeSql,
  SqlStringInterpolation,
}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.daml.lf.data.Ref.Party

import java.sql.Connection

class EventReaderQueries(stringInterning: StringInterning) {
  import EventStorageBackendTemplate.*

  type EventSequentialId = Long

  def fetchContractIdEvents(
      internalContractId: Long,
      requestingParties: Option[Set[Party]],
      endEventSequentialId: EventSequentialId,
  )(
      connection: Connection
  ): (Option[RawThinCreatedEvent], Option[RawArchivedEvent]) = {
    def queryByInternalContractId(
        tableName: String,
        eventTypes: Seq[PersistentEventType],
        categoryO: Option[ParticipantEventUpdateCategory],
        ascending: Boolean,
    )(columns: CompositeSql) =
      categoryO match {
        case Some(category) =>
          SQL"""
         SELECT $columns
         FROM #$tableName
         WHERE
           internal_contract_id = $internalContractId
           AND event_sequential_id <= $endEventSequentialId
           AND event_type IN (#${eventTypes.map(_.asInt).mkString(", ")})
           AND (participant_event_category = ${category.asInt} OR participant_event_category IS NULL)
         ORDER BY event_sequential_id #${if (ascending) "ASC" else "DESC"}
         LIMIT 1
         """
        case None =>
          SQL"""
         SELECT $columns
         FROM #$tableName
         WHERE
           internal_contract_id = $internalContractId
           AND event_sequential_id <= $endEventSequentialId
           AND event_type IN (#${eventTypes.map(_.asInt).mkString(", ")})
         ORDER BY event_sequential_id #${if (ascending) "ASC" else "DESC"}
         LIMIT 1
         """
      }

    def lookupActivateCreated: Option[RawThinCreatedEvent] =
      RowDefs
        .rawThinCreatedEventParser(
          stringInterning = stringInterning,
          allQueryingPartiesO = requestingParties,
          witnessIsAcsDelta = true,
          eventIsAcsDeltaForParticipant = true,
        )
        .querySingleOptRow(
          queryByInternalContractId(
            tableName = "lapi_events_activate_contract",
            eventTypes = Seq(PersistentEventType.Create, PersistentEventType.Add),
            categoryO = Some(AcsImportTxStyle),
            ascending = true,
          )
        )(connection)

    def lookupDeactivateArchived: Option[RawArchivedEvent] =
      RowDefs
        .rawArchivedEventParser(
          stringInterning = stringInterning,
          allQueryingPartiesO = requestingParties,
          acsDeltaForParticipant = true,
        )
        .querySingleOptRow(
          queryByInternalContractId(
            tableName = "lapi_events_deactivate_contract",
            eventTypes = Seq(PersistentEventType.ConsumingExercise),
            ascending = false,
            categoryO = None,
          )
        )(connection)

    // A Remove event is served as an archive whose effective time is its record time
    def lookupDeactivateRemoved: Option[RawArchivedEvent] =
      RowDefs
        .rawRemoveEventParser(
          stringInterning = stringInterning,
          allQueryingPartiesO = requestingParties,
        )
        .querySingleOptRow(
          queryByInternalContractId(
            tableName = "lapi_events_deactivate_contract",
            eventTypes = Seq(PersistentEventType.Remove),
            ascending = false,
            categoryO = Some(PurgeTxStyle),
          )
        )(connection)
        .map(removedAsArchived)

    def lookupWitnessedCreated: Option[RawThinCreatedEvent] =
      RowDefs
        .rawThinCreatedEventParser(
          stringInterning = stringInterning,
          allQueryingPartiesO = requestingParties,
          witnessIsAcsDelta = true,
          eventIsAcsDeltaForParticipant = false,
        )
        .querySingleOptRow(
          queryByInternalContractId(
            tableName = "lapi_events_various_witnessed",
            eventTypes = Seq(PersistentEventType.WitnessedCreate),
            categoryO = None,
            ascending = true,
          )
        )(connection)

    def lookupTransientArchived(createOffset: Long): Option[RawArchivedEvent] =
      RowDefs
        .rawArchivedEventParser(
          stringInterning = stringInterning,
          allQueryingPartiesO = requestingParties,
          acsDeltaForParticipant = false,
        )
        .querySingleOptRow(columns => SQL"""
               SELECT $columns
               FROM lapi_events_various_witnessed
               WHERE
                 internal_contract_id = $internalContractId
                 AND event_sequential_id <= $endEventSequentialId
                 AND event_type = ${PersistentEventType.WitnessedConsumingExercise.asInt}
                 AND event_offset = $createOffset
               ORDER BY event_sequential_id
               LIMIT 1
               """)(connection)

    def lookupDeactivate: Option[RawArchivedEvent] =
      (lookupDeactivateArchived.toList ++ lookupDeactivateRemoved.toList)
        .maxByOption(_.transactionProperties.commonEventProperties.eventSequentialId)

    lookupActivateCreated
      .map(create => Some(create) -> lookupDeactivate)
      .orElse(
        lookupWitnessedCreated.flatMap(create =>
          lookupTransientArchived(
            create.transactionProperties.commonEventProperties.offset
          ).map(transientArchive => Some(create) -> Some(transientArchive))
        )
      )
      .getOrElse(None -> None)
  }

  private def removedAsArchived(remove: RawRemoveEvent): RawArchivedEvent =
    RawArchivedEvent(
      transactionProperties = TransactionProperties(
        commonEventProperties = remove.participantEventProperties.commonEventProperties,
        commonUpdateProperties = remove.participantEventProperties.commonUpdateProperties,
        transactionHash = None,
      ),
      contractId = remove.contractId,
      templateId = remove.templateId,
      filteredStakeholderParties = remove.filteredStakeholderParties,
      ledgerEffectiveTime = remove.participantEventProperties.commonUpdateProperties.recordTime,
      deactivatedEventSeqId = remove.deactivatedEventSeqId,
    )
}
