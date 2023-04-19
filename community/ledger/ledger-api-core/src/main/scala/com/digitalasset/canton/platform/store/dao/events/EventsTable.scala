// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.api.util.TimestampConversion
import com.daml.error.ContextualizedErrorLogger
import com.daml.error.definitions.IndexErrors
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.event.Event
import com.daml.ledger.api.v1.transaction.{
  Transaction as ApiTransaction,
  TransactionTree as ApiTransactionTree,
  TreeEvent,
}
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.digitalasset.canton.platform.ApiOffset
import com.digitalasset.canton.platform.api.v1.event.EventOps.{EventOps, TreeEventOps}
import com.digitalasset.canton.platform.store.ScalaPbStreamingOptimizations.*
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.Entry

object EventsTable {

  object TransactionConversions {

    private def flatTransaction(events: Vector[Entry[Event]]): Option[ApiTransaction] =
      events.headOption.flatMap { first =>
        val flatEvents =
          TransactionConversion.removeTransient(events.iterator.map(_.event).toVector)
        // Allows emitting flat transactions with no events, a use-case needed
        // for the functioning of Daml triggers.
        // (more details in https://github.com/digital-asset/daml/issues/6975)
        if (flatEvents.nonEmpty || first.commandId.nonEmpty)
          Some(
            ApiTransaction(
              transactionId = first.transactionId,
              commandId = first.commandId,
              effectiveAt = Some(TimestampConversion.fromLf(first.ledgerEffectiveTime)),
              workflowId = first.workflowId,
              offset = ApiOffset.toApiString(first.eventOffset),
              events = flatEvents,
            )
          )
        else None
      }

    def toGetTransactionsResponse(
        events: Vector[Entry[Event]]
    ): List[(String, GetTransactionsResponse)] =
      flatTransaction(events).toList.map(tx =>
        tx.offset -> GetTransactionsResponse(Seq(tx)).withPrecomputedSerializedSize()
      )

    def toGetFlatTransactionResponse(
        events: Vector[Entry[Event]]
    ): Option[GetFlatTransactionResponse] =
      flatTransaction(events).map(tx => GetFlatTransactionResponse(Some(tx)))

    def toGetActiveContractsResponse(
        events: Vector[Entry[Event]]
    )(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Vector[GetActiveContractsResponse] = {
      events.map {
        case entry if entry.event.isCreated =>
          GetActiveContractsResponse(
            offset = "", // only the last response will have an offset.
            workflowId = entry.workflowId,
            activeContracts = Seq(entry.event.getCreated),
          ).withPrecomputedSerializedSize()
        case entry =>
          throw IndexErrors.DatabaseErrors.ResultSetError
            .Reject(
              s"Non-create event ${entry.event.eventId} fetched as part of the active contracts"
            )
            .asGrpcError
      }
    }

    private def treeOf(
        events: Vector[Entry[TreeEvent]]
    ): (Map[String, TreeEvent], Vector[String]) = {

      // The identifiers of all visible events in this transactions, preserving
      // the order in which they are retrieved from the index
      val visible = events.map(_.event.eventId)
      val visibleOrder = visible.view.zipWithIndex.toMap

      // All events in this transaction by their identifier, with their children
      // filtered according to those visible for this request
      val eventsById =
        events.iterator
          .map(_.event)
          .map(e =>
            e.eventId -> e
              .filterChildEventIds(visibleOrder.contains)
              // childEventIds need to be returned in the event order in the original transaction.
              // Unfortunately, we did not store them ordered in the past so we have to sort it to recover this order.
              // The order is determined by the order of the events, which follows the event order of the original transaction.
              .sortChildEventIdsBy(visibleOrder)
          )
          .toMap

      // All event identifiers that appear as a child of another item in this response
      val children = eventsById.valuesIterator.flatMap(_.childEventIds).toSet

      // The roots for this request are all visible items
      // that are not a child of some other visible item
      val rootEventIds = visible.filterNot(children)

      (eventsById, rootEventIds)

    }

    private def transactionTree(
        events: Vector[Entry[TreeEvent]]
    ): Option[ApiTransactionTree] =
      events.headOption.map { first =>
        val (eventsById, rootEventIds) = treeOf(events)
        ApiTransactionTree(
          transactionId = first.transactionId,
          commandId = first.commandId,
          workflowId = first.workflowId,
          effectiveAt = Some(TimestampConversion.fromLf(first.ledgerEffectiveTime)),
          offset = ApiOffset.toApiString(first.eventOffset),
          eventsById = eventsById,
          rootEventIds = rootEventIds,
        )
      }

    def toGetTransactionTreesResponse(
        events: Vector[Entry[TreeEvent]]
    ): List[(String, GetTransactionTreesResponse)] =
      transactionTree(events).toList.map(tx =>
        tx.offset -> GetTransactionTreesResponse(Seq(tx)).withPrecomputedSerializedSize()
      )

    def toGetTransactionResponse(
        events: Vector[Entry[TreeEvent]]
    ): Option[GetTransactionResponse] =
      transactionTree(events).map(tx => GetTransactionResponse(Some(tx)))

  }

}