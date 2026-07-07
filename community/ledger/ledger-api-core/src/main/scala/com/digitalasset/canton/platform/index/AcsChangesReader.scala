// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.index

import com.daml.ledger.api.v2.event.Event.Event.Created
import com.daml.ledger.api.v2.reassignment.{Reassignment, ReassignmentEvent}
import com.daml.ledger.api.v2.trace_context.TraceContext as LedgerApiTraceContext
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.api.TransactionShape
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.participant.state.index.IndexUpdateService
import com.digitalasset.canton.ledger.participant.state.index.IndexUpdateService.UpdateResponse
import com.digitalasset.canton.ledger.participant.state.{
  AcsChange,
  ContractStakeholdersAndReassignmentCounter,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.dao.LedgerDaoUpdateReader.DeactivatedContractInfo
import com.digitalasset.canton.platform.store.dao.events.OrderingUtils.offsetOrdering
import com.digitalasset.canton.platform.store.dao.{
  DbDispatcher,
  EventProjectionProperties,
  LedgerDaoUpdateReader,
}
import com.digitalasset.canton.platform.{
  InternalEventFormat,
  InternalTransactionFormat,
  InternalUpdateFormat,
  TemplatePartiesFilter,
}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.{LfPartyId, ReassignmentCounter}
import com.digitalasset.daml.lf.data.Ref.Identifier
import com.google.protobuf.timestamp.Timestamp as ProtoTimestamp
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

class AcsChangesReader(
    updatesReader: LedgerDaoUpdateReader,
    dbDispatcher: DbDispatcher,
    eventStorageBackend: EventStorageBackend,
    metrics: LedgerApiServerMetrics,
)(implicit executionContext: ExecutionContext) {

  import AcsChangesReader.*

  private val dbMetrics = metrics.index.db

  /** Returns a flow that augments the incoming stream of updates with an additional
    * `UpdateResponse.AcsChange` for every transaction and reassignment on `synchronizerId`, merged
    * by offset.
    *
    * The ACS-change side-stream is fetched independently (a party-wildcard ACS-delta read over the
    * same offset range) so that ACS changes are produced regardless of the caller's filters.
    */
  def withAcsChanges(
      synchronizerId: SynchronizerId,
      startInclusive: Offset,
      endInclusive: Offset,
      descendingOrder: Boolean,
      skipPruningChecks: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Flow[(Offset, UpdateResponse), (Offset, UpdateResponse), NotUsed] = {
    val synchronizerIdString = synchronizerId.toProtoPrimitive

    Flow[(Offset, UpdateResponse)].mergeSorted(
      updatesReader
        .getUpdates(
          startInclusive = startInclusive,
          endInclusive = endInclusive,
          internalUpdateFormat = wildcardInternalUpdateFormat,
          descendingOrder = descendingOrder,
          skipPruningChecks = skipPruningChecks,
        )
        // Only transactions and reassignments on the requested synchronizer contribute ACS changes.
        .filter { case (_, updateResponse) =>
          updateResponse match {
            case UpdateResponse.ProtoUpdate(protoUpdate) =>
              protoUpdate.update match {
                case GetUpdatesResponse.Update.Transaction(transaction) =>
                  transaction.synchronizerId == synchronizerIdString
                case GetUpdatesResponse.Update.Reassignment(reassignment) =>
                  reassignment.synchronizerId == synchronizerIdString
                case _ => false
              }
            case _ => false
          }
        }
        .batch(
          max = DeactivationBatchSize.toLong,
          seed = (offsetAndResponse: (Offset, UpdateResponse)) => Vector(offsetAndResponse),
        )(_ :+ _)
        // TODO(#33578) optimize fetch of reassignment counters for archived contracts
        // Fetch the reassignment counters for the archived contracts.
        .mapAsync(4)(acsChangesOf(_, getDeactivatedInfo))
        .mapConcat(identity)
    )(offsetOrdering[UpdateResponse](descendingOrder))
  }

  private def getDeactivatedInfo(transactionOffsets: Vector[Offset])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Map[Offset, Vector[DeactivatedContractInfo]]] =
    if (transactionOffsets.isEmpty) Future.successful(Map.empty)
    else
      dbDispatcher
        .executeSql(dbMetrics.archiveDeactivations)(
          eventStorageBackend.archiveDeactivations(transactionOffsets)
        )
}

object AcsChangesReader {

  private val DeactivationBatchSize = 100

  private val wildcardInternalUpdateFormat: InternalUpdateFormat = {
    val wildcardEventFormat = InternalEventFormat(
      templatePartiesFilter = TemplatePartiesFilter(
        relation = Map.empty,
        // None means all parties known to the participant, i.e. a party-wildcard.
        templateWildcardParties = None,
      ),
      eventProjectionProperties = EventProjectionProperties(
        verbose = false,
        witnessTemplateProjections = Map.empty,
      )(interfaceViewPackageUpgrade =
        // Never invoked: there are no interface filters, hence no interface views to render.
        (interfaceId: Identifier, _: Identifier) =>
          throw new IllegalStateException(
            s"Interface view upgrade is not expected for the ACS-change wildcard format (interface $interfaceId)"
          )
      ),
    )
    InternalUpdateFormat(
      includeTransactions = Some(
        InternalTransactionFormat(
          internalEventFormat = wildcardEventFormat,
          transactionShape = TransactionShape.AcsDelta,
        )
      ),
      includeReassignments = Some(wildcardEventFormat),
      includeTopologyEvents = None,
      includeAcsCommitments = None,
    )
  }

  private def acsChangeUpdateOf(
      acsChange: AcsChange,
      recordTime: Option[ProtoTimestamp],
      offset: Long,
      traceContext: Option[LedgerApiTraceContext],
  ): IndexUpdateService.AcsChangeUpdate = {
    val synchronizerTime = (for {
      ts <- ProtoConverter.required("record_time", recordTime)
      cantonTimestamp <- CantonTimestamp.fromProtoTimestamp(ts)
    } yield cantonTimestamp).fold(
      err => throw new IllegalStateException(s"Could not parse update record time: $err"),
      identity,
    )
    IndexUpdateService.AcsChangeUpdate(
      acsChange = acsChange,
      offset = Offset.tryFromLong(offset),
      recordTime = synchronizerTime,
      traceContext = LedgerClient.traceContextFromLedgerApi(traceContext),
    )
  }

  private def archivedTransactionOffset(updateResponse: UpdateResponse): Option[Offset] =
    updateResponse match {
      case UpdateResponse.ProtoUpdate(protoUpdate) =>
        protoUpdate.update match {
          case GetUpdatesResponse.Update.Transaction(transaction)
              if transaction.events.exists(_.event.isArchived) =>
            Some(Offset.tryFromLong(transaction.offset))
          case _ => None
        }
      case _ => None
    }

  private def acsChangesOf(
      offsetAndResponses: Vector[(Offset, UpdateResponse)],
      getDeactivatedInfo: Vector[Offset] => Future[Map[Offset, Vector[DeactivatedContractInfo]]],
  )(implicit
      executionContext: ExecutionContext
  ): Future[Vector[(Offset, UpdateResponse)]] = {
    // The offsets of the transactions in this batch that contain at least one archive event
    // and therefore require a deactivation lookup.
    val archivedOffsets = offsetAndResponses.flatMap { case (_, updateResponse) =>
      archivedTransactionOffset(updateResponse)
    }
    getDeactivatedInfo(archivedOffsets).map { deactivationsByOffset =>
      offsetAndResponses.flatMap { case (offset, updateResponse) =>
        val deactivations = archivedTransactionOffset(updateResponse)
          .flatMap(deactivationsByOffset.get)
          .getOrElse(Vector.empty)
        acsChangeUpdatesOf(offset, updateResponse, deactivations)
      }
    }
  }

  private def acsChangeUpdatesOf(
      offset: Offset,
      updateResponse: UpdateResponse,
      deactivatedInfos: Vector[DeactivatedContractInfo],
  ): Option[(Offset, UpdateResponse)] =
    updateResponse match {
      case UpdateResponse.ProtoUpdate(protoUpdate) =>
        protoUpdate.update match {
          case GetUpdatesResponse.Update.Transaction(transaction) =>
            Some(
              offset -> UpdateResponse.AcsChange(
                acsChangeUpdateOf(
                  acsChange = acsChangeOf(transaction, deactivatedInfos),
                  recordTime = transaction.recordTime,
                  offset = transaction.offset,
                  traceContext = transaction.traceContext,
                )
              )
            )
          case GetUpdatesResponse.Update.Reassignment(reassignment) =>
            Some(
              offset -> UpdateResponse.AcsChange(
                acsChangeUpdateOf(
                  acsChange = acsChangeOf(reassignment),
                  recordTime = reassignment.recordTime,
                  offset = reassignment.offset,
                  traceContext = reassignment.traceContext,
                )
              )
            )
          case _ => None
        }
      case _ => None
    }

  private def acsChangeOf(reassignment: Reassignment): AcsChange = {
    val activations =
      mutable.Map.empty[LfContractId, ContractStakeholdersAndReassignmentCounter]
    val deactivations =
      mutable.Map.empty[LfContractId, ContractStakeholdersAndReassignmentCounter]

    reassignment.events.foreach(_.event match {
      case ReassignmentEvent.Event.Assigned(assigned) =>
        val createdEvent = assigned.getCreatedEvent
        activations.update(
          LfContractId.assertFromString(createdEvent.contractId),
          ContractStakeholdersAndReassignmentCounter(
            stakeholders = (createdEvent.signatories.view ++ createdEvent.observers.view)
              .map(LfPartyId.assertFromString)
              .toSet,
            reassignmentCounter = ReassignmentCounter(assigned.reassignmentCounter),
          ),
        )
      case ReassignmentEvent.Event.Unassigned(unassigned) =>
        deactivations.update(
          LfContractId.assertFromString(unassigned.contractId),
          ContractStakeholdersAndReassignmentCounter(
            stakeholders = unassigned.witnessParties.view.map(LfPartyId.assertFromString).toSet,
            reassignmentCounter = ReassignmentCounter(unassigned.reassignmentCounter),
          ),
        )
      case ReassignmentEvent.Event.Empty => ()
    })

    AcsChange(activations = activations.toMap, deactivations = deactivations.toMap)
  }

  private def acsChangeOf(
      transaction: Transaction,
      deactivatedInfos: Vector[DeactivatedContractInfo],
  ): AcsChange = {
    val activations =
      mutable.Map.empty[LfContractId, ContractStakeholdersAndReassignmentCounter]

    transaction.events.foreach(_.event match {
      case Created(created) =>
        activations.update(
          LfContractId.assertFromString(created.contractId),
          ContractStakeholdersAndReassignmentCounter(
            stakeholders = (created.signatories.view ++ created.observers.view)
              .map(LfPartyId.assertFromString)
              .toSet,
            reassignmentCounter = ReassignmentCounter(0L),
          ),
        )
      case _ => ()
    })

    AcsChange(
      activations = activations.toMap,
      deactivations = deactivationsMapOf(
        archivedInfos = deactivatedInfos
      ),
    )
  }

  private def deactivationsMapOf(
      archivedInfos: Vector[DeactivatedContractInfo]
  ): Map[LfContractId, ContractStakeholdersAndReassignmentCounter] =
    archivedInfos.view.map { archivedInfo =>
      archivedInfo.contractId -> ContractStakeholdersAndReassignmentCounter(
        stakeholders = archivedInfo.stakeholders,
        reassignmentCounter = archivedInfo.reassignmentCounter,
      )
    }.toMap
}
