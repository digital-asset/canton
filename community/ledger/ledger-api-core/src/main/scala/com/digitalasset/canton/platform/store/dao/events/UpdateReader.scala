// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.event.{ArchivedEvent, CreatedEvent, Event, ExercisedEvent}
import com.daml.ledger.api.v2.reassignment.{
  AssignedEvent,
  Reassignment,
  ReassignmentEvent,
  UnassignedEvent,
}
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.trace_context.TraceContext as DamlTraceContext
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.update_service.GetUpdateResponse
import com.digitalasset.canton.LfValue
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.api.TransactionShape
import com.digitalasset.canton.ledger.api.messages.state.AcsRangeInfo
import com.digitalasset.canton.ledger.api.util.{LfEngineToApi, TimestampConversion}
import com.digitalasset.canton.ledger.participant.state.index.IndexUpdateService.UpdateResponse
import com.digitalasset.canton.logging.{ErrorLoggingContext, LoggingContextWithTrace}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.config.ActiveContractsServiceStreamsConfigOverrides
import com.digitalasset.canton.platform.store.LedgerApiContractStore
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  FatCreatedEventProperties,
  RawArchivedEvent,
  RawEvent,
  RawExercisedEvent,
  RawFatActiveContract,
  RawFatAddEvent,
  RawFatAssignEvent,
  RawFatCreatedEvent,
  RawParticipantEvent,
  RawReassignmentEvent,
  RawRemoveEvent,
  RawThinActiveContract,
  RawThinAddEvent,
  RawThinAssignEvent,
  RawThinCreatedEvent,
  RawThinEvent,
  RawTransactionEvent,
  RawUnassignEvent,
  RawUpdateEvent,
}
import com.digitalasset.canton.platform.store.backend.common.UpdatePointwiseQueries.LookupKey
import com.digitalasset.canton.platform.store.dao.{
  DbDispatcher,
  EventProjectionProperties,
  LedgerDaoUpdateReader,
}
import com.digitalasset.canton.platform.{
  ContractId,
  FatContract,
  InternalUpdateFormat,
  TemplatePartiesFilter,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.daml.lf.data.Ref.ChoiceName
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.{SerializationVersion, Versioned}
import io.opentelemetry.api.trace.Span
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.{Done, NotUsed}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** @param updatesStreamReader
  *   Knows how to stream updates
  * @param updatePointwiseReader
  *   Knows how to fetch a tree transaction by its id or its offset
  * @param dispatcher
  *   Executes the queries prepared by this object
  * @param acsReader
  *   Knows how to streams ACS
  * @param executionContext
  *   Runs transformations on data fetched from the database, including Daml-LF value
  *   deserialization
  */
private[dao] final class UpdateReader(
    updatesStreamReader: UpdatesStreamReader,
    updatePointwiseReader: UpdatePointwiseReader,
    dispatcher: DbDispatcher,
    queryValidRange: QueryValidRange,
    eventStorageBackend: EventStorageBackend,
    metrics: LedgerApiServerMetrics,
    acsReader: ACSReader,
)(implicit executionContext: ExecutionContext)
    extends LedgerDaoUpdateReader {

  private val dbMetrics = metrics.index.db

  override def getUpdates(
      offsetRange: OffsetRange,
      internalUpdateFormat: InternalUpdateFormat,
      descendingOrder: Boolean,
      skipPruningChecks: Boolean,
      limit: Option[Int],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, UpdateResponse), NotUsed] = {
    val futureSource =
      getEventSeqIdRange(offsetRange, skipPruningChecks = skipPruningChecks)
        .map {
          case None =>
            // the offset range contains no events (e.g. only offset-only updates), nothing to stream
            Source.empty[(Offset, UpdateResponse)]
          case Some(queryRange) =>
            updatesStreamReader.streamUpdates(
              queryRange = queryRange,
              internalUpdateFormat = internalUpdateFormat,
              descendingOrder = descendingOrder,
              skipPruningChecks = skipPruningChecks,
              limit = limit,
            )
        }
    Source
      .futureSource(futureSource)
      .mapMaterializedValue((_: Future[NotUsed]) => NotUsed)
  }

  override def lookupUpdateBy(
      lookupKey: LookupKey,
      internalUpdateFormat: InternalUpdateFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetUpdateResponse]] =
    updatePointwiseReader.lookupUpdateBy(
      lookupKey = lookupKey,
      internalUpdateFormat = internalUpdateFormat,
    )

  override def getActiveContracts(
      activeAt: Option[Offset],
      filter: TemplatePartiesFilter,
      eventProjectionProperties: EventProjectionProperties,
      rangeInfo: AcsRangeInfo,
      configOverrides: Option[ActiveContractsServiceStreamsConfigOverrides],
      includeIncompleteReassignments: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[GetActiveContractsResponse, NotUsed] =
    activeAt match {
      case None => Source.empty
      case Some(offset) =>
        val futureSource = getMaxAcsEventSeqId(offset)
          .map(maxSeqId =>
            acsReader.streamActiveContracts(
              filteringConstraints = filter,
              activeAt = offset -> maxSeqId,
              eventProjectionProperties = eventProjectionProperties,
              rangeInfo = rangeInfo,
              configOverrides = configOverrides,
              includeIncompleteReassignments = includeIncompleteReassignments,
            )
          )
        Source
          .futureSource(futureSource)
          .mapMaterializedValue((_: Future[NotUsed]) => NotUsed)
    }

  private def getMaxAcsEventSeqId(activeAt: Offset)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Long] =
    queryValidRange.withOffsetNotBeforePruning(
      offset = activeAt,
      errorPruning = pruned =>
        ACSReader.acsBeforePruningErrorReason(
          acsOffset = activeAt,
          prunedUpToOffset = pruned,
        ),
      errorLedgerEnd = ledgerEnd =>
        ACSReader.acsAfterLedgerEndErrorReason(
          acsOffset = activeAt,
          ledgerEndOffset = ledgerEnd,
        ),
    )(
      dispatcher.executeSql(dbMetrics.getAcsEventSeqIdRange)(
        eventStorageBackend.maxEventSequentialId(Some(activeAt))
      )
    )

  private def getEventSeqIdRange(
      offsetRange: OffsetRange,
      skipPruningChecks: Boolean,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[EventsRange]] = {
    def calculateRange() = dispatcher.executeSql(dbMetrics.getEventSeqIdRange) { connection =>
      eventStorageBackend
        .eventSequentialIdRange(offsetRange)(connection)
        .map(eventSeqIdRange =>
          EventsRange(offsetRange = offsetRange, eventSeqIdRange = eventSeqIdRange)
        )
    }

    if (skipPruningChecks) {
      calculateRange()
    } else {
      queryValidRange.withRangeNotPruned(
        offsetRange = offsetRange,
        errorPruning = (prunedOffset: Offset) =>
          s"Transactions request for $offsetRange precedes pruned offset ${prunedOffset.unwrap}",
        errorLedgerEnd = (ledgerEndOffset: Option[Offset]) =>
          s"Transactions request for $offsetRange is beyond ledger end offset ${ledgerEndOffset
              .fold(0L)(_.unwrap)}",
      )(calculateRange())
    }
  }
  override def highestOffsetBeforeOrFirstAt(
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[Offset]] =
    dispatcher.executeSql(dbMetrics.highestOffsetBeforeOrFirstAtRecordTime)(
      eventStorageBackend.lastSynchronizerOffsetBeforeOrFirstAtRecordTime(
        synchronizerId,
        recordTime.underlying,
      )
    )
}

private[dao] object UpdateReader {

  def endSpanOnTermination[Mat](
      span: Span
  )(mat: Mat, done: Future[Done])(implicit ec: ExecutionContext): Mat = {
    done.onComplete {
      case Failure(exception) =>
        span.recordException(exception)
        span.end()
      case Success(_) =>
        span.end()
    }
    mat
  }

  /** Groups together items of type [[A]] that share an attribute [[K]] over a contiguous stretch of
    * the input [[Source]]. Well suited to perform group-by operations of streams where [[K]]
    * attributes are either sorted or at least show up in blocks.
    *
    * Implementation detail: this method _must_ use concatSubstreams instead of mergeSubstreams to
    * prevent the substreams to be processed in parallel, potentially causing the outputs to be
    * delivered in a different order.
    *
    * Docs: https://doc.akka.io/docs/akka/2.6.10/stream/stream-substream.html#groupby
    */
  def groupContiguous[A, K, Mat](
      source: Source[A, Mat]
  )(by: A => K): Source[Vector[A], Mat] =
    source
      .statefulMapConcat { () =>
        @SuppressWarnings(Array("org.wartremover.warts.Var"))
        var previousSegmentKey: Option[K] = None
        entry => {
          val keyForEntry = by(entry)
          val entryWithSplit = entry -> !previousSegmentKey.contains(keyForEntry)
          previousSegmentKey = Some(keyForEntry)
          List(entryWithSplit)
        }
      }
      .splitWhen(_._2)
      .map(_._1)
      .fold(Vector.empty[A])(_ :+ _)
      .concatSubstreams

  def toUnassignedEvent(
      rawUnassignEvent: RawUnassignEvent
  ): UnassignedEvent =
    UnassignedEvent(
      offset = rawUnassignEvent.offset,
      reassignmentId = rawUnassignEvent.reassignmentId,
      contractId = rawUnassignEvent.contractId.coid,
      templateId = Some(LfEngineToApi.toApiIdentifier(rawUnassignEvent.templateId.toIdentifier)),
      packageName = rawUnassignEvent.templateId.pkgName,
      source = rawUnassignEvent.sourceSynchronizerId,
      target = rawUnassignEvent.targetSynchronizerId,
      submitter = rawUnassignEvent.submitter.getOrElse(""),
      reassignmentCounter = rawUnassignEvent.reassignmentCounter,
      assignmentExclusivity =
        rawUnassignEvent.assignmentExclusivity.map(TimestampConversion.fromLf),
      witnessParties = rawUnassignEvent.witnessParties.toSeq,
      nodeId = rawUnassignEvent.nodeId,
    )

  def toAssignedEvent(
      rawAssignEvent: RawFatAssignEvent,
      createdEvent: CreatedEvent,
  ): AssignedEvent =
    AssignedEvent(
      source = rawAssignEvent.sourceSynchronizerId,
      target = rawAssignEvent.targetSynchronizerId,
      reassignmentId = rawAssignEvent.reassignmentId,
      submitter = rawAssignEvent.submitter.getOrElse(""),
      reassignmentCounter = rawAssignEvent.reassignmentCounter,
      createdEvent = Some(createdEvent),
    )

  def toApiReassignment(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(
      rawReassignmentEvents: Seq[RawReassignmentEvent]
  )(implicit lc: LoggingContextWithTrace, ec: ExecutionContext): Future[Option[Reassignment]] =
    MonadUtil
      .sequentialTraverse(rawReassignmentEvents) {
        case rawAssignEvent: RawFatAssignEvent =>
          lfValueTranslation
            .toApiCreatedEvent(
              eventProjectionProperties = eventProjectionProperties,
              fatContractInstance = rawAssignEvent.fatContract,
              offset = rawAssignEvent.offset,
              nodeId = rawAssignEvent.nodeId,
              representativePackageId = rawAssignEvent.representativePackageId,
              witnesses = rawAssignEvent.witnessParties,
              acsDelta = rawAssignEvent.acsDeltaForWitnesses,
            )
            .map(createdEvent =>
              ReassignmentEvent(
                ReassignmentEvent.Event.Assigned(
                  UpdateReader.toAssignedEvent(rawAssignEvent, createdEvent)
                )
              )
            )

        case rawUnassignEvent: RawUnassignEvent =>
          Future.successful(
            ReassignmentEvent(
              ReassignmentEvent.Event.Unassigned(
                UnassignedEvent(
                  offset = rawUnassignEvent.offset,
                  reassignmentId = rawUnassignEvent.reassignmentId,
                  contractId = rawUnassignEvent.contractId.coid,
                  templateId =
                    Some(LfEngineToApi.toApiIdentifier(rawUnassignEvent.templateId.toIdentifier)),
                  packageName = rawUnassignEvent.templateId.pkgName,
                  source = rawUnassignEvent.sourceSynchronizerId,
                  target = rawUnassignEvent.targetSynchronizerId,
                  submitter = rawUnassignEvent.submitter.getOrElse(""),
                  reassignmentCounter = rawUnassignEvent.reassignmentCounter,
                  assignmentExclusivity =
                    rawUnassignEvent.assignmentExclusivity.map(TimestampConversion.fromLf),
                  witnessParties = rawUnassignEvent.witnessParties.toSeq,
                  nodeId = rawUnassignEvent.nodeId,
                )
              )
            )
          )
      }
      .map(reassignments =>
        rawReassignmentEvents.headOption.map { first =>
          Reassignment(
            updateId = first.updateId,
            commandId = first.commandId.getOrElse(""),
            workflowId = first.workflowId.getOrElse(""),
            offset = first.offset,
            events = reassignments,
            recordTime = Some(TimestampConversion.fromLf(first.recordTime)),
            traceContext = Some(DamlTraceContext.parseFrom(first.traceContext)),
            synchronizerId = first.synchronizerId,
            paidTrafficCost = first.trafficCost,
          )
        }
      )

  private def toApiReassignmentEnvelope(rawEvents: Seq[RawUpdateEvent & RawEvent])(
      events: Seq[ReassignmentEvent]
  ): Option[Reassignment] =
    rawEvents.headOption.map { first =>
      Reassignment(
        updateId = first.updateId,
        commandId = first.commandId.getOrElse(""),
        workflowId = first.workflowId.getOrElse(""),
        offset = first.offset,
        events = events,
        recordTime = Some(TimestampConversion.fromLf(first.recordTime)),
        traceContext = Some(DamlTraceContext.parseFrom(first.traceContext)),
        synchronizerId = first.synchronizerId,
        paidTrafficCost = first.trafficCost,
      )
    }

  def archivedEvent(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(rawArchived: RawArchivedEvent): ArchivedEvent =
    archivedEvent(
      eventProjectionProperties = eventProjectionProperties,
      lfValueTranslation = lfValueTranslation,
      offset = rawArchived.offset,
      nodeId = rawArchived.nodeId,
      contractId = rawArchived.contractId,
      templateId = rawArchived.templateId,
      witnessParties = rawArchived.witnessParties,
    )

  def archivedEvent(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
      offset: Long,
      nodeId: Int,
      contractId: ContractId,
      templateId: Ref.FullIdentifier,
      witnessParties: Set[String],
  ): ArchivedEvent =
    ArchivedEvent(
      offset = offset,
      nodeId = nodeId,
      contractId = contractId.coid,
      templateId = Some(
        LfEngineToApi.toApiIdentifier(templateId.toIdentifier)
      ),
      witnessParties = witnessParties.toSeq,
      packageName = templateId.pkgName,
      implementedInterfaces = lfValueTranslation.implementedInterfaces(
        eventProjectionProperties,
        witnessParties,
        templateId,
      ),
    )

  /** The Remove events don't carry the LF version of the removed contract. As the version doesn't
    * influence the rendering of the empty record and unit values, the minimal version is used.
    */
  val removedContractValueVersion: SerializationVersion = SerializationVersion.minVersion

  /** The choice of the consuming exercise a Remove event is served as, when served in a transaction
    * with [[TransactionShape.LedgerEffects]] shape.
    */
  val archiveChoice: ChoiceName = ChoiceName.assertFromString("Archive")

  val archiveChoiceArgument: LfValue.VersionedValue =
    Versioned(removedContractValueVersion, LfValue.ValueRecord(None, ImmArray.empty))

  val archiveExerciseResult: LfValue.VersionedValue =
    Versioned(removedContractValueVersion, LfValue.ValueUnit)

  def deserializeRawTransactionEvent(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(
      rawTransactionEvent: RawTransactionEvent
  )(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): Future[Event] = rawTransactionEvent match {
    case rawCreated: RawFatCreatedEvent =>
      lfValueTranslation
        .toApiCreatedEvent(
          eventProjectionProperties = eventProjectionProperties,
          fatContractInstance = rawCreated.fatContract,
          offset = rawCreated.offset,
          nodeId = rawCreated.nodeId,
          representativePackageId = rawCreated.representativePackageId,
          witnesses = rawCreated.witnessParties,
          acsDelta = rawCreated.acsDeltaForWitnesses,
        )
        .map(createdEvent => Event(Event.Event.Created(createdEvent)))

    case rawArchived: RawArchivedEvent =>
      Future.successful(
        Event(
          Event.Event.Archived(
            archivedEvent(eventProjectionProperties, lfValueTranslation)(
              rawArchived
            )
          )
        )
      )

    case rawExercisedEvent: RawExercisedEvent =>
      lfValueTranslation
        .deserializeRawExercised(
          eventProjectionProperties,
          rawExercisedEvent,
        )
        .map(exercisedEvent => Event(Event.Event.Exercised(exercisedEvent)))
  }

  /** Participant events are served on the Ledger API as a transaction: Add as Create, Remove as
    * Archive The transaction has no command id and its effective time is its record time.
    */
  def toApiParticipantEventTransaction(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
      transactionShape: TransactionShape,
  )(
      rawParticipantEvents: Seq[RawParticipantEvent]
  )(implicit lc: LoggingContextWithTrace, ec: ExecutionContext): Future[Option[Transaction]] =
    MonadUtil
      .sequentialTraverse(rawParticipantEvents) {
        case rawAddEvent: RawFatAddEvent =>
          lfValueTranslation
            .toApiCreatedEvent(
              eventProjectionProperties = eventProjectionProperties,
              fatContractInstance = rawAddEvent.fatContract,
              offset = rawAddEvent.offset,
              nodeId = rawAddEvent.nodeId,
              representativePackageId = rawAddEvent.representativePackageId,
              witnesses = rawAddEvent.witnessParties,
              acsDelta = rawAddEvent.acsDeltaForWitnesses,
            )
            .map(createdEvent => Event(Event.Event.Created(createdEvent)))

        case rawRemoveEvent: RawRemoveEvent =>
          transactionShape match {
            case TransactionShape.AcsDelta =>
              Future.successful(
                Event(
                  Event.Event.Archived(
                    archivedEvent(
                      eventProjectionProperties = eventProjectionProperties,
                      lfValueTranslation = lfValueTranslation,
                      offset = rawRemoveEvent.offset,
                      nodeId = rawRemoveEvent.nodeId,
                      contractId = rawRemoveEvent.contractId,
                      templateId = rawRemoveEvent.templateId,
                      witnessParties = rawRemoveEvent.witnessParties,
                    )
                  )
                )
              )
            case TransactionShape.LedgerEffects =>
              removedAsArchiveExercisedEvent(
                eventProjectionProperties,
                lfValueTranslation,
                rawRemoveEvent,
              ).map(exercisedEvent => Event(Event.Event.Exercised(exercisedEvent)))
          }
      }
      .map(events =>
        rawParticipantEvents.headOption.map { first =>
          Transaction(
            updateId = first.updateId,
            commandId = "",
            effectiveAt = Some(TimestampConversion.fromLf(first.recordTime)),
            workflowId = first.workflowId.getOrElse(""),
            offset = first.offset,
            events = events,
            synchronizerId = first.synchronizerId,
            traceContext = Some(DamlTraceContext.parseFrom(first.traceContext)),
            recordTime = Some(TimestampConversion.fromLf(first.recordTime)),
            externalTransactionHash = None,
            paidTrafficCost = first.trafficCost,
            transactionHash = None,
          )
        }
      )

  /** A Remove event is served on the Ledger API as a consuming exercise of the `Archive` choice by
    * the signatories of the removed contract.
    */
  def removedAsArchiveExercisedEvent(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
      rawRemoveEvent: RawRemoveEvent,
  )(implicit lc: LoggingContextWithTrace, ec: ExecutionContext): Future[ExercisedEvent] = {
    val templateId = rawRemoveEvent.templateId.toIdentifier
    for {
      choiceArgument <- lfValueTranslation.toApiValue(
        value = archiveChoiceArgument,
        verbose = eventProjectionProperties.verbose,
        attribute = "exercise argument",
        enrich = value =>
          lfValueTranslation.enricher
            .enrichChoiceArgument(templateId, None, archiveChoice, value.unversioned),
      )
      exerciseResult <- lfValueTranslation.toApiValue(
        value = archiveExerciseResult,
        verbose = eventProjectionProperties.verbose,
        attribute = "exercise result",
        enrich = value =>
          lfValueTranslation.enricher
            .enrichChoiceResult(templateId, None, archiveChoice, value.unversioned),
      )
    } yield ExercisedEvent(
      offset = rawRemoveEvent.offset,
      nodeId = rawRemoveEvent.nodeId,
      contractId = rawRemoveEvent.contractId.coid,
      templateId = Some(LfEngineToApi.toApiIdentifier(templateId)),
      interfaceId = None,
      choice = archiveChoice,
      choiceArgument = Some(choiceArgument),
      actingParties = rawRemoveEvent.exerciseActors.toSeq,
      consuming = true,
      witnessParties = rawRemoveEvent.witnessParties.toSeq,
      lastDescendantNodeId = rawRemoveEvent.nodeId,
      exerciseResult = Some(exerciseResult),
      packageName = rawRemoveEvent.templateId.pkgName,
      implementedInterfaces = lfValueTranslation.implementedInterfaces(
        eventProjectionProperties,
        rawRemoveEvent.witnessParties,
        rawRemoveEvent.templateId,
      ),
      acsDelta = rawRemoveEvent.witnessParties.nonEmpty,
    )
  }

  /** Participant events are served on the Ledger API as reassignments */
  def toApiParticipantEventReassignment(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(
      rawParticipantEvents: Seq[RawParticipantEvent]
  )(implicit lc: LoggingContextWithTrace, ec: ExecutionContext): Future[Option[Reassignment]] =
    MonadUtil
      .sequentialTraverse(rawParticipantEvents) {
        case rawAddEvent: RawFatAddEvent =>
          lfValueTranslation
            .toApiCreatedEvent(
              eventProjectionProperties = eventProjectionProperties,
              fatContractInstance = rawAddEvent.fatContract,
              offset = rawAddEvent.offset,
              nodeId = rawAddEvent.nodeId,
              representativePackageId = rawAddEvent.representativePackageId,
              witnesses = rawAddEvent.witnessParties,
              acsDelta = rawAddEvent.acsDeltaForWitnesses,
            )
            .map(createdEvent =>
              ReassignmentEvent(
                ReassignmentEvent.Event.Assigned(
                  AssignedEvent(
                    source = rawAddEvent.synchronizerId,
                    target = rawAddEvent.synchronizerId,
                    reassignmentId = "",
                    submitter = "",
                    reassignmentCounter =
                      rawAddEvent.participantEventProperties.reassignmentCounter,
                    createdEvent = Some(createdEvent),
                  )
                )
              )
            )

        case rawRemoveEvent: RawRemoveEvent =>
          Future.successful(
            ReassignmentEvent(
              ReassignmentEvent.Event.Unassigned(
                UnassignedEvent(
                  offset = rawRemoveEvent.offset,
                  nodeId = rawRemoveEvent.nodeId,
                  reassignmentId = "",
                  contractId = rawRemoveEvent.contractId.coid,
                  templateId =
                    Some(LfEngineToApi.toApiIdentifier(rawRemoveEvent.templateId.toIdentifier)),
                  packageName = rawRemoveEvent.templateId.pkgName,
                  source = rawRemoveEvent.synchronizerId,
                  target = rawRemoveEvent.synchronizerId,
                  submitter = "",
                  reassignmentCounter =
                    rawRemoveEvent.participantEventProperties.reassignmentCounter,
                  assignmentExclusivity = None,
                  witnessParties = rawRemoveEvent.witnessParties.toSeq,
                )
              )
            )
          )
      }
      .map(toApiReassignmentEnvelope(rawParticipantEvents))

  def toApiTransaction(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(
      rawTransactionEvents: Seq[RawTransactionEvent]
  )(implicit lc: LoggingContextWithTrace, ec: ExecutionContext): Future[Option[Transaction]] =
    MonadUtil
      .sequentialTraverse(rawTransactionEvents)(
        deserializeRawTransactionEvent(
          eventProjectionProperties = eventProjectionProperties,
          lfValueTranslation = lfValueTranslation,
        )
      )
      .map(events =>
        rawTransactionEvents.headOption.map { first =>
          Transaction(
            updateId = first.updateId,
            commandId = first.commandId.getOrElse(""),
            effectiveAt = Some(TimestampConversion.fromLf(first.ledgerEffectiveTime)),
            workflowId = first.workflowId.getOrElse(""),
            offset = first.offset,
            events = events,
            synchronizerId = first.synchronizerId,
            traceContext = Some(DamlTraceContext.parseFrom(first.traceContext)),
            recordTime = Some(TimestampConversion.fromLf(first.recordTime)),
            externalTransactionHash = first.transactionHash,
            paidTrafficCost = first.trafficCost,
            transactionHash = first.transactionHash,
          )
        }
      )

  def toApiUpdate[T](
      reassignmentEventProjectionProperties: Option[EventProjectionProperties],
      transactionEventProjectionProperties: Option[EventProjectionProperties],
      lfValueTranslation: LfValueTranslation,
      transactionShape: Option[TransactionShape],
  )(rawEvents: Seq[RawEvent])(
      convertReassignment: Reassignment => T,
      convertTransaction: Transaction => T,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): Future[Option[T]] = {
    val reassignmentEvents = rawEvents.collect { case raw: RawReassignmentEvent =>
      raw
    }
    val transactionEvents = rawEvents.collect { case raw: RawTransactionEvent =>
      raw
    }
    val participantEvents = rawEvents.collect { case raw: RawParticipantEvent =>
      raw
    }
    val numberOfNonEmptyCategories =
      Seq(
        transactionEvents,
        reassignmentEvents,
        participantEvents,
      ).count(_.nonEmpty)

    if (numberOfNonEmptyCategories > 1) {
      throw new IllegalStateException(
        s"transaction, reassignment and participant events mixed for one offset"
      )
    } else if (transactionEvents.nonEmpty) {
      transactionEventProjectionProperties match {
        case Some(eventProjectionProperties) =>
          toApiTransaction(
            eventProjectionProperties = eventProjectionProperties,
            lfValueTranslation = lfValueTranslation,
          )(transactionEvents)
            .map(_.map(convertTransaction))

        case None =>
          throw new IllegalStateException(
            s"no internal event format for transactions and transaction event returned from the stream"
          )
      }
    } else if (reassignmentEvents.nonEmpty) {
      reassignmentEventProjectionProperties match {
        case Some(eventProjectionProperties) =>
          toApiReassignment(
            eventProjectionProperties = eventProjectionProperties,
            lfValueTranslation = lfValueTranslation,
          )(reassignmentEvents)
            .map(_.map(convertReassignment))

        case None =>
          throw new IllegalStateException(
            s"no internal event format for reassignments and reassignment event returned from the stream"
          )
      }
    } else if (participantEvents.nonEmpty) {
      if (participantEvents.headOption.exists(_.category.renderAsTransaction)) {
        transactionEventProjectionProperties.zip(transactionShape) match {
          case Some((eventProjectionProperties, shape)) =>
            toApiParticipantEventTransaction(
              eventProjectionProperties = eventProjectionProperties,
              lfValueTranslation = lfValueTranslation,
              transactionShape = shape,
            )(participantEvents)
              .map(_.map(convertTransaction))
          case None => Future.successful(None)
        }
      } else {
        reassignmentEventProjectionProperties match {
          case Some(eventProjectionProperties) =>
            toApiParticipantEventReassignment(
              eventProjectionProperties = eventProjectionProperties,
              lfValueTranslation = lfValueTranslation,
            )(participantEvents)
              .map(_.map(convertReassignment))

          case None =>
            Future.successful(None)
        }
      }
    } else {
      Future.successful(None)
    }
  }

  def eventFilter(templatePartiesFilterO: Option[TemplatePartiesFilter]): RawEvent => Boolean =
    (event: RawEvent) =>
      templatePartiesFilterO.exists(_(event.templateId.toNameTypeConRef, event.witnessParties))

  def filterRawEvents(
      internalUpdateFormat: InternalUpdateFormat
  )(
      rawEvents: Seq[RawEvent]
  ): Seq[RawEvent] = {
    val reassignmentFilter = eventFilter(
      internalUpdateFormat.includeReassignments.map(_.templatePartiesFilter)
    )
    val transactionFilter = eventFilter(
      internalUpdateFormat.includeTransactions.map(_.internalEventFormat.templatePartiesFilter)
    )
    val participantEventFilter = (re: RawParticipantEvent) => {
      if (re.category.renderAsReassignment) {
        reassignmentFilter(re)
      } else {
        transactionFilter(re)
      }
    }
    rawEvents.collect {
      case tx: RawTransactionEvent if transactionFilter(tx) => tx
      case r: RawReassignmentEvent if reassignmentFilter(r) => r
      case ar: RawParticipantEvent if participantEventFilter(ar) => ar
    }
  }

  val getInternalContractIdO: RawThinEvent => Option[Long] = {
    case raw: RawThinAssignEvent => Some(raw.thinCreatedEventProperties.internalContractId)
    case raw: RawThinCreatedEvent => Some(raw.thinCreatedEventProperties.internalContractId)
    case raw: RawThinActiveContract => Some(raw.thinCreatedEventProperties.internalContractId)
    case raw: RawThinAddEvent => Some(raw.thinCreatedEventProperties.internalContractId)
    case _: RawArchivedEvent => None
    case _: RawUnassignEvent => None
    case _: RawRemoveEvent => None
    case _: RawExercisedEvent => None
  }

  val toFatInstance: (RawThinEvent, Option[FatContract]) => RawEvent = {
    case (raw: RawThinAssignEvent, fatContractO) =>
      RawFatAssignEvent(
        reassignmentProperties = raw.reassignmentProperties,
        fatCreatedEventProperties = FatCreatedEventProperties(
          thinCreatedEventProperties = raw.thinCreatedEventProperties,
          fatContract =
            tryFatContract(raw.thinCreatedEventProperties.internalContractId, fatContractO),
        ),
        sourceSynchronizerId = raw.sourceSynchronizerId,
      )
    case (raw: RawThinCreatedEvent, fatContractO) =>
      RawFatCreatedEvent(
        transactionProperties = raw.transactionProperties,
        fatCreatedEventProperties = FatCreatedEventProperties(
          thinCreatedEventProperties = raw.thinCreatedEventProperties,
          fatContract =
            tryFatContract(raw.thinCreatedEventProperties.internalContractId, fatContractO),
        ),
      )
    case (raw: RawThinActiveContract, fatContractO) =>
      RawFatActiveContract(
        commonEventProperties = raw.commonEventProperties,
        fatCreatedEventProperties = FatCreatedEventProperties(
          thinCreatedEventProperties = raw.thinCreatedEventProperties,
          fatContract =
            tryFatContract(raw.thinCreatedEventProperties.internalContractId, fatContractO),
        ),
      )
    case (raw: RawThinAddEvent, fatContractO) =>
      RawFatAddEvent(
        participantEventProperties = raw.participantEventProperties,
        fatCreatedEventProperties = FatCreatedEventProperties(
          thinCreatedEventProperties = raw.thinCreatedEventProperties,
          fatContract =
            tryFatContract(raw.thinCreatedEventProperties.internalContractId, fatContractO),
        ),
      )
    case (raw: RawArchivedEvent, _) => raw
    case (raw: RawUnassignEvent, _) => raw
    case (raw: RawRemoveEvent, _) => raw
    case (raw: RawExercisedEvent, _) => raw
  }

  def tryFatContract(
      internalContractId: Long,
      fatContractO: Option[FatContract],
  ): FatContract = fatContractO.getOrElse(
    throw new IllegalStateException(
      s"Contract for internal contract id $internalContractId was not found in the contract store."
    )
  )

  def withFatContractIfNeeded(contractStore: LedgerApiContractStore)(
      rawEvents: Vector[RawThinEvent]
  )(implicit
      ec: ExecutionContext,
      ecl: ErrorLoggingContext,
  ): Future[Vector[(RawThinEvent, Option[FatContract])]] =
    contractStore
      .lookupBatchedNonReadThrough(rawEvents.flatMap(getInternalContractIdO))(ecl.traceContext)
      .map(contracts =>
        rawEvents.map(event =>
          event -> getInternalContractIdO(event)
            .flatMap(contracts.get)
            .map(_.inst)
        )
      )

  def tryToResolveFatInstance(
      thinEventsWithFatContract: Vector[(RawThinEvent, Option[FatContract])]
  ): Vector[RawEvent] =
    thinEventsWithFatContract.map(toFatInstance.tupled)
}
