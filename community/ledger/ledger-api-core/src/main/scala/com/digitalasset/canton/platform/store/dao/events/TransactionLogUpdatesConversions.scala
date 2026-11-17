// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.event as apiEvent
import com.daml.ledger.api.v2.reassignment.ReassignmentEvent.Event.{
  Assigned as ApiAssigned,
  Unassigned as ApiUnassigned,
}
import com.daml.ledger.api.v2.reassignment.{
  AssignedEvent as ApiAssignedEvent,
  Reassignment as ApiReassignment,
  ReassignmentEvent as ApiReassignmentEvent,
  UnassignedEvent as ApiUnassignedEvent,
}
import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction
import com.daml.ledger.api.v2.transaction.Transaction as FlatTransaction
import com.daml.ledger.api.v2.update_service.GetUpdateResponse
import com.digitalasset.canton.LfValue
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.TransactionShape.{AcsDelta, LedgerEffects}
import com.digitalasset.canton.ledger.api.util.{LfEngineToApi, TimestampConversion}
import com.digitalasset.canton.ledger.api.{ParticipantAuthorizationFormat, TransactionShape}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.ParticipantEvent
import com.digitalasset.canton.ledger.participant.state.Update.TransactionAccepted.RepresentativePackageId.{
  DedicatedRepresentativePackageId,
  SameAsContractPackageId,
}
import com.digitalasset.canton.ledger.participant.state.index.IndexUpdateService
import com.digitalasset.canton.ledger.participant.state.index.IndexUpdateService.UpdateResponse
import com.digitalasset.canton.logging.{ErrorLoggingContext, LoggingContextWithTrace}
import com.digitalasset.canton.platform.store.ScalaPbStreamingOptimizations.*
import com.digitalasset.canton.platform.store.backend.common.EventStorageBackendTemplate
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties
import com.digitalasset.canton.platform.store.dao.events.EventsTable.TransactionConversions.toTopologyEvent
import com.digitalasset.canton.platform.store.dao.events.TopologyTransactionsStreamReader.{
  CommonTopologyTransactionProperties,
  SynchronizerParametersResponse,
}
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate.{
  CreatedEvent,
  ExercisedEvent,
  FatContractInstanceWithCreatedAt,
  KeyInfo,
}
import com.digitalasset.canton.platform.{
  InternalEventFormat,
  InternalTransactionFormat,
  InternalUpdateFormat,
  Value,
  PackageId as LfPackageId,
}
import com.digitalasset.canton.tracing.SerializableTraceContext
import com.digitalasset.canton.tracing.SerializableTraceContextConverter.SerializableTraceContextExtension
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.daml.lf.data.Ref.{ChoiceName, IdentifierConverter, Party}
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.{
  GlobalKeyWithMaintainers,
  SerializationVersion,
  Versioned,
}
import com.digitalasset.nonempty.NonEmpty

import scala.concurrent.{ExecutionContext, Future}

private[events] object TransactionLogUpdatesConversions {

  def filter(
      internalUpdateFormat: InternalUpdateFormat
  ): TransactionLogUpdate => Option[TransactionLogUpdate] = {
    case transaction: TransactionLogUpdate.TransactionAccepted =>
      internalUpdateFormat.includeTransactions.flatMap { transactionFormat =>
        val transactionEvents = transaction.events.collect {
          case createdEvent: TransactionLogUpdate.CreatedEvent => createdEvent
          case exercisedEvent: TransactionLogUpdate.ExercisedEvent
              if exercisedEvent.consuming || transactionFormat.transactionShape == LedgerEffects =>
            exercisedEvent
        }
        val filteredEvents = transactionEvents
          .filter(transactionPredicate(transactionFormat))

        Option.when(filteredEvents.nonEmpty)(
          transaction.copy(
            events = filteredEvents
          )(transaction.traceContext)
        )

      }

    case _: TransactionLogUpdate.TransactionRejected => None

    case u: TransactionLogUpdate.ReassignmentAccepted =>
      internalUpdateFormat.includeReassignments.flatMap { reassignmentFormat =>
        val filteredReassignments = u.reassignments.iterator.filter { r =>
          reassignmentFormat.templatePartiesFilter(
            r.templateId.toFullIdentifier(r.packageName).toNameTypeConRef,
            r.stakeholders.map(_.toString),
          )
        }
        NonEmpty
          .from(filteredReassignments.toSeq)
          .map(rs => u.copy(reassignments = rs)(u.traceContext))
      }

    case u: TransactionLogUpdate.TopologyTransactionEffective =>
      internalUpdateFormat.includeTopologyEvents
        .filter(_.synchronizerId.forall(_.toProtoPrimitive == u.synchronizerId))
        .flatMap { topologyFormat =>
          val filteredEvents = topologyFormat.participantAuthorizationFormat.fold(
            Vector.empty[TransactionLogUpdate.PartyToParticipantAuthorization]
          )(participantAuthorizationFormat =>
            u.events.filter(topologyEventPredicate(participantAuthorizationFormat))
          )
          val filteredSynchronizerParametersState =
            u.synchronizerParametersState.filter(_ => topologyFormat.synchronizerParametersFormat)
          Option.when(filteredEvents.nonEmpty || filteredSynchronizerParametersState.nonEmpty)(
            u.copy(
              events = filteredEvents,
              synchronizerParametersState = filteredSynchronizerParametersState,
            )(u.traceContext)
          )
        }
    case u: TransactionLogUpdate.ParticipantEventUpdate =>
      // reassignment
      if (u.update.category.renderAsReassignment) {
        internalUpdateFormat.includeReassignments.flatMap { reassignmentFormat =>
          val filteredEvents = u.update.events.filter(event =>
            reassignmentFormat.templatePartiesFilter(
              event.templateId.toFullIdentifier(event.packageName).toNameTypeConRef,
              event.stakeholders.map(_.toString),
            )
          )
          val filtered = u.copy(update = u.update.withEvents(filteredEvents))
          Option.when(filteredEvents.nonEmpty)(filtered)
        }
        // TX
      } else {
        val filteredEvents = u.update.events.filter(event =>
          internalUpdateFormat.includeTransactions.exists(
            _.internalEventFormat.templatePartiesFilter(
              event.templateId.toFullIdentifier(event.packageName).toNameTypeConRef,
              event.stakeholders.map(_.toString),
            )
          )
        )
        Option.when(filteredEvents.nonEmpty)(
          u.copy(update = u.update.withEvents(filteredEvents))
        )

      }
    case commitment: TransactionLogUpdate.ReceivedAcsCommitment =>
      Option.when(
        internalUpdateFormat.includeAcsCommitments.contains(commitment.update.synchronizerId)
      )(commitment)
  }

  def toUpdateResponse(
      internalUpdateFormat: InternalUpdateFormat,
      lfValueTranslation: LfValueTranslation,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ErrorLoggingContext,
      executionContext: ExecutionContext,
  ): TransactionLogUpdate => Future[UpdateResponse] = {

    case transactionAccepted: TransactionLogUpdate.TransactionAccepted =>
      val internalTransactionFormat = internalUpdateFormat.includeTransactions
        .getOrElse(
          ErrorUtil.internalError(
            new IllegalStateException(
              "Transaction cannot be converted as there is no transaction format specified in update format"
            )
          )
        )
      toTransaction(
        transactionAccepted,
        internalTransactionFormat,
        lfValueTranslation,
      )
        .map(transaction =>
          UpdateResponse.ProtoUpdate(
            Some(
              GetUpdateResponse(GetUpdateResponse.Update.Transaction(transaction))
                .withPrecomputedSerializedSize()
            ),
            synchronizerParametersResponse = None,
          )
        )

    case reassignmentAccepted: TransactionLogUpdate.ReassignmentAccepted =>
      val reassignmentInternalEventFormat = internalUpdateFormat.includeReassignments
        .getOrElse(
          ErrorUtil.internalError(
            new IllegalStateException(
              "Reassignment cannot be converted as there is no reassignment specified in update format"
            )
          )
        )
      toReassignment(
        reassignmentAccepted,
        reassignmentInternalEventFormat.templatePartiesFilter.allFilterParties,
        reassignmentInternalEventFormat.eventProjectionProperties,
        lfValueTranslation,
      )
        .map(reassignment =>
          UpdateResponse.ProtoUpdate(
            Some(
              GetUpdateResponse(GetUpdateResponse.Update.Reassignment(reassignment))
                .withPrecomputedSerializedSize()
            ),
            synchronizerParametersResponse = None,
          )
        )

    case topologyTransaction: TransactionLogUpdate.TopologyTransactionEffective =>
      toTopologyTransaction(topologyTransaction).map { transactionO =>
        UpdateResponse.ProtoUpdate(
          response = transactionO.map(transaction =>
            GetUpdateResponse(GetUpdateResponse.Update.TopologyTransaction(transaction))
              .withPrecomputedSerializedSize()
          ),
          synchronizerParametersResponse = toSynchronizerParametersResponse(topologyTransaction),
        )
      }

    case participantEventUpdate: TransactionLogUpdate.ParticipantEventUpdate =>
      if (participantEventUpdate.update.category.renderAsTransaction) {
        val internalTransactionFormat = internalUpdateFormat.includeTransactions
          .getOrElse(
            ErrorUtil.internalError(
              new IllegalStateException(
                "Participant event cannot be converted as there is no transaction format specified in update format"
              )
            )
          )
        toTransaction(
          participantEventUpdateAsTransactionAccepted(participantEventUpdate),
          internalTransactionFormat,
          lfValueTranslation,
        ).map(transaction =>
          UpdateResponse.ProtoUpdate(
            Some(
              GetUpdateResponse(GetUpdateResponse.Update.Transaction(transaction))
                .withPrecomputedSerializedSize()
            ),
            synchronizerParametersResponse = None,
          )
        )
      } else {
        toParticipantEventReassignment(
          participantEventUpdate,
          tryReassignmentInternalEventFormat(internalUpdateFormat),
          lfValueTranslation,
        )
          .map(reassignment =>
            UpdateResponse.ProtoUpdate(
              Some(
                GetUpdateResponse(GetUpdateResponse.Update.Reassignment(reassignment))
                  .withPrecomputedSerializedSize()
              ),
              synchronizerParametersResponse = None,
            )
          )
      }
    case commitment: TransactionLogUpdate.ReceivedAcsCommitment =>
      Future.successful(
        UpdateResponse.AcsCommitment(
          IndexUpdateService.ReceivedAcsCommitment(
            offset = commitment.offset,
            updateId = commitment.update.updateId.toHexString,
            synchronizerId = commitment.update.synchronizerId.toProtoPrimitive,
            recordTime = commitment.update.recordTime.toLf,
            payload = commitment.update.payload,
            traceContext = commitment.traceContext,
          )
        )
      )

    case illegal =>
      ErrorUtil.internalError(new IllegalStateException(s"$illegal is not expected here"))
  }

  def toGetUpdateResponse(
      transactionLogUpdate: TransactionLogUpdate,
      internalUpdateFormat: InternalUpdateFormat,
      lfValueTranslation: LfValueTranslation,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ErrorLoggingContext,
      executionContext: ExecutionContext,
  ): Future[Option[GetUpdateResponse]] =
    filter(internalUpdateFormat)(transactionLogUpdate).fold(
      Future.successful[Option[GetUpdateResponse]](None)
    )(
      toUpdateResponse(internalUpdateFormat, lfValueTranslation)(
        loggingContext,
        errorLoggingContext,
        executionContext,
      )(_)
        .map {
          case UpdateResponse.ProtoUpdate(response, _) => response
          case _ => None
        }
    )

  private def toTransaction(
      transactionAccepted: TransactionLogUpdate.TransactionAccepted,
      internalTransactionFormat: InternalTransactionFormat,
      lfValueTranslation: LfValueTranslation,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): Future[FlatTransaction] = {
    val requestingParties: Option[Set[Party]] =
      internalTransactionFormat.internalEventFormat.templatePartiesFilter.allFilterParties
    val commandId = getCommandId(transactionAccepted.events, requestingParties)
    Future.delegate {
      MonadUtil
        .sequentialTraverse(transactionAccepted.events)(event =>
          toEvent(
            event,
            internalTransactionFormat,
            lfValueTranslation,
          )
        )
        .map(events =>
          FlatTransaction(
            updateId = transactionAccepted.updateId,
            commandId = commandId,
            workflowId = transactionAccepted.workflowId,
            effectiveAt = Some(TimestampConversion.fromLf(transactionAccepted.effectiveAt)),
            events = events,
            offset = transactionAccepted.offset.unwrap,
            synchronizerId = transactionAccepted.synchronizerId,
            traceContext = SerializableTraceContext(transactionAccepted.traceContext).toDamlProto,
            recordTime = Some(TimestampConversion.fromLf(transactionAccepted.recordTime)),
            externalTransactionHash = transactionAccepted.transactionHash.map(_.unwrap),
            paidTrafficCost = transactionAccepted.paidTrafficCost(requestingParties),
            transactionHash = transactionAccepted.transactionHash.map(_.unwrap),
          )
        )
    }
  }

  private def transactionPredicate(
      transactionFormat: InternalTransactionFormat
  )(event: TransactionLogUpdate.Event): Boolean =
    transactionFormat.internalEventFormat.templatePartiesFilter(
      event.templateId.toFullIdentifier(event.packageName).toNameTypeConRef,
      event.witnesses(transactionFormat.transactionShape).map(_.toString),
    )

  private def topologyEventPredicate(
      participantAuthorizationFormat: ParticipantAuthorizationFormat
  )(event: TransactionLogUpdate.PartyToParticipantAuthorization): Boolean =
    matchPartyInSet(event.party)(participantAuthorizationFormat.parties)

  private def toEvent(
      event: TransactionLogUpdate.Event,
      internalTransactionFormat: InternalTransactionFormat,
      lfValueTranslation: LfValueTranslation,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): Future[apiEvent.Event] = {
    val requestingParties =
      internalTransactionFormat.internalEventFormat.templatePartiesFilter.allFilterParties

    event match {
      case createdEvent: TransactionLogUpdate.CreatedEvent =>
        createdToApiCreatedEvent(
          requestingParties,
          internalTransactionFormat.internalEventFormat.eventProjectionProperties,
          lfValueTranslation,
          createdEvent,
          _.witnesses(internalTransactionFormat.transactionShape),
          createdEvent.createdContractInstance,
        ).map(apiCreatedEvent => apiEvent.Event(apiEvent.Event.Event.Created(apiCreatedEvent)))

      case exercisedEvent: TransactionLogUpdate.ExercisedEvent =>
        exercisedToEvent(
          requestingParties,
          exercisedEvent,
          internalTransactionFormat.transactionShape,
          internalTransactionFormat.internalEventFormat.eventProjectionProperties,
          lfValueTranslation,
        )
    }
  }

  private def exercisedToEvent(
      requestingParties: Option[Set[Party]],
      exercisedEvent: ExercisedEvent,
      transactionShape: TransactionShape,
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): Future[apiEvent.Event] =
    transactionShape match {
      case AcsDelta if !exercisedEvent.consuming =>
        Future.failed(
          new IllegalStateException(
            "Non consuming exercise cannot be rendered for ACS delta shape"
          )
        )

      case AcsDelta =>
        val witnessParties = requestingParties match {
          case Some(parties) =>
            parties.iterator.filter(exercisedEvent.flatEventWitnesses).toSeq
          // party-wildcard
          case None => exercisedEvent.flatEventWitnesses.toSeq
        }
        Future.successful(
          apiEvent.Event(
            apiEvent.Event.Event.Archived(
              apiEvent.ArchivedEvent(
                offset = exercisedEvent.eventOffset.unwrap,
                nodeId = exercisedEvent.nodeId,
                contractId = exercisedEvent.contractId.coid,
                templateId = Some(LfEngineToApi.toApiIdentifier(exercisedEvent.templateId)),
                packageName = exercisedEvent.packageName,
                witnessParties = witnessParties,
                implementedInterfaces = lfValueTranslation.implementedInterfaces(
                  eventProjectionProperties,
                  witnessParties.toSet,
                  exercisedEvent.templateId.toFullIdentifier(
                    exercisedEvent.packageName
                  ),
                ),
              )
            )
          )
        )

      case LedgerEffects =>
        val choiceArgumentEnricher = (value: Value) =>
          lfValueTranslation.enricher
            .enrichChoiceArgument(
              exercisedEvent.templateId,
              exercisedEvent.interfaceId,
              Ref.Name.assertFromString(exercisedEvent.choice),
              value.unversioned,
            )

        val eventualChoiceArgument = lfValueTranslation.toApiValue(
          exercisedEvent.exerciseArgument,
          eventProjectionProperties.verbose,
          "exercise argument",
          choiceArgumentEnricher,
        )

        val eventualExerciseResult = exercisedEvent.exerciseResult
          .map { exerciseResult =>
            val choiceResultEnricher = (value: Value) =>
              lfValueTranslation.enricher.enrichChoiceResult(
                exercisedEvent.templateId,
                exercisedEvent.interfaceId,
                Ref.Name.assertFromString(exercisedEvent.choice),
                value.unversioned,
              )

            lfValueTranslation
              .toApiValue(
                value = exerciseResult,
                verbose = eventProjectionProperties.verbose,
                attribute = "exercise result",
                enrich = choiceResultEnricher,
              )
              .map(Some(_))
          }
          .getOrElse(Future.successful(None))

        for {
          choiceArgument <- eventualChoiceArgument
          maybeExerciseResult <- eventualExerciseResult
          witnessParties = requestingParties
            .fold(exercisedEvent.treeEventWitnesses)(
              _.filter(exercisedEvent.treeEventWitnesses)
            )
            .toSeq
          flatEventWitnesses = requestingParties
            .fold(exercisedEvent.flatEventWitnesses)(
              _.filter(exercisedEvent.flatEventWitnesses)
            )
            .toSeq
        } yield apiEvent.Event(
          apiEvent.Event.Event.Exercised(
            apiEvent.ExercisedEvent(
              offset = exercisedEvent.eventOffset.unwrap,
              nodeId = exercisedEvent.nodeId,
              contractId = exercisedEvent.contractId.coid,
              templateId = Some(LfEngineToApi.toApiIdentifier(exercisedEvent.templateId)),
              packageName = exercisedEvent.packageName,
              interfaceId = exercisedEvent.interfaceId.map(LfEngineToApi.toApiIdentifier),
              choice = exercisedEvent.choice,
              choiceArgument = Some(choiceArgument),
              actingParties = exercisedEvent.actingParties.toSeq,
              consuming = exercisedEvent.consuming,
              witnessParties = witnessParties,
              lastDescendantNodeId = exercisedEvent.lastDescendantNodeId,
              exerciseResult = maybeExerciseResult,
              implementedInterfaces =
                if (exercisedEvent.consuming)
                  lfValueTranslation.implementedInterfaces(
                    eventProjectionProperties,
                    witnessParties.toSet,
                    exercisedEvent.templateId.toFullIdentifier(
                      exercisedEvent.packageName
                    ),
                  )
                else Nil,
              acsDelta = flatEventWitnesses.nonEmpty,
            )
          )
        )
    }

  private def createdToApiCreatedEvent(
      requestingPartiesO: Option[Set[Party]],
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
      createdEvent: CreatedEvent,
      createdWitnesses: CreatedEvent => Set[Party],
      contractInstance: FatContractInstanceWithCreatedAt,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): Future[apiEvent.CreatedEvent] =
    createdToApiCreatedEvent(
      requestingPartiesO = requestingPartiesO,
      eventProjectionProperties = eventProjectionProperties,
      lfValueTranslation = lfValueTranslation,
      offset = createdEvent.eventOffset,
      nodeId = createdEvent.nodeId,
      representativePackageId = createdEvent.representativePackageId,
      createdEventWitnesses = createdWitnesses(createdEvent),
      flatEventWitnesses = createdEvent.flatEventWitnesses,
      contractInstance = contractInstance,
    )

  private def createdToApiCreatedEvent(
      requestingPartiesO: Option[Set[Party]],
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
      offset: Offset,
      nodeId: Int,
      representativePackageId: LfPackageId,
      createdEventWitnesses: Set[Party],
      flatEventWitnesses: Set[Party],
      contractInstance: FatContractInstanceWithCreatedAt,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): Future[apiEvent.CreatedEvent] = {

    val witnesses = requestingPartiesO
      .fold(createdEventWitnesses)(_.view.filter(createdEventWitnesses).toSet)
      .map(_.toString)

    val acsDelta =
      requestingPartiesO.fold(flatEventWitnesses.view)(_.view.filter(flatEventWitnesses)).nonEmpty

    lfValueTranslation.toApiCreatedEvent(
      eventProjectionProperties = eventProjectionProperties,
      fatContractInstance = contractInstance,
      offset = offset.unwrap,
      nodeId = nodeId,
      representativePackageId = representativePackageId,
      witnesses = witnesses,
      acsDelta = acsDelta,
    )
  }

  private def matchPartyInSet(party: Party)(optSet: Option[Set[Party]]) =
    optSet match {
      case Some(filterParties) => filterParties.contains(party)
      case None => true
    }

  private def getCommandId(
      flatTransactionEvents: Vector[TransactionLogUpdate.Event],
      requestingPartiesO: Option[Set[Party]],
  ) =
    flatTransactionEvents
      .collectFirst {
        case event
            if EventStorageBackendTemplate.submittersInQueryingParties(
              requestingPartiesO,
              Some(event.submitters.toSeq),
            ) =>
          event.commandId
      }
      .getOrElse("")

  private def toReassignment(
      reassignmentAccepted: TransactionLogUpdate.ReassignmentAccepted,
      requestingParties: Option[Set[Party]],
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): Future[ApiReassignment] = {
    val stringRequestingParties = requestingParties.map(_.map(_.toString))
    val info = reassignmentAccepted.reassignmentInfo
    (MonadUtil
      .sequentialTraverse(reassignmentAccepted.reassignments.toSeq) {
        case assign: state.Reassignment.Assign =>
          val instance = assign.persistedContractInstance.inst
          createdToApiCreatedEvent(
            requestingPartiesO = requestingParties,
            eventProjectionProperties = eventProjectionProperties,
            lfValueTranslation = lfValueTranslation,
            offset = reassignmentAccepted.offset,
            nodeId = assign.nodeId,
            representativePackageId = assign.representativePackageId,
            createdEventWitnesses = instance.stakeholders,
            flatEventWitnesses = instance.stakeholders,
            contractInstance = instance,
          ).map(createdEvent =>
            ApiReassignmentEvent(
              ApiAssigned(
                ApiAssignedEvent(
                  source = info.sourceSynchronizer.unwrap.toProtoPrimitive,
                  target = info.targetSynchronizer.unwrap.toProtoPrimitive,
                  reassignmentId = info.reassignmentId.toProtoPrimitive,
                  submitter = info.submitter.getOrElse(""),
                  reassignmentCounter = assign.reassignmentCounter,
                  createdEvent = Some(createdEvent),
                )
              )
            )
          )

        case unassign: state.Reassignment.Unassign =>
          val stakeholders = unassign.stakeholders
          Future.successful(
            ApiReassignmentEvent(
              ApiUnassigned(
                ApiUnassignedEvent(
                  offset = reassignmentAccepted.offset.unwrap,
                  source = info.sourceSynchronizer.unwrap.toProtoPrimitive,
                  target = info.targetSynchronizer.unwrap.toProtoPrimitive,
                  reassignmentId = info.reassignmentId.toProtoPrimitive,
                  submitter = info.submitter.getOrElse(""),
                  reassignmentCounter = unassign.reassignmentCounter,
                  contractId = unassign.contractId.coid,
                  templateId = Some(LfEngineToApi.toApiIdentifier(unassign.templateId)),
                  packageName = unassign.packageName,
                  assignmentExclusivity =
                    unassign.assignmentExclusivity.map(TimestampConversion.fromLf),
                  witnessParties = requestingParties.fold(stakeholders)(stakeholders.filter).toSeq,
                  nodeId = unassign.nodeId,
                )
              )
            )
          )
      })
      .map(events =>
        ApiReassignment(
          updateId = reassignmentAccepted.updateId,
          commandId = reassignmentAccepted.completionStreamResponseO
            .flatMap(_.completionResponse.completion)
            .filter(completion => stringRequestingParties.fold(true)(completion.actAs.exists))
            .map(_.commandId)
            .getOrElse(""),
          workflowId = reassignmentAccepted.workflowId,
          offset = reassignmentAccepted.offset.unwrap,
          events = events,
          traceContext = SerializableTraceContext(reassignmentAccepted.traceContext).toDamlProto,
          recordTime = Some(TimestampConversion.fromLf(reassignmentAccepted.recordTime)),
          synchronizerId = reassignmentAccepted.synchronizerId,
          paidTrafficCost = reassignmentAccepted.paidTrafficCost(requestingParties),
        )
      )
  }
  private def tryReassignmentInternalEventFormat(
      internalUpdateFormat: InternalUpdateFormat
  )(implicit errorLoggingContext: ErrorLoggingContext): InternalEventFormat =
    internalUpdateFormat.includeReassignments
      .getOrElse(
        ErrorUtil.internalError(
          new IllegalStateException(
            "Participant event update cannot be converted as there is no reassignment format specified in update format"
          )
        )
      )

  private def toParticipantEventReassignment(
      participantEventUpdate: TransactionLogUpdate.ParticipantEventUpdate,
      internalEventFormat: InternalEventFormat,
      lfValueTranslation: LfValueTranslation,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): Future[ApiReassignment] = {
    val requestingParties = internalEventFormat.templatePartiesFilter.allFilterParties
    MonadUtil
      .sequentialTraverse(participantEventUpdate.update.events) {
        case added: ParticipantEvent.Added =>
          createdToApiCreatedEvent(
            requestingPartiesO = requestingParties,
            eventProjectionProperties = internalEventFormat.eventProjectionProperties,
            lfValueTranslation = lfValueTranslation,
            offset = participantEventUpdate.offset,
            nodeId = added.nodeId,
            representativePackageId = added.representativePackageId match {
              case SameAsContractPackageId => added.contractInstance.inst.templateId.packageId
              case DedicatedRepresentativePackageId(packageId) => packageId
            },
            createdEventWitnesses = added.stakeholders,
            flatEventWitnesses = added.stakeholders,
            contractInstance = added.contractInstance.inst,
          ).map(createdEvent =>
            ApiReassignmentEvent(
              ApiAssigned(
                ApiAssignedEvent(
                  source = participantEventUpdate.update.synchronizerId.toProtoPrimitive,
                  target = participantEventUpdate.update.synchronizerId.toProtoPrimitive,
                  reassignmentId = "",
                  submitter = "",
                  reassignmentCounter = added.reassignmentCounter,
                  createdEvent = Some(createdEvent),
                )
              )
            )
          )

        case removed: ParticipantEvent.Removed =>
          Future.successful(
            ApiReassignmentEvent(
              ApiUnassigned(
                ApiUnassignedEvent(
                  offset = participantEventUpdate.offset.unwrap,
                  nodeId = removed.nodeId,
                  reassignmentId = "",
                  contractId = removed.contractId.coid,
                  templateId = Some(LfEngineToApi.toApiIdentifier(removed.templateId)),
                  packageName = removed.packageName,
                  source = participantEventUpdate.update.synchronizerId.toProtoPrimitive,
                  target = participantEventUpdate.update.synchronizerId.toProtoPrimitive,
                  submitter = "",
                  reassignmentCounter = removed.reassignmentCounter,
                  assignmentExclusivity = None,
                  witnessParties =
                    requestingParties.fold(removed.stakeholders)(removed.stakeholders.filter).toSeq,
                )
              )
            )
          )
      }
      .map(events =>
        ApiReassignment(
          updateId = participantEventUpdate.update.updateId.toHexString,
          commandId = "",
          workflowId = participantEventUpdate.update.workflowId.getOrElse(""),
          offset = participantEventUpdate.offset.unwrap,
          events = events,
          traceContext = SerializableTraceContext(participantEventUpdate.traceContext).toDamlProto,
          recordTime =
            Some(TimestampConversion.fromLf(participantEventUpdate.update.recordTime.toLf)),
          synchronizerId = participantEventUpdate.update.synchronizerId.toProtoPrimitive,
          paidTrafficCost = participantEventUpdate.paidTrafficCost(requestingParties),
        )
      )
  }

  def participantEventUpdateAsTransactionAccepted(
      participantEventUpdate: TransactionLogUpdate.ParticipantEventUpdate
  ): TransactionLogUpdate.TransactionAccepted = {
    val update = participantEventUpdate.update
    val updateId = update.updateId.toHexString
    val workflowId = update.workflowId.getOrElse("")
    val recordTime = update.recordTime.toLf
    def keyInfo(
        version: SerializationVersion,
        key: GlobalKeyWithMaintainers,
    ): KeyInfo =
      KeyInfo(
        value = Versioned(version, key.value),
        hash = key.globalKey.hash,
        maintainers = key.maintainers,
      )
    val events: Vector[TransactionLogUpdate.Event] = update.events.iterator.map {
      case added: ParticipantEvent.Added =>
        val contractInstance = added.contractInstance.inst
        TransactionLogUpdate.CreatedEvent(
          eventOffset = participantEventUpdate.offset,
          updateId = updateId,
          nodeId = added.nodeId,
          eventSequentialId = 0L,
          contractId = contractInstance.contractId,
          ledgerEffectiveTime = contractInstance.createdAt.time,
          templateId = added.templateId,
          representativePackageId = added.representativePackageId match {
            case SameAsContractPackageId => contractInstance.templateId.packageId
            case DedicatedRepresentativePackageId(packageId) => packageId
          },
          packageName = added.packageName,
          packageVersion = None,
          commandId = "",
          workflowId = workflowId,
          keyInfo = contractInstance.contractKeyWithMaintainers
            .map(keyInfo(contractInstance.version, _)),
          treeEventWitnesses = added.stakeholders,
          flatEventWitnesses = added.stakeholders,
          submitters = added.stakeholders,
          authenticationData = contractInstance.authenticationData,
          createdContractInstance = contractInstance,
        )

      case removed: ParticipantEvent.Removed =>
        TransactionLogUpdate.ExercisedEvent(
          eventOffset = participantEventUpdate.offset,
          updateId = updateId,
          nodeId = removed.nodeId,
          eventSequentialId = 0L,
          contractId = removed.contractId,
          ledgerEffectiveTime = recordTime,
          templateId = removed.templateId,
          packageName = removed.packageName,
          interfaceId = None,
          commandId = "",
          workflowId = workflowId,
          keyInfo = removed.keyOpt.map(keyInfo(SerializationVersion.minVersion, _)),
          treeEventWitnesses = removed.stakeholders,
          flatEventWitnesses = removed.stakeholders,
          submitters = removed.stakeholders,
          choice = ChoiceName.assertFromString("Archive"),
          actingParties = removed.signatories.getOrElse(removed.stakeholders),
          lastDescendantNodeId = removed.nodeId,
          exerciseArgument =
            Versioned(SerializationVersion.minVersion, LfValue.ValueRecord(None, ImmArray.empty)),
          exerciseResult = Some(Versioned(SerializationVersion.minVersion, LfValue.ValueUnit)),
          consuming = true,
        )
    }.toVector
    TransactionLogUpdate.TransactionAccepted(
      updateId = updateId,
      commandId = "",
      workflowId = workflowId,
      effectiveAt = events
        .collectFirst { case c: TransactionLogUpdate.CreatedEvent =>
          c.ledgerEffectiveTime
        }
        .getOrElse(recordTime),
      offset = participantEventUpdate.offset,
      events = events,
      completionStreamResponseO = None,
      synchronizerId = update.synchronizerId.toProtoPrimitive,
      recordTime = recordTime,
      transactionHash = None,
    )(participantEventUpdate.traceContext)
  }

  private def toTopologyTransaction(
      topologyTransaction: TransactionLogUpdate.TopologyTransactionEffective
  ): Future[Option[TopologyTransaction]] = Future.successful {
    Option.when(topologyTransaction.events.nonEmpty)(
      TopologyTransaction(
        updateId = topologyTransaction.updateId,
        offset = topologyTransaction.offset.unwrap,
        synchronizerId = topologyTransaction.synchronizerId,
        recordTime = Some(TimestampConversion.fromLf(topologyTransaction.effectiveTime)),
        events = topologyTransaction.events.map(event =>
          toTopologyEvent(
            partyId = event.party,
            participantId = event.participant,
            authorizationEvent = event.authorizationEvent,
          )
        ),
        traceContext = SerializableTraceContext(topologyTransaction.traceContext).toDamlProto,
      )
    )
  }

  private def toSynchronizerParametersResponse(
      topologyTransaction: TransactionLogUpdate.TopologyTransactionEffective
  ): Option[SynchronizerParametersResponse] =
    topologyTransaction.synchronizerParametersState
      .map { synchronizerParametersState =>
        SynchronizerParametersResponse(
          commonTopologyTransactionProperties = CommonTopologyTransactionProperties(
            updateId = topologyTransaction.updateId,
            offset = topologyTransaction.offset.unwrap,
            synchronizerId = topologyTransaction.synchronizerId,
            recordTime = Some(TimestampConversion.fromLf(topologyTransaction.effectiveTime)),
            traceContext = SerializableTraceContext(topologyTransaction.traceContext).toDamlProto,
          ),
          synchronizerParametersState = synchronizerParametersState,
        )
      }
}
