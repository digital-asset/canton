// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.event as apiEvent
import com.daml.ledger.api.v2.reassignment.{
  AssignedEvent as ApiAssignedEvent,
  Reassignment as ApiReassignment,
  UnassignedEvent as ApiUnassignedEvent,
}
import com.daml.ledger.api.v2.state_service.ParticipantPermission as ApiParticipantPermission
import com.daml.ledger.api.v2.topology_transaction.{
  ParticipantAuthorizationChanged,
  ParticipantAuthorizationRevoked,
  TopologyEvent,
  TopologyTransaction,
}
import com.daml.ledger.api.v2.transaction.{
  Transaction as FlatTransaction,
  TransactionTree,
  TreeEvent,
}
import com.daml.ledger.api.v2.update_service.{
  GetTransactionResponse,
  GetTransactionTreeResponse,
  GetUpdateTreesResponse,
  GetUpdatesResponse,
}
import com.digitalasset.canton.ledger.api.util.{LfEngineToApi, TimestampConversion}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.store.ScalaPbStreamingOptimizations.*
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate.{
  CreatedEvent,
  ExercisedEvent,
}
import com.digitalasset.canton.platform.store.utils.EventOps.TreeEventOps
import com.digitalasset.canton.platform.{TemplatePartiesFilter, Value}
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{Identifier, Party}
import com.digitalasset.daml.lf.transaction.{FatContractInstance, GlobalKeyWithMaintainers, Node}
import com.digitalasset.daml.lf.value.Value.ContractId
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

private[events] object TransactionLogUpdatesConversions {
  object ToFlatTransaction {
    def filter(
        templateWildcardParties: Option[Set[Party]],
        templateSpecificParties: Map[Identifier, Option[Set[Party]]],
        requestingParties: Option[Set[Party]],
    ): TransactionLogUpdate => Option[TransactionLogUpdate] = {
      case transaction: TransactionLogUpdate.TransactionAccepted =>
        val flatTransactionEvents = transaction.events.collect {
          case createdEvent: TransactionLogUpdate.CreatedEvent => createdEvent
          case exercisedEvent: TransactionLogUpdate.ExercisedEvent if exercisedEvent.consuming =>
            exercisedEvent
        }
        val filteredFlatEvents = flatTransactionEvents
          .filter(flatTransactionPredicate(templateWildcardParties, templateSpecificParties))
        val commandId = getCommandId(filteredFlatEvents, requestingParties)
        val nonTransient = removeTransient(filteredFlatEvents)
        // Allows emitting flat transactions with no events, a use-case needed
        // for the functioning of Daml triggers.
        // (more details in https://github.com/digital-asset/daml/issues/6975)
        Option.when(nonTransient.nonEmpty || commandId.nonEmpty)(
          transaction.copy(
            commandId = commandId,
            events = nonTransient,
          )(transaction.traceContext)
        )
      case _: TransactionLogUpdate.TransactionRejected => None
      case u: TransactionLogUpdate.ReassignmentAccepted =>
        Option.when(
          u.reassignmentInfo.hostedStakeholders.exists(party =>
            templateWildcardParties.fold(true)(parties =>
              parties(party)
            ) || (templateSpecificParties
              .get(u.reassignment match {
                case TransactionLogUpdate.ReassignmentAccepted.Unassigned(unassign) =>
                  unassign.templateId
                case TransactionLogUpdate.ReassignmentAccepted.Assigned(createdEvent) =>
                  createdEvent.templateId
              }) match {
              case Some(Some(ps)) => ps contains party
              case Some(None) => true
              case None => false
            })
          )
        )(u)
      case _: TransactionLogUpdate.PartyAllocationResponse => None
      case u: TransactionLogUpdate.TopologyTransactionEffective =>
        val filteredEvents =
          u.events.filter(topologyEventPredicate(templateWildcardParties, templateSpecificParties))
        Option.when(filteredEvents.nonEmpty)(
          u.copy(events = filteredEvents)(u.traceContext)
        )
    }

    def toGetTransactionsResponse(
        filter: TemplatePartiesFilter,
        eventProjectionProperties: EventProjectionProperties,
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContextWithTrace,
        executionContext: ExecutionContext,
    ): TransactionLogUpdate => Future[GetUpdatesResponse] = {
      case transactionAccepted: TransactionLogUpdate.TransactionAccepted =>
        toFlatTransaction(
          transactionAccepted,
          filter,
          eventProjectionProperties,
          lfValueTranslation,
          transactionAccepted.traceContext,
        )
          .map(transaction =>
            GetUpdatesResponse(GetUpdatesResponse.Update.Transaction(transaction))
              .withPrecomputedSerializedSize()
          )

      case reassignmentAccepted: TransactionLogUpdate.ReassignmentAccepted =>
        toReassignment(
          reassignmentAccepted,
          filter.allFilterParties,
          eventProjectionProperties,
          lfValueTranslation,
          reassignmentAccepted.traceContext,
        )
          .map(reassignment =>
            GetUpdatesResponse(GetUpdatesResponse.Update.Reassignment(reassignment))
              .withPrecomputedSerializedSize()
          )

      case topologyTransaction: TransactionLogUpdate.TopologyTransactionEffective =>
        toTopologyTransaction(topologyTransaction).map(transaction =>
          GetUpdatesResponse(GetUpdatesResponse.Update.TopologyTransaction(transaction))
            .withPrecomputedSerializedSize()
        )

      case illegal => throw new IllegalStateException(s"$illegal is not expected here")
    }

    def toGetFlatTransactionResponse(
        transactionLogUpdate: TransactionLogUpdate,
        requestingParties: Set[Party],
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContextWithTrace,
        executionContext: ExecutionContext,
    ): Future[Option[GetTransactionResponse]] =
      filter(Some(requestingParties), Map.empty, Some(requestingParties))(
        transactionLogUpdate
      )
        .collect { case transactionAccepted: TransactionLogUpdate.TransactionAccepted =>
          toFlatTransaction(
            transactionAccepted = transactionAccepted,
            filter = TemplatePartiesFilter(
              Map.empty,
              Some(requestingParties),
            ),
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              templateWildcardWitnesses = Some(requestingParties.map(_.toString)),
            ),
            lfValueTranslation = lfValueTranslation,
            traceContext = transactionAccepted.traceContext,
          )
        }
        .map(_.map(flatTransaction => Some(GetTransactionResponse(Some(flatTransaction)))))
        .getOrElse(Future.successful(None))

    private def toFlatTransaction(
        transactionAccepted: TransactionLogUpdate.TransactionAccepted,
        filter: TemplatePartiesFilter,
        eventProjectionProperties: EventProjectionProperties,
        lfValueTranslation: LfValueTranslation,
        traceContext: TraceContext,
    )(implicit
        loggingContext: LoggingContextWithTrace,
        executionContext: ExecutionContext,
    ): Future[FlatTransaction] =
      Future.delegate {
        MonadUtil
          .sequentialTraverse(transactionAccepted.events)(event =>
            toFlatEvent(
              event,
              filter.allFilterParties,
              eventProjectionProperties,
              lfValueTranslation,
            )
          )
          .map(flatEvents =>
            FlatTransaction(
              updateId = transactionAccepted.updateId,
              commandId = transactionAccepted.commandId,
              workflowId = transactionAccepted.workflowId,
              effectiveAt = Some(TimestampConversion.fromLf(transactionAccepted.effectiveAt)),
              events = flatEvents,
              offset = transactionAccepted.offset.unwrap,
              synchronizerId = transactionAccepted.synchronizerId,
              traceContext = SerializableTraceContext(traceContext).toDamlProtoOpt,
              recordTime = Some(TimestampConversion.fromLf(transactionAccepted.recordTime)),
            )
          )
      }

    private def removeTransient(aux: Vector[TransactionLogUpdate.Event]) = {
      val permanent = aux.foldLeft(Set.empty[ContractId]) {
        case (contractIds, event) if !contractIds(event.contractId) =>
          contractIds + event.contractId
        case (contractIds, event: ExercisedEvent) if event.consuming =>
          contractIds - event.contractId
        case (contractIds, _) =>
          val prettyCids = contractIds.iterator.map(_.coid).mkString(", ")
          throw new RuntimeException(s"Unexpected non-consuming event for contractIds $prettyCids")
      }
      aux.filter(ev => permanent(ev.contractId))
    }

    private def flatTransactionPredicate(
        templateWildcardParties: Option[Set[Party]],
        templateSpecificParties: Map[Identifier, Option[Set[Party]]],
    )(event: TransactionLogUpdate.Event) = {
      val stakeholdersMatchingParties =
        templateWildcardParties match {
          case Some(parties) => event.flatEventWitnesses.exists(parties)
          case None => true
        }

      stakeholdersMatchingParties || (templateSpecificParties
        .get(event.templateId) match {
        case Some(Some(filterParties)) => filterParties.exists(event.flatEventWitnesses)
        case Some(None) => true // party wildcard
        case None => false // templateId is not in the filter
      })

    }

    private def topologyEventPredicate(
        templateWildcardParties: Option[Set[Party]],
        templateSpecificParties: Map[Identifier, Option[Set[Party]]],
    )(event: TransactionLogUpdate.PartyToParticipantAuthorization) =
      matchPartyInSet(event.party)(templateWildcardParties) || templateSpecificParties.values
        .exists(matchPartyInSet(event.party))

    private def toFlatEvent(
        event: TransactionLogUpdate.Event,
        requestingParties: Option[Set[Party]],
        eventProjectionProperties: EventProjectionProperties,
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContextWithTrace,
        executionContext: ExecutionContext,
    ): Future[apiEvent.Event] =
      event match {
        case createdEvent: TransactionLogUpdate.CreatedEvent =>
          createdToApiCreatedEvent(
            requestingParties,
            eventProjectionProperties,
            lfValueTranslation,
            createdEvent,
            _.flatEventWitnesses,
          ).map(apiCreatedEvent => apiEvent.Event(apiEvent.Event.Event.Created(apiCreatedEvent)))

        case exercisedEvent: TransactionLogUpdate.ExercisedEvent if exercisedEvent.consuming =>
          Future.successful(exercisedToFlatEvent(requestingParties, exercisedEvent))

        case _ => Future.failed(new RuntimeException("Not a flat transaction event"))
      }

    private def exercisedToFlatEvent(
        requestingParties: Option[Set[Party]],
        exercisedEvent: ExercisedEvent,
    ): apiEvent.Event =
      apiEvent.Event(
        apiEvent.Event.Event.Archived(
          apiEvent.ArchivedEvent(
            offset = exercisedEvent.eventOffset.unwrap,
            nodeId = exercisedEvent.nodeId,
            contractId = exercisedEvent.contractId.coid,
            templateId = Some(LfEngineToApi.toApiIdentifier(exercisedEvent.templateId)),
            packageName = exercisedEvent.packageName,
            witnessParties = requestingParties match {
              case Some(parties) => parties.iterator.filter(exercisedEvent.flatEventWitnesses).toSeq
              // party-wildcard
              case None => exercisedEvent.flatEventWitnesses.toSeq
            },
          )
        )
      )
  }

  object ToTransactionTree {
    def filter(
        requestingParties: Option[Set[Party]]
    ): TransactionLogUpdate => Option[TransactionLogUpdate] = {
      case transaction: TransactionLogUpdate.TransactionAccepted =>
        val filteredForVisibility =
          transaction.events.filter(transactionTreePredicate(requestingParties))

        Option.when(filteredForVisibility.nonEmpty)(
          transaction.copy(events = filteredForVisibility)(transaction.traceContext)
        )
      case _: TransactionLogUpdate.TransactionRejected => None
      case u: TransactionLogUpdate.ReassignmentAccepted =>
        Option.when(
          requestingParties.fold(true)(u.reassignmentInfo.hostedStakeholders.exists(_))
        )(u)
      case _: TransactionLogUpdate.PartyAllocationResponse => None
      case u: TransactionLogUpdate.TopologyTransactionEffective =>
        val filteredEvents =
          u.events.filter(event => matchPartyInSet(event.party)(requestingParties))
        Option.when(filteredEvents.nonEmpty)(
          u.copy(events = filteredEvents)(u.traceContext)
        )
    }

    def toGetTransactionResponse(
        transactionLogUpdate: TransactionLogUpdate,
        requestingParties: Set[Party],
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContextWithTrace,
        executionContext: ExecutionContext,
    ): Future[Option[GetTransactionTreeResponse]] =
      filter(Some(requestingParties))(transactionLogUpdate)
        .collect { case tx: TransactionLogUpdate.TransactionAccepted =>
          toTransactionTree(
            transactionAccepted = tx,
            Some(requestingParties),
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              templateWildcardWitnesses = Some(requestingParties.map(_.toString)),
            ),
            lfValueTranslation = lfValueTranslation,
            traceContext = tx.traceContext,
          )
        }
        .map(_.map(transactionTree => Some(GetTransactionTreeResponse(Some(transactionTree)))))
        .getOrElse(Future.successful(None))

    def toGetTransactionTreesResponse(
        requestingParties: Option[Set[Party]],
        eventProjectionProperties: EventProjectionProperties,
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContextWithTrace,
        executionContext: ExecutionContext,
    ): TransactionLogUpdate => Future[GetUpdateTreesResponse] = {
      case transactionAccepted: TransactionLogUpdate.TransactionAccepted =>
        toTransactionTree(
          transactionAccepted,
          requestingParties,
          eventProjectionProperties,
          lfValueTranslation,
          transactionAccepted.traceContext,
        )
          .map(txTree =>
            GetUpdateTreesResponse(GetUpdateTreesResponse.Update.TransactionTree(txTree))
              .withPrecomputedSerializedSize()
          )
      case reassignmentAccepted: TransactionLogUpdate.ReassignmentAccepted =>
        toReassignment(
          reassignmentAccepted,
          requestingParties,
          eventProjectionProperties,
          lfValueTranslation,
          reassignmentAccepted.traceContext,
        )
          .map(reassignment =>
            GetUpdateTreesResponse(GetUpdateTreesResponse.Update.Reassignment(reassignment))
              .withPrecomputedSerializedSize()
          )

      case topologyTransaction: TransactionLogUpdate.TopologyTransactionEffective =>
        toTopologyTransaction(topologyTransaction).map(transaction =>
          GetUpdateTreesResponse(GetUpdateTreesResponse.Update.TopologyTransaction(transaction))
            .withPrecomputedSerializedSize()
        )

      case illegal => throw new IllegalStateException(s"$illegal is not expected here")
    }

    private def toTransactionTree(
        transactionAccepted: TransactionLogUpdate.TransactionAccepted,
        requestingParties: Option[Set[Party]],
        eventProjectionProperties: EventProjectionProperties,
        lfValueTranslation: LfValueTranslation,
        traceContext: TraceContext,
    )(implicit
        loggingContext: LoggingContextWithTrace,
        executionContext: ExecutionContext,
    ): Future[TransactionTree] =
      Future.delegate {
        MonadUtil
          .sequentialTraverse(transactionAccepted.events)(event =>
            toTransactionTreeEvent(
              requestingParties,
              eventProjectionProperties,
              lfValueTranslation,
            )(event)
          )
          .map { treeEvents =>
            val visible = treeEvents.map(_.nodeId)
            val visibleSet = visible.toSet
            val eventsById = treeEvents.iterator
              .map(e => e.nodeId -> e.filterChildNodeIds(visibleSet))
              .toMap

            // All event identifiers that appear as a child of another item in this response
            val children = eventsById.valuesIterator.flatMap(_.childNodeIds).toSet

            // The roots for this request are all visible items
            // that are not a child of some other visible item
            val rootNodeIds = visible.filterNot(children)

            TransactionTree(
              updateId = transactionAccepted.updateId,
              commandId = getCommandId(transactionAccepted.events, requestingParties),
              workflowId = transactionAccepted.workflowId,
              effectiveAt = Some(TimestampConversion.fromLf(transactionAccepted.effectiveAt)),
              offset = transactionAccepted.offset.unwrap,
              eventsById = eventsById,
              rootNodeIds = rootNodeIds,
              synchronizerId = transactionAccepted.synchronizerId,
              traceContext = SerializableTraceContext(traceContext).toDamlProtoOpt,
              recordTime = Some(TimestampConversion.fromLf(transactionAccepted.recordTime)),
            )
          }
      }

    private def toTransactionTreeEvent(
        requestingParties: Option[Set[Party]],
        eventProjectionProperties: EventProjectionProperties,
        lfValueTranslation: LfValueTranslation,
    )(event: TransactionLogUpdate.Event)(implicit
        loggingContext: LoggingContextWithTrace,
        executionContext: ExecutionContext,
    ): Future[TreeEvent] =
      event match {
        case createdEvent: TransactionLogUpdate.CreatedEvent =>
          createdToApiCreatedEvent(
            requestingParties,
            eventProjectionProperties,
            lfValueTranslation,
            createdEvent,
            _.treeEventWitnesses,
          ).map(apiCreatedEvent => TreeEvent(TreeEvent.Kind.Created(apiCreatedEvent)))

        case exercisedEvent: TransactionLogUpdate.ExercisedEvent =>
          exercisedToTransactionTreeEvent(
            requestingParties,
            eventProjectionProperties.verbose,
            lfValueTranslation,
            exercisedEvent,
          )
      }

    private def exercisedToTransactionTreeEvent(
        requestingParties: Option[Set[Party]],
        verbose: Boolean,
        lfValueTranslation: LfValueTranslation,
        exercisedEvent: ExercisedEvent,
    )(implicit
        loggingContext: LoggingContextWithTrace,
        executionContext: ExecutionContext,
    ) = {
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
        verbose,
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
              verbose = verbose,
              attribute = "exercise result",
              enrich = choiceResultEnricher,
            )
            .map(Some(_))
        }
        .getOrElse(Future.successful(None))

      for {
        choiceArgument <- eventualChoiceArgument
        maybeExerciseResult <- eventualExerciseResult
      } yield TreeEvent(
        TreeEvent.Kind.Exercised(
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
            witnessParties = requestingParties
              .fold(exercisedEvent.treeEventWitnesses)(
                _.filter(exercisedEvent.treeEventWitnesses)
              )
              .toSeq,
            childNodeIds = exercisedEvent.children,
            exerciseResult = maybeExerciseResult,
          )
        )
      )
    }

    private def transactionTreePredicate(
        requestingPartiesO: Option[Set[Party]]
    ): TransactionLogUpdate.Event => Boolean = {
      case createdEvent: CreatedEvent =>
        requestingPartiesO.fold(true)(_.exists(createdEvent.treeEventWitnesses))
      case exercised: ExercisedEvent =>
        requestingPartiesO.fold(true)(_.exists(exercised.treeEventWitnesses))
      case _ => false
    }
  }

  private def createdToApiCreatedEvent(
      requestingPartiesO: Option[Set[Party]],
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
      createdEvent: CreatedEvent,
      createdWitnesses: CreatedEvent => Set[Party],
  )(implicit
      loggingContext: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): Future[apiEvent.CreatedEvent] = {

    def getFatContractInstance: Right[Nothing, FatContractInstance] =
      Right(
        FatContractInstance.fromCreateNode(
          Node.Create(
            coid = createdEvent.contractId,
            templateId = createdEvent.templateId,
            packageName = createdEvent.packageName,
            packageVersion = createdEvent.packageVersion,
            arg = createdEvent.createArgument.unversioned,
            signatories = createdEvent.createSignatories,
            stakeholders = createdEvent.createSignatories ++ createdEvent.createObservers,
            keyOpt = createdEvent.createKey.flatMap(k =>
              createdEvent.createKeyMaintainers.map(GlobalKeyWithMaintainers(k, _))
            ),
            version = createdEvent.createArgument.version,
          ),
          createTime = createdEvent.ledgerEffectiveTime,
          cantonData = createdEvent.driverMetadata,
        )
      )

    val createdEventWitnesses = createdWitnesses(createdEvent)
    val witnesses = requestingPartiesO
      .fold(createdEventWitnesses)(_.view.filter(createdEventWitnesses).toSet)
      .map(_.toString)
    lfValueTranslation
      .toApiContractData(
        value = createdEvent.createArgument,
        key = createdEvent.contractKey,
        templateId = createdEvent.templateId,
        witnesses = witnesses,
        eventProjectionProperties = eventProjectionProperties,
        fatContractInstance = getFatContractInstance,
      )
      .map(apiContractData =>
        apiEvent.CreatedEvent(
          offset = createdEvent.eventOffset.unwrap,
          nodeId = createdEvent.nodeId,
          contractId = createdEvent.contractId.coid,
          templateId = Some(LfEngineToApi.toApiIdentifier(createdEvent.templateId)),
          packageName = createdEvent.packageName,
          contractKey = apiContractData.contractKey,
          createArguments = apiContractData.createArguments,
          createdEventBlob = apiContractData.createdEventBlob.getOrElse(ByteString.EMPTY),
          interfaceViews = apiContractData.interfaceViews,
          witnessParties = witnesses.toSeq,
          signatories = createdEvent.createSignatories.toSeq,
          observers = createdEvent.createObservers.toSeq,
          createdAt = Some(TimestampConversion.fromLf(createdEvent.ledgerEffectiveTime)),
        )
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
        case event if requestingPartiesO.fold(true)(_.exists(event.submitters)) =>
          event.commandId
      }
      .getOrElse("")

  private def toReassignment(
      reassignmentAccepted: TransactionLogUpdate.ReassignmentAccepted,
      requestingParties: Option[Set[Party]],
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
      traceContext: TraceContext,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): Future[ApiReassignment] = {
    val stringRequestingParties = requestingParties.map(_.map(_.toString))
    val info = reassignmentAccepted.reassignmentInfo
    (reassignmentAccepted.reassignment match {
      case TransactionLogUpdate.ReassignmentAccepted.Assigned(createdEvent) =>
        createdToApiCreatedEvent(
          requestingPartiesO = requestingParties,
          eventProjectionProperties = eventProjectionProperties,
          lfValueTranslation = lfValueTranslation,
          createdEvent = createdEvent,
          createdWitnesses = _.flatEventWitnesses,
        ).map(createdEvent =>
          ApiReassignment.Event.AssignedEvent(
            ApiAssignedEvent(
              source = info.sourceSynchronizer.unwrap.toProtoPrimitive,
              target = info.targetSynchronizer.unwrap.toProtoPrimitive,
              unassignId = info.unassignId.toMicros.toString,
              submitter = info.submitter.getOrElse(""),
              reassignmentCounter = info.reassignmentCounter,
              createdEvent = Some(createdEvent),
            )
          )
        )

      case TransactionLogUpdate.ReassignmentAccepted.Unassigned(unassign) =>
        val stakeholders = reassignmentAccepted.reassignmentInfo.hostedStakeholders
        Future.successful(
          ApiReassignment.Event.UnassignedEvent(
            ApiUnassignedEvent(
              source = info.sourceSynchronizer.unwrap.toProtoPrimitive,
              target = info.targetSynchronizer.unwrap.toProtoPrimitive,
              unassignId = info.unassignId.toMicros.toString,
              submitter = info.submitter.getOrElse(""),
              reassignmentCounter = info.reassignmentCounter,
              contractId = unassign.contractId.coid,
              templateId = Some(LfEngineToApi.toApiIdentifier(unassign.templateId)),
              packageName = unassign.packageName,
              assignmentExclusivity =
                unassign.assignmentExclusivity.map(TimestampConversion.fromLf),
              witnessParties = requestingParties.fold(stakeholders)(stakeholders.filter),
            )
          )
        )
    }).map(event =>
      ApiReassignment(
        updateId = reassignmentAccepted.updateId,
        commandId = reassignmentAccepted.completionStreamResponse
          .flatMap(_.completionResponse.completion)
          .filter(completion => stringRequestingParties.fold(true)(completion.actAs.exists))
          .map(_.commandId)
          .getOrElse(""),
        workflowId = reassignmentAccepted.workflowId,
        offset = reassignmentAccepted.offset.unwrap,
        event = event,
        traceContext = SerializableTraceContext(traceContext).toDamlProtoOpt,
        recordTime = Some(TimestampConversion.fromLf(reassignmentAccepted.recordTime)),
      )
    )
  }

  private def toPermissionLevel(permission: AuthorizationLevel): Option[ApiParticipantPermission] =
    permission match {
      case AuthorizationLevel.Submission =>
        Some(ApiParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION)
      case AuthorizationLevel.Observation =>
        Some(ApiParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION)
      case AuthorizationLevel.Confirmation =>
        Some(ApiParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION)
      case AuthorizationLevel.Revoked => None
    }

  private def toTopologyTransaction(
      topologyTransaction: TransactionLogUpdate.TopologyTransactionEffective
  ): Future[TopologyTransaction] = Future.successful(
    TopologyTransaction(
      updateId = topologyTransaction.updateId,
      offset = topologyTransaction.offset.unwrap,
      synchronizerId = topologyTransaction.synchronizerId,
      recordTime = Some(TimestampConversion.fromLf(topologyTransaction.effectiveTime)),
      events = topologyTransaction.events.map(event =>
        toPermissionLevel(event.level).fold(
          TopologyEvent(
            TopologyEvent.Event.ParticipantAuthorizationRevoked(
              ParticipantAuthorizationRevoked(
                partyId = event.party,
                participantId = event.participant,
              )
            )
          )
        )(permission =>
          TopologyEvent(
            TopologyEvent.Event.ParticipantAuthorizationChanged(
              ParticipantAuthorizationChanged(
                partyId = event.party,
                participantId = event.participant,
                particiantPermission = permission,
              )
            )
          )
        )
      ),
      traceContext = SerializableTraceContext(topologyTransaction.traceContext).toDamlProtoOpt,
    )
  )

}
