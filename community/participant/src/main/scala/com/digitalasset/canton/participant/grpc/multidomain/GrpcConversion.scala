// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.grpc.multidomain

import cats.syntax.traverse.*
import com.daml.api.util.TimestampConversion
import com.daml.ledger.api.v1.command_completion_service.Checkpoint
import com.daml.ledger.api.v1.contract_metadata.ContractMetadata
import com.daml.ledger.api.v1.event.{CreatedEvent, ExercisedEvent}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.TreeEvent
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.ledger.api.v2 as mdProto
import com.daml.lf.data.Ref.HexString
import com.daml.lf.engine.Blinding
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.Transaction.ChildrenRecursion
import com.daml.lf.transaction.{Node, NodeId}
import com.digitalasset.canton.ledger.api.DeduplicationPeriod
import com.digitalasset.canton.ledger.api.DeduplicationPeriod.{
  DeduplicationDuration,
  DeduplicationOffset,
}
import com.digitalasset.canton.ledger.participant.state.v2.CompletionInfo
import com.digitalasset.canton.ledger.participant.state.v2.ReadService.ConnectedDomainResponse
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.ledger.api.multidomain.RequestInclusiveFilter
import com.digitalasset.canton.participant.ledger.api.multidomain.StateApiService.ActiveContractsResponse
import com.digitalasset.canton.participant.ledger.api.multidomain.UpdateApiService.Update
import com.digitalasset.canton.participant.sync.LedgerSyncEvent.{
  CommandRejected,
  TransactionAccepted,
}
import com.digitalasset.canton.participant.sync.{LedgerSyncEvent, UpstreamOffsetConvert}
import com.digitalasset.canton.platform.api.v1.event.EventOps.TreeEventOps
import com.digitalasset.canton.platform.participant.util.LfEngineToApi
import com.digitalasset.canton.protocol.{DriverContractMetadata, SerializableContract}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LedgerTransactionId, LfPartyId, LfTimestamp}
import com.google.protobuf.ByteString
import com.google.protobuf.duration.Duration
import com.google.protobuf.timestamp.Timestamp
import io.grpc.Status

private[grpc] object GrpcConversion {

  def toApiLedgerOffset(globalOffset: Option[GlobalOffset]): LedgerOffset =
    globalOffset match {
      case Some(offset) => UpstreamOffsetConvert.toLedgerOffset(offset)
      case None =>
        LedgerOffset.of(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))
    }

  def toApi(globalOffset: GlobalOffset, update: Update)(
      filters: RequestInclusiveFilter
  ): Either[String, mdProto.update_service.GetUpdateTreesResponse] =
    update.domainId
      .map { domainId =>
        update match {
          case Update.TransactionAccepted(tx) =>
            Right(
              mdProto.update_service.GetUpdateTreesResponse(
                mdProto.update_service.GetUpdateTreesResponse.Update.TransactionTree(
                  toApiTransactionTree(globalOffset, tx, domainId)(filters.parties)
                )
              )
            )

          case Update.TransferredOut(out) =>
            toApiTransferredOut(globalOffset, out)(filters.parties).map(transfer =>
              mdProto.update_service.GetUpdateTreesResponse(
                mdProto.update_service.GetUpdateTreesResponse.Update.Reassignment(transfer)
              )
            )

          case Update.TransferredIn(in) =>
            Right(
              mdProto.update_service.GetUpdateTreesResponse(
                mdProto.update_service.GetUpdateTreesResponse.Update.Reassignment(
                  toApiTransferredIn(globalOffset, in)(filters.parties)
                )
              )
            )
        }
      }
      .getOrElse(Left("missing domain ID in update"))

  private def toApiIncompleteTransferEvents(
      incompleteTransfer: ActiveContractsResponse.IncompleteTransfer
  )(
      requestingParties: Set[LfPartyId]
  ): Either[String, mdProto.state_service.GetActiveContractsResponse] = {
    val contractEntryE = incompleteTransfer match {
      case ActiveContractsResponse.IncompleteTransferredOut(out, contract, protocolVersion) =>
        toApiOutEvent(out)(requestingParties)
          .map { transferredOutEvent =>
            mdProto.state_service.IncompleteUnassigned(
              createdEvent = Some(toApiCreatedEvent(contract, protocolVersion)(requestingParties)),
              unassignedEvent = Some(transferredOutEvent),
            )
          }
          .map(mdProto.state_service.GetActiveContractsResponse.ContractEntry.IncompleteUnassigned)

      case ActiveContractsResponse.IncompleteTransferredIn(in) =>
        val incompleteTransferredIn = mdProto.state_service.IncompleteAssigned(
          assignedEvent = Some(toApiInEvent(in)(requestingParties))
        )

        Right(
          mdProto.state_service.GetActiveContractsResponse.ContractEntry.IncompleteAssigned(
            incompleteTransferredIn
          )
        )
    }

    contractEntryE.map { contractEntry =>
      mdProto.state_service.GetActiveContractsResponse(
        contractEntry = contractEntry,
        workflowId = incompleteTransfer.workflowId.getOrElse(""),
      )
    }
  }

  def toApi(
      activeContractsResponse: ActiveContractsResponse
  )(
      requestingParties: Set[LfPartyId]
  ): Either[String, Seq[mdProto.state_service.GetActiveContractsResponse]] = {

    val apiIncompleteTransfers = activeContractsResponse.incompleteTransfers
      .traverse(
        toApiIncompleteTransferEvents(_)(requestingParties)
      )

    val apiContracts = activeContractsResponse.activeContracts.flatMap {
      case ActiveContractsResponse.ActiveContracts(contracts, domainId, protocolVersion) =>
        contracts.map { case (contract, transferCounter) =>
          val apiActiveContract = mdProto.state_service.ActiveContract(
            createdEvent = Some(toApiCreatedEvent(contract, protocolVersion)(requestingParties)),
            domainId = domainId.toProtoPrimitive,
            reassignmentCounter = transferCounter.v,
          )

          val contractEntry =
            mdProto.state_service.GetActiveContractsResponse.ContractEntry.ActiveContract(
              apiActiveContract
            )

          mdProto.state_service.GetActiveContractsResponse(
            contractEntry = contractEntry,
            // We don't have this information which relates to the creating transaction
            workflowId = "",
          )
        }
    }

    apiIncompleteTransfers.map { apiIncompleteTransfers =>
      apiIncompleteTransfers ++ apiContracts
    }
  }

  def toApi(
      response: ConnectedDomainResponse
  ): Either[String, mdProto.state_service.GetConnectedDomainsResponse] = {
    val connectedDomainsE = response.connectedDomains.traverse { connected =>
      val permissionE = connected.permission match {
        case ParticipantPermission.Submission =>
          Right(com.daml.ledger.api.v2.state_service.ParticipantPermission.Submission)

        case ParticipantPermission.Confirmation =>
          Right(com.daml.ledger.api.v2.state_service.ParticipantPermission.Confirmation)
        case ParticipantPermission.Observation =>
          Right(com.daml.ledger.api.v2.state_service.ParticipantPermission.Observation)
        case ParticipantPermission.Disabled =>
          Left("ParticipantPermission.Disabled should not be used in Daml 3.0")
      }

      permissionE.map { permission =>
        mdProto.state_service.GetConnectedDomainsResponse
          .ConnectedDomain(
            connected.domainAlias.toProtoPrimitive,
            connected.domainId.toProtoPrimitive,
            permission,
          )
      }
    }

    connectedDomainsE.map { connectedDomains =>
      mdProto.state_service.GetConnectedDomainsResponse(connectedDomains = connectedDomains)
    }
  }

  /*
    `TransferredOutEvent.transferOutId` correspond to `ts.toMicros.toString`, where
    `ts` is the sequencing time of the transfer-out response.
   */
  def toApiTransferId(ts: LfTimestamp): String = {
    val instant = ts.toInstant
    /*
      LfTimestamp has micro resolution so dividing getNano by 1000 does
      not incur loss of precision.
     */
    val secondsToMicros = 1000000L
    val epochMicros = instant.getEpochSecond * secondsToMicros + instant.getNano / 1000
    epochMicros.toString
  }

  def toAbsoluteOffset(globalOffset: GlobalOffset): HexString =
    UpstreamOffsetConvert.fromGlobalOffset(globalOffset).toHexString

  def toParticipantOffset(globalOffset: GlobalOffset): ParticipantOffset = {
    val absoluteValue = ParticipantOffset.Value.Absolute(
      UpstreamOffsetConvert.fromGlobalOffset(globalOffset).toHexString
    )

    ParticipantOffset(absoluteValue)
  }

  private def toProtoInstant(ts: LfTimestamp): Timestamp =
    TimestampConversion.fromInstant(ts.toInstant)

  private def toApiOutEvent(out: LedgerSyncEvent.TransferredOut)(
      requestingParties: Set[LfPartyId]
  ): Either[String, mdProto.reassignment.UnassignedEvent] = {
    val transferOutId = toApiTransferId(out.recordTime)
    out.templateId match {
      case Some(templateId) =>
        Right(
          mdProto.reassignment.UnassignedEvent(
            unassignId = transferOutId,
            contractId = out.contractId.coid,
            source = out.sourceDomain.toProtoPrimitive,
            target = out.targetDomain.toProtoPrimitive,
            submitter = out.submitter,
            reassignmentCounter = out.transferCounter.v,
            assignmentExclusivity = out.transferInExclusivity.map(toProtoInstant),
            templateId = Some(LfEngineToApi.toApiIdentifier(templateId)),
            witnessParties = requestingParties.intersect(out.contractStakeholders).toSeq,
          )
        )
      case None => Left(s"templateId should not be empty in transfer-id: $transferOutId")
    }
  }

  private def toApiTransferredOut(
      globalOffset: GlobalOffset,
      out: LedgerSyncEvent.TransferredOut,
  )(requestingParties: Set[LfPartyId]): Either[String, mdProto.reassignment.Reassignment] =
    toApiOutEvent(out)(requestingParties).map { transferOutEvent =>
      mdProto.reassignment.Reassignment(
        updateId = out.updateId,
        commandId = out.optCompletionInfo.fold("")(_.commandId),
        workflowId = out.workflowId.getOrElse(""),
        offset = toAbsoluteOffset(globalOffset),
        event = mdProto.reassignment.Reassignment.Event.UnassignedEvent(transferOutEvent),
      )
    }

  private def toApiInEvent(
      in: LedgerSyncEvent.TransferredIn
  )(requestingParties: Set[LfPartyId]): mdProto.reassignment.AssignedEvent = {

    val create = in.createNode

    val contractMetadata = ContractMetadata(
      createdAt = Some(TimestampConversion.fromInstant(in.ledgerCreateTime.toInstant)),
      contractKeyHash = create.gkeyOpt.fold(ByteString.EMPTY)(_.hash.bytes.toByteString),
      driverMetadata = in.contractMetadata.toByteString,
    )

    mdProto.reassignment.AssignedEvent(
      source = in.sourceDomain.toProtoPrimitive,
      target = in.targetDomain.toProtoPrimitive,
      unassignId = toApiTransferId(in.transferId.transferOutTimestamp.toLf),
      submitter = in.submitter,
      reassignmentCounter = in.transferCounter.v,
      createdEvent = Some(
        toApiCreatedEvent(
          creatingTransactionId = in.creatingTransactionId,
          nodeId = NodeId(0),
          create = create,
          contractMetadata = contractMetadata,
        )(
          requestingParties = requestingParties
        )
      ),
    )
  }

  private def toApiTransferredIn(
      globalOffset: GlobalOffset,
      in: LedgerSyncEvent.TransferredIn,
  )(requestingParties: Set[LfPartyId]): mdProto.reassignment.Reassignment = {

    val create = in.createNode

    val contractMetadata = ContractMetadata(
      createdAt = Some(TimestampConversion.fromInstant(in.ledgerCreateTime.toInstant)),
      contractKeyHash = create.gkeyOpt.fold(ByteString.EMPTY)(_.hash.bytes.toByteString),
      driverMetadata = in.contractMetadata.toByteString,
    )

    val inEvent = mdProto.reassignment.AssignedEvent(
      source = in.sourceDomain.toProtoPrimitive,
      target = in.targetDomain.toProtoPrimitive,
      unassignId = toApiTransferId(in.transferId.transferOutTimestamp.toLf),
      submitter = in.submitter,
      reassignmentCounter = in.transferCounter.v,
      createdEvent = Some(
        toApiCreatedEvent(
          creatingTransactionId = in.creatingTransactionId,
          nodeId = NodeId(0),
          create = create,
          contractMetadata = contractMetadata,
        )(
          requestingParties = requestingParties
        )
      ),
    )

    mdProto.reassignment.Reassignment(
      updateId = in.updateId,
      commandId = in.optCompletionInfo.fold("")(_.commandId),
      workflowId = in.workflowId.getOrElse(""),
      offset = toAbsoluteOffset(globalOffset),
      event = mdProto.reassignment.Reassignment.Event.AssignedEvent(inEvent),
    )
  }

  private def toApiTransactionTree(
      globalOffset: GlobalOffset,
      txAccepted: TransactionAccepted,
      domainId: DomainId,
  )(requestingParties: Set[LfPartyId]): mdProto.transaction.TransactionTree = {
    val blinding = txAccepted.blindingInfoO.getOrElse(Blinding.blind(txAccepted.transaction))
    val events = txAccepted.transaction
      .foldInExecutionOrder(List.empty[(NodeId, Node)])(
        exerciseBegin = (acc, nid, node) => ((nid -> node) :: acc, ChildrenRecursion.DoRecurse),
        rollbackBegin = (acc, _, _) => (acc, ChildrenRecursion.DoNotRecurse),
        leaf = (acc, nid, node) => (nid -> node) :: acc,
        exerciseEnd = (acc, _, _) => acc,
        rollbackEnd = (acc, _, _) => acc,
      )
      .reverse
      .collect {
        case (nodeId, create: Node.Create)
            if blinding.disclosure
              .getOrElse(nodeId, Set.empty)
              .exists(requestingParties) =>
          TreeEvent(
            TreeEvent.Kind.Created(
              toApiCreatedEvent(
                creatingTransactionId = txAccepted.transactionId,
                nodeId = nodeId,
                create = create,
                contractMetadata = toApiContractMetadata(create, txAccepted),
              )(requestingParties)
            )
          )

        case (nodeId, exercise: Node.Exercise)
            if blinding.disclosure
              .getOrElse(nodeId, Set.empty)
              .exists(requestingParties) =>
          TreeEvent(toApiExercised(nodeId, exercise, txAccepted.transactionId)(requestingParties))
      }
    val visible = events.map(_.eventId)
    val visibleOrder = visible.view.zipWithIndex.toMap
    val eventsById = events.iterator
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

    mdProto.transaction.TransactionTree(
      updateId = txAccepted.transactionId,
      commandId = txAccepted.completionInfoO
        .flatMap(completionInfo =>
          if (completionInfo.actAs.intersect(requestingParties.toSeq).nonEmpty)
            Some(completionInfo.commandId)
          else None
        )
        .getOrElse(""),
      workflowId = txAccepted.transactionMeta.workflowId.getOrElse(""),
      effectiveAt = Some(
        toProtoInstant(txAccepted.transactionMeta.ledgerEffectiveTime)
      ),
      offset = toAbsoluteOffset(globalOffset),
      eventsById = eventsById,
      rootEventIds = rootEventIds,
      domainId = domainId.toProtoPrimitive,
    )
  }

  private def toApiCreatedEvent(
      contract: SerializableContract,
      protocolVersion: ProtocolVersion,
  )(requestingParties: Set[LfPartyId]): CreatedEvent = {

    val signatories = contract.metadata.signatories
    val stakeholders = contract.metadata.stakeholders
    val observers = stakeholders.diff(signatories)

    val contractInstance = contract.rawContractInstance.contractInstance

    val driverMetadata = contract.contractSalt
      .map(DriverContractMetadata(_))
      .map(_.toLfBytes(protocolVersion))
      .map(_.toByteString)
      .getOrElse(ByteString.EMPTY)

    val metadata = ContractMetadata(
      createdAt = Some(TimestampConversion.fromInstant(contract.ledgerCreateTime.toInstant)),
      contractKeyHash =
        contract.metadata.maybeKey.fold(ByteString.EMPTY)(_.hash.bytes.toByteString),
      driverMetadata = driverMetadata,
    )

    /*
      Notes related to CreatedEvent attribute:
      - `eventId`: since we don't have the nodeId, we set the whole `eventId` to empty rather to have
                   a partially filled value.
      - `agreementText`: setting it to empty for now, with the goal of deleting in 3.0
     */
    CreatedEvent(
      eventId = "",
      contractId = contract.contractId.coid,
      templateId = Some(LfEngineToApi.toApiIdentifier(contractInstance.unversioned.template)),
      contractKey = contract.metadata.maybeKeyWithMaintainers
        .map(_.value)
        .map(lfValue =>
          LfEngineToApi.assertOrRuntimeEx(
            failureContext = s"attempting to serialize key to API value",
            LfEngineToApi.lfValueToApiValue(verbose = false, lfValue),
          )
        ),
      createArguments = Some(
        LfEngineToApi.assertOrRuntimeEx(
          failureContext = s"attempting to serialize create arguments to API record",
          LfEngineToApi.lfValueToApiRecord(
            verbose = false,
            recordValue = contractInstance.unversioned.arg,
          ),
        )
      ),
      createArgumentsBlob = None,
      interfaceViews = Seq.empty,
      witnessParties = requestingParties.intersect(stakeholders).toVector,
      signatories = signatories.toVector,
      observers = observers.toVector,
      agreementText = Some(""),
      metadata = Some(metadata),
    )
  }

  private def toApiCreatedEvent(
      creatingTransactionId: LedgerTransactionId,
      nodeId: NodeId,
      create: Node.Create,
      contractMetadata: ContractMetadata,
  )(requestingParties: Set[LfPartyId]): CreatedEvent =
    CreatedEvent(
      eventId = EventId(creatingTransactionId, nodeId).toLedgerString,
      contractId = create.coid.coid,
      templateId = Some(LfEngineToApi.toApiIdentifier(create.templateId)),
      contractKey = create.keyOpt
        .map(_.value)
        .map(lfValue =>
          LfEngineToApi.assertOrRuntimeEx(
            failureContext = s"attempting to serialize key to API value",
            LfEngineToApi.lfValueToApiValue(verbose = false, lfValue),
          )
        ),
      createArguments = Some(
        LfEngineToApi.assertOrRuntimeEx(
          failureContext = s"attempting to serialize create arguments to API record",
          LfEngineToApi.lfValueToApiRecord(verbose = false, create.arg),
        )
      ),
      createArgumentsBlob = None,
      interfaceViews = Seq.empty,
      witnessParties =
        create.stakeholders.map(_.toString).intersect(requestingParties.map(_.toString)).toSeq,
      signatories = create.signatories.view.map(_.toString).toVector,
      observers = create.stakeholders.diff(create.signatories).view.map(_.toString).toVector,
      agreementText = Some(create.agreementText),
      metadata = Some(contractMetadata),
    )

  private def toApiContractMetadata(
      create: Node.Create,
      txAccepted: TransactionAccepted,
  ): ContractMetadata = {
    ContractMetadata(
      createdAt = Some(
        TimestampConversion
          .fromInstant(txAccepted.transactionMeta.ledgerEffectiveTime.toInstant)
      ),
      contractKeyHash = create.gkeyOpt.fold(ByteString.EMPTY)(_.hash.bytes.toByteString),
      driverMetadata = txAccepted.contractMetadata
        .get(create.coid)
        .map(_.toByteString)
        .getOrElse(ByteString.EMPTY),
    )
  }

  private def toApiExercised(
      nodeId: NodeId,
      exercise: Node.Exercise,
      transactionId: LedgerTransactionId,
  )(requestingParties: Set[LfPartyId]): TreeEvent.Kind.Exercised =
    TreeEvent.Kind.Exercised(
      ExercisedEvent(
        eventId = EventId(transactionId, nodeId).toLedgerString,
        contractId = exercise.targetCoid.coid,
        templateId = Some(LfEngineToApi.toApiIdentifier(exercise.templateId)),
        interfaceId = exercise.interfaceId.map(LfEngineToApi.toApiIdentifier),
        choice = exercise.qualifiedChoiceName.choiceName.toString,
        choiceArgument = Some(
          LfEngineToApi.assertOrRuntimeEx(
            failureContext = s"attempting to serialize choice argument to API value",
            LfEngineToApi.lfValueToApiValue(verbose = false, exercise.chosenValue),
          )
        ),
        witnessParties = requestingParties.toSeq,
        actingParties = exercise.actingParties.view.map(_.toString).toVector,
        consuming = exercise.consuming,
        childEventIds = exercise.children.iterator
          .map(EventId(transactionId, _).toLedgerString.toString)
          .toVector,
        exerciseResult = exercise.exerciseResult.map(result =>
          LfEngineToApi.assertOrRuntimeEx(
            failureContext = s"attempting to serialize exercise result to API value",
            LfEngineToApi.lfValueToApiValue(verbose = false, result),
          )
        ),
      )
    )

  private def toApiDeduplicationPeriod(
      deduplicationPeriod: Option[DeduplicationPeriod]
  ): mdProto.completion.Completion.DeduplicationPeriod = {
    import mdProto.completion.Completion

    deduplicationPeriod.fold[Completion.DeduplicationPeriod](Completion.DeduplicationPeriod.Empty) {
      case DeduplicationOffset(offset) =>
        Completion.DeduplicationPeriod.DeduplicationOffset(offset.toHexString)
      case DeduplicationDuration(duration) =>
        Completion.DeduplicationPeriod.DeduplicationDuration(
          Duration(seconds = duration.getSeconds, nanos = duration.getNano)
        )
    }
  }

  private[grpc] def toApiCompletion(
      globalOffset: GlobalOffset,
      update: Either[CommandRejected, Update],
      completionInfo: CompletionInfo,
  ): mdProto.command_completion_service.CompletionStreamResponse = {
    val completionData = CompletionData.create(update)

    mdProto.command_completion_service.CompletionStreamResponse(
      checkpoint = Some(
        Checkpoint(
          recordTime = Some(toProtoInstant(completionData.recordTime)),
          offset = Some(toApiLedgerOffset(Some(globalOffset))),
        )
      ),
      completion = Option(
        mdProto.completion.Completion(
          commandId = completionInfo.commandId,
          status = Some(completionData.status),
          updateId = completionData.updateId,
          applicationId = completionInfo.applicationId,
          actAs =
            Nil, // TODO(#11002) we have this information, not populating it to be aligned with old implementation
          submissionId = completionInfo.submissionId.getOrElse(""),
          deduplicationPeriod = toApiDeduplicationPeriod(completionInfo.optDeduplicationPeriod),
        )
      ),
      domainId = completionData.domainId,
    )
  }

  final case class CompletionData(
      recordTime: LfTimestamp,
      updateId: String,
      status: com.google.rpc.status.Status,
      domainId: String,
  )

  object CompletionData {
    def create(update: Either[CommandRejected, Update]): CompletionData = {
      val (recordTime, updateId, status, domainId) = update.fold(
        commandRejected =>
          (
            commandRejected.recordTime,
            "",
            commandRejected.reasonTemplate match {
              case CommandRejected.FinalReason(status) => status
            },
            commandRejected.domainId.fold("")(_.toProtoPrimitive),
          ),
        update =>
          (
            update.recordTime,
            update.updateId,
            com.google.rpc.status.Status.of(Status.Code.OK.value(), "", Seq.empty),
            update.domainId.fold("")(_.toProtoPrimitive),
          ),
      )

      CompletionData(recordTime, updateId, status, domainId)
    }
  }
}
