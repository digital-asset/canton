// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.grpc.multidomain

import cats.syntax.traverse.*
import com.daml.api.util.TimestampConversion
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.contract_metadata.ContractMetadata
import com.daml.ledger.api.v1.event.{CreatedEvent, ExercisedEvent}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
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
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.ledger.api.multidomain.StateApiService.{
  ActiveContractsResponse,
  ConnectedDomainResponse,
}
import com.digitalasset.canton.participant.ledger.api.multidomain.UpdateApiService.Update
import com.digitalasset.canton.participant.protocol.v0.multidomain as mdProto
import com.digitalasset.canton.participant.sync.LedgerSyncEvent.{
  CommandRejected,
  TransactionAccepted,
}
import com.digitalasset.canton.participant.sync.{LedgerSyncEvent, UpstreamOffsetConvert}
import com.digitalasset.canton.platform.api.v1.event.EventOps.TreeEventOps
import com.digitalasset.canton.platform.participant.util.LfEngineToApi
import com.digitalasset.canton.protocol.{DriverContractMetadata, SerializableContract}
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
      requestingParty: LfPartyId
  ): Either[String, mdProto.GetTreeUpdatesResponse] =
    update.domainId
      .map(_.toProtoPrimitive)
      .map { domainId =>
        update match {
          case Update.TransactionAccepted(tx) =>
            Right(
              mdProto.GetTreeUpdatesResponse(
                mdProto.GetTreeUpdatesResponse.Update.TransactionTree(
                  toApiTransactionTree(globalOffset, tx)(requestingParty)
                ),
                domainId,
              )
            )

          case Update.TransferredOut(out) =>
            toApiTransferredOut(globalOffset, out)(Set(requestingParty)).map(transfer =>
              mdProto.GetTreeUpdatesResponse(
                mdProto.GetTreeUpdatesResponse.Update.Transfer(transfer),
                domainId,
              )
            )

          case Update.TransferredIn(in) =>
            Right(
              mdProto.GetTreeUpdatesResponse(
                mdProto.GetTreeUpdatesResponse.Update.Transfer(
                  toApiTransferredIn(globalOffset, in)(requestingParty)
                ),
                domainId,
              )
            )
        }
      }
      .getOrElse(Left("missing domain ID in update"))

  private def toApiIncompleteTransferEvents(
      incompleteTransfer: ActiveContractsResponse.IncompleteTransfer
  )(
      requestingParties: Set[LfPartyId]
  ): Either[String, mdProto.ContractStateComponent.ContractStateComponent] =
    incompleteTransfer match {
      case ActiveContractsResponse.IncompleteTransferredOut(out, contract, protocolVersion) =>
        toApiOutEvent(out)(requestingParties)
          .map { transferredOutEvent =>
            mdProto.IncompleteTransferredOut(
              createdEvent = Some(toApiCreatedEvent(contract, protocolVersion)(requestingParties)),
              transferredOutEvent = Some(transferredOutEvent),
            )
          }
          .map(mdProto.ContractStateComponent.ContractStateComponent.IncompleteTransferredOut)

      case ActiveContractsResponse.IncompleteTransferredIn(in) =>
        val incompleteTransferredIn = mdProto.IncompleteTransferredIn(
          transferredInEvent = Some(toApiInEvent(in)(requestingParties))
        )

        Right(
          mdProto.ContractStateComponent.ContractStateComponent.IncompleteTransferredIn(
            incompleteTransferredIn
          )
        )
    }

  def toApi(
      activeContractsResponse: ActiveContractsResponse
  )(
      requestingParties: Set[LfPartyId]
  ): Either[String, mdProto.GetActiveContractsResponse] = {

    val apiIncompleteTransfers = activeContractsResponse.incompleteTransfers
      .traverse(
        toApiIncompleteTransferEvents(_)(requestingParties)
      )
      .map(_.map(mdProto.ContractStateComponent.apply))

    val apiContracts = activeContractsResponse.activeContracts.flatMap {
      case ActiveContractsResponse.ActiveContracts(contracts, domainId, protocolVersion) =>
        contracts.map { contract =>
          val apiActiveContract = mdProto.ActiveContract(
            createdEvent = Some(toApiCreatedEvent(contract, protocolVersion)(requestingParties)),
            domainId = domainId.toProtoPrimitive,
          )

          mdProto.ContractStateComponent(
            mdProto.ContractStateComponent.ContractStateComponent.ActiveContract(apiActiveContract)
          )
        }

    }

    apiIncompleteTransfers.map { apiIncompleteTransfers =>
      mdProto.GetActiveContractsResponse(
        offset = toAbsoluteOffset(activeContractsResponse.validAt),
        contractStateComponents = apiIncompleteTransfers ++ apiContracts,
      )
    }
  }

  def toApi(response: ConnectedDomainResponse): mdProto.GetConnectedDomainsResponse = {
    val repeated = response.connectedDomains.map(connected =>
      mdProto.GetConnectedDomainsResponse
        .ConnectedDomain(
          connected.domainAlias.toProtoPrimitive,
          connected.domainId.toProtoPrimitive,
          connected.permission.toProtoEnum,
        )
    )
    mdProto.GetConnectedDomainsResponse(repeated)
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

  private def toProtoInstant(ts: LfTimestamp): Timestamp =
    TimestampConversion.fromInstant(ts.toInstant)

  private def toApiOutEvent(out: LedgerSyncEvent.TransferredOut)(
      requestingParties: Set[LfPartyId]
  ): Either[String, mdProto.TransferredOutEvent] = {
    val transferOutId = toApiTransferId(out.recordTime)
    out.templateId match {
      case Some(templateId) =>
        Right(
          mdProto.TransferredOutEvent(
            transferOutId = transferOutId,
            contractId = out.contractId.coid,
            source = out.sourceDomain.toProtoPrimitive,
            target = out.targetDomain.toProtoPrimitive,
            submitter = out.submitter,
            transferInExclusivity = out.transferInExclusivity.map(toProtoInstant),
            templateId = templateId.toString,
            witnessParties = requestingParties.intersect(out.contractStakeholders).toSeq,
          )
        )
      case None => Left(s"templateId should not be empty in transfer-id: $transferOutId")
    }
  }

  private def toApiTransferredOut(
      globalOffset: GlobalOffset,
      out: LedgerSyncEvent.TransferredOut,
  )(requestingParties: Set[LfPartyId]): Either[String, mdProto.Transfer] =
    toApiOutEvent(out)(requestingParties).map { transferOutEvent =>
      mdProto.Transfer(
        updateId = out.updateId,
        commandId = out.optCompletionInfo.fold("")(_.commandId),
        workflowId = out.workflowId.getOrElse(""),
        offset = toAbsoluteOffset(globalOffset),
        event = mdProto.Transfer.Event.TransferOutEvent(transferOutEvent),
      )
    }

  private def toApiInEvent(
      in: LedgerSyncEvent.TransferredIn
  )(requestingParties: Set[LfPartyId]): mdProto.TransferredInEvent = {

    val create = in.createNode

    val contractMetadata = ContractMetadata(
      createdAt = Some(TimestampConversion.fromInstant(in.ledgerCreateTime.toInstant)),
      contractKeyHash = create.gkeyOpt.fold(ByteString.EMPTY)(_.hash.bytes.toByteString),
      driverMetadata = in.contractMetadata.toByteString,
    )

    mdProto.TransferredInEvent(
      source = in.sourceDomain.toProtoPrimitive,
      target = in.targetDomain.toProtoPrimitive,
      transferOutId = toApiTransferId(in.transferId.transferOutTimestamp.toLf),
      submitter = in.submitter,
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
  )(requestingParty: LfPartyId): mdProto.Transfer = {

    val create = in.createNode

    val contractMetadata = ContractMetadata(
      createdAt = Some(TimestampConversion.fromInstant(in.ledgerCreateTime.toInstant)),
      contractKeyHash = create.gkeyOpt.fold(ByteString.EMPTY)(_.hash.bytes.toByteString),
      driverMetadata = in.contractMetadata.toByteString,
    )

    val inEvent = mdProto.TransferredInEvent(
      source = in.sourceDomain.toProtoPrimitive,
      target = in.targetDomain.toProtoPrimitive,
      transferOutId = toApiTransferId(in.transferId.transferOutTimestamp.toLf),
      submitter = in.submitter,
      createdEvent = Some(
        toApiCreatedEvent(
          creatingTransactionId = in.creatingTransactionId,
          nodeId = NodeId(0),
          create = create,
          contractMetadata = contractMetadata,
        )(
          requestingParties = Set(requestingParty)
        )
      ),
    )

    mdProto.Transfer(
      updateId = in.updateId,
      commandId = in.optCompletionInfo.fold("")(_.commandId),
      workflowId = in.workflowId.getOrElse(""),
      offset = toAbsoluteOffset(globalOffset),
      event = mdProto.Transfer.Event.TransferInEvent(inEvent),
    )
  }

  private def toApiTransactionTree(
      globalOffset: GlobalOffset,
      txAccepted: TransactionAccepted,
  )(requestingParty: LfPartyId): TransactionTree = {
    val blinding = txAccepted.blindingInfo.getOrElse(Blinding.blind(txAccepted.transaction))
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
            if blinding.disclosure.getOrElse(nodeId, Set.empty)(requestingParty) =>
          TreeEvent(
            TreeEvent.Kind.Created(
              toApiCreatedEvent(
                creatingTransactionId = txAccepted.transactionId,
                nodeId = nodeId,
                create = create,
                contractMetadata = toApiContractMetadata(create, txAccepted),
              )(Set(requestingParty))
            )
          )

        case (nodeId, exercise: Node.Exercise)
            if blinding.disclosure.getOrElse(nodeId, Set.empty)(requestingParty) =>
          TreeEvent(toApiExercised(nodeId, exercise, txAccepted.transactionId)(requestingParty))
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

    TransactionTree(
      transactionId = txAccepted.transactionId,
      commandId = txAccepted.optCompletionInfo
        .flatMap(completionInfo =>
          if (completionInfo.actAs.contains(requestingParty)) Some(completionInfo.commandId)
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
  )(requestingParty: LfPartyId): TreeEvent.Kind.Exercised =
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
        witnessParties = Seq(requestingParty),
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
  ): Completion.DeduplicationPeriod =
    deduplicationPeriod.fold[Completion.DeduplicationPeriod](Completion.DeduplicationPeriod.Empty) {
      case DeduplicationOffset(offset) =>
        Completion.DeduplicationPeriod.DeduplicationOffset(offset.toHexString)
      case DeduplicationDuration(duration) =>
        Completion.DeduplicationPeriod.DeduplicationDuration(
          Duration(seconds = duration.getSeconds, nanos = duration.getNano)
        )
    }

  private[grpc] def toApiCompletion(
      globalOffset: GlobalOffset,
      update: Either[CommandRejected, Update],
      completionInfo: CompletionInfo,
  ): mdProto.CompletionStreamResponse = {
    val completionData = CompletionData.create(update)

    mdProto.CompletionStreamResponse(
      checkpoint = Some(
        mdProto.Checkpoint(
          recordTime = Some(toProtoInstant(completionData.recordTime)),
          offset = Some(toApiLedgerOffset(Some(globalOffset))),
        )
      ),
      completion = Option(
        Completion(
          commandId = completionInfo.commandId,
          status = Some(completionData.status),
          transactionId = completionData.updateId,
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
