// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.multidomain

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.canton.ledger.participant.state.v2.CompletionInfo
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.ledger.api.multidomain.UpdateApiService.{
  CompletionRequest,
  UpdateRequest,
}
import com.digitalasset.canton.participant.store.MultiDomainEventLog
import com.digitalasset.canton.participant.sync.LedgerSyncEvent
import com.digitalasset.canton.participant.sync.LedgerSyncEvent.CommandRejected
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.{LedgerApplicationId, LedgerTransactionId, LfPartyId, LfTimestamp}

import scala.concurrent.{ExecutionContext, Future}

private[participant] trait UpdateApiService {
  def updates(request: UpdateRequest)(implicit
      traceContext: TraceContext
  ): Source[(GlobalOffset, UpdateApiService.Update), NotUsed]

  def ledgerEnds()(implicit traceContext: TraceContext): Source[GlobalOffset, NotUsed]
  def ledgerEnd()(implicit traceContext: TraceContext): Future[Option[GlobalOffset]]

  def completions(completionRequest: CompletionRequest)(implicit
      traceContext: TraceContext
  ): Source[
    (GlobalOffset, Either[CommandRejected, UpdateApiService.Update], CompletionInfo),
    NotUsed,
  ]
}

private[participant] object UpdateApiService {
  sealed trait Update extends Product with Serializable {
    def event: LedgerSyncEvent.WithDomainId
    def optCompletionInfo: Option[CompletionInfo]
    def updateId: LedgerTransactionId
    final def recordTime: LfTimestamp = event.recordTime
    final def domainId: Option[DomainId] = event.domainId
  }

  object Update {
    final case class TransactionAccepted(event: LedgerSyncEvent.TransactionAccepted)
        extends Update {
      override def optCompletionInfo: Option[CompletionInfo] = event.optCompletionInfo
      override def updateId: LedgerTransactionId = event.transactionId
    }

    final case class TransferredOut(event: LedgerSyncEvent.TransferredOut) extends Update {
      override def optCompletionInfo: Option[CompletionInfo] = event.optCompletionInfo
      override def updateId: LedgerTransactionId = event.updateId
    }

    final case class TransferredIn(event: LedgerSyncEvent.TransferredIn) extends Update {
      override def optCompletionInfo: Option[CompletionInfo] = event.optCompletionInfo
      override def updateId: LedgerTransactionId = event.updateId
    }
  }

  final case class UpdateRequest(
      fromExclusive: GlobalOffset,
      toInclusive: GlobalOffset,
      requestingParty: LfPartyId,
  )

  final case class CompletionRequest(
      applicationId: LedgerApplicationId,
      parties: Set[LfPartyId],
      startExclusive: Option[GlobalOffset],
  )
}

final class UpdateApiServiceImpl(
    multiDomainEventLog: MultiDomainEventLog,
    namedLoggerFactory: NamedLoggerFactory,
)(implicit
    serviceExecutionContext: ExecutionContext
) extends UpdateApiService
    with NamedLogging {
  import UpdateApiService.*

  override def updates(request: UpdateRequest)(implicit
      traceContext: TraceContext
  ): Source[(GlobalOffset, Update), NotUsed] =
    multiDomainEventLog
      .subscribe(Some(request.fromExclusive + 1)) // `subscribe`'s start offset is inclusive
      .takeWhile { case (globalOffset, _) => globalOffset <= request.toInclusive }
      .collect {
        case (globalOffset, Traced(txAccepted: LedgerSyncEvent.TransactionAccepted))
            if txAccepted.transaction.informees(request.requestingParty) =>
          globalOffset -> Update.TransactionAccepted(txAccepted)

        case (globalOffset, Traced(tfOut: LedgerSyncEvent.TransferredOut))
            if tfOut.contractStakeholders.contains(request.requestingParty) =>
          globalOffset -> Update.TransferredOut(tfOut)

        case (globalOffset, Traced(tfIn: LedgerSyncEvent.TransferredIn))
            if tfIn.createNode.stakeholders.contains(request.requestingParty) =>
          globalOffset -> Update.TransferredIn(tfIn)
      }

  override def ledgerEnd()(implicit traceContext: TraceContext): Future[Option[GlobalOffset]] =
    multiDomainEventLog
      .lastGlobalOffset()
      .value

  override def ledgerEnds()(implicit
      traceContext: TraceContext
  ): Source[GlobalOffset, NotUsed] = {

    val currentLedgerEndF = ledgerEnd()
      .map(_.getOrElse(MultiDomainEventLog.ledgerFirstOffset))

    Source
      .future(currentLedgerEndF)
      .flatMapConcat(
        multiDomainEventLog.subscribeForLedgerEnds
      )
  }

  override def completions(completionRequest: CompletionRequest)(implicit
      traceContext: TraceContext
  ): Source[
    (GlobalOffset, Either[CommandRejected, UpdateApiService.Update], CompletionInfo),
    NotUsed,
  ] = {

    def completionInfoFilter(completionInfo: CompletionInfo): Boolean = {
      completionInfo.applicationId == completionRequest.applicationId &&
      completionInfo.actAs.exists(completionRequest.parties)
    }

    multiDomainEventLog
      .subscribe(
        completionRequest.startExclusive.map(_ + 1)
      ) // `subscribe`'s start offset is inclusive
      .collect {
        case (globalOffset, Traced(txAccepted: LedgerSyncEvent.TransactionAccepted)) =>
          globalOffset -> (Right(
            Update.TransactionAccepted(txAccepted)
          ), txAccepted.optCompletionInfo)

        case (globalOffset, Traced(out: LedgerSyncEvent.TransferredOut)) =>
          globalOffset -> (Right(Update.TransferredOut(out)), out.optCompletionInfo)

        case (globalOffset, Traced(in: LedgerSyncEvent.TransferredIn)) =>
          globalOffset -> (Right(Update.TransferredIn(in)), in.optCompletionInfo)

        case (globalOffset, Traced(commandRejected: LedgerSyncEvent.CommandRejected)) =>
          globalOffset -> (Left(commandRejected), Some(commandRejected.completionInfo))
      }
      .collect {
        case (globalOffset, (result, Some(completionInfo: CompletionInfo)))
            if completionInfoFilter(completionInfo) =>
          (globalOffset, result, completionInfo)
      }
  }

  override protected def loggerFactory: NamedLoggerFactory = namedLoggerFactory
}
