// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.multidomain

import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.foldable.*
import cats.syntax.functorFilter.*
import cats.syntax.traverse.*
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.error.TransactionErrorImpl
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.ledger.participant.state.v2.ReadService.{
  ConnectedDomainRequest,
  ConnectedDomainResponse,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.participant.ledger.api.multidomain.StateApiService.Error.StateRequestValidation
import com.digitalasset.canton.participant.ledger.api.multidomain.StateApiService.*
import com.digitalasset.canton.participant.protocol.transfer.IncompleteTransferData
import com.digitalasset.canton.participant.store.EventLogId.DomainEventLogId
import com.digitalasset.canton.participant.store.{
  MultiDomainEventLog,
  StoredContract,
  SyncDomainPersistentState,
}
import com.digitalasset.canton.participant.sync.{CantonSyncService, LedgerSyncEvent}
import com.digitalasset.canton.participant.{GlobalOffset, LocalOffset}
import com.digitalasset.canton.protocol.{LfContractId, SerializableContract}
import com.digitalasset.canton.store.IndexedDomain
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfWorkflowId, RequestCounter, TransferCounter}

import scala.concurrent.{ExecutionContext, Future}

private[participant] trait StateApiService {
  def getActiveContracts(request: ActiveContractsRequest)(implicit
      traceContext: TraceContext
  ): Future[ActiveContractsResponse]

  def getConnectedDomains(request: ConnectedDomainRequest)(implicit
      traceContext: TraceContext
  ): Future[ConnectedDomainResponse]

  def ledgerEnd()(implicit traceContext: TraceContext): Future[GlobalOffset]
}

object StateApiService {

  /** @param validAt Offset for the snapshot. If not set, the current ledger end will be used.
    * @param filters Each pair `(partyId, templatesO)` will be used to filter the contracts as follows:
    *                - `partyId` must be a stakeholder of the contract
    *                - `templatesO` if defined, the contract instance is one of the `templatesO`
    */
  final case class ActiveContractsRequest(
      validAt: Option[GlobalOffset],
      filters: RequestInclusiveFilter,
  )

  final case class ActiveContractsResponse(
      validAt: GlobalOffset,
      incompleteTransfers: Seq[ActiveContractsResponse.IncompleteTransfer],
      activeContracts: Seq[ActiveContractsResponse.ActiveContracts],
  )

  object ActiveContractsResponse {
    sealed trait IncompleteTransfer extends Product with Serializable {
      def workflowId: Option[LfWorkflowId]
    }
    final case class IncompleteTransferredOut(
        out: LedgerSyncEvent.TransferredOut,
        contract: SerializableContract,
        protocolVersion: ProtocolVersion,
    ) extends IncompleteTransfer {
      override def workflowId: Option[LfWorkflowId] = out.workflowId
    }
    final case class IncompleteTransferredIn(
        in: LedgerSyncEvent.TransferredIn
    ) extends IncompleteTransfer {
      override def workflowId: Option[LfWorkflowId] = in.workflowId
    }

    final case class ActiveContracts(
        contracts: Seq[(SerializableContract, TransferCounter)],
        domain: DomainId,
        protocolVersion: ProtocolVersion,
    )
  }

  object Error extends LedgerApiErrors.RequestValidation {
    @Explanation("This error results if a state request failed validation.")
    @Resolution("Check the request.")
    object StateRequestValidation
        extends ErrorCode(
          id = "STATE_REQUEST_VALIDATION",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(error: String)
          extends TransactionErrorImpl(cause = "Cannot process submitted request.")
    }
  }
}

class StateApiServiceImpl(
    cantonSyncService: CantonSyncService,
    multiDomainEventLog: MultiDomainEventLog,
    namedLoggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends StateApiService
    with NamedLogging {
  import StateApiService.*

  override def getConnectedDomains(request: ConnectedDomainRequest)(implicit
      traceContext: TraceContext
  ): Future[ConnectedDomainResponse] = cantonSyncService.getConnectedDomains(request)

  override def getActiveContracts(
      request: ActiveContractsRequest
  )(implicit traceContext: TraceContext): Future[ActiveContractsResponse] =
    for {
      ledgerBeginO <- multiDomainEventLog.locateOffset(0).value
      ledgerEnd <- ledgerEnd()
      validAt = request.validAt.getOrElse(ledgerEnd)
      _ <- toFuture(StateApiServiceImpl.validateValidAt(validAt, ledgerBeginO, ledgerEnd))

      syncDomainPersistentStates =
        cantonSyncService.syncDomainPersistentStateManager.getAll.values.toList

      incompleteTransferData <- cantonSyncService
        .incompleteTransferData(validAt, request.filters.parties)
        .map(
          _.filter(incomplete =>
            request.filters.satisfyFilter(
              incomplete.contract.metadata.stakeholders,
              incomplete.contract.contractInstance.unversioned.template,
            )
          )
        )

      incompleteTransfers <- enrichWithEventData(incompleteTransferData)

      activeContracts <- MonadUtil.sequentialTraverse(
        syncDomainPersistentStates
      )(getActiveContracts(_, validAt, request.filters, ledgerBeginO, ledgerEnd))

    } yield ActiveContractsResponse(validAt, incompleteTransfers, activeContracts)

  private def getActiveContracts(
      syncDomainPersistentState: SyncDomainPersistentState,
      validAt: GlobalOffset,
      filters: RequestInclusiveFilter,
      ledgerBeginO: Option[GlobalOffset],
      ledgerEnd: GlobalOffset,
  )(implicit traceContext: TraceContext): Future[ActiveContractsResponse.ActiveContracts] = {

    for {
      localOffset <- getLocalOffset(
        domainId = syncDomainPersistentState.domainId,
        validAt = validAt,
        ledgerBegin = ledgerBeginO,
        ledgerEnd = ledgerEnd,
      )

      domainId = syncDomainPersistentState.domainId.domainId

      protocolVersionET = EitherT(
        syncDomainPersistentState.parameterStore.lastParameters.map(
          _.toRight(s"Unable to find static domain parameters for domain $domainId")
        )
      ).map(_.protocolVersion)
      protocolVersion <- toFuture(protocolVersionET)

      contracts <- localOffset
        .traverse(getActiveContractsForLocalOffset(syncDomainPersistentState, filters, _))
        .map(_.getOrElse(Nil))
        .map(_.map { case (storedContract, transferCounter) =>
          (storedContract.contract, transferCounter)
        })

    } yield ActiveContractsResponse.ActiveContracts(contracts, domainId, protocolVersion)
  }

  private def enrichWithEventData(incompleteTransferData: List[IncompleteTransferData])(implicit
      traceContext: TraceContext
  ): Future[List[ActiveContractsResponse.IncompleteTransfer]] = {
    val globalOffsets = incompleteTransferData.map(_.transferEventGlobalOffset.globalOffset)

    for {
      events <- multiDomainEventLog.lookupByGlobalOffsets(globalOffsets).map(_.toMap)

      (missingEvents, incompleteTransfers) = incompleteTransferData.partitionMap { transferData =>
        events
          .get(transferData.transferEventGlobalOffset.globalOffset)
          .map(_.event)
          .map {
            case out: LedgerSyncEvent.TransferredOut =>
              Right(
                ActiveContractsResponse.IncompleteTransferredOut(
                  contract = transferData.contract,
                  out = out,
                  protocolVersion = transferData.sourceProtocolVersion.v,
                )
              )

            case in: LedgerSyncEvent.TransferredIn =>
              Right(
                ActiveContractsResponse.IncompleteTransferredIn(in)
              )

            case other =>
              ErrorUtil.internalError(new RuntimeException(s"Unexpected event $other"))
          }
          .getOrElse(Left(transferData.transferEventGlobalOffset.globalOffset))
      }

      _ =
        if (missingEvents.isEmpty) Future.unit
        else
          Future.failed(
            new RuntimeException(s"Unable to query events with global offset: $missingEvents")
          )
    } yield incompleteTransfers
  }

  override def ledgerEnd()(implicit
      traceContext: TraceContext
  ): Future[GlobalOffset] = multiDomainEventLog
    .lastGlobalOffset()
    .value
    .map(_.getOrElse(MultiDomainEventLog.ledgerFirstOffset))

  private def toFuture[T](result: EitherT[Future, String, T])(implicit
      traceContext: TraceContext
  ): Future[T] = CantonGrpcUtil.mapErrNew(
    result.leftMap(StateRequestValidation.Error.apply)
  )

  private def toFuture[T](result: Either[String, T])(implicit
      traceContext: TraceContext
  ): Future[T] = CantonGrpcUtil.mapErrNew(
    EitherT.fromEither[Future](result).leftMap(StateRequestValidation.Error.apply)
  )

  /*
      Get the local offset corresponding to a global offset
      If `validAt` is non-empty, validate that it is not before the ledger begin and not after the ledger end.
   */
  private def getLocalOffset(
      domainId: IndexedDomain,
      validAt: GlobalOffset,
      ledgerBegin: Option[GlobalOffset],
      ledgerEnd: GlobalOffset,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[LocalOffset]] =
    for {
      _ <- toFuture(
        EitherT.fromEither[Future](
          StateApiServiceImpl.validateValidAt(validAt, ledgerBegin, ledgerEnd)
        )
      )

      eventLogId = DomainEventLogId(domainId)

      localOffsetO <- multiDomainEventLog
        .lastLocalOffsetBeforeOrAt(eventLogId, validAt, None)

    } yield localOffsetO

  private def getActiveContractsForLocalOffset(
      syncDomainPersistentState: SyncDomainPersistentState,
      filters: RequestInclusiveFilter,
      localOffset: LocalOffset,
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[(StoredContract, TransferCounter)]] = {

    def contractPredicate(contract: StoredContract): Boolean =
      filters.satisfyFilter(
        contract.contract.metadata.stakeholders,
        contract.contract.contractInstance.unversioned.template,
      )

    def fetchContracts(
        contracts: Seq[(LfContractId, TransferCounter)]
    ): Future[Seq[(StoredContract, TransferCounter)]] = {
      val cids = contracts.map { case (cid, _) => cid }

      syncDomainPersistentState.contractStore.lookupManyUncached(cids).map { foundContracts =>
        contracts.zip(foundContracts).mapFilter {
          case ((cid, _), None) =>
            logger.warn(s"Unable to find contract $cid in ACS snapshot")
            None

          case ((_, transferCounter), Some(contract)) =>
            Option.when(contractPredicate(contract))((contract, transferCounter))
        }
      }
    }

    for {
      snapshot <- syncDomainPersistentState.activeContractStore
        .snapshot(
          RequestCounter(localOffset)
        )
        .map(_.toSeq.map { case (cid, (_, transferCounter)) => (cid, transferCounter) })

      contracts <- MonadUtil.batchedSequentialTraverse(
        parallelism = StateApiServiceImpl.dbBatchParallelism,
        chunkSize = StateApiServiceImpl.dbBatchChunkSize,
      )(snapshot)(fetchContracts)

    } yield contracts
  }

  override protected def loggerFactory: NamedLoggerFactory = namedLoggerFactory
}

private[participant] object StateApiServiceImpl {

  // TODO(i9270) extract magic numbers
  private val dbBatchParallelism: Int = 20
  private val dbBatchChunkSize: Int = 500

  /** Validate that `validAt` is not before than ledger begin and not after than ledger end
    */
  def validateValidAt(
      validAt: GlobalOffset,
      ledgerBegin: Option[GlobalOffset],
      ledgerEnd: GlobalOffset,
  ): Either[String, Unit] = for {
    _ <- Either.cond(
      validAt <= ledgerEnd,
      (),
      "validAt should not be greater than ledger end",
    )

    _ <- ledgerBegin.traverse_ { minOffset =>
      Either.cond(validAt >= minOffset, (), "validAt should not be smaller than ledger begin")
    }
  } yield ()
}
