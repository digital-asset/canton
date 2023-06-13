// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.multidomain

import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.foldable.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.daml.lf.data.Ref.Identifier
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.error.TransactionErrorImpl
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.participant.ledger.api.multidomain.StateApiService.Error.StateRequestValidation
import com.digitalasset.canton.participant.ledger.api.multidomain.StateApiService.*
import com.digitalasset.canton.participant.protocol.transfer.{IncompleteTransferData, TransferData}
import com.digitalasset.canton.participant.store.EventLogId.DomainEventLogId
import com.digitalasset.canton.participant.store.{
  MultiDomainEventLog,
  StoredContract,
  SyncDomainPersistentState,
}
import com.digitalasset.canton.participant.sync.{
  CantonSyncService,
  LedgerSyncEvent,
  TimestampedEvent,
}
import com.digitalasset.canton.participant.{GlobalOffset, LocalOffset}
import com.digitalasset.canton.protocol.{
  LfContractId,
  LfTemplateId,
  SerializableContract,
  SourceDomainId,
}
import com.digitalasset.canton.store.IndexedDomain
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.OptionUtils.OptionExtension
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DomainAlias, LfPartyId, RequestCounter}

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
      filters: Map[LfPartyId, Option[NonEmpty[Seq[LfTemplateId]]]],
  )

  final case class ActiveContractsResponse(
      validAt: GlobalOffset,
      incompleteTransfers: Seq[ActiveContractsResponse.IncompleteTransfer],
      activeContracts: Seq[ActiveContractsResponse.ActiveContracts],
  )

  object ActiveContractsResponse {
    sealed trait IncompleteTransfer extends Product with Serializable
    final case class IncompleteTransferredOut(
        out: LedgerSyncEvent.TransferredOut,
        contract: SerializableContract,
        protocolVersion: ProtocolVersion,
    ) extends IncompleteTransfer
    final case class IncompleteTransferredIn(
        in: LedgerSyncEvent.TransferredIn
    ) extends IncompleteTransfer

    final case class ActiveContracts(
        contracts: Seq[SerializableContract],
        domain: DomainId,
        protocolVersion: ProtocolVersion,
    )
  }

  final case class TransferWithStoredContract(
      transfer: LedgerSyncEvent.TransferredOut,
      serializableContract: SerializableContract,
  )

  final case class ConnectedDomainRequest(party: LfPartyId)
  final case class ConnectedDomainResponse(
      connectedDomains: Seq[ConnectedDomainResponse.ConnectedDomain]
  )

  object ConnectedDomainResponse {
    final case class ConnectedDomain(
        domainAlias: DomainAlias,
        domainId: DomainId,
        permission: ParticipantPermission,
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

  private val submittingParticipant = cantonSyncService.participantId

  override def getConnectedDomains(request: ConnectedDomainRequest)(implicit
      traceContext: TraceContext
  ): Future[ConnectedDomainResponse] = {

    def getSnapshot(domainAlias: DomainAlias, domainId: DomainId): Future[TopologySnapshot] =
      cantonSyncService.syncCrypto.ips
        .forDomain(domainId)
        .toFuture(
          new Exception(
            s"Failed retrieving DomainTopologyClient for domain: $domainAlias"
          )
        )
        .map(_.currentSnapshotApproximation)

    val result = cantonSyncService.readyDomains
      // keep only healthy domains
      .collect { case (domainAlias, (domainId, true)) =>
        for {
          topology <- getSnapshot(domainAlias, domainId)
          attributesO <- topology.hostedOn(request.party, participantId = submittingParticipant)
        } yield attributesO.map(attributes =>
          ConnectedDomainResponse.ConnectedDomain(
            domainAlias,
            domainId,
            attributes.permission,
          )
        )
      }.toSeq

    Future.sequence(result).map(_.flatten).map(ConnectedDomainResponse.apply)
  }

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

      incompleteTransferData <-
        syncDomainPersistentStates
          .parTraverse { syncDomainPersistentState =>
            syncDomainPersistentState.transferStore.findIncomplete(
              sourceDomain = None,
              validAt = validAt,
              stakeholders = NonEmpty.from(request.filters.keySet),
              limit = NonNegativeInt.maxValue,
            )
          }
          .map(_.flatten)
          .map(
            _.filter(incomplete =>
              StateApiServiceImpl.contractFilter(request.filters)(
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
      filters: Map[LfPartyId, Option[NonEmpty[Seq[LfTemplateId]]]],
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
        .map(_.map(_.contract))

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
      filters: Map[LfPartyId, Option[NonEmpty[Seq[LfTemplateId]]]],
      localOffset: LocalOffset,
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[StoredContract]] = {

    def contractPredicate(contract: StoredContract): Boolean =
      StateApiServiceImpl.contractFilter(filters)(
        contract.contract.metadata.stakeholders,
        contract.contract.contractInstance.unversioned.template,
      )

    def fetchContracts(cids: Seq[LfContractId]): Future[Seq[StoredContract]] = {
      syncDomainPersistentState.contractStore.lookupManyUncached(cids).map { foundContracts =>
        cids.zip(foundContracts).mapFilter {
          case (cid, None) =>
            logger.warn(s"Unable to find contract $cid in ACS snapshot")
            None

          case (_, Some(contract)) =>
            Option.when(contractPredicate(contract))(contract)
        }
      }
    }

    for {
      snapshot <- syncDomainPersistentState.activeContractStore.snapshot(
        RequestCounter(localOffset)
      )

      cids = snapshot.keys.toSeq

      contracts <- MonadUtil.batchedSequentialTraverse(
        parallelism = StateApiServiceImpl.dbBatchParallelism,
        chunkSize = StateApiServiceImpl.dbBatchChunkSize,
      )(cids)(fetchContracts)

    } yield contracts.sortBy(_.requestCounter)
  }

  private def inFlightTransfersForLocalOffset(
      syncDomainPersistentState: SyncDomainPersistentState,
      stakeholders: NonEmpty[Seq[LfPartyId]],
      localOffset: LocalOffset,
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[TransferWithStoredContract]] = {

    for {
      transfersData <-
        syncDomainPersistentState.transferStore.findInFlight(
          sourceDomain = SourceDomainId(syncDomainPersistentState.domainId.domainId),
          onlyCompletedTransferOut = true,
          transferOutRequestNotAfter = localOffset,
          stakeholders = Some(stakeholders.toSet),
          limit = NonNegativeInt.maxValue, // TODO(#11735): Fix when we do the streaming
        )

      localOffsets = transfersData.map(_.transferOutRequestCounter.asLocalOffset)

      events <-
        multiDomainEventLog.lookupByLocalOffsets(
          DomainEventLogId(syncDomainPersistentState.domainId),
          localOffsets,
        )

      eventPerLocalOffset = events.map { case (globalOffset, event) =>
        event.localOffset -> (globalOffset, event)
      }.toMap

      (missingEvents, transfersWithEvents) = transfersData.partitionMap { d =>
        val localOffset = d.transferOutRequestCounter.asLocalOffset
        eventPerLocalOffset
          .get(localOffset)
          .map { case (globalOffset, event) => (globalOffset, event, d) }
          .toRight(localOffset)
      }

      _ =
        if (missingEvents.isEmpty) Future.unit
        else
          Future.failed(
            new RuntimeException(
              s"Unable to query events for domain ${syncDomainPersistentState.domainId}: $missingEvents"
            )
          )

      outs = collectTransferOut(transfersWithEvents)

    } yield outs
  }

  private def collectTransferOut(
      events: Seq[(GlobalOffset, TimestampedEvent, TransferData)]
  )(implicit
      traceContext: TraceContext
  ): Seq[TransferWithStoredContract] = {

    val (outs, others) = events.partitionMap { case (globalOffset, event, transferData) =>
      (event.event, transferData) match {
        case (out: LedgerSyncEvent.TransferredOut, transferData) =>
          Left(TransferWithStoredContract(out, transferData.contract))
        case _ => Right(globalOffset)
      }
    }

    // If we get events that are not transfer-out
    NonEmpty.from(others.toList).foreach { others =>
      logger.warn(
        s"Expected transferred-out and found other kind for events: ${others.mkString(", ")}"
      )
    }

    outs
  }

  private def getSyncDomainPersistentState(
      domainId: DomainId
  )(implicit traceContext: TraceContext): Future[SyncDomainPersistentState] =
    cantonSyncService.syncDomainPersistentStateManager.get(domainId) match {
      case Some(syncDomainPersistentState) =>
        Future.successful(syncDomainPersistentState)

      case None =>
        Future.failed(
          LedgerApiErrors.RequestValidation.InvalidArgument
            .Reject(s"Domain ID not found: $domainId")
            .asGrpcError
        )
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

  /** Check whether a contract (here, stakeholders and templateId) pass the provided filter
    * @param filters List of filters passed with the requests
    * @param stakeholders Stakeholders of the contract
    * @param templateId Template of the contract
    * @return true if the contract satisfies the filter
    */
  def contractFilter(
      filters: Map[LfPartyId, Option[NonEmpty[Seq[LfTemplateId]]]]
  )(stakeholders: Set[LfPartyId], templateId: Identifier): Boolean = {
    // At least one stakeholder should pass the per-party filter
    stakeholders.exists { stakeholder =>
      filters.get(stakeholder) match {
        case Some(templatesFilterO) =>
          templatesFilterO match {
            case Some(templates) =>
              templates.contains(templateId)

            case None => true // There is no restriction on the template
          }

        // stakeholder is not in the request
        case None => false
      }
    }
  }
}
