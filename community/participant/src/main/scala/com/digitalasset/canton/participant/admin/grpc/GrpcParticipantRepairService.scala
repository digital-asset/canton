// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.syntax.all.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.ProtoDeserializationError.{OtherError, ValueConversionError}
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.admin.participant.v30.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.data.{
  ActiveContract,
  ContractImportMode,
  LateLsuRequest,
  RepairContract,
  RepresentativePackageIdOverride,
}
import com.digitalasset.canton.participant.admin.grpc.GrpcParticipantRepairService.ValidExportAcsRequest
import com.digitalasset.canton.participant.admin.repair.RepairServiceError
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{
  ParticipantId,
  PartyId,
  PhysicalSynchronizerId,
  SynchronizerId,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{EitherTUtil, GrpcStreamingUtils, OptionUtil}
import com.digitalasset.canton.{
  LfPartyId,
  ProtoDeserializationError,
  ReassignmentCounter,
  SequencerCounter,
  SynchronizerAlias,
  protocol,
}
import io.grpc.stub.StreamObserver
import org.apache.pekko.actor.ActorSystem

import java.io.OutputStream
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.GZIPOutputStream
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Success, Try}

final class GrpcParticipantRepairService(
    sync: CantonSyncService,
    parameters: ParticipantNodeParameters,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContextExecutor,
    actorSystem: ActorSystem,
) extends ParticipantRepairServiceGrpc.ParticipantRepairService
    with NamedLogging {

  private val synchronizerMigrationInProgress = new AtomicReference[Boolean](false)

  private val processingTimeout: ProcessingTimeout = parameters.processingTimeouts

  /** purge contracts
    */
  override def purgeContracts(request: PurgeContractsRequest): Future[PurgeContractsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val res: Either[RepairServiceError, Unit] = for {
      cids <- request.contractIds
        .traverse(LfContractId.fromString)
        .leftMap(RepairServiceError.InvalidArgument.Error(_))
      synchronizerAlias <- SynchronizerAlias
        .fromProtoPrimitive(request.synchronizerAlias)
        .leftMap(_.toString)
        .leftMap(RepairServiceError.InvalidArgument.Error(_))

      cidsNE <- NonEmpty
        .from(cids)
        .toRight(RepairServiceError.InvalidArgument.Error("Missing contract ids to purge"))

      _ <- sync.repairService
        .purgeContracts(synchronizerAlias, cidsNE, request.ignoreAlreadyPurged)
        .leftMap(RepairServiceError.ContractPurgeError.Error(synchronizerAlias, _))
    } yield ()

    res.fold(
      err => Future.failed(err.asGrpcError),
      _ => Future.successful(PurgeContractsResponse()),
    )
  }

  override def exportAcs(
      request: v30.ExportAcsRequest,
      responseObserver: StreamObserver[v30.ExportAcsResponse],
  ): Unit = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    GrpcStreamingUtils.streamToClient(
      (out: OutputStream) => processExportAcs(request, new GZIPOutputStream(out)),
      responseObserver,
      byteString => v30.ExportAcsResponse(byteString),
      processingTimeout.unbounded.duration,
      chunkSizeO = None,
    )
  }

  private def processExportAcs(
      request: v30.ExportAcsRequest,
      out: OutputStream,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val res = for {
      ledgerEnd <- EitherT
        .fromEither[FutureUnlessShutdown](ParticipantCommon.findLedgerEnd(sync))
        .leftMap(RepairServiceError.InvalidState.Error(_))
      allLogicalSynchronizerIds = sync.syncPersistentStateManager.getAllLatest.keySet

      validRequest <- EitherT.fromEither[FutureUnlessShutdown](
        validateExportAcsRequest(request, ledgerEnd, allLogicalSynchronizerIds)
      )
      indexService <- EitherT.fromOption[FutureUnlessShutdown](
        sync.internalIndexService,
        RepairServiceError.InvalidState.Error("Unavailable internal state service"),
      )

      snapshot <- ParticipantCommon
        .writeAcsSnapshot(
          indexService,
          validRequest.parties,
          validRequest.atOffset,
          out,
          validRequest.excludedStakeholders,
          validRequest.synchronizerId,
          validRequest.contractSynchronizerRenames,
        )(ec, traceContext, actorSystem)
        .leftMap(msg => RepairServiceError.IOStream.Error(msg): RepairServiceError)
    } yield snapshot

    mapErrNewEUS(res.leftMap(_.toCantonRpcError))
  }

  private def validateExportAcsRequest(
      request: v30.ExportAcsRequest,
      ledgerEnd: Offset,
      synchronizerIds: Set[SynchronizerId],
  )(implicit
      elc: ErrorLoggingContext
  ): Either[RepairServiceError, ValidExportAcsRequest] = {
    val parsingResult = for {
      parties <- request.partyIds.traverse(party =>
        UniqueIdentifier.fromProtoPrimitive(party, "party_ids").map(PartyId(_))
      )
      parsedFilterSynchronizerId <- OptionUtil
        .emptyStringAsNone(request.synchronizerId)
        .traverse(SynchronizerId.fromProtoPrimitive(_, "filter_synchronizer_id"))
      filterSynchronizerId <- Either.cond(
        parsedFilterSynchronizerId.forall(synchronizerIds.contains),
        parsedFilterSynchronizerId,
        OtherError(s"Filter synchronizer id $parsedFilterSynchronizerId is unknown"),
      )
      parsedOffset <- ProtoConverter
        .parseOffset("ledger_offset", request.ledgerOffset)
      ledgerOffset <- Either.cond(
        parsedOffset <= ledgerEnd,
        parsedOffset,
        OtherError(
          s"Ledger offset $parsedOffset needs to be smaller or equal to the ledger end $ledgerEnd"
        ),
      )
      contractSynchronizerRenames <- request.contractSynchronizerRenames.toList.traverse {
        case (source, v30.ExportAcsTargetSynchronizer(target)) =>
          for {
            _ <- SynchronizerId.fromProtoPrimitive(source, "source synchronizer id")
            _ <- SynchronizerId.fromProtoPrimitive(target, "target synchronizer id")
          } yield (source, target)
      }
      excludedStakeholders <- request.excludedStakeholderIds.traverse(party =>
        UniqueIdentifier.fromProtoPrimitive(party, "excluded_stakeholder_ids").map(PartyId(_))
      )

    } yield ValidExportAcsRequest(
      parties.toSet,
      ledgerOffset,
      excludedStakeholders.toSet,
      filterSynchronizerId,
      contractSynchronizerRenames.toMap,
    )
    parsingResult.leftMap(error => RepairServiceError.InvalidArgument.Error(error.message))
  }

  override def importAcs(
      responseObserver: StreamObserver[ImportAcsResponse]
  ): StreamObserver[ImportAcsRequest] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    type ImportArgs =
      (
          Option[String],
          ContractImportMode,
          Set[LfPartyId],
          RepresentativePackageIdOverride,
          SynchronizerId,
      )

    def extractImportArgs(
        request: ImportAcsRequest
    ): Try[ImportArgs] = {
      val resultE = for {
        contractImportMode <- ProtoConverter.parseRequired(
          ContractImportMode.fromProtoV30,
          "contract_import_mode",
          request.contractImportMode,
        )
        excludedStakeholders <- request.excludedStakeholderIds
          .traverse(party =>
            UniqueIdentifier
              .fromProtoPrimitive(party, "excluded_stakeholder_ids")
              .map(PartyId(_).toLf)
          )
        representativePackageIdOverride <- request.representativePackageIdOverride
          .traverse(RepresentativePackageIdOverride.fromProtoV30)
          .map(_.getOrElse(RepresentativePackageIdOverride.NoOverride))
        synchronizerId <- ProtoConverter.parseRequired(
          SynchronizerId.fromProtoPrimitive(_, "synchronizer_id"),
          "synchronizer_id",
          request.synchronizerId,
        )
        workflowIdPrefix = request.workflowIdPrefix
          .flatMap(OptionUtil.emptyStringAsNone)
          .orElse(Some(s"import-${UUID.randomUUID}"))
      } yield (
        workflowIdPrefix,
        contractImportMode,
        excludedStakeholders.toSet,
        representativePackageIdOverride,
        synchronizerId,
      )

      resultE
        .leftMap(ProtoDeserializationError.ProtoDeserializationFailure.Wrap(_).asGrpcError)
        .toTry
    }

    GrpcStreamingUtils
      .streamGzippedChunksFromClient[
        ImportAcsRequest,
        ImportAcsResponse,
        ImportArgs,
        ActiveContract,
      ](
        responseObserver,
        Success(ImportAcsResponse()),
        getGzippedBytes = _.acsSnapshot,
        parseMessage = ActiveContract.parseDelimitedFromTrusted(_),
      )(contextFromFirstRequest = extractImportArgs) {
        case (
              (
                workFlowIdPrefix,
                contractImportMode,
                excludedStakeholders,
                representativePackageIdOverride,
                synchronizerId,
              ),
              source,
            ) =>
          val repairSource = source
            .map(activeContract =>
              RepairContract
                .fromLapiActiveContract(activeContract.contract)
                .valueOr(err =>
                  throw ProtoDeserializationError.ProtoDeserializationFailure
                    .Wrap(ProtoDeserializationError.ValueConversionError("contract", err))
                    .asGrpcError
                )
            )

          val filteredSource = if (excludedStakeholders.isEmpty) {
            repairSource
          } else {
            repairSource.filter(
              _.contract.stakeholders.intersect(excludedStakeholders).isEmpty
            )
          }
          val resultEUS = sync.repairService.addContracts(
            synchronizerId,
            filteredSource,
            contractImportMode,
            sync.getPackageMetadataSnapshot,
            representativePackageIdOverride,
            workflowIdPrefix = workFlowIdPrefix,
          )
          EitherTUtil.toFutureUnlessShutdown(
            resultEUS.bimap(
              err => RepairServiceError.ImportAcsError.Error(err).asGrpcError,
              _ => ImportAcsResponse(),
            )
          )

      }
  }

  override def migrateSynchronizer(
      request: MigrateSynchronizerRequest
  ): Future[MigrateSynchronizerResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    // ensure here we don't process migration requests concurrently
    if (!synchronizerMigrationInProgress.getAndSet(true)) {
      val migratedSourceSynchronizer = for {
        sourceSynchronizerAlias <- EitherT.fromEither[Future](
          SynchronizerAlias.create(request.sourceSynchronizerAlias).map(Source(_))
        )
        conf <- EitherT
          .fromEither[Future](for {
            confP <- request.targetSynchronizerConnectionConfig
              .toRight("The target synchronizer connection configuration is required")
            conf <- SynchronizerConnectionConfig.fromProtoV30(confP).leftMap(_.toString)
            _ <- conf.sequencerConnections.submissionRequestAmplification.validate.leftMap(
              message => s"Target synchronizer connection config error in amplification: $message"
            )
          } yield Target(conf))
        _ <- EitherT(
          sync
            .migrateSynchronizer(sourceSynchronizerAlias, conf, force = request.force)
            .leftMap(_.asGrpcError.getStatus.getDescription)
            .value
            .onShutdown {
              Left("Aborted due to shutdown.")
            }
        )
      } yield MigrateSynchronizerResponse()

      EitherTUtil
        .toFuture(
          migratedSourceSynchronizer.leftMap(err =>
            io.grpc.Status.CANCELLED.withDescription(err).asRuntimeException()
          )
        )
        .thereafter { _ =>
          synchronizerMigrationInProgress.set(false)
        }
    } else
      Future.failed(
        io.grpc.Status.ABORTED
          .withDescription(
            s"migrate_synchronizer for participant: ${sync.participantId} is already in progress"
          )
          .asRuntimeException()
      )
  }

  override def changeAssignation(
      request: ChangeAssignationRequest
  ): Future[ChangeAssignationResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    def synchronizerIdFromAlias(
        synchronizerAliasP: String
    ): EitherT[FutureUnlessShutdown, RepairServiceError, SynchronizerId] =
      for {
        alias <- EitherT
          .fromEither[FutureUnlessShutdown](SynchronizerAlias.create(synchronizerAliasP))
          .leftMap(err =>
            RepairServiceError.InvalidArgument
              .Error(s"Unable to parse $synchronizerAliasP as synchronizer alias: $err")
          )

        id <- EitherT.fromEither[FutureUnlessShutdown](
          sync.aliasManager
            .synchronizerIdForAlias(alias)
            .toRight[RepairServiceError](
              RepairServiceError.InvalidArgument
                .Error(s"Unable to find synchronizer id for alias $alias")
            )
        )
      } yield id

    val result = for {
      sourceSynchronizerId <- synchronizerIdFromAlias(request.sourceSynchronizerAlias).map(
        Source(_)
      )
      targetSynchronizerId <- synchronizerIdFromAlias(request.targetSynchronizerAlias).map(
        Target(_)
      )

      contracts <- EitherT.fromEither[FutureUnlessShutdown](request.contracts.traverse {
        case ChangeAssignationRequest.Contract(cidP, reassignmentCounterPO) =>
          val reassignmentCounter = reassignmentCounterPO.map(ReassignmentCounter(_))

          LfContractId
            .fromString(cidP)
            .leftMap[RepairServiceError](err =>
              RepairServiceError.InvalidArgument.Error(s"Unable to parse contract id `$cidP`: $err")
            )
            .map((_, reassignmentCounter))
      })

      _ <- NonEmpty.from(contracts) match {
        case None => EitherTUtil.unitUS[RepairServiceError]
        case Some(contractsNE) =>
          sync.repairService
            .changeAssignation(
              contracts = contractsNE,
              sourceSynchronizer = sourceSynchronizerId,
              targetSynchronizer = targetSynchronizerId,
              skipInactive = request.skipInactive,
            )
            .leftMap[RepairServiceError](RepairServiceError.ContractAssignationChangeError.Error(_))
      }
    } yield ()

    EitherTUtil
      .toFutureUnlessShutdown(
        result.bimap(
          _.asGrpcError,
          _ => ChangeAssignationResponse(),
        )
      )
      .asGrpcResponse

  }

  override def purgeDeactivatedSynchronizer(
      request: PurgeDeactivatedSynchronizerRequest
  ): Future[PurgeDeactivatedSynchronizerResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val res = for {
      synchronizerAlias <- EitherT.fromEither[FutureUnlessShutdown](
        SynchronizerAlias
          .fromProtoPrimitive(request.synchronizerAlias)
          .leftMap(_.toString)
          .leftMap(RepairServiceError.InvalidArgument.Error(_))
      )
      _ <- sync.purgeDeactivatedSynchronizer(synchronizerAlias).leftWiden[RpcError]
    } yield ()

    EitherTUtil
      .toFutureUnlessShutdown(
        res.bimap(
          _.asGrpcError,
          _ => PurgeDeactivatedSynchronizerResponse(),
        )
      )
      .asGrpcResponse
  }

  override def ignoreEvents(request: IgnoreEventsRequest): Future[IgnoreEventsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val res = for {
      synchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
        PhysicalSynchronizerId
          .fromProtoPrimitive(request.physicalSynchronizerId, "physical_synchronizer_id")
          .leftMap(_.message)
      )
      _ <- sync.repairService.ignoreEvents(
        synchronizerId,
        SequencerCounter(request.fromInclusive),
        SequencerCounter(request.toInclusive),
        force = request.force,
      )
    } yield IgnoreEventsResponse()

    EitherTUtil
      .toFutureUnlessShutdown(
        res.leftMap(err => io.grpc.Status.CANCELLED.withDescription(err).asRuntimeException())
      )
      .asGrpcResponse
  }

  override def unignoreEvents(request: UnignoreEventsRequest): Future[UnignoreEventsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val res = for {
      synchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
        PhysicalSynchronizerId
          .fromProtoPrimitive(request.physicalSynchronizerId, "physical_synchronizer_id")
          .leftMap(_.message)
      )
      _ <- sync.repairService.unignoreEvents(
        synchronizerId,
        SequencerCounter(request.fromInclusive),
        SequencerCounter(request.toInclusive),
        force = request.force,
      )
    } yield UnignoreEventsResponse()

    EitherTUtil
      .toFutureUnlessShutdown(
        res.leftMap(err => io.grpc.Status.CANCELLED.withDescription(err).asRuntimeException())
      )
      .asGrpcResponse
  }

  override def rollbackUnassignment(
      request: RollbackUnassignmentRequest
  ): Future[RollbackUnassignmentResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val res = for {
      reassignmentId <- EitherT.fromEither[FutureUnlessShutdown](
        protocol.ReassignmentId
          .fromProtoPrimitive(request.reassignmentId)
          .leftMap(err => ValueConversionError("reassignment_id", err.message).message)
      )
      sourceSynchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
        SynchronizerId
          .fromProtoPrimitive(request.sourceSynchronizerId, "source")
          .map(Source(_))
          .leftMap(_.message)
      )
      targetSynchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
        SynchronizerId
          .fromProtoPrimitive(request.targetSynchronizerId, "target")
          .map(Target(_))
          .leftMap(_.message)
      )

      _ <- sync.repairService.rollbackUnassignment(
        reassignmentId,
        sourceSynchronizerId,
        targetSynchronizerId,
      )

    } yield RollbackUnassignmentResponse()

    EitherTUtil
      .toFutureUnlessShutdown(
        res.leftMap(err => io.grpc.Status.CANCELLED.withDescription(err).asRuntimeException())
      )
      .asGrpcResponse
  }

  override def repairCommitmentsUsingAcs(
      request: RepairCommitmentsUsingAcsRequest
  ): Future[RepairCommitmentsUsingAcsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val result = for {
      synchronizerIds <- wrapErrUS(
        request.synchronizerIds.traverse(SynchronizerId.fromProtoPrimitive(_, "synchronizer_id"))
      )
      counterParticipantsIds <- wrapErrUS(
        request.counterParticipantIds.traverse(
          ParticipantId.fromProtoPrimitive(_, "counter_participant_id")
        )
      )

      partyIds <- wrapErrUS(
        request.partyIds.traverse(party =>
          UniqueIdentifier.fromProtoPrimitive(party, "party_ids").map(PartyId(_))
        )
      )

      timeoutSeconds <- wrapErrUS(
        ProtoConverter.parseRequired(
          NonNegativeFiniteDuration.fromProtoPrimitive("initialRetryDelay"),
          "timeoutSeconds",
          request.timeoutSeconds,
        )
      )

      res <- EitherT
        .right[RpcError](
          sync.commitmentsService
            .reinitializeCommitmentsUsingAcs(
              synchronizerIds.toSet,
              counterParticipantsIds,
              partyIds,
              timeoutSeconds,
            )
        )

    } yield v30.RepairCommitmentsUsingAcsResponse(res.map {
      case (synchronizerId: SynchronizerId, stringOrMaybeTimestamp) =>
        stringOrMaybeTimestamp match {
          case Left(err) =>
            v30.RepairCommitmentsStatus(
              synchronizerId.toProtoPrimitive,
              v30.RepairCommitmentsStatus.Status.ErrorMessage(err),
            )
          case Right(ts) =>
            v30.RepairCommitmentsStatus(
              synchronizerId.toProtoPrimitive,
              v30.RepairCommitmentsStatus.Status.CompletedRepairTimestamp(ts match {
                case Some(value) => value.toProtoTimestamp
                case None => CantonTimestamp.MinValue.toProtoTimestamp
              }),
            )
        }
    }.toSeq)

    CantonGrpcUtil.mapErrNewEUS(result)
  }

  override def performLateLsu(
      request: PerformLateLsuRequest
  ): Future[PerformLateLsuResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val res = for {
      validatedRequest <- CantonGrpcUtil.wrapErrUS(LateLsuRequest.fromProtoV30(request))
      _ <- CantonGrpcUtil.wrapErrUS(
        validatedRequest.successorConfig.sequencerConnections.submissionRequestAmplification.validate
          .leftMap(
            ProtoDeserializationError.ValueConversionError(
              "SynchronizerConnectionConfig.SequencerConnections.SubmissionRequestAmplification",
              _,
            )
          )
      )

      _ <- sync
        .performLateLsu(validatedRequest)
        .leftMap[RpcError](
          RepairServiceError.SynchronizerUpgradeError.Error(validatedRequest.successorPsid, _)
        )
    } yield PerformLateLsuResponse()

    CantonGrpcUtil.mapErrNewEUS(res)
  }
}

object GrpcParticipantRepairService {

  private final case class ValidExportAcsRequest(
      parties: Set[PartyId],
      atOffset: Offset,
      excludedStakeholders: Set[PartyId],
      synchronizerId: Option[SynchronizerId],
      contractSynchronizerRenames: Map[String, String],
  )

}
