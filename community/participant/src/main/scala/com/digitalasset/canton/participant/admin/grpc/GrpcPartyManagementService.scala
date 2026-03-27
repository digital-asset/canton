// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction as LapiTopologyTransaction
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.admin.participant.v30.*
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.{InternalIndexService, SynchronizerIndex}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.{GrpcErrors, mapErrNewEUS}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.data.{
  ActiveContract,
  ContractImportMode,
  PartyOnboardingFlagStatus,
  RepairContract,
  RepresentativePackageIdOverride,
}
import com.digitalasset.canton.participant.admin.party.*
import com.digitalasset.canton.participant.admin.party.PartyReplicationAdminWorkflow.PartyReplicationArguments
import com.digitalasset.canton.participant.protocol.party.OnboardingClearanceOperation
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.topology.TopologyLookup
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SynchronizerOffset
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.transaction.{
  ParticipantPermission,
  PartyToParticipant,
  TopologyMapping,
}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherUtil.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{EitherTUtil, GrpcStreamingUtils, OptionUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import com.google.protobuf.duration.Duration
import io.grpc.stub.StreamObserver
import io.grpc.{Status, StatusRuntimeException}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Sink

import java.io.{ByteArrayOutputStream, OutputStream}
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.zip.GZIPOutputStream
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

/** grpc service to allow modifying party hosting on participants
  */
class GrpcPartyManagementService(
    participantId: ParticipantId,
    partyReplicatorO: Option[PartyReplicator],
    sync: CantonSyncService,
    topologyLookup: TopologyLookup,
    parameters: ParticipantNodeParameters,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContextExecutor,
    actorSystem: ActorSystem,
) extends v30.PartyManagementServiceGrpc.PartyManagementService
    with NamedLogging {

  override def addPartyAsync(
      request: v30.AddPartyAsyncRequest
  ): Future[v30.AddPartyAsyncResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    EitherTUtil.toFuture(for {
      partyReplicator <- EitherT.fromEither[Future](
        ensureOnlinePartyReplicationEnabled()
      )

      argsP <- EitherT
        .fromEither[Future](
          ProtoConverter
            .required("arguments", request.arguments)
            .leftMap(err => toStatusRuntimeException(Status.INVALID_ARGUMENT)(err.message))
        )

      args <- EitherT.fromEither[Future](
        verifyArguments(argsP).leftMap(toStatusRuntimeException(Status.INVALID_ARGUMENT))
      )

      hash <- partyReplicator
        .addPartyAsync(args)
        .leftMap(toStatusRuntimeException(Status.FAILED_PRECONDITION))
        .onShutdown(Left(GrpcErrors.AbortedDueToShutdown.Error().asGrpcError))
    } yield v30.AddPartyAsyncResponse(addPartyRequestId = hash.toHexString))
  }

  override def addPartyWithAcsAsync(
      responseObserver: StreamObserver[AddPartyWithAcsAsyncResponse]
  ): StreamObserver[AddPartyWithAcsAsyncRequest] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    // TODO(#23818): This buffer will contain the whole ACS snapshot.
    val outputStream = new ByteArrayOutputStream()
    val arguments = new AtomicReference[Option[PartyReplicationArguments]](None)
    // for extracting the arguments on the first request
    val isFirst = new AtomicBoolean(true)

    new StreamObserver[AddPartyWithAcsAsyncRequest] {

      override def onNext(request: AddPartyWithAcsAsyncRequest): Unit = {
        val processedNext = if (isFirst.getAndSet(false)) {
          for {
            argsP <- ProtoConverter
              .required("arguments", request.arguments)
              .leftMap(err => s"Arguments must be set on the first request: $err")
            args <- verifyArguments(argsP)
          } yield {
            arguments.set(Some(args))
            outputStream.write(request.acsSnapshot.toByteArray)
          }
        } else {
          for {
            _ <- Either.cond(
              request.arguments.isEmpty,
              (),
              s"Arguments must not be set on any request other that the first request: ${request.arguments}",
            )
          } yield {
            outputStream.write(request.acsSnapshot.toByteArray)
          }
        }

        processedNext.valueOr(errorMessage =>
          // On failure: Signal the error, that is throw an exception.
          // Observer's top-level onError will handle cleanup.
          responseObserver.onError(new IllegalArgumentException(errorMessage))
        )
      }

      override def onError(t: Throwable): Unit =
        try {
          outputStream.close()
        } finally {
          responseObserver.onError(t)
        }

      override def onCompleted(): Unit = {
        // Synchronously try to get the snapshot and start the import
        val result = for {
          args <- EitherT.fromEither[Future](
            arguments
              .get()
              .toRight(toStatusRuntimeException(Status.INVALID_ARGUMENT)("Arguments not set"))
          )
          partyReplicator <- EitherT.fromEither[Future](
            ensureOnlinePartyReplicationEnabled()
          )
          acsByteString <- EitherT.fromEither[Future](
            Try(ByteString.copyFrom(outputStream.toByteArray)).toEither.leftMap(t =>
              toStatusRuntimeException(Status.FAILED_PRECONDITION)(t.getMessage)
            )
          )
          activeContracts <- EitherT.fromEither[Future](
            ActiveContract
              .loadAcsSnapshot(acsByteString)
              .leftMap(toStatusRuntimeException(Status.INVALID_ARGUMENT))
          )
          requestId <- partyReplicator
            .addPartyWithAcsAsync(args, activeContracts.iterator)
            .leftMap(toStatusRuntimeException(Status.FAILED_PRECONDITION))
            .onShutdown(Left(GrpcErrors.AbortedDueToShutdown.Error().asGrpcError))
        } yield requestId

        result
          .thereafter(_ => outputStream.close())
          .value
          .onComplete {
            case Failure(exception) => responseObserver.onError(exception)
            case Success(Left(exception)) => responseObserver.onError(exception)
            case Success(Right(requestId)) =>
              responseObserver.onNext(AddPartyWithAcsAsyncResponse(requestId.toHexString))
              responseObserver.onCompleted()
          }
      }
    }
  }

  private def verifyArguments(
      argsP: v30.AddPartyArguments
  ): Either[String, PartyReplicationArguments] =
    for {
      partyId <- convert(argsP.partyId, "party_id", PartyId(_))
      sourceParticipantId <- convert(
        argsP.sourceParticipantUid,
        "source_participant_uid",
        ParticipantId(_),
      )
      synchronizerId <- convert(
        argsP.synchronizerId,
        "synchronizer_id",
        SynchronizerId(_),
      )
      serial <- ProtoConverter
        .parsePositiveInt("topology_serial", argsP.topologySerial)
        .leftMap(_.message)
      participantPermission <- ProtoConverter
        .parseEnum[ParticipantPermission, v30.ParticipantPermission](
          PartyParticipantPermission.fromProtoV30,
          "participant_permission",
          argsP.participantPermission,
        )
        .leftMap(_.message)
    } yield PartyReplicationArguments(
      partyId,
      synchronizerId,
      sourceParticipantId,
      serial,
      participantPermission,
    )

  private def convert[T](
      rawId: String,
      field: String,
      wrap: UniqueIdentifier => T,
  ): Either[String, T] =
    UniqueIdentifier.fromProtoPrimitive(rawId, field).bimap(_.toString, wrap)

  override def getAddPartyStatus(
      request: v30.GetAddPartyStatusRequest
  ): Future[v30.GetAddPartyStatusResponse] =
    (for {
      partyReplicator <- ensureOnlinePartyReplicationEnabled()

      requestId <- Hash
        .fromHexString(request.addPartyRequestId)
        .leftMap(err => toStatusRuntimeException(Status.INVALID_ARGUMENT)(err.message))

      status <- partyReplicator
        .getAddPartyStatus(requestId)
        .toRight(
          toStatusRuntimeException(Status.UNKNOWN)(
            s"Add party request id ${request.addPartyRequestId} not found"
          )
        )
      apiStatus = com.digitalasset.canton.participant.admin.data.PartyReplicationStatus
        .fromInternal(status)
    } yield v30.GetAddPartyStatusResponse(Some(apiStatus.toProtoV30))).toFuture(identity)

  private def toStatusRuntimeException(status: Status)(err: String): StatusRuntimeException =
    status.withDescription(err).asRuntimeException()

  private def ensureOnlinePartyReplicationEnabled() = partyReplicatorO
    .toRight(
      toStatusRuntimeException(Status.UNIMPLEMENTED)(
        "Online party replication commands require the `alpha_online_party_replication_support` configuration"
      )
    )

  override def exportPartyAcs(
      request: v30.ExportPartyAcsRequest,
      responseObserver: StreamObserver[v30.ExportPartyAcsResponse],
  ): Unit = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    GrpcStreamingUtils.streamToClient(
      (out: OutputStream) => processExportPartyAcsRequest(request, new GZIPOutputStream(out)),
      responseObserver,
      byteString => v30.ExportPartyAcsResponse(byteString),
      parameters.processingTimeouts.unbounded.duration,
      chunkSizeO = None,
    )
  }

  private def processExportPartyAcsRequest(
      request: v30.ExportPartyAcsRequest,
      out: OutputStream,
  )(implicit traceContext: TraceContext): Future[Unit] = {

    val res = for {
      ledgerEnd <- EitherT
        .fromEither[FutureUnlessShutdown](ParticipantCommon.findLedgerEnd(sync))
        .leftMap(PartyManagementServiceError.InvalidState.Error(_))
      allLogicalSynchronizerIds = sync.syncPersistentStateManager.getAllLatest.keySet

      validRequest <- validatePartyReplicationCommonRequestParams(
        request.partyId,
        request.synchronizerId,
        request.beginOffsetExclusive,
        request.waitForActivationTimeout,
      )(ledgerEnd, allLogicalSynchronizerIds)

      ValidPartyReplicationCommonRequestParams(
        party,
        synchronizerId,
        beginOffsetExclusive,
        waitForActivationTimeout,
      ) = validRequest

      indexService <- EitherT.fromOption[FutureUnlessShutdown](
        sync.internalIndexService,
        PartyManagementServiceError.InvalidState.Error("Unavailable internal index service"),
      )

      targetParticipant <- EitherT.fromEither[FutureUnlessShutdown](
        UniqueIdentifier
          .fromProtoPrimitive(request.targetParticipantUid, "target_participant_uid")
          .map(ParticipantId(_))
          .leftMap(error => PartyManagementServiceError.InvalidArgument.Error(error.message))
      )

      topologyTx <-
        findSinglePartyActivationTopologyTransaction(
          indexService,
          party,
          beginOffsetExclusive,
          synchronizerId,
          targetParticipant = targetParticipant,
          waitForActivationTimeout,
        )

      (activationOffset, activationTimestamp) = extractOffsetAndTimestamp(topologyTx)

      snapshot <- topologyLookup
        .maybeOfflineAwaitTopologySnapshot(
          synchronizerId,
          activationTimestamp.value.immediateSuccessor,
        )
        .leftMap(err =>
          PartyManagementServiceError.InvalidState
            .Error(
              s"Unable to query topology for $synchronizerId at ${activationTimestamp.value.immediateSuccessor}: $err"
            )
        )

      // TODO(#28208) - Indirection because LAPI topology transaction does not include the onboarding flag
      activeParticipants <- EitherT.right(snapshot.activeParticipantsOf(party.toLf))
      _ <-
        EitherT.cond[FutureUnlessShutdown](
          activeParticipants.exists { case (participantId, participantAttributes) =>
            participantId == targetParticipant &&
            participantAttributes.onboarding
          },
          (),
          PartyManagementServiceError.InvalidState
            .AbortAcsExportForMissingOnboardingFlag(
              party,
              targetParticipant,
            ): PartyManagementServiceError,
        )

      partiesHostedByTargetParticipant <- EitherT.right(
        snapshot.inspectKnownParties(
          filterParty = "",
          filterParticipant = targetParticipant.filterString,
          // we cannot filter by participant in the db, therefore we also cannot impose a limit.
          limit = Int.MaxValue,
        )
      )

      otherPartiesHostedByTargetParticipant =
        partiesHostedByTargetParticipant excl party excl targetParticipant.adminParty

      _ <- ParticipantCommon
        .writeAcsSnapshot(
          indexService,
          Set(party),
          atOffset = activationOffset,
          out,
          excludedStakeholders = otherPartiesHostedByTargetParticipant,
          Some(synchronizerId),
        )(ec, traceContext, actorSystem)
        .leftMap(msg =>
          PartyManagementServiceError.IOStream.Error(msg): PartyManagementServiceError
        )
    } yield ()

    mapErrNewEUS(res.leftMap(_.toCantonRpcError))
  }

  private def validatePartyReplicationCommonRequestParams(
      partyId: String,
      synchronizerId: String,
      beginOffsetExclusive: Long,
      waitForActivationTimeout: Option[Duration],
  )(
      ledgerEnd: Offset,
      synchronizerIds: Set[SynchronizerId],
  )(implicit
      elc: ErrorLoggingContext
  ): EitherT[
    FutureUnlessShutdown,
    PartyManagementServiceError,
    ValidPartyReplicationCommonRequestParams,
  ] = {
    val parsingResult = for {
      party <- UniqueIdentifier
        .fromProtoPrimitive(partyId, "party_id")
        .map(PartyId(_))
      parsedSynchronizerId <- SynchronizerId.fromProtoPrimitive(
        synchronizerId,
        "synchronizer_id",
      )
      synchronizerId <- Either.cond(
        synchronizerIds.contains(parsedSynchronizerId),
        parsedSynchronizerId,
        OtherError(s"Synchronizer ID $parsedSynchronizerId is unknown"),
      )
      parsedBeginOffsetExclusive <- ProtoConverter
        .parseOffset("begin_offset_exclusive", beginOffsetExclusive)
      beginOffsetExclusive <- Either.cond(
        parsedBeginOffsetExclusive <= ledgerEnd,
        parsedBeginOffsetExclusive,
        OtherError(
          s"Begin ledger offset $parsedBeginOffsetExclusive needs to be smaller or equal to the ledger end $ledgerEnd"
        ),
      )
      waitForActivationTimeout <- waitForActivationTimeout.traverse(
        NonNegativeFiniteDuration.fromProtoPrimitive("wait_for_activation_timeout")(_)
      )
    } yield ValidPartyReplicationCommonRequestParams(
      party,
      synchronizerId,
      beginOffsetExclusive,
      waitForActivationTimeout,
    )
    EitherT.fromEither[FutureUnlessShutdown](
      parsingResult.leftMap(error =>
        PartyManagementServiceError.InvalidArgument.Error(error.message)
      )
    )
  }

  // TODO(#24065) - There may be multiple party on- and offboarding transactions which may break this method
  private def findSinglePartyActivationTopologyTransaction(
      indexService: InternalIndexService,
      party: PartyId,
      beginOffsetExclusive: Offset,
      synchronizerId: SynchronizerId,
      targetParticipant: ParticipantId,
      waitForActivationTimeout: Option[NonNegativeFiniteDuration],
  )(implicit
      ec: ExecutionContextExecutor,
      traceContext: TraceContext,
      actorSystem: ActorSystem,
  ): EitherT[FutureUnlessShutdown, PartyManagementServiceError, LapiTopologyTransaction] =
    for {
      topologyTx <- EitherT
        .apply[Future, PartyManagementServiceError, LapiTopologyTransaction](
          indexService
            .topologyTransactions(party.toLf, beginOffsetExclusive)
            .filter(_.synchronizerId == synchronizerId.toProtoPrimitive)
            .filter { topologyTransaction =>
              topologyTransaction.events.exists { event =>
                // Search for onboarding or added event and let caller decide whether we found the right event
                (event.event.isParticipantAuthorizationOnboarding &&
                  event.getParticipantAuthorizationOnboarding.participantId == targetParticipant.uid.toProtoPrimitive)
                || (event.event.isParticipantAuthorizationAdded &&
                  event.getParticipantAuthorizationAdded.participantId == targetParticipant.uid.toProtoPrimitive)
              }
            }
            .take(1)
            .completionTimeout(
              waitForActivationTimeout.getOrElse(NonNegativeFiniteDuration.tryOfMinutes(2)).toScala
            )
            .runWith(Sink.head)
            .transform {
              case Success(tx) => Success(Right(tx))
              case Failure(e) =>
                val message = s"${e.getMessage} – Possibly missing party activation?"
                Success(Left(PartyManagementServiceError.IOStream.Error(message)))
            }
        )
        .mapK(FutureUnlessShutdown.outcomeK)
    } yield topologyTx

  private def extractOffsetAndTimestamp(
      topologyTransaction: LapiTopologyTransaction
  ): (Offset, EffectiveTime) = (for {
    offset <- ProtoConverter.parseOffset("offset", topologyTransaction.offset)
    effectiveTime <- ProtoConverter.parseRequired(
      CantonTimestamp.fromProtoTimestamp,
      "record_time",
      topologyTransaction.recordTime,
    )
  } yield (offset, EffectiveTime(effectiveTime))).valueOr(error =>
    throw new IllegalStateException(s"Unable to parse topology data from LAPI: ${error.message}")
  )

  /** Parse the global parameters that can be set only in the first message of the stream.
    */
  private def parseImportPartyAcsStreamingRequestGlobal(
      request: ImportPartyAcsRequest
  ): ParsingResult[
    (
        SynchronizerId,
        Option[PartyId],
        Option[String],
        ContractImportMode,
        RepresentativePackageIdOverride,
    )
  ] =
    for {
      synchronizerId <- ProtoConverter.parseRequired(
        SynchronizerId.fromProtoPrimitive(_, "synchronizer_id"),
        "synchronizer_id",
        request.synchronizerId,
      )
      partyIdO <- request.partyId.traverse(
        UniqueIdentifier
          .fromProtoPrimitive(_, "party_id")
          .map(PartyId(_))
      )
      representativePackageIdOverride <- request.representativePackageIdOverride
        .traverse(RepresentativePackageIdOverride.fromProtoV30)
        .map(_.getOrElse(RepresentativePackageIdOverride.NoOverride))

      contractImportMode <- ProtoConverter.parseRequired(
        ContractImportMode.fromProtoV30,
        "contract_import_mode",
        request.contractImportMode,
      )
      workflowIdPrefix = request.workflowIdPrefix
        .flatMap(OptionUtil.emptyStringAsNone)
        .orElse(Some(s"import-${UUID.randomUUID}"))
    } yield (
      synchronizerId,
      partyIdO,
      workflowIdPrefix,
      contractImportMode,
      representativePackageIdOverride,
    )

  override def importPartyAcs(
      responseObserver: StreamObserver[ImportPartyAcsResponse]
  ): StreamObserver[ImportPartyAcsRequest] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    type ImportContext =
      (
          SynchronizerId,
          Option[PartyId],
          Option[String],
          ContractImportMode,
          RepresentativePackageIdOverride,
      )

    GrpcStreamingUtils.streamGzippedChunksFromClient[
      ImportPartyAcsRequest,
      ImportPartyAcsResponse,
      ImportContext,
      ActiveContract,
    ](
      responseObserver,
      Success(ImportPartyAcsResponse()),
      getGzippedBytes = _.acsSnapshot,
      parseMessage = ActiveContract.parseDelimitedFromTrusted,
    )(contextFromFirstRequest =
      firstRequest =>
        parseImportPartyAcsStreamingRequestGlobal(firstRequest)
          .leftMap(error =>
            PartyManagementServiceError.InvalidArgument.Error(error.message).asGrpcError
          )
          .toTry
    ) {
      case (
            (
              synchronizerId,
              partyIdO,
              workflowIdPrefix,
              contractImportMode,
              representativePackageIdOverride,
            ),
            source,
          ) =>
        val repairContractSource = source
          .map { case (activeContract) =>
            RepairContract
              .fromLapiActiveContract(activeContract.contract)
              // Use InvalidArgument if the provided contracts are malformed
              .valueOr(err =>
                throw PartyManagementServiceError.InvalidArgument.Error(err).asGrpcError
              )
          }
        val resultET = for {
          // Pre-import Topology Check: Abort import if a party is provided but no PTP mapping is found.
          // This prevents creating a phantom "pending clearance" record for a party that isn't actually
          // onboarding here, which would otherwise remain stranded in the database requiring manual cleanup.
          effectiveTimestampO <- partyIdO match {
            case Some(partyId) =>
              for {
                store <- EitherT.fromEither[FutureUnlessShutdown](
                  topologyLookup.topologyStore(synchronizerId).leftMap { err =>
                    PartyManagementServiceError.InvalidState
                      .Error(s"Topology store not available for $synchronizerId: $err")
                      .asGrpcError
                  }
                )

                // Query for both effective AND proposed transactions
                effectiveTxs <- EitherT.right[StatusRuntimeException](
                  store.findPositiveTransactions(
                    asOf = CantonTimestamp.MaxValue,
                    asOfInclusive = false,
                    isProposal = false,
                    types = Seq(TopologyMapping.Code.PartyToParticipant),
                    filterUid = Some(NonEmpty(Seq, partyId.uid)),
                    filterNamespace = Some(NonEmpty(Seq, partyId.namespace)),
                  )
                )

                proposalTxs <- EitherT.right[StatusRuntimeException](
                  store.findPositiveTransactions(
                    asOf = CantonTimestamp.MaxValue,
                    asOfInclusive = false,
                    isProposal = true,
                    types = Seq(TopologyMapping.Code.PartyToParticipant),
                    filterUid = Some(NonEmpty(Seq, partyId.uid)),
                    filterNamespace = Some(NonEmpty(Seq, partyId.namespace)),
                  )
                )

                allTxs = effectiveTxs.result ++ proposalTxs.result

                onboardingTxs = allTxs.filter { tx =>
                  tx.mapping match {
                    case ptp: PartyToParticipant =>
                      ptp.participants.exists(p => p.participantId == participantId && p.onboarding)
                    case _ => false
                  }
                }

                _ <- EitherT.cond[FutureUnlessShutdown](
                  onboardingTxs.nonEmpty,
                  (),
                  PartyManagementServiceError.InvalidState
                    .Error(
                      s"Refuse to import ACS for party $partyId: No topology transaction found onboarding this party on participant $participantId."
                    )
                    .asGrpcError,
                )

                // If an effective transaction already exists (unexpected, but possible not following OffPR steps), extract its timestamp
                effectiveTimestampO = onboardingTxs
                  .filterNot(_.transaction.isProposal)
                  .map(_.validFrom)
                  .maxOption

              } yield effectiveTimestampO

            case None =>
              EitherT.rightT[FutureUnlessShutdown, StatusRuntimeException](
                Option.empty[EffectiveTime]
              )
          }

          // Import ACS
          _ <- sync.repairService
            .addContractsPekko(
              synchronizerId = synchronizerId,
              contracts = repairContractSource,
              contractImportMode = contractImportMode,
              packageMetadataSnapshot = sync.getPackageMetadataSnapshot,
              representativePackageIdOverride = representativePackageIdOverride,
              workflowIdPrefix = workflowIdPrefix,
            )
            .leftMap(err => PartyManagementServiceError.IOStream.Error(err).asGrpcError)

          // Post-import pending operation insertion which should never fail this endpoint
          _ <- partyIdO match {
            case Some(partyId) =>
              val processPendingOperationET: EitherT[FutureUnlessShutdown, Unit, Unit] = for {
                psid <- EitherT.fromOption[FutureUnlessShutdown](
                  sync.activePsidForLsid(synchronizerId),
                  logger.warn(
                    s"No active physical synchronizer found for $synchronizerId. Skipping pending operation insertion."
                  ),
                )
                persistentState <- EitherT.fromOption[FutureUnlessShutdown](
                  sync.syncPersistentStateManager.get(psid),
                  logger.warn(
                    s"No persistent state found for $psid. Skipping pending operation insertion."
                  ),
                )
                pv = persistentState.staticSynchronizerParameters.protocolVersion

                _ <-
                  if (pv >= ProtocolVersion.v35) {
                    val _ = effectiveTimestampO match {
                      case Some(timestamp) => // Case 2: Unexpected (Operator error)
                        logger.warn(
                          s"Found an already effective party-to-participant mapping with the onboarding flag set " +
                            s"for $partyId on synchronizer $synchronizerId (valid from $timestamp). " +
                            s"The target participant has not been taken offline at the appropriate time, indicating " +
                            s"the offline party replication steps were not strictly followed. " +
                            s"A pending onboarding flag clearance operation will be recorded using $timestamp as reference."
                        )
                      case None => // Case 1: Expected (Proposal)
                        logger.info(
                          s"Found a proposed party-to-participant mapping with the onboarding flag set " +
                            s"for $partyId on synchronizer $synchronizerId. Recording pending onboarding flag clearance operation."
                        )
                    }

                    val pendingOp = OnboardingClearanceOperation(effectiveTimestampO)(
                      OnboardingClearanceOperation.protocolVersionRepresentativeFor(pv)
                    ).toPendingOperation(synchronizerId, partyId)

                    EitherT.right[Unit] {
                      persistentState.pendingOnboardingClearanceStore
                        .insert(pendingOp)
                        .fold(
                          err => {
                            logger.warn(
                              s"Failed to insert pending onboarding operation for party $partyId on synchronizer $synchronizerId: $err. " +
                                s"You may recover by manually invoking the onboarding flag clearance endpoint after (re)connecting to the synchronizer."
                            )
                          },
                          _ => (),
                        )
                    }
                  } else {
                    EitherT.rightT[FutureUnlessShutdown, Unit] {
                      logger.debug(
                        s"Skipping insertion of pending onboarding flag clearance operation for party $partyId " +
                          s"on synchronizer $synchronizerId because protocol version $pv is older than v35."
                      )
                    }
                  }
              } yield ()

              // Map the successful (or safely warned) future back to our gRPC response so the endpoint never fails
              EitherT.right[StatusRuntimeException](processPendingOperationET.value.map(_ => ()))

            case None =>
              // No party ID provided, proceed without pending operation insertion
              logger.warn(
                "Skipping insertion of pending onboarding flag clearance operation as no party ID was provided. " +
                  "Please manually invoke the onboarding flag clearance endpoint after (re)connecting to the synchronizer."
              )
              EitherT.rightT[FutureUnlessShutdown, StatusRuntimeException](())
          }

        } yield ImportPartyAcsResponse()

        EitherTUtil.toFutureUnlessShutdown(resultET)
    }

  }

  override def getHighestOffsetByTimestamp(
      request: v30.GetHighestOffsetByTimestampRequest
  ): Future[v30.GetHighestOffsetByTimestampResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val allSynchronizerIds = sync.syncPersistentStateManager.getAllLatest.keySet

    val res = for {
      synchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
        SynchronizerId
          .fromProtoPrimitive(
            request.synchronizerId,
            "synchronizer_id",
          )
          .leftMap(error => PartyManagementServiceError.InvalidArgument.Error(error.message))
      )
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        Either.cond(
          allSynchronizerIds.contains(synchronizerId),
          (),
          PartyManagementServiceError.InvalidArgument.Error(
            s"Synchronizer id ${synchronizerId.uid} is unknown"
          ),
        )
      )
      timestamp <- EitherT.fromEither[FutureUnlessShutdown](
        ProtoConverter
          .parseRequired(
            CantonTimestamp.fromProtoTimestamp,
            "timestamp",
            request.timestamp,
          )
          .leftMap(error => PartyManagementServiceError.InvalidArgument.Error(error.message))
      )
      forceFlag = request.force

      _ = logger.debug(
        s"Find highest offset for: timestamp=$timestamp, forceFlag=$forceFlag, synchronizerId=$synchronizerId"
      )

      invalidTimestampError = (reason: String) =>
        PartyManagementServiceError.InvalidTimestamp
          .Error(
            synchronizerId,
            timestamp,
            forceFlag,
            reason,
          )

      cleanSynchronizerIndex <- EitherT
        .fromOptionF[FutureUnlessShutdown, PartyManagementServiceError, SynchronizerIndex](
          sync.participantNodePersistentState.value.ledgerApiStore
            .cleanSynchronizerIndex(synchronizerId),
          invalidTimestampError("Cannot use clean synchronizer index because it is empty"),
        )
      _ = logger.debug(s"Retrieved cleanSynchronizerIndex: $cleanSynchronizerIndex")

      // Retrieve the ledger end offset for potential use in force mode. Do so before retrieving the synchronizer
      // offset to prevent a race condition of the ledger end being bumped after the synchronizer offset retrieval.
      ledgerEnd <- EitherT.fromOption[FutureUnlessShutdown](
        sync.participantNodePersistentState.value.ledgerApiStore.ledgerEndCache.apply(),
        invalidTimestampError("Cannot find the ledger end"),
      )
      _ = logger.debug(s"Retrieved ledgerEnd from cache: $ledgerEnd")

      synchronizerOffsetBeforeOrAtRequestedTimestamp <- EitherT
        .fromOptionF[FutureUnlessShutdown, PartyManagementServiceError, SynchronizerOffset](
          sync.participantNodePersistentState.value.ledgerApiStore
            .lastSynchronizerOffsetBeforeOrAtRecordTime(
              synchronizerId,
              timestamp,
            ),
          invalidTimestampError(
            s"The participant does not yet have a ledger offset before or at the requested timestamp: $timestamp"
          ),
        )
      _ = logger.debug(
        s"Retrieved synchronizerOffsetBeforeOrAtRequestedTimestamp: $synchronizerOffsetBeforeOrAtRequestedTimestamp"
      )

      offset <- EitherT.fromEither[FutureUnlessShutdown](
        GrpcPartyManagementService.identifyHighestOffsetByTimestamp(
          requestedTimestamp = timestamp,
          synchronizerOffsetBeforeOrAtRequestedTimestamp =
            synchronizerOffsetBeforeOrAtRequestedTimestamp,
          forceFlag = forceFlag,
          cleanSynchronizerTimestamp = cleanSynchronizerIndex.recordTime,
          ledgerEnd = ledgerEnd,
          synchronizerId = synchronizerId,
        )
      )

      syncOffset <- EitherT
        .fromOptionF[FutureUnlessShutdown, PartyManagementServiceError, SynchronizerOffset](
          sync.participantNodePersistentState.value.ledgerApiStore.synchronizerOffset(offset),
          PartyManagementServiceError.InvalidState.MissingSynchronizerOffset(offset, timestamp),
        )

      foundRecordTime <- EitherT
        .fromEither[FutureUnlessShutdown](
          CantonTimestamp.fromInstant(syncOffset.recordTime.toInstant)
        )
        .leftMap(PartyManagementServiceError.InvalidState.Error(_): PartyManagementServiceError)

      _ <- EitherT.cond[FutureUnlessShutdown](
        forceFlag || foundRecordTime == timestamp,
        (),
        PartyManagementServiceError.InvalidState.SynchronizerOffsetRecordTimeInvariantViolation(
          offset,
          syncOffset.offset,
          timestamp,
          foundRecordTime,
        ): PartyManagementServiceError,
      )

      _ = logger.debug(s"Found highest offset ${offset.unwrap}")

    } yield v30.GetHighestOffsetByTimestampResponse(offset.unwrap)
    mapErrNewEUS(res.leftMap(_.toCantonRpcError))
  }

  override def clearPartyOnboardingFlag(
      request: v30.ClearPartyOnboardingFlagRequest
  ): Future[v30.ClearPartyOnboardingFlagResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = processClearPartyOnboardingFlagRequest(request).map { status =>
      val (onboarded, timestamp) = PartyOnboardingFlagStatus.toProtoV30(status)
      v30.ClearPartyOnboardingFlagResponse(onboarded, timestamp)
    }
    mapErrNewEUS(res.leftMap(_.toCantonRpcError))
  }

  private def processClearPartyOnboardingFlagRequest(
      request: v30.ClearPartyOnboardingFlagRequest
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    PartyManagementServiceError,
    PartyOnboardingFlagStatus,
  ] =
    for {
      ledgerEnd <- EitherT
        .fromEither[FutureUnlessShutdown](ParticipantCommon.findLedgerEnd(sync))
        .leftMap(PartyManagementServiceError.InvalidState.Error(_))
      allLogicalSynchronizerIds = sync.syncPersistentStateManager.getAllLatest.keySet

      validRequest <- validateClearPartyOnboardingFlagRequest(
        request,
        ledgerEnd,
        allLogicalSynchronizerIds,
      )
      ValidPartyReplicationCommonRequestParams(
        party,
        synchronizerId,
        beginOffsetExclusive,
        waitForActivationTimeout,
      ) = validRequest

      connectedSynchronizer <- EitherT.fromOption[FutureUnlessShutdown](
        sync.readyConnectedSynchronizerById(synchronizerId),
        PartyManagementServiceError.InvalidState.Error(
          s"A connection to synchronizer $synchronizerId is required to perform this operation."
        ),
      )

      indexService <- EitherT.fromOption[FutureUnlessShutdown](
        sync.internalIndexService,
        PartyManagementServiceError.InvalidState.Error("Unavailable internal index service"),
      )

      topologyTx <-
        findSinglePartyActivationTopologyTransaction(
          indexService,
          party,
          beginOffsetExclusive,
          synchronizerId,
          targetParticipant = participantId,
          waitForActivationTimeout,
        )

      (_activationOffset, activationTimestamp) = extractOffsetAndTimestamp(topologyTx)

      snapshot <- EitherT.right(
        connectedSynchronizer.topologyClient.awaitSnapshot(
          activationTimestamp.immediateSuccessor.value
        )
      )

      // TODO(#28208) - Indirection because LAPI topology transaction does not include the onboarding flag
      activeParticipants <- EitherT.right(
        snapshot.activeParticipantsOf(party.toLf)
      )
      _ <- EitherT.cond[FutureUnlessShutdown](
        activeParticipants.exists { case (pId, participantAttributes) =>
          pId == participantId &&
          participantAttributes.onboarding
        },
        (),
        PartyManagementServiceError.InvalidState.MissingOnboardingFlagCannotCompleteOnboarding(
          party,
          participantId,
        ): PartyManagementServiceError,
      )

      psid <- EitherT.fromOption[FutureUnlessShutdown](
        sync.activePsidForLsid(synchronizerId),
        PartyManagementServiceError.InvalidState.Error(
          s"No active physical synchronizer found for $synchronizerId"
        ),
      )

      persistentState <- EitherT.fromOption[FutureUnlessShutdown](
        sync.syncPersistentStateManager.get(psid),
        PartyManagementServiceError.InvalidState.Error(
          s"No persistent state found for $psid"
        ),
      )

      protocolVersion = persistentState.staticSynchronizerParameters.protocolVersion

      _ = persistentState.pendingOnboardingClearanceStore.insert(
        OnboardingClearanceOperation(Some(activationTimestamp))(
          OnboardingClearanceOperation.protocolVersionRepresentativeFor(protocolVersion)
        ).toPendingOperation(synchronizerId, party)
      )

      onboardingFlagClearanceOutcome <-
        connectedSynchronizer.ephemeral.onboardingClearanceScheduler
          .requestClearance(
            party,
            activationTimestamp,
          )
          .leftMap(PartyManagementServiceError.InvalidState.Error(_): PartyManagementServiceError)

    } yield onboardingFlagClearanceOutcome

  private def validateClearPartyOnboardingFlagRequest(
      request: v30.ClearPartyOnboardingFlagRequest,
      ledgerEnd: Offset,
      synchronizerIds: Set[SynchronizerId],
  )(implicit
      elc: ErrorLoggingContext
  ): EitherT[
    FutureUnlessShutdown,
    PartyManagementServiceError,
    ValidPartyReplicationCommonRequestParams,
  ] =
    validatePartyReplicationCommonRequestParams(
      request.partyId,
      request.synchronizerId,
      request.beginOffsetExclusive,
      request.waitForActivationTimeout,
    )(ledgerEnd, synchronizerIds)

}

object GrpcPartyManagementService {

  /** OffPR getHighestOffsetByTimestamp computation of offset from timestamp placed in a pure
    * function for unit testing.
    */
  def identifyHighestOffsetByTimestamp(
      requestedTimestamp: CantonTimestamp,
      synchronizerOffsetBeforeOrAtRequestedTimestamp: SynchronizerOffset,
      forceFlag: Boolean,
      cleanSynchronizerTimestamp: CantonTimestamp,
      ledgerEnd: LedgerEnd,
      synchronizerId: SynchronizerId,
  )(implicit
      elc: ErrorLoggingContext
  ): Either[PartyManagementServiceError, Offset] = {
    val synchronizerTimestampBeforeOrAtRequestedTimestamp =
      CantonTimestamp(synchronizerOffsetBeforeOrAtRequestedTimestamp.recordTime)
    for {
      _ <- Either.cond(
        synchronizerTimestampBeforeOrAtRequestedTimestamp <= requestedTimestamp,
        (),
        PartyManagementServiceError.InvalidTimestamp.Error(
          synchronizerId,
          requestedTimestamp,
          forceFlag,
          s"Coding bug: Returned offset record time $synchronizerTimestampBeforeOrAtRequestedTimestamp must be before or at the requested timestamp $requestedTimestamp.",
        ),
      )
      _ <- Either.cond(
        forceFlag || requestedTimestamp <= cleanSynchronizerTimestamp,
        (),
        PartyManagementServiceError.InvalidTimestamp.Error(
          synchronizerId,
          requestedTimestamp,
          forceFlag,
          s"Not all events have been processed fully and/or published to the Ledger API DB until the requested timestamp: $requestedTimestamp",
        ),
      )
      offsetBeforeOrAtRequestedTimestamp <-
        // Use the ledger end offset only if the requested timestamp is at least
        // the clean synchronizer timestamp which caps the ledger end offset.
        if (forceFlag && requestedTimestamp >= cleanSynchronizerTimestamp)
          ledgerEnd.lastOffset.asRight[PartyManagementServiceError]
        else {
          // Sanity check that the synchronizer offset is less than or equal to the ledger end offset.
          Either.cond(
            synchronizerOffsetBeforeOrAtRequestedTimestamp.offset <= ledgerEnd.lastOffset,
            synchronizerOffsetBeforeOrAtRequestedTimestamp.offset,
            PartyManagementServiceError.InvalidTimestamp.Error(
              synchronizerId,
              requestedTimestamp,
              forceFlag,
              s"The synchronizer offset ${synchronizerOffsetBeforeOrAtRequestedTimestamp.offset} is not less than or equal to the ledger end offset ${ledgerEnd.lastOffset}",
            ),
          )
        }
    } yield offsetBeforeOrAtRequestedTimestamp
  }
}

private final case class ValidPartyReplicationCommonRequestParams(
    party: PartyId,
    synchronizerId: SynchronizerId,
    beginOffsetExclusive: Offset,
    waitForActivationTimeout: Option[NonNegativeFiniteDuration],
)
