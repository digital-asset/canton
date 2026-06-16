// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import cats.implicits.toTraverseOps
import cats.syntax.either.*
import com.daml.ledger.api.v2.admin.party_management_alpha_service.{
  AddPartyArguments,
  AddPartyWithAcsRequest,
  AddPartyWithAcsResponse,
  ExportPartyAcsRequest,
  ExportPartyAcsResponse,
  GetAddPartyStatusRequest,
  GetAddPartyStatusResponse,
}
import com.daml.ledger.api.v2.state_service.ParticipantPermission
import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction as LapiTopologyTransaction
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.InternalIndexService
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.participant.admin.grpc.ParticipantCommon
import com.digitalasset.canton.participant.admin.party.PartyReplicationAdminWorkflow.PartyReplicationArguments
import com.digitalasset.canton.participant.admin.party.PartyReplicationEndpoints.{
  ValidPartyReplicationCommonRequestParams,
  extractOffsetAndTimestamp,
  findSinglePartyActivationTopologyTransaction,
  validatePartyReplicationCommonRequestParams,
}
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.topology.TopologyLookup
import com.digitalasset.canton.platform.apiserver.services.admin.PartyReplicationEndpoints
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.transaction.ParticipantPermission as TopologyParticipantPermission
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherUtil.RichEither
import com.digitalasset.canton.util.{EitherTUtil, GrpcStreamingUtils}
import com.google.protobuf.duration.Duration
import io.grpc.stub.StreamObserver
import io.grpc.{Status, StatusRuntimeException}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Sink

import java.io.OutputStream
import java.util.zip.GZIPOutputStream
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

object PartyReplicationEndpoints {
  def apply(
      partyReplicator: PartyReplicator,
      sync: CantonSyncService,
      topologyLookup: TopologyLookup,
      timeouts: ProcessingTimeout,
  )(implicit
      loggingContext: ErrorLoggingContext,
      ec: ExecutionContextExecutor,
      actorSystem: ActorSystem,
  ): PartyReplicationEndpoints =
    new PartyReplicationEndpointsImpl(partyReplicator, sync, topologyLookup, timeouts)

  private[admin] final case class ValidPartyReplicationCommonRequestParams(
      party: PartyId,
      synchronizerId: SynchronizerId,
      beginOffsetExclusive: Offset,
      waitForActivationTimeout: Option[NonNegativeFiniteDuration],
  )

  private[admin] def validatePartyReplicationCommonRequestParams(
      partyId: String,
      synchronizerId: String,
      beginOffsetExclusive: Long,
      waitForActivationTimeout: Option[Duration],
  )(
      ledgerEnd: Offset,
      synchronizerIds: Set[SynchronizerId],
  ): ParsingResult[ValidPartyReplicationCommonRequestParams] =
    for {
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

  private[admin] def findSinglePartyActivationTopologyTransaction(
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
      loggingContext: ErrorLoggingContext,
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

  private[admin] def extractOffsetAndTimestamp(
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
}

class PartyReplicationEndpointsImpl(
    partyReplicator: PartyReplicator,
    sync: CantonSyncService,
    topologyLookup: TopologyLookup,
    timeouts: ProcessingTimeout,
)(implicit
    loggingContext: ErrorLoggingContext,
    ec: ExecutionContextExecutor,
    actorSystem: ActorSystem,
) extends PartyReplicationEndpoints {

  override def exportPartyAcs(
      request: ExportPartyAcsRequest,
      responseObserver: StreamObserver[ExportPartyAcsResponse],
  ): Unit = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    GrpcStreamingUtils.streamToClient(
      (out: OutputStream) => processExportPartyAcsRequest(request, new GZIPOutputStream(out)),
      responseObserver,
      byteString => ExportPartyAcsResponse(byteString),
      timeouts.unbounded.duration,
      chunkSizeO = None,
    )
  }

  private def processExportPartyAcsRequest(
      request: ExportPartyAcsRequest,
      out: OutputStream,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val res = for {
      ledgerEnd <- EitherT
        .fromEither[FutureUnlessShutdown](ParticipantCommon.findLedgerEnd(sync))
        .leftMap(PartyManagementServiceError.InvalidState.Error(_))
      allLogicalSynchronizerIds = sync.syncPersistentStateManager.getAllLatest.keySet

      validRequest <- EitherT.fromEither[FutureUnlessShutdown](
        validatePartyReplicationCommonRequestParams(
          request.partyId,
          request.synchronizerId,
          request.beginOffsetExclusive,
          request.waitForActivationTimeout,
        )(ledgerEnd, allLogicalSynchronizerIds)
          .leftMap(error => PartyManagementServiceError.InvalidArgument.Error(error.message))
      )

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

      activeParticipants <- EitherT.right(snapshot.activeParticipantsOf(party.toLf))
      _ <-
        EitherT.cond[FutureUnlessShutdown](
          activeParticipants.exists { case (participantId, participantAttributes) =>
            participantId == targetParticipant &&
            participantAttributes.onboarding
          },
          (),
          PartyManagementServiceError.AcsExportMissingTargetOnboardingMapping.Error(
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

      // Set removal (excl) is O(1); filterNot is O(N)
      otherPartiesHostedByTargetParticipant = partiesHostedByTargetParticipant
        .excl(party)
        .excl(targetParticipant.adminParty)

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

    CantonGrpcUtil.mapErrNewEUS(res.leftMap(_.toCantonRpcError))
  }

  override def addPartyWithAcs(
      responseObserver: StreamObserver[AddPartyWithAcsResponse]
  ): StreamObserver[AddPartyWithAcsRequest] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    GrpcStreamingUtils.streamGzippedChunksFromClient[
      AddPartyWithAcsRequest,
      AddPartyWithAcsResponse,
      PartyReplicationArguments,
      ActiveContract,
    ](
      responseObserver,
      Failure(
        new IllegalArgumentException(
          "The request stream must contain at least one message with the required AddPartyArguments."
        )
      ),
      getGzippedBytes = _.acsSnapshot,
      parseMessage = ActiveContract.parseDelimitedFromTrusted,
    )(contextFromFirstRequest =
      firstRequest =>
        (for {
          argsP <- ProtoConverter
            .required("arguments", firstRequest.arguments)
            .leftMap(err => s"Arguments must be set on the first request: $err")
          args <- verifyArguments(argsP)
        } yield args)
          .leftMap(err => new IllegalArgumentException(err))
          .toTry
    ) { case (args, source) =>
      val resultET = for {
        requestId <- partyReplicator
          .addPartyWithAcsAsync(args, source)
          .leftMap(toStatusRuntimeException(Status.FAILED_PRECONDITION))
      } yield AddPartyWithAcsResponse(requestId.toHexString)

      EitherTUtil.toFutureUnlessShutdown(resultET)
    }
  }

  private def verifyArguments(
      argsP: AddPartyArguments
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
      participantPermission <- argsP.participantPermission match {
        case ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION =>
          Right(TopologyParticipantPermission.Submission)
        case ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION =>
          Right(TopologyParticipantPermission.Observation)
        case ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION =>
          Right(TopologyParticipantPermission.Confirmation)
        case invalidPermission => Left(s"Invalid permission $invalidPermission")
      }
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
      request: GetAddPartyStatusRequest
  ): Future[GetAddPartyStatusResponse] = (for {
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
  } yield GetAddPartyStatusResponse(Some(apiStatus.toLapiProto))).toFuture(identity)

  private def toStatusRuntimeException(status: Status)(err: String): StatusRuntimeException =
    status.withDescription(err).asRuntimeException()
}
