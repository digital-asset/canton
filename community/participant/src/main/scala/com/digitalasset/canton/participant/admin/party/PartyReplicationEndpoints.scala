// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.ledger.api.v2.admin.party_management_alpha_service.{
  AuthorizePartyUpdateRequest,
  GeneratePartyTopologyUpdateRequest,
  GeneratePartyTopologyUpdateResponse,
  GetAddPartyStatusRequest,
  GetAddPartyStatusResponse,
}
import com.daml.ledger.api.v2.state_service.ParticipantPermission
import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction as LapiTopologyTransaction
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.api.validation.CryptoValidator
import com.digitalasset.canton.ledger.participant.state.InternalIndexService
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.platform.apiserver.services.admin.PartyReplicationEndpoints
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.transaction.{
  ParticipantPermission as TopologyParticipantPermission,
  PartyToParticipant,
  SingleTransactionSignature,
  TopologyChangeOp,
  TopologyTransaction,
}
import com.digitalasset.canton.topology.{
  ParticipantId,
  Party,
  PartyId,
  SynchronizerId,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherUtil.RichEither
import com.google.protobuf.duration.Duration
import io.grpc.{Status, StatusRuntimeException}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Sink

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

object PartyReplicationEndpoints {
  def apply(
      partyReplicator: PartyReplicator,
      sync: CantonSyncService,
  )(implicit
      loggingContext: ErrorLoggingContext,
      ec: ExecutionContextExecutor,
  ): PartyReplicationEndpoints =
    new PartyReplicationEndpointsImpl(partyReplicator, sync)

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
)(implicit
    loggingContext: ErrorLoggingContext,
    ec: ExecutionContextExecutor,
) extends PartyReplicationEndpoints {

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

  override def generatePartyTopologyUpdate(
      request: GeneratePartyTopologyUpdateRequest
  ): Future[GeneratePartyTopologyUpdateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val resultET = for {
      partyId <- EitherT.fromEither[FutureUnlessShutdown](
        convert(request.partyId, "party_id", PartyId(_))
          .leftMap(toStatusRuntimeException(Status.INVALID_ARGUMENT))
      )
      synchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
        convert(request.synchronizerId, "synchronizer_id", SynchronizerId(_))
          .leftMap(toStatusRuntimeException(Status.INVALID_ARGUMENT))
      )
      targetParticipantId <- EitherT.fromEither[FutureUnlessShutdown](
        convert(request.targetParticipantUid, "target_participant_uid", ParticipantId(_))
          .leftMap(toStatusRuntimeException(Status.INVALID_ARGUMENT))
      )
      permission <- EitherT
        .fromEither[FutureUnlessShutdown](
          request.participantPermission match {
            case ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION =>
              Right(TopologyParticipantPermission.Submission)
            case ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION =>
              Right(TopologyParticipantPermission.Observation)
            case ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION =>
              Right(TopologyParticipantPermission.Confirmation)
            case invalidPermission =>
              Left(
                toStatusRuntimeException(Status.INVALID_ARGUMENT)(
                  s"Invalid permission $invalidPermission"
                )
              )
          }
        )

      response <- partyReplicator
        .generatePartyTopologyUpdate(partyId, synchronizerId, targetParticipantId, permission)
        .leftMap(toStatusRuntimeException(Status.INVALID_ARGUMENT)(_))

    } yield GeneratePartyTopologyUpdateResponse(
      transaction = response.toByteStringChecked,
      hash = response.hash.hash.unwrap,
    )

    resultET.value.unwrap.flatMap {
      case AbortedDueToShutdown =>
        Future.failed(toStatusRuntimeException(Status.UNAVAILABLE)("Shutdown in progress"))
      case Outcome(Left(err)) =>
        Future.failed(err)
      case Outcome(Right(res)) =>
        Future.successful(res)
    }
  }

  override def authorizePartyUpdate(
      request: AuthorizePartyUpdateRequest
  ): Future[(Party, Boolean)] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val resultET = for {

      synchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
        UniqueIdentifier
          .fromProtoPrimitive(request.synchronizerId, "synchronizer_id")
          .map(SynchronizerId(_))
          .leftMap(error => toStatusRuntimeException(Status.INVALID_ARGUMENT)(error.toString))
      )

      txBytes <- EitherT.cond[FutureUnlessShutdown](
        !request.transaction.isEmpty,
        request.transaction,
        toStatusRuntimeException(Status.INVALID_ARGUMENT)(
          "transaction cannot be empty"
        ),
      )

      // TODO(#33640) – Use configurable size to limit the unbounded signature collection (once it is available)
      _ <- EitherT.cond[FutureUnlessShutdown](
        request.signatures.sizeIs <= 10,
        (),
        toStatusRuntimeException(Status.INVALID_ARGUMENT)(
          s"Too many signatures provided. Maximum allowed is 10, but got ${request.signatures.size}."
        ),
      )

      parsedSignatures <- EitherT.fromEither[FutureUnlessShutdown](
        request.signatures.toList.traverse(
          CryptoValidator.validateSignature(_, "signatures")
        )
      )

      // Look up the expected protocol version for fail-fast validation
      connectedSynchronizer <- EitherT.fromOption[FutureUnlessShutdown](
        sync.readyConnectedSynchronizerById(synchronizerId),
        toStatusRuntimeException(Status.FAILED_PRECONDITION)(
          s"Not connected to synchronizer $synchronizerId"
        ),
      )
      protocolVersion = connectedSynchronizer.staticSynchronizerParameters.protocolVersion

      genericTx <- EitherT.fromEither[FutureUnlessShutdown](
        TopologyTransaction
          .fromByteString(
            expectedProtocolVersion = protocolVersion,
            txBytes,
          )
          .leftMap(err =>
            toStatusRuntimeException(Status.INVALID_ARGUMENT)(
              s"Failed to parse topology transaction: $err"
            )
          )
      )

      ptpTx <- EitherT.fromEither[FutureUnlessShutdown](
        genericTx
          .select[TopologyChangeOp.Replace, PartyToParticipant]
          .toRight(
            toStatusRuntimeException(Status.INVALID_ARGUMENT)(
              "Transaction must be a Replace PartyToParticipant mapping"
            )
          )
      )

      // Convert raw crypto signatures into SingleTransactionSignatures covering this transaction's hash
      topologySignatures = parsedSignatures.map(sig => SingleTransactionSignature(ptpTx.hash, sig))

      _ <- partyReplicator
        .authorizePartyUpdate(
          synchronizerId,
          ptpTx,
          topologySignatures,
        )
        .leftMap(toStatusRuntimeException(Status.INVALID_ARGUMENT)(_))

      // Determine if the local participant is one of the target participants
      // This flag signals to the caller (API service) whether it should perform local IAM provisioning
      isTargetParticipant = ptpTx.mapping.participants
        .exists(p => p.onboarding && p.participantId == sync.participantId)

    } yield (ptpTx.mapping.partyId, isTargetParticipant)

    resultET.value.unwrap.flatMap {
      case AbortedDueToShutdown =>
        Future.failed(toStatusRuntimeException(Status.UNAVAILABLE)("Shutdown in progress"))
      case Outcome(Left(err)) =>
        Future.failed(err)
      case Outcome(Right(res)) =>
        Future.successful(res)
    }
  }

}
