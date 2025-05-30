// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcFUSExtended
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencer.api.v30 as proto
import com.digitalasset.canton.sequencer.api.v30.SequencerConnect
import com.digitalasset.canton.sequencer.api.v30.SequencerConnect.GetSynchronizerParametersResponse.Parameters
import com.digitalasset.canton.sequencer.api.v30.SequencerConnect.{
  GetSynchronizerIdRequest,
  GetSynchronizerIdResponse,
  GetSynchronizerParametersRequest,
  GetSynchronizerParametersResponse,
  HandshakeRequest,
  HandshakeResponse,
  RegisterOnboardingTopologyTransactionsResponse,
  VerifyActiveRequest,
  VerifyActiveResponse,
}
import com.digitalasset.canton.synchronizer.sequencing.authentication.grpc.IdentityContextHelper
import com.digitalasset.canton.synchronizer.service.HandshakeValidator
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{SignedTopologyTransaction, TopologyMapping}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import io.grpc.{Status, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}

/** Sequencer connect service on gRPC
  */
class GrpcSequencerConnectService(
    synchronizerId: PhysicalSynchronizerId,
    sequencerId: SequencerId,
    staticSynchronizerParameters: StaticSynchronizerParameters,
    synchronizerTopologyManager: SynchronizerTopologyManager,
    cryptoApi: SynchronizerCryptoClient,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends proto.SequencerConnectServiceGrpc.SequencerConnectService
    with NamedLogging {

  protected val serverProtocolVersion: ProtocolVersion =
    staticSynchronizerParameters.protocolVersion

  override def getSynchronizerId(
      request: GetSynchronizerIdRequest
  ): Future[GetSynchronizerIdResponse] =
    Future.successful(
      GetSynchronizerIdResponse(
        physicalSynchronizerId = synchronizerId.toProtoPrimitive,
        sequencerUid = sequencerId.uid.toProtoPrimitive,
      )
    )

  override def getSynchronizerParameters(
      request: GetSynchronizerParametersRequest
  ): Future[GetSynchronizerParametersResponse] = {
    val response = staticSynchronizerParameters.protoVersion.v match {
      case 30 => Future.successful(Parameters.ParametersV1(staticSynchronizerParameters.toProtoV30))
      case unsupported =>
        Future.failed(
          new IllegalStateException(
            s"Unsupported Proto version $unsupported for static synchronizer parameters"
          )
        )
    }

    response.map(GetSynchronizerParametersResponse(_))
  }

  override def verifyActive(request: VerifyActiveRequest): Future[VerifyActiveResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val resultF = for {
      participant <- EitherT.fromEither[FutureUnlessShutdown](getParticipantFromGrpcContext())
      isActive <- EitherT(
        cryptoApi.ips.currentSnapshotApproximation
          .isParticipantActive(participant)
          .map(_.asRight[String])
      )
    } yield VerifyActiveResponse.Success(isActive)

    resultF
      .fold[VerifyActiveResponse.Value](
        reason => VerifyActiveResponse.Value.Failure(VerifyActiveResponse.Failure(reason)),
        success =>
          VerifyActiveResponse.Value.Success(VerifyActiveResponse.Success(success.isActive)),
      )
      .map(VerifyActiveResponse(_))
      .asGrpcResponse
  }

  override def handshake(request: HandshakeRequest): Future[HandshakeResponse] =
    Future.successful {
      val response = HandshakeValidator
        .clientIsCompatible(
          serverProtocolVersion,
          request.clientProtocolVersions,
          request.minimumProtocolVersion,
        )
        .fold[HandshakeResponse.Value](
          failure =>
            HandshakeResponse.Value
              .Failure(SequencerConnect.HandshakeResponse.Failure(failure)),
          _ =>
            HandshakeResponse.Value
              .Success(SequencerConnect.HandshakeResponse.Success()),
        )
      HandshakeResponse(serverProtocolVersion.toProtoPrimitive, response)
    }

  override def registerOnboardingTopologyTransactions(
      request: SequencerConnect.RegisterOnboardingTopologyTransactionsRequest
  ): Future[SequencerConnect.RegisterOnboardingTopologyTransactionsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val resultET = for {
      member <- EitherT.fromEither[Future](
        IdentityContextHelper.getCurrentStoredMember.toRight(
          invalidRequest("Unable to find member id in gRPC context")
        )
      )
      isKnown <- CantonGrpcUtil.mapErrNewETUS(
        EitherT.right(
          cryptoApi.ips.headSnapshot.isMemberKnown(member)
        )
      )
      _ <- EitherTUtil.condUnitET[Future](
        !isKnown,
        failedPrecondition(s"Member $member is already known on the synchronizer"),
      )
      // check that the onboarding member is not attempting to re-onboard
      // TODO(#14045) Topology Pruning: Make sure that we retain evidence that a member was offboarded
      firstKnownAtO <- CantonGrpcUtil.mapErrNewETUS(
        EitherT.right(cryptoApi.ips.headSnapshot.memberFirstKnownAt(member))
      )
      _ <- EitherTUtil.condUnitET[Future](
        firstKnownAtO.isEmpty,
        failedPrecondition(
          s"Member $member has previously been off-boarded and cannot onboard again."
        ),
      )
      transactions <- CantonGrpcUtil.mapErrNew(
        request.topologyTransactions
          .traverse(
            SignedTopologyTransaction
              .fromProtoV30(ProtocolVersionValidation(serverProtocolVersion), _)
          )
          .leftMap(ProtoDeserializationFailure.Wrap(_))
      )

      _ <- checkForOnlyOnboardingTransactions(member, transactions)
      _ <- CantonGrpcUtil.mapErrNewETUS(
        synchronizerTopologyManager
          .add(transactions, ForceFlags.all, expectFullAuthorization = false)
      )
    } yield RegisterOnboardingTopologyTransactionsResponse.defaultInstance

    EitherTUtil
      .toFuture(resultET)
  }

  private def checkForOnlyOnboardingTransactions(
      member: Member,
      transactions: Seq[GenericSignedTopologyTransaction],
  ): EitherT[Future, StatusRuntimeException, Unit] = {
    val unexpectedUids = transactions.filter(_.mapping.maybeUid.exists(_ != member.uid))
    val unexpectedNamespaces = transactions.filter(_.mapping.namespace != member.namespace)

    val expectedMappings =
      if (member.code == ParticipantId.Code) TopologyStore.initialParticipantDispatchingSet
      else Set(TopologyMapping.Code.NamespaceDelegation, TopologyMapping.Code.OwnerToKeyMapping)
    val submittedMappings = transactions.map(_.mapping.code).toSet
    val unexpectedMappings = submittedMappings -- expectedMappings
    val missingMappings = expectedMappings -- submittedMappings

    val unexpectedProposals = transactions.filter(_.isProposal)

    val resultET = for {
      _ <- EitherTUtil.condUnitET[Future](
        unexpectedMappings.isEmpty,
        s"Unexpected topology mappings for onboarding $member: $unexpectedMappings",
      )
      _ <- EitherTUtil.condUnitET[Future](
        unexpectedUids.isEmpty,
        s"Mappings for unexpected UIDs for onboarding $member: $unexpectedUids",
      )
      _ <- EitherTUtil.condUnitET[Future](
        unexpectedNamespaces.isEmpty,
        s"Mappings for unexpected namespaces for onboarding $member: $unexpectedNamespaces",
      )
      _ <- EitherTUtil.condUnitET[Future](
        missingMappings.isEmpty,
        s"Missing mappings for onboarding $member: $missingMappings",
      )
      _ <- EitherTUtil.condUnitET[Future](
        unexpectedProposals.isEmpty,
        s"Unexpected proposals for onboarding $member: $missingMappings",
      )
    } yield ()

    resultET.leftMap(invalidRequest)
  }

  private def invalidRequest(message: String): StatusRuntimeException =
    Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException()

  private def failedPrecondition(message: String): StatusRuntimeException =
    Status.FAILED_PRECONDITION.withDescription(message).asRuntimeException()

  /*
   Note: we only get the participantId from the context; we have no idea
   whether the member is authenticated or not.
   */
  private def getParticipantFromGrpcContext(): Either[String, ParticipantId] =
    IdentityContextHelper.getCurrentStoredMember
      .toRight("Unable to find participant id in gRPC context")
      .flatMap {
        case participantId: ParticipantId => Right(participantId)
        case member => Left(s"Expecting participantId ; found $member")
      }
}
