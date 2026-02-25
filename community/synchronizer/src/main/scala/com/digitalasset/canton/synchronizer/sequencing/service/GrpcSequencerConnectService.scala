// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
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
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Remove
import com.digitalasset.canton.topology.transaction.{
  OwnerToKeyMapping,
  SignedTopologyTransaction,
  TopologyMapping,
}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import io.grpc.{Status, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}

/** Sequencer connect service on gRPC
  */
class GrpcSequencerConnectService(
    psid: PhysicalSynchronizerId,
    sequencerId: SequencerId,
    staticSynchronizerParameters: StaticSynchronizerParameters,
    synchronizerTopologyManager: SynchronizerTopologyManager,
    cryptoApi: SynchronizerCryptoClient,
    clock: Clock,
    sequencingTimeLowerBoundExclusive: Option[CantonTimestamp],
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
        physicalSynchronizerId = psid.toProtoPrimitive,
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
      // for status reads use the currentSnapshotApproximation
      topologySnapshot <- EitherT.liftF(
        cryptoApi.ips.currentSnapshotApproximation
      )
      isActive <- EitherT(
        topologySnapshot
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

      // check that the header has a member ID
      member <- EitherT.fromEither[Future](
        IdentityContextHelper.getCurrentStoredMember.toRight(
          invalidRequest("Unable to find member id in gRPC context")
        )
      )

      // refuse requests from non-participants
      participantId <- member match {
        case p: ParticipantId => EitherT.rightT[Future, StatusRuntimeException](p)
        case other =>
          EitherT.leftT[Future, ParticipantId](
            failedPrecondition(s"This endpoint is only for participants. Refused: ${other.code}")
          )
      }

      now = clock.now

      // Time check
      /*
      This checks allows for a better UX. Without it, the sequencer will fail to dispatch the onboarding
      topology transactions, which causes
      - unnecessary warnings in the logs
      - unnecessary delay for the participant to figure out that onboarding has failed
       */
      _ <- sequencingTimeLowerBoundExclusive match {
        case Some(boundExclusive) if now <= boundExclusive =>
          EitherT.leftT[Future, Unit](
            failedPrecondition(
              s"Onboarding is possible only from $boundExclusive but current time is $now"
            )
          )

        case _ => EitherTUtil.unit[StatusRuntimeException]
      }

      // grab the head snapshot to use in all subsequent checks
      // use head snapshot here to make sure we see the results of all sequenced transactions
      topologySnapshot = cryptoApi.ips.headSnapshot

      // get the synchronizer parameters from the snapshot
      params <- EitherT(
        topologySnapshot.findDynamicSynchronizerParameters().asGrpcFuture
      ).leftMap(err => failedPrecondition(s"Could not fetch synchronizer parameters: $err"))

      // Reject request if the synchronizer is locked
      _ <- EitherTUtil.condUnitET[Future](
        !params.parameters.onboardingRestriction.isLocked,
        failedPrecondition("Synchronizer is locked for onboarding."),
      )

      transactions <- CantonGrpcUtil.mapErrNew(
        request.topologyTransactions
          .traverse(
            SignedTopologyTransaction
              .fromProtoV30(ProtocolVersionValidation(serverProtocolVersion), _)
          )
          .leftMap(ProtoDeserializationFailure.Wrap(_))
          .map(_.distinctBy(_.mapping.uniqueKey))
      )

      // Perform validations on the transactions
      // Pass a limit of 7 for total number of transactions
      // We enforce exactly 1 OTK and STC. An allowance of 5 NSDs
      // should more than suffice during onboarding. Assuming even one NSD for each of
      // the other mappings (STC, OTK), one to manage them, plus one root mapping
      // requires 4 NSDs in total. More mappings can be added after onboarding
      _ <- validateOnboardingTransactions(participantId, transactions, PositiveInt.tryCreate(7))

      // query whether the participant has ever onboarded before (regardless of whether it is presently active)
      wasEverOnboarded <- EitherT.liftF(
        topologySnapshot.wasEverOnboarded(participantId).asGrpcFuture
      )

      _ <- EitherTUtil.condUnitET[Future](
        !wasEverOnboarded,
        failedPrecondition(
          s"Participant $participantId is either active on the synchronizer or has previously been offboarded."
        ),
      )

      _ <- CantonGrpcUtil.mapErrNewETUS(
        synchronizerTopologyManager
          // TopologyStateProcessor.validateAndApply sets expectFullAuthorization = expectFullAuthorization || !tx.isProposal,
          // so in the usual case (isProposal=false, also checked above that this holds), the authorization checks are active
          // even with expectFullAuthorization = false. But to be safe, set it to be true.
          .add(transactions, ForceFlags.all, expectFullAuthorization = true)
      )
    } yield RegisterOnboardingTopologyTransactionsResponse.defaultInstance

    EitherTUtil
      .toFuture(resultET)
  }

  private[service] def validateOnboardingTransactions(
      participantId: ParticipantId,
      transactions: Seq[GenericSignedTopologyTransaction],
      maxMappings: PositiveInt,
  ): EitherT[Future, StatusRuntimeException, Unit] = {

    val expectedMappings = TopologyStore.initialParticipantDispatchingSet.forgetNE

    val resultET = for {
      _ <- {
        // 0. Reject the transactions if the number exceeds the limit
        val totalCount = transactions.size
        EitherTUtil.condUnitET[Future](
          totalCount <= maxMappings.value,
          s"Too many topology transactions. Limit: ${maxMappings.value}, Found: $totalCount",
        )
      }

      stcCount = transactions.count(
        _.mapping.code == TopologyMapping.Code.SynchronizerTrustCertificate
      )
      otks = transactions.flatMap(_.mapping.select[OwnerToKeyMapping])

      // 1. Participants must have exactly 1 STC
      _ <- EitherTUtil.condUnitET[Future](
        stcCount == 1,
        s"Exactly one SynchronizerTrustCertificate is required for Participants. Found: $stcCount",
      )

      // 2. All members must send exactly 1 OTK at a time
      singleOtk <- otks.toList match {
        case single :: Nil => EitherT.rightT[Future, String](single)
        case Nil =>
          EitherT.leftT[Future, OwnerToKeyMapping](
            "Exactly one OwnerToKeyMapping is required. Found: 0"
          )
        case more =>
          EitherT.leftT[Future, OwnerToKeyMapping](
            s"Exactly one OwnerToKeyMapping is required. Found: ${more.size}"
          )
      }

      // 3. check for unexpected mappings
      _ <- {
        val firstUnexpectedMapping =
          transactions.find(t => !expectedMappings.contains(t.mapping.code))
        EitherTUtil.condUnitET[Future](
          firstUnexpectedMapping.isEmpty,
          s"Unexpected topology mapping found. Allowed: $expectedMappings. Found: ${firstUnexpectedMapping.map(_.mapping.code).toString}",
        )
      }

      // 4. check for missing mappings
      // at present this is just a check that there is at least one Namespace delegation
      // But we use this slightly complexer check for future robustness in case expectedMappings is extended
      _ <- {
        val submittedMappings = transactions.map(_.mapping.code).toSet
        val missingMappings = expectedMappings -- submittedMappings
        EitherTUtil.condUnitET[Future](
          missingMappings.isEmpty,
          s"Missing mappings for onboarding $participantId. Missing: $missingMappings",
        )
      }

      // 5. check for proposals. if any is found, reject the request
      _ <- {
        val firstProposal = transactions.find(_.isProposal)
        EitherTUtil.condUnitET[Future](
          firstProposal.isEmpty,
          s"Unexpected proposals for onboarding $participantId. Found: ${firstProposal.toString}",
        )
      }

      // 6. check for removals. if any is found, reject the request
      _ <- {
        val firstRemoval = transactions.find(_.operation.equals(Remove))
        EitherTUtil.condUnitET[Future](
          firstRemoval.isEmpty,
          s"Unexpected removals for onboarding $participantId. Found: ${firstRemoval.toString}",
        )
      }

      // 7. check for unexpected UIDs
      _ <- {
        val firstBadUidTx = transactions.find(_.mapping.maybeUid.exists(_ != participantId.uid))
        EitherTUtil.condUnitET[Future](
          firstBadUidTx.isEmpty,
          s"Mappings for unexpected UIDs for onboarding $participantId: ${firstBadUidTx.toString}",
        )
      }

      // 8. check for unexpected namespaces
      _ <- {
        val firstUnexpectedNamespace =
          transactions.find(_.mapping.namespace != participantId.namespace)
        EitherTUtil.condUnitET[Future](
          firstUnexpectedNamespace.isEmpty,
          s"Mappings for unexpected namespaces for onboarding $participantId: ${firstUnexpectedNamespace.toString}",
        )
      }
    } yield ()
    resultET.leftMap(msg => invalidRequest(msg))
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
