// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
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
import com.digitalasset.canton.synchronizer.sequencer.time.LsuSequencingBounds
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
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{EitherTUtil, EitherUtil}
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import io.grpc.{Status, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Sequencer connect service on gRPC
  */
class GrpcSequencerConnectService(
    psid: PhysicalSynchronizerId,
    sequencerId: SequencerId,
    staticSynchronizerParameters: StaticSynchronizerParameters,
    synchronizerTopologyManager: SynchronizerTopologyManager,
    cryptoApi: SynchronizerCryptoClient,
    clock: Clock,
    lsuSequencingBounds: Option[LsuSequencingBounds],
    sanitizePublicErrorMessages: Boolean,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends proto.SequencerConnectServiceGrpc.SequencerConnectService
    with NamedLogging {

  protected val serverProtocolVersion: ProtocolVersion =
    staticSynchronizerParameters.protocolVersion

  override def getSynchronizerId(
      request: GetSynchronizerIdRequest
  ): Future[GetSynchronizerIdResponse] =
    // No need to sanitize error messages, as long as this cannot fail in any way.
    Future.successful(
      GetSynchronizerIdResponse(
        physicalSynchronizerId = psid.toProtoPrimitive,
        sequencerUid = sequencerId.uid.toProtoPrimitive,
      )
    )

  override def getSynchronizerParameters(
      request: GetSynchronizerParametersRequest
  ): Future[GetSynchronizerParametersResponse] =
    mapErrorEither(
      staticSynchronizerParameters.protoVersion.v match {
        case 30 =>
          Right(
            GetSynchronizerParametersResponse(
              Parameters.ParametersV1(staticSynchronizerParameters.toProtoV30)
            )
          )
        case unsupported =>
          // If we hit this branch, something is severely broken. Therefore the extra error logging.
          logger.error(
            s"Unable to serialize StaticSynchronizerParameters to Proto version $unsupported. Failing..."
          )(TraceContext.empty)
          Left(
            internal(
              new IllegalStateException(
                s"Unable to serialize StaticSynchronizerParameters to Proto version $unsupported. Failing..."
              )
            )
          )
      }
    )

  override def verifyActive(request: VerifyActiveRequest): Future[VerifyActiveResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    mapErrorEitherFUS(
      for {
        participant <- EitherT.fromEither[FutureUnlessShutdown](getParticipantFromGrpcContext())

        // for status reads use the currentSnapshotApproximation
        topologySnapshot <- EitherT.liftF(
          cryptoApi.ips.currentSnapshotApproximation
        )

        isActive <- EitherT.liftF(
          topologySnapshot.isParticipantActive(participant)
        ): EitherT[FutureUnlessShutdown, Status, Boolean]
      } yield VerifyActiveResponse(
        VerifyActiveResponse.Value.Success(VerifyActiveResponse.Success(isActive))
      )
    )
  }

  override def handshake(request: HandshakeRequest): Future[HandshakeResponse] =
    mapErrorEither(
      HandshakeValidator
        .clientIsCompatible(
          serverProtocolVersion,
          request.clientProtocolVersions,
          request.minimumProtocolVersion,
        )
        .map(_ =>
          HandshakeResponse(
            serverProtocolVersion.toProtoPrimitive,
            HandshakeResponse.Value
              .Success(SequencerConnect.HandshakeResponse.Success()),
          )
        )
    )

  override def registerOnboardingTopologyTransactions(
      request: SequencerConnect.RegisterOnboardingTopologyTransactionsRequest
  ): Future[SequencerConnect.RegisterOnboardingTopologyTransactionsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    mapErrorEitherF(for {
      // refuse requests from non-participants
      participantId <- EitherT.fromEither[Future](getParticipantFromGrpcContext())

      now = clock.now

      // Time check
      /*
      This checks allows for a better UX. Without it, the sequencer will fail to dispatch the onboarding
      topology transactions, which causes
      - unnecessary warnings in the logs
      - unnecessary delay for the participant to figure out that onboarding has failed
       */
      upgradeTimeExclusive = lsuSequencingBounds.map(_.upgradeTime)
      _ <- EitherTUtil.condUnitET[Future](
        upgradeTimeExclusive.forall(now > _),
        failedPrecondition(
          show"Onboarding is possible only after ${upgradeTimeExclusive.showValue}. Aborting..."
        ),
      )

      // grab the head snapshot to use in all subsequent checks
      // use head snapshot here to make sure we see the results of all sequenced transactions
      topologySnapshot = cryptoApi.ips.headSnapshot

      // get the synchronizer parameters from the snapshot
      params <- EitherT(
        topologySnapshot.findDynamicSynchronizerParameters().asGrpcFuture
      )
        .leftMap(err =>
          internal(new IllegalStateException(s"Unable to fetch synchronizer parameters: $err"))
        )

      // Reject request if the synchronizer is locked
      _ <- EitherTUtil.condUnitET[Future](
        !params.parameters.onboardingRestriction.isLocked,
        failedPrecondition("Synchronizer is locked for onboarding."),
      )

      transactions <-
        EitherT.fromEither[Future](
          request.topologyTransactions
            .traverse(
              SignedTopologyTransaction
                .fromProtoV30(ProtocolVersionValidation(serverProtocolVersion), _)
            )
            .leftMap(ProtoDeserializationFailure.Wrap(_))
            .map(_.distinctBy(_.mapping.uniqueKey))
            .leftMap(_.asGrpcError.getStatus)
        )

      // Perform validations on the transactions
      // Pass a limit of 7 for total number of transactions
      // We enforce exactly 1 OTK and STC. An allowance of 5 NSDs
      // should more than suffice during onboarding. Assuming even one NSD for each of
      // the other mappings (STC, OTK), one to manage them, plus one root mapping
      // requires 4 NSDs in total. More mappings can be added after onboarding
      _ <- EitherT.fromEither[Future](
        validateOnboardingTransactions(participantId, transactions, PositiveInt.tryCreate(7))
      )

      // query whether the participant has ever onboarded before (regardless of whether it is presently active)
      wasEverOnboarded <- leftOnShutdown(
        EitherT
          .liftF(
            topologySnapshot.wasEverOnboarded(participantId)
          )
      )

      _ <- EitherTUtil.condUnitET[Future](
        !wasEverOnboarded,
        failedPrecondition(
          if (sanitizePublicErrorMessages) {
            // Sanitize so that an attacker cannot determine who is or has been registered.
            "Unable to register onboarding topology transactions"
          } else
            s"Participant $participantId is either active on the synchronizer or has previously been offboarded."
        ),
      )

      _ <- leftOnShutdown(
        synchronizerTopologyManager
          // TopologyStateProcessor.validateAndApply sets expectFullAuthorization = expectFullAuthorization || !tx.isProposal,
          // so in the usual case (isProposal=false, also checked above that this holds), the authorization checks are active
          // even with expectFullAuthorization = false. But to be safe, set it to be true.
          .add(
            transactions,
            ForceFlags.all,
            expectFullAuthorization = true,
          )
          .leftMap(err =>
            if (sanitizePublicErrorMessages) {
              // Sanitize because it is too hard to reason about the contents of err.
              failedPrecondition("Unable to register onboarding topology transactions")
            } else err.asGrpcError.getStatus
          )
      )
    } yield RegisterOnboardingTopologyTransactionsResponse.defaultInstance)
  }

  private[service] def validateOnboardingTransactions(
      participantId: ParticipantId,
      transactions: Seq[GenericSignedTopologyTransaction],
      maxMappings: PositiveInt,
  ): Either[Status, Unit] = {

    val expectedMappings = TopologyStore.initialParticipantDispatchingSet.forgetNE

    for {
      _ <- {
        // 0. Reject the transactions if the number exceeds the limit
        val totalCount = transactions.size
        EitherUtil.condUnit(
          totalCount <= maxMappings.value,
          invalidArgument(
            s"Too many topology transactions. Limit: ${maxMappings.value}, Found: $totalCount"
          ),
        )
      }

      stcCount = transactions.count(
        _.mapping.code == TopologyMapping.Code.SynchronizerTrustCertificate
      )
      otks = transactions.flatMap(_.mapping.select[OwnerToKeyMapping])

      // 1. Participants must have exactly 1 STC
      _ <- EitherUtil.condUnit(
        stcCount == 1,
        invalidArgument(
          s"Exactly one SynchronizerTrustCertificate is required for Participants. Found: $stcCount"
        ),
      )

      // 2. All members must send exactly 1 OTK at a time
      _ <- otks.toList match {
        case _single :: Nil => Right(())
        case notSingle =>
          Left(
            invalidArgument(s"Exactly one OwnerToKeyMapping is required. Found: ${notSingle.size}")
          )
      }

      // 3. check for unexpected mappings
      _ <- {
        val firstUnexpectedMapping =
          transactions.find(t => !expectedMappings.contains(t.mapping.code))
        EitherUtil.condUnit(
          firstUnexpectedMapping.isEmpty,
          invalidArgument(
            s"Unexpected topology mapping found. Allowed: $expectedMappings. Found: ${firstUnexpectedMapping.map(_.mapping.code).toString}"
          ),
        )
      }

      // 4. check for missing mappings
      // at present this is just a check that there is at least one Namespace delegation
      // But we use this slightly complexer check for future robustness in case expectedMappings is extended
      _ <- {
        val submittedMappings = transactions.map(_.mapping.code).toSet
        val missingMappings = expectedMappings -- submittedMappings
        EitherUtil.condUnit(
          missingMappings.isEmpty,
          invalidArgument(
            s"Missing mappings for onboarding $participantId. Missing: $missingMappings"
          ),
        )
      }

      // 5. check for proposals. if any is found, reject the request
      _ <- {
        val firstProposal = transactions.find(_.isProposal)
        EitherUtil.condUnit(
          firstProposal.isEmpty,
          invalidArgument(
            s"Unexpected proposals for onboarding $participantId. Found: ${firstProposal.toString}"
          ),
        )
      }

      // 6. check for removals. if any is found, reject the request
      _ <- {
        val firstRemoval = transactions.find(_.operation.equals(Remove))
        EitherUtil.condUnit(
          firstRemoval.isEmpty,
          invalidArgument(
            s"Unexpected removals for onboarding $participantId. Found: ${firstRemoval.toString}"
          ),
        )
      }

      // 7. check for unexpected UIDs
      _ <- {
        val firstBadUidTx = transactions.find(_.mapping.maybeUid.exists(_ != participantId.uid))
        EitherUtil.condUnit(
          firstBadUidTx.isEmpty,
          invalidArgument(
            s"Mappings for unexpected UIDs for onboarding $participantId: ${firstBadUidTx.toString}"
          ),
        )
      }

      // 8. check for unexpected namespaces
      _ <- {
        val firstUnexpectedNamespace =
          transactions.find(_.mapping.namespace != participantId.namespace)
        EitherUtil.condUnit(
          firstUnexpectedNamespace.isEmpty,
          invalidArgument(
            s"Mappings for unexpected namespaces for onboarding $participantId: ${firstUnexpectedNamespace.toString}"
          ),
        )
      }
    } yield ()
  }

  /*
   Note: we only get the participantId from the context; we have no idea
   whether the member is authenticated or not.
   */
  private def getParticipantFromGrpcContext(): Either[Status, ParticipantId] = for {
    member <- IdentityContextHelper.getCurrentStoredMember
      .toRight(
        invalidArgument("Unable to find participant id in gRPC context")
      )

    participantId <- member match {
      case participantId: ParticipantId => Right(participantId)
      case member =>
        Left(
          invalidArgument(
            s"Only participants should use this endpoint, but found member of type ${member.getClass.getSimpleName} in the gRPC context."
          )
        )
    }
  } yield participantId

  private def invalidArgument(message: String): Status =
    Status.INVALID_ARGUMENT.withDescription(message)

  private def failedPrecondition(message: String): Status =
    Status.FAILED_PRECONDITION.withDescription(message)

  private def internal(cause: Throwable): Status =
    Status.INTERNAL
      .withDescription(
        // Deliberately hardcoding a generic error message here, as GRPC forwards it to clients.
        "An error has occurred. Please contact the operator and inquire about the request."
      )
      // Including cause so that ApiRequestLogger logs it server side.
      // Grpc does not forward exceptions to clients.
      .withCause(cause)

  private def leftOnShutdown[A](f: EitherT[FutureUnlessShutdown, Status, A])(implicit
      traceContext: TraceContext
  ): EitherT[Future, Status, A] =
    // Deliberately converting to io.grpc.Status here to remove any metadata (e.g., line number)
    // that should not be disclosed to clients.
    f.onShutdown(Left(AbortedDueToShutdown.Error().asGrpcError.getStatus))

  private def mapErrorEitherFUS[A](f: EitherT[FutureUnlessShutdown, Status, A])(implicit
      traceContext: TraceContext
  ): Future[A] =
    mapErrorEitherF(
      f.onShutdown(
        Left(
          // Deliberately converting to io.grpc.Status here, as this strips out any metadata (e.g. line number)
          // that should not be disclosed to clients.
          GrpcErrors.AbortedDueToShutdown.Error().asGrpcError.getStatus
        )
      )
    )

  private def mapErrorEitherF[A](f: EitherT[Future, Status, A]): Future[A] =
    f.value.transformWith {
      case Success(resultOrErr) => mapErrorEither(resultOrErr)
      case Failure(ex: StatusRuntimeException) => Future.failed(ex)
      case Failure(ex) =>
        Future.failed(
          // Calling internal here, as GRPC would disclose the exception message to clients.
          internal(ex).asRuntimeException()
        )
    }

  private def mapErrorEither[A](f: Either[Status, A]): Future[A] =
    Future.fromTry(f.leftMap(_.asRuntimeException()).toTry)
}
