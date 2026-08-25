// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.ledger.api.v2.admin.party_management_alpha_service.PartyManagementAlphaServiceGrpc.PartyManagementAlphaService
import com.daml.ledger.api.v2.admin.party_management_alpha_service.{
  AuthorizePartyUpdateRequest,
  AuthorizePartyUpdateResponse,
  GeneratePartyTopologyUpdateRequest,
  GeneratePartyTopologyUpdateResponse,
  GetAddPartyStatusRequest,
  GetAddPartyStatusResponse,
  PartyManagementAlphaServiceGrpc,
}
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.tracing.TraceContextGrpc
import com.digitalasset.canton.user.store.UserManagementStore
import com.digitalasset.canton.user.{IdentityProviderId, UserRight}
import com.digitalasset.daml.lf.data.Ref
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class ApiPartyManagementAlphaService(
    partyReplicationEndpoints: PartyReplicationEndpoints,
    userManagementStore: UserManagementStore,
    identityProviderExists: IdentityProviderExists,
    maxSelfAllocatedParties: NonNegativeInt,
    pendingPartyAllocations: PendingPartyAllocations,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends PartyManagementAlphaService
    with GrpcApiService
    with AuthenticatedUserContextResolver
    with NamedLogging {

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    PartyManagementAlphaServiceGrpc.bindService(this, executionContext)

  override def getAddPartyStatus(
      request: GetAddPartyStatusRequest
  ): Future[GetAddPartyStatusResponse] =
    partyReplicationEndpoints.getAddPartyStatus(request)

  override def generatePartyTopologyUpdate(
      request: GeneratePartyTopologyUpdateRequest
  ): Future[GeneratePartyTopologyUpdateResponse] =
    partyReplicationEndpoints.generatePartyTopologyUpdate(request)

  override def authorizePartyUpdate(
      request: AuthorizePartyUpdateRequest
  ): Future[AuthorizePartyUpdateResponse] = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)(TraceContextGrpc.fromGrpcContext)
    implicit val errorLoggingContext: ErrorLoggingContext =
      ErrorLoggingContext(logger, loggingContextWithTrace)

    val userIdStr = request.userId
    val idpStr = request.identityProviderId
    val authenticatedUserContextF = resolveAuthenticatedUserContext

    // Validate IAM inputs before interacting with the Topology Manager to prevent
    // proposing invalid topology changes if the IAM parameters are faulty.
    val preFlightCheckET =
      if (userIdStr.nonEmpty) {
        for {
          userId <- EitherT.fromEither[Future](
            Ref.UserId
              .fromString(userIdStr)
              .leftMap(err =>
                new StatusRuntimeException(io.grpc.Status.INVALID_ARGUMENT.withDescription(err))
              )
          )
          idpId <- EitherT.fromEither[Future] {
            val parsed: Either[StatusRuntimeException, IdentityProviderId] =
              if (idpStr.isEmpty) Right(IdentityProviderId.Default)
              else
                IdentityProviderId.Id
                  .fromString(idpStr)
                  .leftMap(err =>
                    new StatusRuntimeException(io.grpc.Status.INVALID_ARGUMENT.withDescription(err))
                  )
            parsed
          }
          idpExists <- EitherT.right(identityProviderExists(idpId))
          _ <- EitherT.cond[Future](
            idpExists,
            (),
            new StatusRuntimeException(
              io.grpc.Status.NOT_FOUND.withDescription(s"Identity provider $idpId not found")
            ),
          )
        } yield Some((userId, idpId)): Option[(Ref.UserId, IdentityProviderId)]
      } else {
        EitherT.rightT[Future, StatusRuntimeException](
          Option.empty[(Ref.UserId, IdentityProviderId)]
        )
      }

    preFlightCheckET.value.flatMap {
      case Left(validationError) =>
        Future.failed(validationError)

      case Right(iamDetailsO) =>
        val userIdO = iamDetailsO.map(_._1)
        val idpId = iamDetailsO.map(_._2).getOrElse(IdentityProviderId.Default)

        pendingPartyAllocations.withUser(userIdO) { outstandingCalls =>
          for {
            userInfo <- Utils.getUserIfUserSpecified(userIdO, idpId, userManagementStore)
            _ <- Utils.checkUserLimitsIfUserSpecified(
              userInfo.map(_.rights),
              outstandingCalls,
              authenticatedUserContextF,
              maxSelfAllocatedParties.unwrap,
            )

            partyAndTarget <- partyReplicationEndpoints.authorizePartyUpdate(request)
            (party, isAuthorizingOnTargetParticipant) = partyAndTarget

            _ <- iamDetailsO match {
              case Some((userId, idpId)) if isAuthorizingOnTargetParticipant =>
                userManagementStore
                  .grantRights(userId, Set(UserRight.CanActAs(party.toLf)), idpId)
                  .map {
                    case Left(error) =>
                      // The topology transaction is already in-flight/applied, but the local DB failed.
                      // We log a warning because we do not roll back the topology transaction.
                      // The operator can fix this by calling the UserManagementService.GrantUserRights API later.
                      logger.warn(
                        // TODO(#31414) – Instead of logging warning, return it as a status in the gRPC response?
                        //  Or expose this condition through the dedicated status endpoint?
                        s"Topology update authorized for party $party, but local IAM provisioning failed for user $userId: $error." +
                          s"Please invoke UserManagementService.GrantUserRights API manually later."
                      )(loggingContextWithTrace.traceContext)
                    case Right(_) => ()
                  }
              case Some(_) =>
                // TODO(#34589) – Should be a parameter validation error? (Depends on parsed PTP tx; whether this is the target participant)
                // The client provided user details but called a non-target participant (e.g., a source participant
                // counter-signing a local party proposal).
                logger.info(
                  s"IAM provisioning for user skipped as this node is not an onboarding target participant."
                )(loggingContextWithTrace.traceContext)
                Future.successful(())
              case None =>
                Future.successful(())
            }
          } yield AuthorizePartyUpdateResponse()
        }
    }
  }
}

private[apiserver] object ApiPartyManagementAlphaService {
  def createApiService(
      partyReplicationEndpoints: PartyReplicationEndpoints,
      userManagementStore: UserManagementStore,
      identityProviderExists: IdentityProviderExists,
      maxSelfAllocatedParties: NonNegativeInt,
      pendingPartyAllocations: PendingPartyAllocations,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): PartyManagementAlphaService & GrpcApiService =
    new ApiPartyManagementAlphaService(
      partyReplicationEndpoints,
      userManagementStore,
      identityProviderExists,
      maxSelfAllocatedParties,
      pendingPartyAllocations,
      loggerFactory,
    )
}
