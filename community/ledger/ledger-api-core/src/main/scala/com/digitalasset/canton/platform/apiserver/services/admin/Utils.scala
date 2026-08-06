// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.ledger.api.v2.admin as proto_admin
import com.digitalasset.canton.auth.AuthorizationChecksErrors
import com.digitalasset.canton.ledger.error.groups.UserManagementServiceErrors
import com.digitalasset.canton.logging.{ErrorLoggingContext, LoggingContextWithTrace}
import com.digitalasset.canton.platform.apiserver.services.admin.AuthenticatedUserContextResolver.AuthenticatedUserContext
import com.digitalasset.canton.user.store.UserManagementStore
import com.digitalasset.canton.user.store.UserManagementStore.UserInfo
import com.digitalasset.canton.user.{IdentityProviderId, ObjectMeta, UserRight}
import com.digitalasset.daml.lf.data.Ref

import scala.concurrent.{ExecutionContext, Future}

object Utils {
  def toProtoObjectMeta(meta: ObjectMeta): proto_admin.object_meta.ObjectMeta =
    proto_admin.object_meta.ObjectMeta(
      resourceVersion = serializeResourceVersion(meta.resourceVersionO),
      annotations = meta.annotations,
    )

  private def serializeResourceVersion(resourceVersionO: Option[Long]): String =
    resourceVersionO.fold("")(_.toString)

  def handleResult[T](operation: String)(
      result: UserManagementStore.Result[T]
  )(implicit errorLogger: ErrorLoggingContext): Future[T] =
    result match {
      case Left(UserManagementStore.PermissionDenied(id)) =>
        Future.failed(
          AuthorizationChecksErrors.PermissionDenied
            .Reject(s"User $id belongs to another Identity Provider")
            .asGrpcError
        )
      case Left(UserManagementStore.UserNotFound(id)) =>
        Future.failed(
          UserManagementServiceErrors.UserNotFound
            .Reject(operation, id)
            .asGrpcError
        )

      case Left(UserManagementStore.UserDeletedWhileUpdating(id)) =>
        Future.failed(
          UserManagementServiceErrors.UserDeletedWhileUpdating
            .Reject(operation, id)
            .asGrpcError
        )

      case Left(UserManagementStore.UserExists(id)) =>
        Future.failed(
          UserManagementServiceErrors.UserAlreadyExists
            .Reject(operation, id)
            .asGrpcError
        )

      case Left(UserManagementStore.TooManyUserRights(id)) =>
        Future.failed(
          UserManagementServiceErrors.TooManyUserRights
            .Reject(operation, id: String)
            .asGrpcError
        )
      case Left(e: UserManagementStore.ConcurrentUserUpdate) =>
        Future.failed(
          UserManagementServiceErrors.ConcurrentUserUpdateDetected
            .Reject(userId = e.userId)
            .asGrpcError
        )

      case Left(e: UserManagementStore.MaxAnnotationsSizeExceeded) =>
        Future.failed(
          UserManagementServiceErrors.MaxUserAnnotationsSizeExceeded
            .Reject(userId = e.userId)
            .asGrpcError
        )

      case scala.util.Right(t) =>
        Future.successful(t)
    }

  def getUserIfUserSpecified(
      userId: Option[Ref.UserId],
      identityProviderId: IdentityProviderId,
      userManagementStore: UserManagementStore,
  )(implicit
      errorLogger: ErrorLoggingContext,
      loggingContextWithTrace: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): Future[Option[UserInfo]] =
    userId.fold[Future[Option[UserInfo]]](Future.successful(None))(
      userManagementStore
        .getUserInfo(_, identityProviderId)
        .flatMap(result => Utils.handleResult("checking user's existence")(result).map(Some(_)))
    )

  def checkUserLimitsIfUserSpecified(
      userRights: Option[Set[UserRight]],
      outstandingCalls: Int,
      authenticatedUserContextF: Future[AuthenticatedUserContext],
      maxSelfAllocatedParties: Int,
  )(implicit executionContext: ExecutionContext, errorLogger: ErrorLoggingContext): Future[Unit] =
    userRights match {
      case None => Future.unit
      case Some(rights) =>
        for {
          authenticatedUserContext <- authenticatedUserContextF
          resultingRightsCount = rights.flatMap(_.getParty).size + outstandingCalls
          _ <-
            if (
              authenticatedUserContext.isRegularUser && resultingRightsCount > maxSelfAllocatedParties
            )
              Future.failed(
                AuthorizationChecksErrors.PermissionDenied
                  .Reject(s"User quota of party allocations exhausted")
                  .asGrpcError
              )
            else
              Future.unit
        } yield ()
    }

}
