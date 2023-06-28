// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.localstore.api

import com.daml.error.ContextualizedErrorLogger
import com.daml.lf.data.Ref
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.domain.{IdentityProviderId, User, UserRight}
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLogging}

import scala.concurrent.{ExecutionContext, Future}

final case class UserUpdate(
    id: Ref.UserId,
    identityProviderId: IdentityProviderId,
    primaryPartyUpdateO: Option[Option[Ref.Party]] = None,
    isDeactivatedUpdateO: Option[Boolean] = None,
    metadataUpdate: ObjectMetaUpdate,
)

final case class ObjectMetaUpdate(
    resourceVersionO: Option[Long],
    annotationsUpdateO: Option[Map[String, String]],
)

object ObjectMetaUpdate {
  def empty: ObjectMetaUpdate = ObjectMetaUpdate(
    resourceVersionO = None,
    annotationsUpdateO = None,
  )
}

trait UserManagementStore {
  self: NamedLogging =>

  import UserManagementStore.*

  // read access

  def getUserInfo(id: Ref.UserId, identityProviderId: IdentityProviderId)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[UserInfo]]

  /** Always returns `maxResults` if possible, i.e. if a call to this method
    * returned fewer than `maxResults` users, then the next page (as of calling this method) was empty.
    */
  def listUsers(
      fromExcl: Option[Ref.UserId],
      maxResults: Int,
      identityProviderId: IdentityProviderId,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[UsersPage]]

  // write access

  def createUser(user: User, rights: Set[UserRight])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[User]]

  def updateUser(userUpdate: UserUpdate)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[User]]

  def deleteUser(id: Ref.UserId, identityProviderId: IdentityProviderId)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[Unit]]

  def grantRights(id: Ref.UserId, rights: Set[UserRight], identityProviderId: IdentityProviderId)(
      implicit loggingContext: LoggingContextWithTrace
  ): Future[Result[Set[UserRight]]]

  def revokeRights(id: Ref.UserId, rights: Set[UserRight], identityProviderId: IdentityProviderId)(
      implicit loggingContext: LoggingContextWithTrace
  ): Future[Result[Set[UserRight]]]

  def updateUserIdp(
      id: Ref.UserId,
      sourceIdp: IdentityProviderId,
      targetIdp: IdentityProviderId,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[User]]
  // read helpers

  // TODO(#13019) Replace parasitic with DirectExecutionContext
  @SuppressWarnings(Array("com.digitalasset.canton.GlobalExecutionContext"))
  final def getUser(id: Ref.UserId, identityProviderId: IdentityProviderId)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[User]] = {
    getUserInfo(id, identityProviderId).map(_.map(_.user))(ExecutionContext.parasitic)
  }

  // TODO(#13019) Replace parasitic with DirectExecutionContext
  @SuppressWarnings(Array("com.digitalasset.canton.GlobalExecutionContext"))
  final def listUserRights(id: Ref.UserId, identityProviderId: IdentityProviderId)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[Set[UserRight]]] = {
    getUserInfo(id, identityProviderId).map(_.map(_.rights))(ExecutionContext.parasitic)
  }

  def createExtraAdminUser(rawUserId: String)(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): Future[Unit] = {
    val userId = Ref.UserId.assertFromString(rawUserId)
    createUser(
      user = domain.User(
        id = userId,
        primaryParty = None,
        identityProviderId = IdentityProviderId.Default,
      ),
      rights = Set(UserRight.ParticipantAdmin),
    )
      .flatMap {
        case Left(UserManagementStore.UserExists(_)) =>
          logger.info(
            s"Creating admin user with id $userId failed. User with this id already exists"
          )
          Future.successful(())
        case other =>
          handleResult("creating extra admin user")(other).map(_ => ())
      }
  }

}

object UserManagementStore {

  val DefaultParticipantAdminUserId = "participant_admin"

  type Result[T] = Either[Error, T]

  final case class UsersPage(users: Seq[User]) {
    def lastUserIdOption: Option[Ref.UserId] = users.lastOption.map(_.id)
  }

  final case class UserInfo(user: User, rights: Set[UserRight])

  sealed trait Error
  final case class UserNotFound(userId: Ref.UserId) extends Error
  final case class UserExists(userId: Ref.UserId) extends Error
  final case class TooManyUserRights(userId: Ref.UserId) extends Error
  final case class ConcurrentUserUpdate(userId: Ref.UserId) extends Error
  final case class MaxAnnotationsSizeExceeded(userId: Ref.UserId) extends Error
  final case class PermissionDenied(userId: Ref.UserId) extends Error

  def handleResult[T](operation: String)(
      result: UserManagementStore.Result[T]
  )(implicit errorLogger: ContextualizedErrorLogger): Future[T] =
    result match {
      case Left(UserManagementStore.PermissionDenied(id)) =>
        Future.failed(
          LedgerApiErrors.AuthorizationChecks.PermissionDenied
            .Reject(s"User $id belongs to another Identity Provider")
            .asGrpcError
        )
      case Left(UserManagementStore.UserNotFound(id)) =>
        Future.failed(
          LedgerApiErrors.Admin.UserManagement.UserNotFound
            .Reject(operation, id.toString)
            .asGrpcError
        )

      case Left(UserManagementStore.UserExists(id)) =>
        Future.failed(
          LedgerApiErrors.Admin.UserManagement.UserAlreadyExists
            .Reject(operation, id.toString)
            .asGrpcError
        )

      case Left(UserManagementStore.TooManyUserRights(id)) =>
        Future.failed(
          LedgerApiErrors.Admin.UserManagement.TooManyUserRights
            .Reject(operation, id: String)
            .asGrpcError
        )
      case Left(e: UserManagementStore.ConcurrentUserUpdate) =>
        Future.failed(
          LedgerApiErrors.Admin.UserManagement.ConcurrentUserUpdateDetected
            .Reject(userId = e.userId)
            .asGrpcError
        )

      case Left(e: UserManagementStore.MaxAnnotationsSizeExceeded) =>
        Future.failed(
          LedgerApiErrors.Admin.UserManagement.MaxUserAnnotationsSizeExceeded
            .Reject(userId = e.userId)
            .asGrpcError
        )

      case scala.util.Right(t) =>
        Future.successful(t)
    }

}
