// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.auth.{ClaimPublic, ClaimSet, UserBasedClaimResolver}
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.CantonFixture
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.user.store.UserManagementStore
import com.digitalasset.canton.user.{IdentityProviderId, User, UserRight, UserUpdate}
import com.digitalasset.daml.lf.data.Ref
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class UserBasedClaimResolverTest extends CantonFixture with Matchers with SecurityTags {

  def serviceCallName: String = "user-based auth resolver"

  "user-based claim resolver should" should {
    "strip IDP Admin claims from users in the default IDP" taggedAs securityAsset
      .setAttack(
        attackInvalidArgument("default-IDP user with IDP Admin claims")
      ) in { implicit env =>
      implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.empty

      val user = UserManagementStore.UserInfo(
        user = User(
          id = Ref.UserId.assertFromString("potato"),
          primaryParty = None,
          identityProviderId = IdentityProviderId.Default,
        ),
        rights = Set(UserRight.IdentityProviderAdmin), // IDP admin right
      )

      val userBasedClaimResolver = new UserBasedClaimResolver(
        Some(SingletonUserManagementStore(user, loggerFactory)),
        env.executionContext,
      )

      val claims = userBasedClaimResolver(
        ClaimSet.AuthenticatedUser(None, "potato", Some("participant1"), None)
      ).futureValue

      claims shouldBe
        ClaimSet.Claims(
          claims = List(ClaimPublic), // No IDP admin claim
          participantId = Some("participant1"),
          userId = Some("potato"),
          expiration = None,
          resolvedFromUser = true,
          identityProviderId = None,
        )
    }
  }

  /** Any respectable user management store should prevent creating invalid combinations, such as an
    * IDP admin without an associated identity provider.
    *
    * However, we want to apply defense-in-depth and make sure user-based claims are resolved
    * securily, even when a user management store would contain such an invalid user (perhaps a
    * legacy one).
    *
    * This simple class mocks such a store with a possibly invalid user.
    */
  protected case class SingletonUserManagementStore(
      userInfo: UserManagementStore.UserInfo,
      protected val loggerFactory: NamedLoggerFactory,
  ) extends NamedLogging
      with UserManagementStore {
    def getUserInfo(id: Ref.UserId, identityProviderId: IdentityProviderId)(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[UserManagementStore.Result[UserManagementStore.UserInfo]] =
      Future.successful(Right(userInfo))

    def listUsers(
        fromExcl: Option[Ref.UserId],
        maxResults: Int,
        identityProviderId: IdentityProviderId,
    )(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[UserManagementStore.Result[UserManagementStore.UsersPage]] = ???

    def createUser(user: User, rights: Set[UserRight])(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[UserManagementStore.Result[User]] = ???

    def updateUser(userUpdate: UserUpdate)(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[UserManagementStore.Result[User]] = ???

    def deleteUser(id: Ref.UserId, identityProviderId: IdentityProviderId)(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[UserManagementStore.Result[Unit]] = ???

    def grantRights(id: Ref.UserId, rights: Set[UserRight], identityProviderId: IdentityProviderId)(
        implicit loggingContext: LoggingContextWithTrace
    ): Future[UserManagementStore.Result[Set[UserRight]]] = ???

    def revokeRights(
        id: Ref.UserId,
        rights: Set[UserRight],
        identityProviderId: IdentityProviderId,
    )(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[UserManagementStore.Result[Set[UserRight]]] = ???

    def updateUserIdp(
        id: Ref.UserId,
        sourceIdp: IdentityProviderId,
        targetIdp: IdentityProviderId,
    )(implicit loggingContext: LoggingContextWithTrace): Future[UserManagementStore.Result[User]] =
      ???
  }
}
