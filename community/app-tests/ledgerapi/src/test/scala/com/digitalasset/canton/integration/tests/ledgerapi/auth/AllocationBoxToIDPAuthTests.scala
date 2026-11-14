// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.{
  ApiPartyManagementServiceSuppressionRule,
  AuthServiceJWTSuppressionRule,
}

import java.util.UUID
import scala.concurrent.Future

trait AllocationBoxToIDPAuthTests
    extends ServiceCallAuthTests
    with IdentityProviderConfigAuth
    with ErrorsAssertions {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  protected def allocateFunction(
      serviceCallContext: ServiceCallContext,
      party: String,
      userId: String = "",
      identityProviderIdOverride: Option[String] = None,
  )(implicit
      env: TestConsoleEnvironment
  ): Future[String]

  override protected def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = ???

  serviceCallName should {
    "deny IDP Admin granting permissions to users which do not exist" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "Grant rights to non existing users")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnknownResource {
          val suffix = UUID.randomUUID().toString
          for {
            (_, idpAdminContext, _) <- createIDPBundle(canBeAnAdmin, suffix)
            _ <- allocateFunction(
              idpAdminContext,
              "party-" + suffix,
              "user-" + suffix,
            )
          } yield ()
        }
      }
    }

    "allow IDP Admin granting permissions to users which are allocated in the same IDP" taggedAs adminSecurityAsset
      .setHappyCase(
        "IDP admin can grant user rights to parties in the same IDP"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectSuccess {
          val suffix = UUID.randomUUID().toString
          val userId = "user-" + suffix
          for {
            (_, idpAdminContext, idpConfig) <- createIDPBundle(canBeAnAdmin, suffix)
            _ <- createUser(userId, idpAdminContext)

            _ <- allocateFunction(
              idpAdminContext,
              "party-" + suffix,
              userId,
            )
          } yield ()
        }
      }
    }

    "deny IDP Admin granting permissions to users which are outside of any IDP" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "Grant user rights to users outside of any IDP")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(
        AuthServiceJWTSuppressionRule || ApiPartyManagementServiceSuppressionRule
      ) {
        expectPermissionDenied {
          val suffix = UUID.randomUUID().toString
          val userId = "user-" + suffix
          for {
            (_, idpAdminContext, _) <- createIDPBundle(canBeAnAdmin, suffix)
            _ <- createUser(userId, canBeAnAdmin)

            _ <- allocateFunction(
              idpAdminContext,
              "party-" + suffix,
              userId,
            )
          } yield ()
        }
      }
    }

    "deny IDP Admin granting permissions in other IDPs" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "Grant rights to parties in other IDPs")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(
        AuthServiceJWTSuppressionRule || ApiPartyManagementServiceSuppressionRule
      ) {
        expectPermissionDenied {
          val suffix = UUID.randomUUID().toString
          val userId = "user-" + suffix
          for {
            (_, idpAdminContext, _) <- createIDPBundle(canBeAnAdmin, suffix)
            (_, otherIdpAdminContext, _) <- createIDPBundle(canBeAnAdmin, "other-" + suffix)
            _ <- createUser(userId, otherIdpAdminContext)

            _ <- allocateFunction(
              idpAdminContext,
              "party-" + suffix,
              userId,
            )
          } yield ()
        }
      }
    }

    "deny Admin granting permissions to users which do not exist" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "Grant rights to non existing users")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnknownResource {
          val suffix = UUID.randomUUID().toString
          for {
            _ <- allocateFunction(
              canBeAnAdmin,
              "party-" + suffix,
              "user-" + suffix,
            )
          } yield ()
        }
      }
    }

    "allow Admin granting permissions to users which are allocated in the same IDP" taggedAs adminSecurityAsset
      .setHappyCase(
        "Grant user rights to parties in the same IDP"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectSuccess {
          val suffix = UUID.randomUUID().toString
          val userId = "user-" + suffix
          for {
            (_, idpAdminContext, idpConfig) <- createIDPBundle(canBeAnAdmin, suffix)
            _ <- createUser(userId, idpAdminContext)

            _ <- allocateFunction(
              canBeAnAdmin,
              "party-" + suffix,
              userId,
              Some(idpAdminContext.identityProviderId),
            )
          } yield ()
        }
      }
    }

    "deny Admin granting permissions to users which are outside of any IDP" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "Grant user rights to users outside of any IDP")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(
        AuthServiceJWTSuppressionRule || ApiPartyManagementServiceSuppressionRule
      ) {
        expectPermissionDenied {
          val suffix = UUID.randomUUID().toString
          val userId = "user-" + suffix
          for {
            (_, idpAdminContext, _) <- createIDPBundle(canBeAnAdmin, suffix)
            _ <- createUser(userId, canBeAnAdmin)

            _ <- allocateFunction(
              canBeAnAdmin,
              "party-" + suffix,
              userId,
              Some(idpAdminContext.identityProviderId),
            )
          } yield ()
        }
      }
    }

    "deny Admin granting permissions in other IDPs" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "Grant rights to parties in other IDPs")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(
        AuthServiceJWTSuppressionRule || ApiPartyManagementServiceSuppressionRule
      ) {
        expectPermissionDenied {
          val suffix = UUID.randomUUID().toString
          val userId = "user-" + suffix
          for {
            (_, idpAdminContext, _) <- createIDPBundle(canBeAnAdmin, suffix)
            (_, otherIdpAdminContext, _) <- createIDPBundle(canBeAnAdmin, "other-" + suffix)
            _ <- createUser(userId, otherIdpAdminContext)

            _ <- allocateFunction(
              canBeAnAdmin,
              "party-" + suffix,
              userId,
              Some(idpAdminContext.identityProviderId),
            )
          } yield ()
        }
      }
    }
  }

}
