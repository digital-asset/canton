// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.user_management_service as uproto
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.AuthServiceJWTSuppressionRule

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

trait IDPBoxingServiceCallOutTests
    extends ServiceCallAuthTests
    with IdentityProviderConfigAuth
    with ErrorsAssertions {

  override protected def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = ???

  protected def boxedCall(
      userId: String,
      serviceCallContext: ServiceCallContext,
      rights: Vector[uproto.Right],
  )(implicit ec: ExecutionContext): Future[Any]

  serviceCallName should {
    "deny IDP Admin granting permissions to parties which do not exist" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "Grant rights to non existing parties")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnknownResource {
          val suffix = UUID.randomUUID().toString
          for {
            (_, idpAdminContext, _) <- createIDPBundle(canBeAnAdmin, suffix)
            readAsParty = s"read-$suffix"
            actAsParty = s"act-$suffix"

            _ <- boxedCall(
              "user-" + suffix,
              idpAdminContext,
              readAndActRights(readAsParty, actAsParty),
            )
          } yield ()
        }
      }
    }

    "allow IDP Admin granting permissions to parties which are allocated in the same IDP" taggedAs adminSecurityAsset
      .setHappyCase(
        "IDP admin can grant user rights to parties in the same IDP"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectSuccess {
          val suffix = UUID.randomUUID().toString
          for {
            (_, idpAdminContext, idpConfig) <- createIDPBundle(canBeAnAdmin, suffix)
            readAsParty <- allocateParty(
              canBeAnAdmin,
              s"read-$suffix",
              "",
              Some(idpConfig.identityProviderId),
            )
            actAsParty <- allocateParty(
              canBeAnAdmin,
              s"act-$suffix",
              "",
              Some(idpConfig.identityProviderId),
            )

            _ <- boxedCall(
              "user-" + suffix,
              idpAdminContext,
              readAndActRights(readAsParty, actAsParty),
            )
          } yield ()
        }
      }
    }

    "deny IDP Admin granting permissions to parties which are outside of any IDP" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "Grant user rights to parties outside of any IDP")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnknownResource {
          val suffix = UUID.randomUUID().toString
          for {
            (_, idpAdminContext, _) <- createIDPBundle(canBeAnAdmin, suffix)
            readAsParty <- allocateParty(canBeAnAdmin, s"read-$suffix")
            actAsParty <- allocateParty(canBeAnAdmin, s"act-$suffix")

            _ <- boxedCall(
              "user-" + suffix,
              idpAdminContext,
              readAndActRights(readAsParty, actAsParty),
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
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnknownResource {
          val suffix = UUID.randomUUID().toString
          for {
            (_, idpAdminContext, _) <- createIDPBundle(canBeAnAdmin, suffix)
            otherIdpConfig <- createConfig(canBeAnAdmin)
            readAsParty <- allocateParty(
              canBeAnAdmin,
              s"read-$suffix",
              "",
              Some(otherIdpConfig.identityProviderId),
            )
            actAsParty <- allocateParty(
              canBeAnAdmin,
              s"act-$suffix",
              "",
              Some(otherIdpConfig.identityProviderId),
            )

            _ <- boxedCall(
              "user-" + suffix,
              idpAdminContext,
              readAndActRights(readAsParty, actAsParty),
            )
          } yield ()
        }
      }
    }

    "allow Admin granting permissions to parties which do not exist" taggedAs adminSecurityAsset
      .setHappyCase(
        "Grant rights to non existing parties"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectSuccess {
          val suffix = UUID.randomUUID().toString
          for {
            (_, _, _) <- createIDPBundle(canBeAnAdmin, suffix)
            readAsParty = s"read-$suffix"
            actAsParty = s"act-$suffix"

            _ <- boxedCall(
              "user-" + suffix,
              canBeAnAdmin,
              readAndActRights(readAsParty, actAsParty),
            )
          } yield ()
        }
      }
    }

    "allow Admin granting permissions to parties which are allocated in the same IDP" taggedAs adminSecurityAsset
      .setHappyCase(
        "Grant user rights to parties in the same IDP"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectSuccess {
          val suffix = UUID.randomUUID().toString
          for {
            (_, _, idpConfig) <- createIDPBundle(canBeAnAdmin, suffix)
            readAsParty <- allocateParty(
              canBeAnAdmin,
              s"read-$suffix",
              "",
              Some(idpConfig.identityProviderId),
            )
            actAsParty <- allocateParty(
              canBeAnAdmin,
              s"act-$suffix",
              "",
              Some(idpConfig.identityProviderId),
            )

            _ <- boxedCall(
              "user-" + suffix,
              canBeAnAdmin,
              readAndActRights(readAsParty, actAsParty),
            )
          } yield ()
        }
      }
    }

    "allow Admin granting permissions to parties which are outside of any IDP" taggedAs adminSecurityAsset
      .setHappyCase(
        "Grant user rights to parties outside of any IDP"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectSuccess {
          val suffix = UUID.randomUUID().toString
          for {
            (_, _, _) <- createIDPBundle(canBeAnAdmin, suffix)
            readAsParty <- allocateParty(canBeAnAdmin, s"read-$suffix")
            actAsParty <- allocateParty(canBeAnAdmin, s"act-$suffix")

            _ <- boxedCall(
              "user-" + suffix,
              canBeAnAdmin,
              readAndActRights(readAsParty, actAsParty),
            )
          } yield ()
        }
      }
    }

    "allow Admin granting permissions in other IDPs" taggedAs adminSecurityAsset
      .setHappyCase(
        "Grant rights to parties in other IDPs"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectSuccess {
          val suffix = UUID.randomUUID().toString
          for {
            (_, _, _) <- createIDPBundle(canBeAnAdmin, suffix)
            otherIdpConfig <- createConfig(canBeAnAdmin)
            readAsParty <- allocateParty(
              canBeAnAdmin,
              s"read-$suffix",
              "",
              Some(otherIdpConfig.identityProviderId),
            )
            actAsParty <- allocateParty(
              canBeAnAdmin,
              s"act-$suffix",
              "",
              Some(otherIdpConfig.identityProviderId),
            )

            _ <- boxedCall(
              "user-" + suffix,
              canBeAnAdmin,
              readAndActRights(readAsParty, actAsParty),
            )
          } yield ()
        }
      }
    }
  }

  protected def readAndActRights(readAsParty: String, actAsParty: String): Vector[uproto.Right] =
    Vector(
      uproto.Right(
        uproto.Right.Kind.CanReadAs(uproto.Right.CanReadAs(readAsParty))
      ),
      uproto.Right(uproto.Right.Kind.CanActAs(uproto.Right.CanActAs(actAsParty))),
    )
}
