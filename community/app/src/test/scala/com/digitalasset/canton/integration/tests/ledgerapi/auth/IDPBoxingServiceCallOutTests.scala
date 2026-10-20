// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v2.admin.{
  party_management_service as pproto,
  user_management_service as uproto,
}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.AuthServiceJWTSuppressionRule
import com.digitalasset.canton.integration.{ConfigTransforms, TestConsoleEnvironment}
import com.digitalasset.canton.logging.SuppressionRule
import com.google.protobuf.field_mask.FieldMask
import monocle.macros.syntax.lens.*

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

trait IDPBoxingServiceCallOutTests
    extends ServiceCallAuthTests
    with IdentityProviderConfigAuth
    with UserManagementAuth
    with ErrorsAssertions {

  // Allow the use of the admin API console, this is needed for setup in one of the tests,
  // where we explicitly want to exercise parties created through this path.
  override lazy val environmentDefinition = super.environmentDefinition
    .addConfigTransform(ConfigTransforms.updateParticipantConfig("participant1") { config =>
      config
        .focus(_.ledgerApi.adminTokenConfig.adminClaim)
        .replace(true)
    })

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

    "IDP admin granting rights that transcend IDP boundaries" should {
      for (
        (description, kind) <- List[(String, uproto.Right.Kind)](
          (
            "read as any party",
            uproto.Right.Kind.CanReadAsAnyParty(uproto.Right.CanReadAsAnyParty()),
          ),
          (
            "execute as any party",
            uproto.Right.Kind.CanExecuteAsAnyParty(uproto.Right.CanExecuteAsAnyParty()),
          ),
          (
            "act as any party",
            uproto.Right.Kind.CanActAsAnyParty(uproto.Right.CanActAsAnyParty()),
          ),
          (
            "participant admin",
            uproto.Right.Kind.ParticipantAdmin(uproto.Right.ParticipantAdmin()),
          ),
        )
      ) {
        s"deny granting $description rights" taggedAs adminSecurityAsset
          .setAttack(
            attackUnknownResource(threat = s"Grant $description rights")
          ) in { implicit env =>
          import env.*
          loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
            expectPermissionDenied {
              val suffix = UUID.randomUUID().toString
              for {
                (_, idpAdminContext, _) <- createIDPBundle(canBeAnAdmin, suffix)

                _ <- boxedCall(
                  "user-" + suffix,
                  idpAdminContext,
                  Vector(uproto.Right(kind)),
                )
              } yield ()
            }
          }
        }
      }
    }

    "deny IDP Admin claiming unassigned users" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "Claim users without IDP")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectPermissionDenied {
          val suffix = UUID.randomUUID().toString
          for {
            (_, idpAdminContext, idpConfig) <- createIDPBundle(canBeAnAdmin, suffix)
            unassigned <- createFreshUser(
              token = canBeAnAdmin.token,
              identityProviderId = "",
            )
            _ <- stub(uproto.UserManagementServiceGrpc.stub(channel), idpAdminContext.token)
              .updateUserIdentityProviderId(
                uproto.UpdateUserIdentityProviderIdRequest(
                  userId = unassigned.user.value.id,
                  sourceIdentityProviderId = "",
                  targetIdentityProviderId = idpConfig.identityProviderId,
                )
              )
          } yield ()
        }
      }
    }

    "deny IDP Admin updating details on unassigned parties" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "Claim parties without a party record")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(SuppressionRule.LoggerNameContains("PartyManagementService")) {
        val suffix = UUID.randomUUID().toString
        // Use the admin console to create the party.
        // This does not create a party record associating this with an IDP.
        val partyId = participant1.parties.enable(s"party-$suffix")
        val (_, idpAdminContext, idpConfig) = createIDPBundle(canBeAnAdmin, suffix).futureValue
        expectInvalidArgument {
          stub(pproto.PartyManagementServiceGrpc.stub(channel), idpAdminContext.token)
            .updatePartyDetails(
              pproto.UpdatePartyDetailsRequest(
                partyDetails = Some(
                  pproto.PartyDetails(
                    party = partyId.toProtoPrimitive,
                    isLocal = true,
                    identityProviderId = idpConfig.identityProviderId,
                    localMetadata = Some(
                      ObjectMeta(
                        resourceVersion = "",
                        annotations = Map("hello" -> "potato"),
                      )
                    ),
                  )
                ),
                updateMask = Some(FieldMask(paths = Seq("local_metadata.annotations"))),
              )
            )
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
