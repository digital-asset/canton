// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.party_management_service.GetPartiesRequest
import com.daml.ledger.api.v2.admin.user_management_service.{GetUserRequest, ListUserRightsRequest}
import com.daml.ledger.api.v2.admin.{
  party_management_service as pproto,
  user_management_service as uproto,
}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.{
  ApiPartyManagementServiceSuppressionRule,
  AuthServiceJWTSuppressionRule,
}
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentDefinition,
  TestConsoleEnvironment,
}
import monocle.Monocle.toAppliedFocusOps

import java.util.UUID
import scala.concurrent.Future

class SelfAdminAuthIT
    extends ServiceCallAuthTests
    with IdentityProviderConfigAuth
    with ErrorsAssertions {

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.addConfigTransform(
      ConfigTransforms.updateAllParticipantConfigs_(
        _.focus(_.ledgerApi.partyManagementService.maxSelfAllocatedParties)
          .replace(NonNegativeInt.tryCreate(2))
      )
    )

  override protected def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = ???

  serviceCallName should {

    "allow user querying about own parties" taggedAs adminSecurityAsset
      .setHappyCase(
        "User can query the details of the own parties"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectSuccess {
          val suffix = UUID.randomUUID().toString
          for {
            read <- allocateParty(asAdmin, s"read-$suffix")
            act <- allocateParty(asAdmin, s"act-$suffix")
            execute <- allocateParty(asAdmin, s"execute-$suffix")
            extern <- allocateExternalParty(asAdmin, s"extern-$suffix")

            inputParties = Seq(read, act, execute, extern)

            (_, user1Context) <- createUserByAdmin(
              "user-1-" + suffix,
              rights = Vector(
                uproto.Right(uproto.Right.Kind.CanReadAs(uproto.Right.CanReadAs(read))),
                uproto.Right(uproto.Right.Kind.CanActAs(uproto.Right.CanActAs(act))),
                uproto.Right(uproto.Right.Kind.CanExecuteAs(uproto.Right.CanExecuteAs(execute))),
                uproto.Right(uproto.Right.Kind.CanExecuteAs(uproto.Right.CanExecuteAs(extern))),
              ),
            )
            (_, user2Context) <- createUserByAdmin(
              "user-2-" + suffix,
              rights = Vector(
                uproto.Right(uproto.Right.Kind.CanReadAsAnyParty(uproto.Right.CanReadAsAnyParty())),
                uproto.Right(uproto.Right.Kind.CanActAs(uproto.Right.CanActAs(act))),
                uproto.Right(uproto.Right.Kind.CanExecuteAs(uproto.Right.CanExecuteAs(execute))),
                uproto.Right(uproto.Right.Kind.CanExecuteAs(uproto.Right.CanExecuteAs(extern))),
              ),
            )
            (_, user3Context) <- createUserByAdmin(
              "user-3-" + suffix,
              rights = Vector(
                uproto.Right(uproto.Right.Kind.CanReadAs(uproto.Right.CanReadAs(read))),
                uproto.Right(uproto.Right.Kind.CanActAs(uproto.Right.CanActAs(act))),
                uproto.Right(
                  uproto.Right.Kind.CanExecuteAsAnyParty(uproto.Right.CanExecuteAsAnyParty())
                ),
              ),
            )

            parties1 <- stub(pproto.PartyManagementServiceGrpc.stub(channel), user1Context.token)
              .getParties(
                GetPartiesRequest(parties = inputParties, identityProviderId = "")
              )
            parties2 <- stub(pproto.PartyManagementServiceGrpc.stub(channel), user2Context.token)
              .getParties(
                GetPartiesRequest(parties = inputParties, identityProviderId = "")
              )
            parties3 <- stub(pproto.PartyManagementServiceGrpc.stub(channel), user3Context.token)
              .getParties(
                GetPartiesRequest(parties = inputParties, identityProviderId = "")
              )

          } yield (
            parties1.partyDetails.map(_.party) should contain theSameElementsAs inputParties,
            parties2.partyDetails.map(_.party) should contain theSameElementsAs inputParties,
            parties3.partyDetails.map(_.party) should contain theSameElementsAs inputParties,
          )
        }
      }
    }

    "deny user querying about parties it doesn't own" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "User querying status of parties it doesn't own")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectPermissionDenied {
          val suffix = UUID.randomUUID().toString
          for {
            read <- allocateParty(asAdmin, s"read-$suffix")
            act <- allocateParty(asAdmin, s"act-$suffix")
            execute <- allocateParty(asAdmin, s"execute-$suffix")
            extern <- allocateExternalParty(asAdmin, s"extern-$suffix")

            inputParties = Seq(read, act, execute, extern)
            outputParties = Seq(read, act)

            (_, userContext) <- createUserByAdmin(
              "user-" + suffix,
              rights = Vector(
                uproto.Right(uproto.Right.Kind.CanReadAs(uproto.Right.CanReadAs(read))),
                uproto.Right(uproto.Right.Kind.CanActAs(uproto.Right.CanActAs(act))),
              ),
            )

            returnedParties <- stub(
              pproto.PartyManagementServiceGrpc.stub(channel),
              userContext.token,
            )
              .getParties(
                GetPartiesRequest(parties = inputParties, identityProviderId = "")
              )

          } yield returnedParties.partyDetails.map(
            _.party
          ) should contain theSameElementsAs outputParties
        }
      }
    }

    "allow user querying own records" taggedAs adminSecurityAsset
      .setHappyCase(
        "User can query own records"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectSuccess {
          val suffix = UUID.randomUUID().toString
          for {
            (user, userContext) <- createUserByAdmin("user-" + suffix)
            _ <- stub(uproto.UserManagementServiceGrpc.stub(channel), userContext.token).getUser(
              GetUserRequest(user.id, "")
            )
            _ <- stub(uproto.UserManagementServiceGrpc.stub(channel), userContext.token)
              .listUserRights(
                ListUserRightsRequest(user.id, "")
              )
              .map(_.rights)

          } yield ()
        }
      }
    }

    "deny user querying someone else's records" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "User querying records of other users")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectPermissionDenied {
          val suffix = UUID.randomUUID().toString
          for {
            (_, userContext) <- createUserByAdmin("user-1-" + suffix)
            (anotherUser, _) <- createUserByAdmin("user-2-" + suffix)
            _ <- stub(uproto.UserManagementServiceGrpc.stub(channel), userContext.token).getUser(
              GetUserRequest(anotherUser.id, "")
            )
            _ <- stub(uproto.UserManagementServiceGrpc.stub(channel), userContext.token)
              .listUserRights(
                ListUserRightsRequest(anotherUser.id, "")
              )
              .map(_.rights)
          } yield ()
        }
      }
    }

    def defineTests(
        partyType: String,
        allocateFun: (
            String,
            ServiceCallContext,
            String,
        ) => TestConsoleEnvironment => Future[String],
    ) = {

      s"allow user allocating own $partyType party" taggedAs adminSecurityAsset
        .setHappyCase(
          s"User can allocate own $partyType party"
        ) in { implicit env =>
        import env.*
        loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
          expectSuccess {
            val suffix = UUID.randomUUID().toString
            for {
              (user, userContext) <- createUserByAdmin("user-" + suffix)
              party <- allocateFun(s"party-$suffix", userContext, user.id)(env)
              parties <- stub(pproto.PartyManagementServiceGrpc.stub(channel), userContext.token)
                .getParties(
                  GetPartiesRequest(parties = Seq(party), identityProviderId = "")
                )
              rights <- stub(uproto.UserManagementServiceGrpc.stub(channel), userContext.token)
                .listUserRights(
                  ListUserRightsRequest(user.id, "")
                )
                .map(_.rights)

            } yield (
              parties.partyDetails.map(_.party) should contain theSameElementsAs Seq(party),
              rights should contain theSameElementsAs Vector(
                uproto.Right(uproto.Right.Kind.CanActAs(uproto.Right.CanActAs(party)))
              ),
            )
          }
        }
      }

      s"deny user allocating $partyType party for another user" taggedAs adminSecurityAsset
        .setAttack(
          attackUnknownResource(threat = s"User allocating $partyType party for another user")
        ) in { implicit env =>
        import env.*
        loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
          expectPermissionDenied {
            val suffix = UUID.randomUUID().toString
            for {
              (_, userContext) <- createUserByAdmin("user-1-" + suffix)
              (anotherUser, _) <- createUserByAdmin("user-2-" + suffix)
              _ <- allocateFun(s"party-$suffix", userContext, anotherUser.id)(env)
            } yield ()
          }
        }
      }

      s"deny user allocating $partyType party without specifying who will own it" taggedAs adminSecurityAsset
        .setAttack(
          attackUnknownResource(threat =
            s"User allocating $partyType party without specifying who will own it"
          )
        ) in { implicit env =>
        import env.*
        loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
          expectPermissionDenied {
            val suffix = UUID.randomUUID().toString
            for {
              (_, userContext) <- createUserByAdmin("user-" + suffix)
              _ <- allocateFun(s"party-$suffix", userContext, "")(env)
            } yield ()
          }
        }
      }

      s"deny user allocating $partyType party above the quota" taggedAs adminSecurityAsset
        .setAttack(
          attackUnknownResource(threat = s"User allocating $partyType party above the quota")
        ) in { implicit env =>
        import env.*
        loggerFactory.suppress(
          AuthServiceJWTSuppressionRule || ApiPartyManagementServiceSuppressionRule
        ) {
          expectPermissionDenied {
            val suffix = UUID.randomUUID().toString
            for {
              (user, userContext) <- createUserByAdmin("user-1-" + suffix)
              _ <- allocateFun(s"party-1-$suffix", userContext, user.id)(env)
              _ <- allocateFun(s"party-2-$suffix", userContext, user.id)(env)
              _ <- allocateFun(s"party-3-$suffix", userContext, user.id)(env)
            } yield ()
          }
        }
      }

      s"deny user allocating $partyType party above the quota, when other parties added by admin" taggedAs adminSecurityAsset
        .setAttack(
          attackUnknownResource(threat = s"User allocating $partyType party above the quota")
        ) in { implicit env =>
        import env.*
        loggerFactory.suppress(
          AuthServiceJWTSuppressionRule || ApiPartyManagementServiceSuppressionRule
        ) {
          expectPermissionDenied {
            val suffix = UUID.randomUUID().toString
            for {
              party1 <- allocateFun(s"party-1-$suffix", asAdmin, "")(env)
              party2 <- allocateFun(s"party-2-$suffix", asAdmin, "")(env)

              (user, userContext) <- createUserByAdmin(
                "user-1-" + suffix,
                rights = Vector(
                  uproto.Right(uproto.Right.Kind.CanReadAs(uproto.Right.CanReadAs(party1))),
                  uproto.Right(uproto.Right.Kind.CanActAs(uproto.Right.CanActAs(party2))),
                ),
              )
              _ <- allocateFun(s"party-3-$suffix", userContext, user.id)(env)
            } yield ()
          }
        }
      }

      s"allow user allocating $partyType party below the quota, when rights contain few distinctive parties" taggedAs adminSecurityAsset
        .setAttack(
          attackUnknownResource(threat = s"User allocating $partyType party above the quota")
        ) in { implicit env =>
        import env.*
        loggerFactory.suppress(
          AuthServiceJWTSuppressionRule || ApiPartyManagementServiceSuppressionRule
        ) {
          expectSuccess {
            val suffix = UUID.randomUUID().toString
            for {
              party1 <- allocateFun(s"party-1-$suffix", asAdmin, "")(env)

              (user, userContext) <- createUserByAdmin(
                "user-1-" + suffix,
                rights = Vector(
                  uproto.Right(uproto.Right.Kind.CanReadAs(uproto.Right.CanReadAs(party1))),
                  uproto.Right(uproto.Right.Kind.CanActAs(uproto.Right.CanActAs(party1))),
                ),
              )
              _ <- allocateFun(s"party-3-$suffix", userContext, user.id)(env)
            } yield ()
          }
        }
      }
    }

    defineTests(
      "internal",
      (partyId, context, userId) => implicit env => allocateParty(context, partyId, userId),
    )
    defineTests(
      "external",
      (partyId, context, userId) => implicit env => allocateExternalParty(context, partyId, userId),
    )
  }

  protected def readAndActRights(readAsParty: String, actAsParty: String): Vector[uproto.Right] =
    Vector(
      uproto.Right(
        uproto.Right.Kind.CanReadAs(uproto.Right.CanReadAs(readAsParty))
      ),
      uproto.Right(uproto.Right.Kind.CanActAs(uproto.Right.CanActAs(actAsParty))),
    )

  override def serviceCallName: String = "GetParties"
}
