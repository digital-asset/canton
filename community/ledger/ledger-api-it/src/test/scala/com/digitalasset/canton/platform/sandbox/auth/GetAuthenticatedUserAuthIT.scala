// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v1.admin.user_management_service.{
  GetUserRequest,
  GetUserResponse,
  User,
  UserManagementServiceGrpc,
}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import org.scalatest.Assertion

import java.util.UUID
import scala.concurrent.Future

/** Tests covering the special behaviour of GetUser wrt the authenticated user. */
class GetAuthenticatedUserAuthIT extends ServiceCallAuthTests {
  private val testId = UUID.randomUUID().toString

  override def serviceCallName: String = "UserManagementService#GetUser(<authenticated-user>)"

  override def serviceCall(context: ServiceCallContext): Future[Any] =
    stub(UserManagementServiceGrpc.stub(channel), context.token)
      .getUser(GetUserRequest(identityProviderId = context.identityProviderId))

  private def expectUser(context: ServiceCallContext, expectedUser: User): Future[Assertion] =
    serviceCall(context).map(assertResult(GetUserResponse(Some(expectedUser)))(_))

  private def getUser(context: ServiceCallContext, userId: String) =
    stub(UserManagementServiceGrpc.stub(channel), context.token)
      .getUser(GetUserRequest(userId, identityProviderId = context.identityProviderId))

  behavior of serviceCallName

  it should "deny unauthenticated access" taggedAs securityAsset.setAttack(
    attackUnauthenticated(threat = "Do not present JWT")
  ) in {
    expectUnauthenticated(serviceCall(noToken))
  }

  it should "deny access for a standard token referring to an unknown user" taggedAs securityAsset
    .setAttack(
      attackPermissionDenied(threat = "Present JWT with an unknown user")
    ) in {
    expectPermissionDenied(serviceCall(canReadAsUnknownUserStandardJWT))
  }

  it should "return the 'participant_admin' user when using its standard token" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a call with a standard JWT"
    ) in {
    expectUser(
      canReadAsAdminStandardJWT,
      User(
        "participant_admin",
        "",
        isDeactivated = false,
        metadata = Some(ObjectMeta("0", Map.empty)),
      ),
    )
  }

  it should "return invalid argument for custom token" taggedAs securityAsset.setAttack(
    attackInvalidArgument(threat = "Present a custom JWT")
  ) in {
    expectInvalidArgument(serviceCall(canReadAsAdmin))
  }

  it should "allow access to a non-admin user's own user record" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can read non-admin user's own record"
    ) in {
    for {
      // admin creates user
      (alice, aliceTokenContext) <- createUserByAdmin(testId + "-alice")
      // user accesses its own user record without specifying the id
      aliceRetrieved1 <- getUser(aliceTokenContext, "")
      // user accesses its own user record with specifying the id
      aliceRetrieved2 <- getUser(aliceTokenContext, alice.id)
      expected = GetUserResponse(Some(alice))
    } yield assertResult((expected, expected))((aliceRetrieved1, aliceRetrieved2))
  }
}