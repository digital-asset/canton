// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.jwt.StandardJWTPayload
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.auth.ClaimApiArea

/** Helper to verify authorization based on groups of Ledger API Methods.
  * @see
  *   [[ClaimApiArea]]
  */
trait ApiAreaScopeServiceCallAuthTests extends SuccessfulServiceCallAuthTests {

  /** Defines a base token, which should be able to access the tested service call. */
  def apiAreaScopeBaseToken: StandardJWTPayload

  /** Defines a set of restrictions. For every restriction, we limit [[apiAreaScopeBaseToken]] to
    * that claim, and then check that the specified requested behaviour (allow/deny) holds.
    */
  def apiAreaScopeRestrictions: List[(List[ClaimApiArea.Area], Boolean)]

  serviceCallName should {
    "allow when no token restrictions are present" taggedAs adminSecurityAsset
      .setHappyCase(s"baseline without scope restrictions") in { implicit env =>
      import env.*
      successfulBehavior(
        serviceCall(ServiceCallContext(Some(toHeader(apiAreaScopeBaseToken))))
      )
    }

    for ((restriction, allow) <- apiAreaScopeRestrictions) {
      val scope = restriction.map(_.toJwtScope).mkString(" ")
      val restrictedContext = ServiceCallContext(
        Some(
          toHeader(
            apiAreaScopeBaseToken.copy(
              scope = Some(
                apiAreaScopeBaseToken.scope.fold(scope)(_ + s" $scope")
              )
            )
          )
        )
      )
      if (allow) {
        s"allow when restricted to $scope" taggedAs adminSecurityAsset
          .setHappyCase(s"restrict JWT scope to $scope") in { implicit env =>
          import env.*
          successfulBehavior(serviceCall(restrictedContext))
        }
      } else {
        s"deny when restricted to $scope" taggedAs securityAsset.setAttack(
          attackUnauthenticated(threat = s"restrict JWT scope to $scope")
        ) in { implicit env =>
          import env.*
          expectPermissionDenied(serviceCall(restrictedContext))
        }
      }
    }
  }
}
