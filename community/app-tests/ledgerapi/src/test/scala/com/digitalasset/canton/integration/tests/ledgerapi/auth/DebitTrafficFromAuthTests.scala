// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.user_management_service as proto
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.integration.TestConsoleEnvironment
import org.scalatest.Assertion

import scala.concurrent.{ExecutionContext, Future}

trait DebitTrafficFromAuthTests { self: ServiceCallWithMainActorAuthTests =>

  def successfulBehavior(f: Future[Any])(implicit ec: ExecutionContext): Assertion

  def serviceCallWithAccountId(context: ServiceCallContext, accountId: String)(implicit
      env: TestConsoleEnvironment
  ): Future[Any]

  private val accountId = "acme/billing"

  private def createUser(userPrefix: String, rights: Seq[proto.Right.Kind])(implicit
      env: TestConsoleEnvironment
  ): ServiceCallContext = {
    import env.*
    createUserByAdmin(
      userPrefix + mainActor,
      rights = rights.map(proto.Right(_)).toVector,
    ).futureValue._2
  }

  private def canActAsMainActorRight(implicit env: TestConsoleEnvironment): proto.Right.Kind =
    proto.Right.Kind.CanActAs(proto.Right.CanActAs(getMainActorId))

  serviceCallName should {
    "deny calls with an account_id the user can't debit" taggedAs securityAsset.setAttack(
      attackPermissionDenied(threat = "Charge the submission to a traffic account of someone else")
    ) in { implicit env =>
      import env.*
      val context = createUser("debit-denied-", Seq(canActAsMainActorRight))
      expectPermissionDenied(serviceCallWithAccountId(context, accountId))
    }

    "allow calls with an account_id the user can debit" taggedAs securityAsset.setHappyCase(
      "Ledger API client with the can-debit-traffic-from right can charge that account"
    ) in { implicit env =>
      import env.*
      val context = createUser(
        "debit-allowed-",
        Seq(
          canActAsMainActorRight,
          proto.Right.Kind.CanDebitTrafficFrom(proto.Right.CanDebitTrafficFrom(accountId)),
        ),
      )
      successfulBehavior(serviceCallWithAccountId(context, accountId))
    }
  }
}
