// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.user_management_service as proto
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentDefinition,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.platform.config.TrafficAccountingConfig
import com.digitalasset.canton.tea.v1.{GetAccountRequest, TrafficServiceGrpc}
import monocle.Monocle.toAppliedFocusOps

import scala.concurrent.Future

final class GetAccountAuthIT extends SyncServiceCallAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.addConfigTransform(
      ConfigTransforms.updateParticipantConfig("participant1")(
        _.focus(_.trafficAccounting).replace(TrafficAccountingConfig(enabled = true))
      )
    )

  override def serviceCallName: String = "TrafficService#GetAccount"

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] =
    stub(TrafficServiceGrpc.stub(channel), context.token)
      .getAccount(GetAccountRequest(accountId = getMainActorId))

  serviceCallName should {
    "allow calls with execute-as token for the account party" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can get account balance with execute-as claim for the party"
      ) in { implicit env =>
      import env.*
      expectSuccess(serviceCall(canExecuteAsMainActor))
    }
    "allow calls with execute-as-any-party token" taggedAs securityAsset.setHappyCase(
      "Ledger API client can get account balance with execute-as-any-party claim"
    ) in { implicit env =>
      import env.*
      expectSuccess(serviceCall(canExecuteAsAnyParty))
    }
    "allow calls with user token that can-debit-traffic-from the account" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can get account balance with a debit right for the account"
      ) in { implicit env =>
      import env.*
      expectSuccess(
        serviceCallWithMainActorUser(
          "debit1",
          proto.Right.Kind.CanDebitTrafficFrom(proto.Right.CanDebitTrafficFrom(getMainActorId)),
        )
      )
    }
    "deny calls with user token that can-debit-traffic-from another account" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat = "Present a user JWT with a debit right for another account")
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(
        serviceCallWithMainActorUser(
          "debit2",
          proto.Right.Kind.CanDebitTrafficFrom(proto.Right.CanDebitTrafficFrom("acme/billing")),
        )
      )
    }
    "allow calls with user token that can-debit-traffic-from-any-account" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can get account balance with a debit right for any account"
      ) in { implicit env =>
      import env.*
      expectSuccess(
        serviceCallWithMainActorUser(
          "debit3",
          proto.Right.Kind.CanDebitTrafficFromAnyAccount(
            proto.Right.CanDebitTrafficFromAnyAccount()
          ),
        )
      )
    }
  }
}
