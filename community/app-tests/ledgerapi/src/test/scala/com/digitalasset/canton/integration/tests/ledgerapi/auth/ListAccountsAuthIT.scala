// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.user_management_service as proto
import com.daml.ledger.api.v2.admin.user_management_service.Right.{
  CanActAs,
  CanActAsAnyParty,
  CanDebitTrafficFrom,
  CanDebitTrafficFromAnyAccount,
  CanExecuteAs,
  CanExecuteAsAnyParty,
  CanReadAs,
  CanReadAsAnyParty,
  Kind,
  ParticipantAdmin,
}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentDefinition,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.platform.config.TrafficAccountingConfig
import com.digitalasset.canton.tea.v1.{ListAccountsRequest, TrafficServiceGrpc}
import monocle.Monocle.toAppliedFocusOps

import scala.concurrent.Future

final class ListAccountsAuthIT extends ServiceCallWithMainActorAuthTests {
  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.addConfigTransform(
      ConfigTransforms.updateParticipantConfig("participant1")(
        _.focus(_.trafficAccounting).replace(TrafficAccountingConfig(enabled = true))
      )
    )

  override def serviceCallName: String = "TrafficService#ListAccounts"

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] =
    stub(TrafficServiceGrpc.stub(channel), context.token)
      .listAccounts(ListAccountsRequest(pageSize = None, pageToken = None, accountIdPrefix = None))

  private def syncServiceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Any = serviceCall(context).futureValue

  private val allowedRights: Seq[(?, TestConsoleEnvironment => Any)] = Seq(
    (
      CanDebitTrafficFromAnyAccount,
      serviceCallWithMainActorUser(
        "listDebitAny-",
        Vector(
          Kind.CanDebitTrafficFromAnyAccount(
            proto.Right.CanDebitTrafficFromAnyAccount()
          )
        ),
      )(_).futureValue,
    ),
    (CanExecuteAsAnyParty, syncServiceCall(canExecuteAsAnyParty)(_)),
    (CanActAsAnyParty, syncServiceCall(canActAsAnyParty)(_)),
  )

  private val deniedRights: Seq[(?, TestConsoleEnvironment => Future[Any])] = Seq(
    (
      CanDebitTrafficFrom,
      { implicit env: TestConsoleEnvironment =>
        serviceCallWithMainActorUser(
          "listDebitOne-",
          Vector(
            proto.Right.Kind.CanDebitTrafficFrom(proto.Right.CanDebitTrafficFrom(getMainActorId))
          ),
        )
      },
    ),
    (CanExecuteAs, serviceCall(canExecuteAsMainActor)(_)),
    (CanActAs, serviceCall(canActAsMainActor)(_)),
    (CanReadAs, serviceCall(canReadAsMainActor)(_)),
    (CanReadAsAnyParty, serviceCall(canReadAsAnyParty)(_)),
    (ParticipantAdmin, serviceCall(canBeAnAdmin)(_)),
  )

  serviceCallName should {
    allowedRights.foreach { case (rightKind, test) =>
      s"allow calls with ${prettyObj(rightKind)} rights" taggedAs securityAsset
        .setHappyCase(
          s"Ledger API client can list accounts with $rightKind claim"
        ) in test
    }

    deniedRights.foreach { case (rightKind, call) =>
      val kindName = prettyObj(rightKind)
      s"deny calls with $kindName" taggedAs securityAsset.setAttack(
        attackPermissionDenied(threat =
          s"Present a user JWT with only $kindName right to list traffic accounts"
        )
      ) in { implicit env =>
        import env.*
        expectPermissionDenied(call(env))
      }
    }

    def prettyObj(obj: Any): String = obj.getClass.getSimpleName.stripSuffix("$")
  }
}
