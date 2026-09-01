// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentDefinition,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.platform.config.TrafficAccountingConfig
import com.digitalasset.canton.tea.v1.{TrafficServiceGrpc, UpdateAccountRequest}
import com.google.protobuf.timestamp.Timestamp
import monocle.Monocle.toAppliedFocusOps

import java.util.UUID
import scala.concurrent.Future

abstract class TrafficServiceAdminAuthIT extends AdminServiceCallAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.addConfigTransform(
      ConfigTransforms.updateParticipantConfig("participant1")(
        _.focus(_.trafficAccounting).replace(TrafficAccountingConfig(enabled = true))
      )
    )
}

final class UpdateAccountAuthIT extends TrafficServiceAdminAuthIT {
  override def serviceCallName: String = "TrafficService#UpdateAccount"

  private val accountId = "UpdateAccountAuthIT-account-" + UUID.randomUUID().toString

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] =
    stub(TrafficServiceGrpc.stub(channel), context.token)
      .updateAccount(
        UpdateAccountRequest(
          accountId = accountId,
          balanceDelta = Some(1L),
          deduplicationId = UUID.randomUUID().toString,
        )
      )
}

final class PruneEventsAuthIT extends TrafficServiceAdminAuthIT {
  override def serviceCallName: String = "TrafficService#PruneEvents"

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] =
    stub(TrafficServiceGrpc.stub(channel), context.token)
      .pruneEvents(
        com.digitalasset.canton.tea.v1.PruneEventsRequest(
          beforeOrAt = Some(Timestamp.defaultInstance)
        )
      )
}
