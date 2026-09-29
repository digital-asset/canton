// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.state_service.ConvertRecordTimeToOffsetRequest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.util.GrpcServices.StateService
import org.scalatest.Assertion

import scala.concurrent.{ExecutionContext, Future}

final class ConvertRecordTimeToOffsetAuthIT extends PublicServiceCallAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "StateService#ConvertRecordTimeToOffset"

  private def request(implicit env: TestConsoleEnvironment) = ConvertRecordTimeToOffsetRequest(
    Some(CantonTimestamp.MinValue.toProtoTimestamp),
    env.synchronizer1Id.logical.toProtoPrimitive,
  )

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] =
    stub(StateService.stub(channel), context.token).convertRecordTimeToOffset(request)

  override protected def expectSuccess(f: Future[Any])(implicit ec: ExecutionContext): Assertion =
    // If we request existing record time, the test was flaky in CI due to fact, that we cannot easily detect the exact time when messages are exchanges with synchronizer. With CantonTimetamp.MinValue we always have failure to detect.
    // This way we always expect NOT_FOUND when the authorization succeeded.
    expectUnknownResource(
      f
    )

}
