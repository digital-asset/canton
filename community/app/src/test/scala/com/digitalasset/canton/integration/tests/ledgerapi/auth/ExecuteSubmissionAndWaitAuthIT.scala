// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.digitalasset.canton.auth.ClaimApiArea
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.services.SubmitDummyPreparedSubmission
import org.scalatest.Assertion

import scala.concurrent.{ExecutionContext, Future}

// ExecuteSubmissionAndWait authorizes like a submission command
final class ExecuteSubmissionAndWaitAuthIT
    extends SyncServiceCallAuthTests
    with SubmitDummyPreparedSubmission
    with ExecuteAsAuthTests
    with ActAsAnyPartyAuthTests
    with ApiAreaScopeServiceCallAuthTests
    with DebitTrafficFromAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def successfulBehavior(f: Future[Any])(implicit ec: ExecutionContext): Assertion =
    expectInvalidArgument(f)

  override def serviceCallName: String = "InteractiveSubmissionService#ExecuteSubmissionAndWait"
  override def executeAsShouldSucceed: Boolean = true

  override def apiAreaScopeBaseToken = standardToken(mainActorActUser)
  override def apiAreaScopeRestrictions = List(
    List(ClaimApiArea.ledgerExecute.area) -> true,
    List(ClaimApiArea.ledgerAct.area) -> true,
    List(ClaimApiArea.ledgerRead.area) -> false,
  )

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] =
    serviceCallWithAccountId(context, accountId = "")

  override def serviceCallWithAccountId(context: ServiceCallContext, accountId: String)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = {
    import env.*
    for {
      preparedSubmission <- prepareSubmission(
        canActAsMainActor.token,
        getMainActorId,
        context.userId,
      )
      executeResp <- executeSubmissionAndWait(
        context.token,
        context.userId,
        preparedSubmission,
        accountId,
      )
    } yield executeResp

  }

}
