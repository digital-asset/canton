// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.grpc.test.StreamConsumer
import com.daml.ledger.api.v2.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionStreamResponse,
  GetCompletionsRequest,
}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.services.SubmitAndWaitDummyCommand
import io.grpc.stub.StreamObserver

import scala.concurrent.Future

final class GetCompletionsAuthIT
    extends ExpiringStreamServiceCallAuthTests[CompletionStreamResponse]
    with SubmitAndWaitDummyCommand {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "CommandCompletionService#GetCompletions"

  override protected def stream(
      context: ServiceCallContext,
      env: TestConsoleEnvironment,
  ): StreamObserver[CompletionStreamResponse] => Unit =
    streamFor(context)

  private def mkRequest(parties: List[String]) =
    GetCompletionsRequest(parties, 0)

  private def streamFor(
      context: ServiceCallContext
  ): StreamObserver[CompletionStreamResponse] => Unit =
    observer =>
      stub(CommandCompletionServiceGrpc.stub(channel), context.token)
        .getCompletions(mkRequest(List(context.mainActorId)), observer)

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = {
    import env.*
    val mainActorId = getMainActorId
    submitAndWaitAsMainActor(mainActorId).flatMap(_ =>
      new StreamConsumer[CompletionStreamResponse](
        streamFor(context.copy(mainActorId = mainActorId))
      ).first()
    )
  }

  serviceCallName should {
    "allow calls with valid parties" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a GetCompletions call for its own parties"
      ) in { implicit env =>
      import env.*
      expectSuccess(serviceCall(canActAsMainActor))
    }

    "deny calls with empty parties for user without CanReadAsAnyParty" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT without CanReadAsAnyParty and request completions with empty parties"
        )
      ) in { implicit env =>
      import env.*
      val mainActorId = getMainActorId
      expectPermissionDenied(
        submitAndWaitAsMainActor(mainActorId).flatMap { _ =>
          new StreamConsumer[CompletionStreamResponse](observer =>
            stub(CommandCompletionServiceGrpc.stub(channel), canActAsMainActor.token)
              .getCompletions(mkRequest(List.empty), observer)
          ).first()
        }
      )
    }

    "allow calls with empty parties for CanReadAsAnyParty user" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client with CanReadAsAnyParty can call GetCompletions with empty parties"
      ) in { implicit env =>
      import env.*
      val mainActorId = getMainActorId
      expectSuccess(
        submitAndWaitAsMainActor(mainActorId).flatMap { _ =>
          new StreamConsumer[CompletionStreamResponse](observer =>
            stub(CommandCompletionServiceGrpc.stub(channel), canReadAsAnyParty.token)
              .getCompletions(mkRequest(List.empty), observer)
          ).first()
        }
      )
    }

    "deny calls requesting a party the user is not authorized for" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT authorized for the main actor and request completions for a foreign party"
        )
      ) in { implicit env =>
      import env.*
      val mainActorId = getMainActorId
      val foreignParty = getRandomPartyId
      expectPermissionDenied(
        submitAndWaitAsMainActor(mainActorId).flatMap { _ =>
          new StreamConsumer[CompletionStreamResponse](observer =>
            stub(CommandCompletionServiceGrpc.stub(channel), canActAsMainActor.token)
              .getCompletions(mkRequest(List(foreignParty)), observer)
          ).first()
        }
      )
    }
  }
}
