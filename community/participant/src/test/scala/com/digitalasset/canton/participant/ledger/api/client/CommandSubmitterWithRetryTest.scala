// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.client

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Flow
import com.daml.error.{ErrorCategory, ErrorClass, ErrorCode}
import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.client.services.commands.CommandSubmission
import com.daml.util.Ctx
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.participant.ledger.api.client.CommandSubmitterWithRetry.CommandsCtx
import com.google.rpc.code.Code
import com.google.rpc.status.Status
import org.scalatest.*
import org.scalatest.wordspec.FixtureAsyncWordSpec

import scala.concurrent.Await
import scala.concurrent.duration.*

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
class CommandSubmitterWithRetryTest extends FixtureAsyncWordSpec with BaseTest {
  private val maxRetries = 10
  private val commands = Commands(
    ledgerId = "ledgerId",
    workflowId = "workflowId",
    applicationId = "applicationId",
    commandId = "commandId",
    party = "party",
    commands = Nil,
  )

  final class Fixture {
    implicit val system: ActorSystem = ActorSystem(
      classOf[CommandSubmitterWithRetryTest].getSimpleName
    )
    implicit val materializer: Materializer = Materializer(system)
    private var sut: CommandSubmitterWithRetry = _

    def createSut(completion: => Completion): CommandSubmitterWithRetry = {
      val flow = Flow[Ctx[CommandsCtx, CommandSubmission]]
        .map[Ctx[CommandsCtx, Completion]](ctx => Ctx(ctx.context, completion))
      sut =
        new CommandSubmitterWithRetry(10, flow, DefaultProcessingTimeouts.testing, loggerFactory)
      sut
    }

    def close(): Unit = {
      sut.close()
      materializer.shutdown()
      Await.result(system.terminate(), 10.seconds)
    }
  }

  override type FixtureParam = Fixture

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val fixture = new Fixture

    complete {
      super.withFixture(test.toNoArgAsyncTest(fixture))
    } lastly {
      fixture.close()
    }
  }

  "CommandSubmitterWithRetry" should {
    "complete successfully" in { f =>
      val completion = Completion("commandId", Some(Status(Code.OK.value, "all good", Nil)), "txId")
      val sut = f.createSut(completion)
      for {
        result <- sut.submitCommands(commands)
      } yield result shouldBe CommandSubmitterWithRetry.Success(completion)
    }

    "complete with failure" in { f =>
      val completion =
        Completion(
          "commandId",
          Some(Status(Code.FAILED_PRECONDITION.value, "oh no man", Nil)),
          "txId",
        )
      val sut = f.createSut(completion)
      for {
        result <- sut.submitCommands(commands)
      } yield result shouldBe CommandSubmitterWithRetry.Failed(completion)
    }

    "retry on timeouts at most given maxRetries" in { f =>
      var timesCalled = 0
      val code =
        new ErrorCode(id = "TIMEOUT", ErrorCategory.ContentionOnSharedResources)(
          new ErrorClass(Nil)
        ) {
          override def errorConveyanceDocString: Option[String] = None
        }
      val completion =
        Completion(
          "commandId",
          Some(Status(Code.ABORTED.value, code.toMsg(s"now try that", None), Nil)),
          "txId",
        )
      val sut = f.createSut({
        timesCalled = timesCalled + 1
        completion
      })
      loggerFactory.suppressWarningsAndErrors {
        for {
          result <- sut.submitCommands(commands)
        } yield {
          result shouldBe CommandSubmitterWithRetry.MaxRetriesReached(completion)
          timesCalled shouldBe maxRetries + 1
        }
      }
    }

    "Gracefully reject commands submitted after closing" in { f =>
      val sut = f.createSut(fail("The command should not be submitted."))
      sut.close()
      for {
        result <- sut.submitCommands(commands)
      } yield {
        val status =
          Status(
            code = Code.UNAVAILABLE.value,
            message = "Command rejected, as the participant is shutting down.",
          )
        val completion = Completion(commandId = commands.commandId, status = Some(status))

        result shouldBe CommandSubmitterWithRetry.Failed(completion)
      }
    }

  }

}
