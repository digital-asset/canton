// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import java.util.concurrent.ScheduledExecutorService
import com.daml.ledger.api.refinements.ApiTypes.WorkflowId
import com.daml.ledger.client.binding.{Contract, Primitive => P}
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.topology.UniqueIdentifier
import com.digitalasset.canton.participant.admin.workflows.{PingPong => M}
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, SimClock}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Var",
    "com.digitalasset.canton.DiscardedFuture", // TODO(#8448) Do not discard futures
  )
)
class PingServiceTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with BeforeAndAfterEach {

  var scheduler: ScheduledExecutorService = _
  var service: PingService = _

  override def afterEach(): Unit = {
    Option(service).foreach(_.close())
    Option(scheduler).foreach(_.shutdown())
  }

  "PingServiceTest" should {

    val clock = new SimClock(loggerFactory = loggerFactory)
    val alice = Converters.toParty(UniqueIdentifier.tryFromProtoPrimitive("alice::default"))
    val bobId = "bob::default"
    val charlieId = "Charlie::default"

    def setupTest(recipients: Set[String], pingTimeoutMs: Long, gracePeriod: Long) = {
      val submitter = new MockLedgerSubmit(logger)
      scheduler = Threading.singleThreadScheduledExecutor("ping-service-tests", logger)
      service = new PingService(
        submitter,
        alice,
        maxLevelSupported = 10,
        pingDeduplicationTime = NonNegativeFiniteDuration.ofMinutes(5),
        isActive = true,
        loggerFactory,
        clock,
      )(parallelExecutionContext, scheduler)
      val res = service.ping(recipients, Set(), pingTimeoutMs, gracePeriod)
      val cmd = Await.result(submitter.lastCommand, 10.seconds)
      val id = for {
        createCmd <- cmd.command.create
        record <- createCmd.getCreateArguments.fields.find(_.label == "id")
        value <- record.value
      } yield value.getText
      (service, id.value, res)
    }

    def verifySuccess(pingRes: Future[PingService.Result]) = {
      val pinged = Await.result(pingRes, 10.seconds)
      pinged match {
        case PingService.Success(_, _) => ()
        case PingService.Failure => fail("ping test failed")
      }
    }

    def verifyFailure(pingRes: Future[PingService.Result]) = {
      val pinged = Await.result(pingRes, 10.seconds)
      pinged match {
        case PingService.Success(_, _) => fail("ping test should have failed but didnt")
        case PingService.Failure => ()
      }
    }

    def respond(id: String, responder: String, service: PingService, observers: Set[String]) =
      service.processPongs(
        Seq(
          Contract(
            P.ContractId("12345"),
            M.Pong(id, alice, List(), P.Party(responder), observers.map(P.Party(_)).toList),
            None,
            observers.toSeq,
            observers.toSeq,
            None,
          )
        ),
        WorkflowId("workflowId"),
      )

    "happy ping path of single ping is reported correctly" in {
      val recipients = Set(bobId)
      val (service, id, pingRes) = setupTest(recipients, 5000, 5)
      respond(id, bobId, service, recipients)
      verifySuccess(pingRes)
    }

    "happy bong is reported correctly" in {
      val recipients = Set(bobId, charlieId)
      val (service, id, pingRes) = setupTest(recipients, 5000, 5)
      respond(id, charlieId, service, recipients)
      verifySuccess(pingRes)
    }

    "ping times out gracefully" in {
      val (_, _, pingRes) = setupTest(Set(bobId, charlieId), 2, 5)
      verifyFailure(pingRes)
    }

    "discovers duplicate pongs correctly" in {
      loggerFactory.suppressWarningsAndErrors {
        val recipients = Set(bobId, charlieId)
        val (service, id, pingRes) = setupTest(recipients, 5000, 2000)
        // we don't really care about the results of these responses
        // but we want to avoid running them concurrently to mirror how transactions are processed (one at a time)
        respond(id, charlieId, service, recipients)
          .flatMap(_ => respond(id, bobId, service, Set(bobId, charlieId)))

        verifyFailure(pingRes)
      }
    }
  }
}
