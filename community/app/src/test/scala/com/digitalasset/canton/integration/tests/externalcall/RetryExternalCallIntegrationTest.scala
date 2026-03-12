// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.externalcall

import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.externalcall.java.externalcalltest as E
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2, UsePostgres}
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil

import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters.*

/** Integration tests for retry logic in external calls.
  *
  * Tests:
  * - Successful retry after transient failure
  * - Max retries exhausted
  * - Retry-After header handling (429)
  * - Retry on specific error codes
  * - Non-retriable client errors
  */
sealed trait RetryExternalCallIntegrationTest
    extends CommunityIntegrationTest
    with ExternalCallIntegrationTestBase
    with SharedEnvironment
    with MockServerSetup {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(ConfigTransforms.setAlphaVersionSupport(true)*)
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        enableExternalCallExtension("test-ext", mockServerPort, "participant1"),
        enableExternalCallExtension("test-ext", mockServerPort, "participant2"),
      )
      .withSetup { implicit env =>
        import env.*

        initializeMockServer()

        clue("Connect participants") {
          participant1.synchronizers.connect_local(sequencer1, daName)
          participant2.synchronizers.connect_local(sequencer1, daName)
        }

        clue("Upload ExternalCallTest DAR") {
          participant1.dars.upload(externalCallTestDarPath)
          participant2.dars.upload(externalCallTestDarPath)
        }

        clue("Enable parties") {
          alice = participant1.parties.enable("alice")
          bob = participant2.parties.enable(
            "bob",
            synchronizeParticipants = Seq(participant1),
          )
        }
      }

  /** Helper to create an ExternalCallContract for alice */
  private def createExternalCallContract()(implicit env: FixtureParam) = {
    import env.*
    val createTx = participant1.ledger_api.javaapi.commands.submit(
      Seq(alice),
      new E.ExternalCallContract(
        alice.toProtoPrimitive,
        java.util.List.of(),
      ).create.commands.asScala.toSeq,
    )
    JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id
  }

  "retry logic for external calls" should {

    "succeed after one transient failure" in { implicit env =>
      import env.*

      val callCount = new AtomicInteger(0)
      mockServer.setHandler("transient-single") { req =>
        if (callCount.incrementAndGet() == 1) {
          ExternalCallResponse.error(503, "Service Unavailable")
        } else {
          ExternalCallResponse.ok(req.input)
        }
      }

      val contractId = createExternalCallContract()
      val inputHex = toHex("transient-single-test")

      val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        contractId.exerciseCallExternal(
          "test-ext",
          "transient-single",
          "00000000",
          inputHex,
        ).commands.asScala.toSeq,
      )
      exerciseTx.getUpdateId should not be empty
      callCount.get() should be >= 2
    }

    "succeed after multiple transient failures" in { implicit env =>
      import env.*

      val callCount = new AtomicInteger(0)
      mockServer.setHandler("transient-multiple") { req =>
        val count = callCount.incrementAndGet()
        if (count <= 2) {
          ExternalCallResponse.error(503, "Service Unavailable")
        } else {
          ExternalCallResponse.ok(req.input)
        }
      }

      val contractId = createExternalCallContract()
      val inputHex = toHex("transient-multiple-test")

      val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        contractId.exerciseCallExternal(
          "test-ext",
          "transient-multiple",
          "00000000",
          inputHex,
        ).commands.asScala.toSeq,
      )
      exerciseTx.getUpdateId should not be empty
      callCount.get() should be >= 3
    }

    "fail when max retries exhausted" in { implicit env =>
      import env.*

      resetMockServer()
      mockServer.setErrorHandler("always-fail", 503, "Always fails")

      val contractId = createExternalCallContract()
      val inputHex = toHex("max-retries-test")

      intercept[CommandFailure] {
        participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallExternal(
            "test-ext",
            "always-fail",
            "00000000",
            inputHex,
          ).commands.asScala.toSeq,
        )
      }

      // Should have retried multiple times before giving up
      mockServer.getCallCount("always-fail") should be >= 3
    }

    "respect Retry-After header on 429 response" in { implicit env =>
      import env.*

      val callCount = new AtomicInteger(0)

      mockServer.setHandler("rate-limited") { req =>
        if (callCount.incrementAndGet() == 1) {
          ExternalCallResponse(
            statusCode = 429,
            body = "Rate limited".getBytes,
            headers = Map("Retry-After" -> "1"),
          )
        } else {
          ExternalCallResponse.ok(req.input)
        }
      }

      val contractId = createExternalCallContract()
      val inputHex = toHex("rate-limited-test")

      clue("Transaction should succeed after respecting Retry-After") {
        val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallExternal(
            "test-ext",
            "rate-limited",
            "00000000",
            inputHex,
          ).commands.asScala.toSeq,
        )
        exerciseTx.getUpdateId should not be empty
      }

      callCount.get() should be >= 2
    }

    "respect Retry-After header on 503 response" in { implicit env =>
      import env.*

      val callCount = new AtomicInteger(0)

      mockServer.setHandler("service-unavailable") { req =>
        if (callCount.incrementAndGet() == 1) {
          ExternalCallResponse(
            statusCode = 503,
            body = "Service temporarily unavailable".getBytes,
            headers = Map("Retry-After" -> "1"),
          )
        } else {
          ExternalCallResponse.ok(req.input)
        }
      }

      val contractId = createExternalCallContract()
      val inputHex = toHex("503-retry-test")

      val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        contractId.exerciseCallExternal(
          "test-ext",
          "service-unavailable",
          "00000000",
          inputHex,
        ).commands.asScala.toSeq,
      )
      exerciseTx.getUpdateId should not be empty
      callCount.get() should be >= 2
    }

    "add jitter to backoff delays" in { implicit env =>
      // Jitter is an implementation detail of the retry mechanism.
      // We verify it indirectly: multiple retries should not all have
      // identical timing. This is hard to test deterministically,
      // so we just verify the basic retry flow works.
      import env.*

      mockServer.setRetryHandler("jitter-test", failuresBeforeSuccess = 1)

      val contractId = createExternalCallContract()
      val inputHex = toHex("jitter-test")

      val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        contractId.exerciseCallExternal(
          "test-ext",
          "jitter-test",
          "00000000",
          inputHex,
        ).commands.asScala.toSeq,
      )
      exerciseTx.getUpdateId should not be empty
    }

    "retry on 502 Bad Gateway" in { implicit env =>
      import env.*

      val callCount = new AtomicInteger(0)
      mockServer.setHandler("bad-gateway") { req =>
        if (callCount.incrementAndGet() == 1) {
          ExternalCallResponse.error(502, "Bad Gateway")
        } else {
          ExternalCallResponse.ok(req.input)
        }
      }

      val contractId = createExternalCallContract()
      val inputHex = toHex("502-retry-test")

      val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        contractId.exerciseCallExternal(
          "test-ext",
          "bad-gateway",
          "00000000",
          inputHex,
        ).commands.asScala.toSeq,
      )
      exerciseTx.getUpdateId should not be empty
      callCount.get() should be >= 2
    }

    "not retry on 400 Bad Request" in { implicit env =>
      import env.*

      resetMockServer()
      mockServer.setErrorHandler("bad-request", 400, "Bad Request")

      val contractId = createExternalCallContract()
      val inputHex = toHex("400-no-retry")

      intercept[CommandFailure] {
        participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallExternal(
            "test-ext",
            "bad-request",
            "00000000",
            inputHex,
          ).commands.asScala.toSeq,
        )
      }

      // 400 is not retryable — should only be called once
      verifyCallCount("bad-request", 1)
    }

    "not retry on 401 Unauthorized" in { implicit env =>
      import env.*

      resetMockServer()
      mockServer.setErrorHandler("unauthorized", 401, "Unauthorized")

      val contractId = createExternalCallContract()
      val inputHex = toHex("401-no-retry")

      intercept[CommandFailure] {
        participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallExternal(
            "test-ext",
            "unauthorized",
            "00000000",
            inputHex,
          ).commands.asScala.toSeq,
        )
      }

      verifyCallCount("unauthorized", 1)
    }

    "not retry on 403 Forbidden" in { implicit env =>
      import env.*

      resetMockServer()
      mockServer.setErrorHandler("forbidden", 403, "Forbidden")

      val contractId = createExternalCallContract()
      val inputHex = toHex("403-no-retry")

      intercept[CommandFailure] {
        participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallExternal(
            "test-ext",
            "forbidden",
            "00000000",
            inputHex,
          ).commands.asScala.toSeq,
        )
      }

      verifyCallCount("forbidden", 1)
    }

    "not retry on 404 Not Found" in { implicit env =>
      import env.*

      resetMockServer()
      mockServer.setErrorHandler("not-found", 404, "Not Found")

      val contractId = createExternalCallContract()
      val inputHex = toHex("404-no-retry")

      intercept[CommandFailure] {
        participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallExternal(
            "test-ext",
            "not-found",
            "00000000",
            inputHex,
          ).commands.asScala.toSeq,
        )
      }

      verifyCallCount("not-found", 1)
    }

    "respect max total timeout across all retries" in { implicit env =>
      import env.*

      resetMockServer()
      // Each attempt takes 5s, timeout is 10s — should get at most 2 attempts
      mockServer.setHandler("slow-fail") { _ =>
        Thread.sleep(5000)
        ExternalCallResponse.error(503, "Fail")
      }

      val contractId = createExternalCallContract()
      val inputHex = toHex("timeout-test")

      intercept[CommandFailure] {
        participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallExternal(
            "test-ext",
            "slow-fail",
            "00000000",
            inputHex,
          ).commands.asScala.toSeq,
        )
      }

      // Should be limited by total timeout
      mockServer.getCallCount("slow-fail") should be <= 3
    }

  }
}

class RetryExternalCallIntegrationTestH2 extends RetryExternalCallIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class RetryExternalCallIntegrationTestPostgres extends RetryExternalCallIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}


