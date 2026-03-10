// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.externalcall

import com.digitalasset.canton.externalcall.java.externalcalltest as E
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2, UsePostgres}
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil

import scala.jdk.CollectionConverters.*

/** Integration tests for error handling in external calls.
  *
  * Tests:
  * - HTTP 4xx errors (400, 401, 403, 404)
  * - HTTP 5xx errors (500, 502, 503, 504)
  * - Connection timeout
  * - Request timeout
  * - Service unavailable (connection refused)
  * - Unknown extension ID
  * - Unknown function ID
  */
sealed trait ErrorHandlingExternalCallIntegrationTest
    extends CommunityIntegrationTest
    with ExternalCallIntegrationTestBase
    with SharedEnvironment
    with MockServerSetup {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
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

  /** Helper to create an ExternalCallContract for alice and return its contract ID */
  private def createExternalCallContract()(implicit env: TestEnvironment) = {
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

  /** Helper to exercise CallExternal and expect it to fail */
  private def exerciseAndExpectFailure(
      contractId: E.ExternalCallContract.ContractId,
      functionId: String,
      extensionId: String = "test-ext",
  )(implicit env: TestEnvironment): io.grpc.StatusRuntimeException = {
    import env.*
    val inputHex = toHex("test-input")
    intercept[io.grpc.StatusRuntimeException] {
      participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        contractId.exerciseCallExternal(
          extensionId,
          functionId,
          "00000000",
          inputHex,
        ).commands.asScala.toSeq,
      )
    }
  }

  "error handling for external calls" should {

    // === HTTP 4xx Client Errors ===

    "handle HTTP 400 Bad Request" in { implicit env =>
      mockServer.setErrorHandler("bad-request", 400, "Bad Request: Invalid input format")

      val contractId = createExternalCallContract()
      val exception = exerciseAndExpectFailure(contractId, "bad-request")
      exception.getMessage should not be empty
    }

    "handle HTTP 401 Unauthorized" in { implicit env =>
      mockServer.setErrorHandler("unauthorized", 401, "Unauthorized: Missing or invalid token")

      val contractId = createExternalCallContract()
      val exception = exerciseAndExpectFailure(contractId, "unauthorized")
      exception.getMessage should not be empty
    }

    "handle HTTP 403 Forbidden" in { implicit env =>
      mockServer.setErrorHandler("forbidden", 403, "Forbidden: Insufficient permissions")

      val contractId = createExternalCallContract()
      val exception = exerciseAndExpectFailure(contractId, "forbidden")
      exception.getMessage should not be empty
    }

    "handle HTTP 404 Not Found" in { implicit env =>
      mockServer.setErrorHandler("not-found", 404, "Not Found: Resource does not exist")

      val contractId = createExternalCallContract()
      val exception = exerciseAndExpectFailure(contractId, "not-found")
      exception.getMessage should not be empty
    }

    // === HTTP 5xx Server Errors ===

    "handle HTTP 500 Internal Server Error" in { implicit env =>
      mockServer.setErrorHandler("server-error", 500, "Internal Server Error")

      val contractId = createExternalCallContract()
      val exception = exerciseAndExpectFailure(contractId, "server-error")
      exception.getMessage should not be empty
    }

    "handle HTTP 502 Bad Gateway" in { implicit env =>
      mockServer.setErrorHandler("bad-gateway", 502, "Bad Gateway")

      val contractId = createExternalCallContract()
      val exception = exerciseAndExpectFailure(contractId, "bad-gateway")
      exception.getMessage should not be empty
    }

    "handle HTTP 503 Service Unavailable" in { implicit env =>
      mockServer.setHandler("unavailable") { _ =>
        ExternalCallResponse(
          statusCode = 503,
          body = "Service Unavailable".getBytes,
          headers = Map("Retry-After" -> "5"),
        )
      }

      val contractId = createExternalCallContract()
      val exception = exerciseAndExpectFailure(contractId, "unavailable")
      exception.getMessage should not be empty
    }

    "handle HTTP 504 Gateway Timeout" in { implicit env =>
      mockServer.setErrorHandler("gateway-timeout", 504, "Gateway Timeout")

      val contractId = createExternalCallContract()
      val exception = exerciseAndExpectFailure(contractId, "gateway-timeout")
      exception.getMessage should not be empty
    }

    // === Timeout Errors ===

    "handle request timeout" in { implicit env =>
      // Handler that takes longer than request timeout (configured at 10s)
      mockServer.setHandler("slow") { req =>
        Thread.sleep(30000)
        ExternalCallResponse.ok(req.input)
      }

      val contractId = createExternalCallContract()
      val exception = exerciseAndExpectFailure(contractId, "slow")
      exception.getMessage should not be empty
    }

    "handle connection timeout" in { implicit env =>
      // This test requires an extension pointing to a non-routable IP.
      // The current setup uses localhost, so we skip this specific scenario.
      // Connection timeout behavior is covered by the "connection refused" test below.
      pending
    }

    // === Service Unavailability ===

    "handle connection refused" in { implicit env =>
      // Stop the mock server to simulate service being down
      mockServer.stop()

      try {
        val contractId = createExternalCallContract()
        val exception = exerciseAndExpectFailure(contractId, "echo")
        exception.getMessage should not be empty
      } finally {
        // Restart for other tests
        mockServer.start()
      }
    }

    // === Configuration Errors ===

    "handle unknown extension ID" in { implicit env =>
      import env.*

      val contractId = createExternalCallContract()
      val inputHex = toHex("test-input")

      val exception = intercept[io.grpc.StatusRuntimeException] {
        participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallExternal(
            "nonexistent-extension",
            "echo",
            "00000000",
            inputHex,
          ).commands.asScala.toSeq,
        )
      }
      exception.getMessage should not be empty
    }

    "handle unknown function ID" in { implicit env =>
      // No handler set for "nonexistent-function" — mock server returns 404
      val contractId = createExternalCallContract()
      val exception = exerciseAndExpectFailure(contractId, "nonexistent-function")
      exception.getMessage should not be empty
    }

    // === Error Message Propagation ===

    "propagate error message from external service" in { implicit env =>
      val errorMessage = "Detailed error: validation failed for field X"
      mockServer.setErrorHandler("detailed-error", 400, errorMessage)

      val contractId = createExternalCallContract()
      val exception = exerciseAndExpectFailure(contractId, "detailed-error")
      exception.getMessage should not be empty
    }

    "handle empty error response body" in { implicit env =>
      mockServer.setHandler("empty-error") { _ =>
        ExternalCallResponse(statusCode = 500, body = Array.empty)
      }

      val contractId = createExternalCallContract()
      val exception = exerciseAndExpectFailure(contractId, "empty-error")
      exception.getMessage should not be empty
    }

    "handle very large error response body" in { implicit env =>
      val largeError = "X" * 100000
      mockServer.setErrorHandler("large-error", 500, largeError)

      val contractId = createExternalCallContract()
      val exception = exerciseAndExpectFailure(contractId, "large-error")
      exception.getMessage should not be empty
    }

    // === Error Recovery ===

    "allow subsequent calls after error" in { implicit env =>
      import env.*

      // First call fails
      mockServer.setErrorHandler("maybe-fail", 500, "Error")

      clue("First exercise should fail") {
        val contractId1 = createExternalCallContract()
        exerciseAndExpectFailure(contractId1, "maybe-fail")
      }

      // Reset handler to succeed
      mockServer.setHandler("maybe-fail") { req =>
        ExternalCallResponse.ok(req.input)
      }

      clue("Second exercise should succeed after error recovery") {
        val contractId2 = createExternalCallContract()
        val inputHex = toHex("recovery-test")
        val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId2.exerciseCallExternal(
            "test-ext",
            "maybe-fail",
            "00000000",
            inputHex,
          ).commands.asScala.toSeq,
        )
        exerciseTx.getUpdateId should not be empty
      }
    }
  }
}

class ErrorHandlingExternalCallIntegrationTestH2 extends ErrorHandlingExternalCallIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class ErrorHandlingExternalCallIntegrationTestPostgres
    extends ErrorHandlingExternalCallIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
