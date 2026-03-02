// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.externalcall

import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2, UsePostgres}
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import io.grpc.Status.Code

import scala.concurrent.duration.*

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

        clue("Enable parties") {
          alice = participant1.parties.enable("alice")
          bob = participant2.parties.enable(
            "bob",
            synchronizeParticipants = Seq(participant1),
          )
        }
      }

  "error handling for external calls" should {

    // === HTTP 4xx Client Errors ===

    "handle HTTP 400 Bad Request" in { implicit env =>
      import env.*

      mockServer.setErrorHandler("bad-request", 400, "Bad Request: Invalid input format")

      // TODO: Exercise external call with function "bad-request"
      // Expect: Transaction fails with appropriate error
      // The error should propagate to the caller

      pending
    }

    "handle HTTP 401 Unauthorized" in { implicit env =>
      import env.*

      mockServer.setErrorHandler("unauthorized", 401, "Unauthorized: Missing or invalid token")

      // Scenario: External service rejects due to auth
      // Transaction should fail with auth error

      pending
    }

    "handle HTTP 403 Forbidden" in { implicit env =>
      import env.*

      mockServer.setErrorHandler("forbidden", 403, "Forbidden: Insufficient permissions")

      // Scenario: Authenticated but not authorized
      // Transaction should fail

      pending
    }

    "handle HTTP 404 Not Found" in { implicit env =>
      import env.*

      mockServer.setErrorHandler("not-found", 404, "Not Found: Resource does not exist")

      // Scenario: Requested resource doesn't exist
      // Transaction should fail

      pending
    }

    // === HTTP 5xx Server Errors ===

    "handle HTTP 500 Internal Server Error" in { implicit env =>
      import env.*

      mockServer.setErrorHandler("server-error", 500, "Internal Server Error")

      // Scenario: External service has internal error
      // Transaction should fail (after retries if configured)

      pending
    }

    "handle HTTP 502 Bad Gateway" in { implicit env =>
      import env.*

      mockServer.setErrorHandler("bad-gateway", 502, "Bad Gateway")

      // Scenario: Proxy/gateway error
      // May be retryable

      pending
    }

    "handle HTTP 503 Service Unavailable" in { implicit env =>
      import env.*

      mockServer.setHandler("unavailable") { _ =>
        ExternalCallResponse(
          statusCode = 503,
          body = "Service Unavailable".getBytes,
          headers = Map("Retry-After" -> "5"),
        )
      }

      // Scenario: Service temporarily unavailable
      // Should respect Retry-After header if configured

      pending
    }

    "handle HTTP 504 Gateway Timeout" in { implicit env =>
      import env.*

      mockServer.setErrorHandler("gateway-timeout", 504, "Gateway Timeout")

      // Scenario: Upstream timeout
      // May be retryable

      pending
    }

    // === Timeout Errors ===

    "handle request timeout" in { implicit env =>
      import env.*

      // Handler that takes longer than request timeout
      mockServer.setHandler("slow") { req =>
        Thread.sleep(30000) // 30 seconds - longer than typical timeout
        ExternalCallResponse.ok(req.input)
      }

      // Scenario: External service is too slow
      // Transaction should fail with timeout error

      // Note: Actual timeout depends on ExtensionServiceConfig.requestTimeout

      pending
    }

    "handle connection timeout" in { implicit env =>
      import env.*

      // This test would need to configure extension pointing to unreachable host
      // e.g., 10.255.255.1 (non-routable IP)

      // Scenario: Cannot establish connection to external service
      // Transaction should fail with connection timeout

      pending
    }

    // === Service Unavailability ===

    "handle connection refused" in { implicit env =>
      import env.*

      // Stop the mock server to simulate service being down
      mockServer.stop()

      // Scenario: External service is not running
      // Transaction should fail with connection refused

      // TODO: Exercise external call
      // Expect connection refused error

      // Restart for other tests
      mockServer.start()

      pending
    }

    // === Configuration Errors ===

    "handle unknown extension ID" in { implicit env =>
      import env.*

      // Scenario: Contract tries to call extension that's not configured
      // External call with extensionId = "unknown-extension"

      // TODO: Exercise CallExternal with extensionId = "nonexistent"
      // Expect: Error indicating unknown extension

      pending
    }

    "handle unknown function ID" in { implicit env =>
      import env.*

      // Extension is configured but function doesn't exist
      // No handler set for this function

      // TODO: Exercise CallExternal with functionId = "nonexistent-function"
      // Expect: 404 from mock server

      pending
    }

    // === Error Message Propagation ===

    "propagate error message from external service" in { implicit env =>
      import env.*

      val errorMessage = "Detailed error: validation failed for field X"
      mockServer.setErrorHandler("detailed-error", 400, errorMessage)

      // Scenario: Verify the error message from external service
      // is properly propagated to the transaction failure

      // TODO: Verify exception contains the error message

      pending
    }

    "handle empty error response body" in { implicit env =>
      import env.*

      mockServer.setHandler("empty-error") { _ =>
        ExternalCallResponse(statusCode = 500, body = Array.empty)
      }

      // Scenario: External service returns error with empty body
      // Should still fail gracefully

      pending
    }

    "handle very large error response body" in { implicit env =>
      import env.*

      val largeError = "X" * 100000 // 100KB error message
      mockServer.setErrorHandler("large-error", 500, largeError)

      // Scenario: External service returns huge error message
      // Should handle without memory issues

      pending
    }

    // === Error Recovery ===

    "allow subsequent calls after error" in { implicit env =>
      import env.*

      // First call fails
      mockServer.setErrorHandler("maybe-fail", 500, "Error")

      // TODO: First exercise fails
      // Reset handler to succeed
      mockServer.setHandler("maybe-fail") { req =>
        ExternalCallResponse.ok(req.input)
      }

      // TODO: Second exercise succeeds
      // Verify system recovers properly

      pending
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
