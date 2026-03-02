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

import java.util.concurrent.atomic.AtomicInteger

/** Integration tests for retry logic in external calls.
  *
  * Tests:
  * - Successful retry after transient failure
  * - Max retries exhausted
  * - Retry-After header handling (429)
  * - Exponential backoff behavior
  * - Retry on specific error codes
  */
sealed trait RetryExternalCallIntegrationTest
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

  "retry logic for external calls" should {

    "succeed after one transient failure" in { _ =>

      // Fail once, then succeed
      mockServer.setRetryHandler("retry-once", failuresBeforeSuccess = 1)

      // Scenario:
      // 1. First call fails with 503
      // 2. Retry succeeds
      // 3. Transaction completes successfully

      // TODO: Exercise external call
      // Verify call count is 2 (original + 1 retry)

      pending
    }

    "succeed after multiple transient failures" in { _ =>

      // Fail twice, then succeed
      mockServer.setRetryHandler("retry-twice", failuresBeforeSuccess = 2)

      // Scenario: Two retries needed
      // Verify call count is 3

      pending
    }

    "fail when max retries exhausted" in { _ =>

      // Always fail
      mockServer.setErrorHandler("always-fail", 503, "Always failing")

      // Scenario:
      // 1. All retry attempts fail
      // 2. Transaction fails after exhausting retries

      // TODO: Verify transaction fails
      // Verify call count matches configured max retries + 1

      pending
    }

    "respect Retry-After header on 429 response" in { _ =>

      val callTimes = scala.collection.mutable.ListBuffer[Long]()
      val callCount = new AtomicInteger(0)

      mockServer.setHandler("rate-limited") { req =>
        callTimes += System.currentTimeMillis()
        if (callCount.incrementAndGet() == 1) {
          ExternalCallResponse(
            statusCode = 429,
            body = "Rate limited".getBytes,
            headers = Map("Retry-After" -> "2"), // Wait 2 seconds
          )
        } else {
          ExternalCallResponse.ok(req.input)
        }
      }

      // Scenario:
      // 1. First call returns 429 with Retry-After: 2
      // 2. System waits at least 2 seconds
      // 3. Retry succeeds

      // TODO: Verify timing between calls respects Retry-After

      pending
    }

    "respect Retry-After header on 503 response" in { _ =>

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

      // 503 with Retry-After should also be respected

      pending
    }

    "use exponential backoff" in { _ =>

      val callTimes = scala.collection.mutable.ListBuffer[Long]()
      val callCount = new AtomicInteger(0)

      mockServer.setHandler("backoff-test") { req =>
        callTimes += System.currentTimeMillis()
        if (callCount.incrementAndGet() < 4) {
          ExternalCallResponse.error(503, "Fail")
        } else {
          ExternalCallResponse.ok(req.input)
        }
      }

      // Scenario:
      // Verify delays between retries follow exponential pattern
      // e.g., 1s, 2s, 4s (or similar based on config)

      // TODO: Verify timing pattern

      pending
    }

    "add jitter to backoff delays" in { _ =>

      // Multiple calls should have slightly different retry timings
      // due to jitter (randomization in backoff)

      // This helps prevent thundering herd problems

      pending
    }

    "retry on 502 Bad Gateway" in { _ =>

      val callCount = new AtomicInteger(0)
      mockServer.setHandler("bad-gateway") { req =>
        if (callCount.incrementAndGet() == 1) {
          ExternalCallResponse.error(502, "Bad Gateway")
        } else {
          ExternalCallResponse.ok(req.input)
        }
      }

      // 502 should be retryable

      pending
    }

    "not retry on 400 Bad Request" in { _ =>

      mockServer.setErrorHandler("bad-request", 400, "Bad Request")

      // 400 is a client error - should NOT be retried
      // Verify only 1 call is made

      pending
    }

    "not retry on 401 Unauthorized" in { _ =>

      mockServer.setErrorHandler("unauthorized", 401, "Unauthorized")

      // Auth errors should not be retried
      // Verify only 1 call is made

      pending
    }

    "not retry on 403 Forbidden" in { _ =>

      mockServer.setErrorHandler("forbidden", 403, "Forbidden")

      // Permission errors should not be retried

      pending
    }

    "not retry on 404 Not Found" in { _ =>

      mockServer.setErrorHandler("not-found", 404, "Not Found")

      // Resource not found should not be retried

      pending
    }

    "retry on connection reset" in { _ =>

      // Simulate connection being reset mid-request
      // Should trigger retry

      pending
    }

    "handle retry with different result" in { _ =>

      val callCount = new AtomicInteger(0)
      mockServer.setHandler("changing-result") { _ =>
        val count = callCount.incrementAndGet()
        if (count == 1) {
          ExternalCallResponse.error(503, "Fail")
        } else {
          // Return different result on retry - this is a determinism issue
          ExternalCallResponse.ok(s"result-$count".getBytes)
        }
      }

      // Note: If external service is not deterministic,
      // this could cause issues. The result from the successful
      // retry is what gets stored in the transaction.

      pending
    }

    "respect max total timeout across all retries" in { _ =>

      // Even with retries, there should be a max total time limit
      // After which the call fails regardless of retry config

      mockServer.setHandler("slow-fail") { _ =>
        Thread.sleep(5000) // 5 seconds per attempt
        ExternalCallResponse.error(503, "Fail")
      }

      // If max total timeout is, say, 10 seconds, and each attempt
      // takes 5 seconds, we should only get 2 attempts max

      pending
    }

    "maintain idempotency across retries" in { _ =>

      // External service should receive the same request ID
      // on retries to enable idempotency handling

      val requestIds = scala.collection.mutable.Set[String]()
      mockServer.setHandler("idempotent") { req =>
        req.requestId.foreach(requestIds.add)
        if (requestIds.sizeIs == 1) {
          ExternalCallResponse.error(503, "Fail")
        } else {
          ExternalCallResponse.ok(req.input)
        }
      }

      // TODO: Verify same request ID on retry
      // (depends on implementation sending X-Request-Id header)

      pending
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
