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


/** Integration tests for rollback handling with external calls.
  *
  * Tests how external calls interact with Daml's try-catch and exception handling,
  * including rollback scopes.
  *
  * Tests:
  * - External call before a rolled-back scope
  * - External call inside a rolled-back scope
  * - Nested rollback scopes with external calls
  * - External call after catching an exception
  */
sealed trait RollbackExternalCallIntegrationTest
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

  "rollback handling with external calls" should {

    "preserve external call result when made before rolled-back scope" in { _ =>

      setupEchoHandler()

      // Scenario using ExternalCallThenRollback choice:
      // 1. External call is made (result = input)
      // 2. Then a try block fails (e.g., division by zero)
      // 3. Exception is caught
      // 4. External call result should be preserved and returned

      // The external call happened OUTSIDE the try block,
      // so its result should not be affected by the rollback

      // TODO: Exercise ExternalCallThenRollback
      // Verify: result equals input (external call succeeded)
      // Verify: transaction succeeded (exception was caught)

      pending
    }

    "discard external call result when made inside rolled-back scope" in { _ =>

      setupEchoHandler()

      // Scenario using ExternalCallInRollback choice with shouldFail=true:
      // 1. Enter try block
      // 2. External call is made
      // 3. Then the block fails (error called)
      // 4. Exception is caught
      // 5. External call result should be discarded

      // The external call was INSIDE the try block,
      // so when the block rolls back, the call is "undone"
      // (result not stored in transaction)

      // TODO: Exercise ExternalCallInRollback with shouldFail=true
      // Verify: result is None (external call was rolled back)
      // Verify: HTTP call was still made (side effect happened)

      pending
    }

    "preserve external call result when inside try block that succeeds" in { _ =>

      setupEchoHandler()

      // Scenario using ExternalCallInRollback choice with shouldFail=false:
      // 1. Enter try block
      // 2. External call is made
      // 3. Block succeeds (no error)
      // 4. External call result is preserved

      // TODO: Exercise ExternalCallInRollback with shouldFail=false
      // Verify: result is Some(input) (external call preserved)

      pending
    }

    "handle nested rollback scopes with external calls" in { _ =>

      setupEchoHandler()

      // Scenario:
      // try {
      //   external_call_1()  // This should be preserved
      //   try {
      //     external_call_2()  // This should be rolled back
      //     fail()
      //   } catch { ... }
      //   external_call_3()  // This should be preserved
      // }
      //
      // external_call_1 and external_call_3 should be in transaction
      // external_call_2 should NOT be in transaction

      // TODO: Create template with nested try blocks

      pending
    }

    "correctly handle external call followed by exception in same scope" in { _ =>

      val callCount = new java.util.concurrent.atomic.AtomicInteger(0)
      mockServer.setHandler("count") { req =>
        callCount.incrementAndGet()
        ExternalCallResponse.ok(req.input)
      }

      // Scenario:
      // 1. External call succeeds
      // 2. Immediately after, exception is thrown
      // 3. Both are in same scope (no try-catch)
      // 4. Entire transaction fails

      // The HTTP call was made, but transaction doesn't commit
      // So the external call result is lost

      // TODO: Verify HTTP call was made but transaction failed

      pending
    }

    "handle external call in catch block" in { _ =>

      setupEchoHandler()

      // Scenario:
      // try {
      //   fail()
      // } catch {
      //   external_call()  // Call in catch handler
      // }
      //
      // External call in catch block should be preserved

      pending
    }

    "handle multiple external calls with partial rollback" in { _ =>

      val callOrder = scala.collection.mutable.ListBuffer[String]()
      mockServer.setHandler("track") { req =>
        val input = new String(req.input, "UTF-8")
        callOrder += input
        ExternalCallResponse.ok(req.input)
      }

      // Scenario:
      // external_call("A")  // preserved
      // try {
      //   external_call("B")  // rolled back
      //   fail()
      // } catch { ... }
      // external_call("C")  // preserved
      //
      // Final result should have A and C but not B

      // TODO: Verify callOrder contains A, B, C (all HTTP calls made)
      // But transaction only contains results for A and C

      pending
    }

    "maintain rollback scope in deep transaction with external calls" in { _ =>

      setupEchoHandler()

      // Scenario:
      // Deep nesting where rollback happens at intermediate level
      // level0 -> level1 -> level2 (external_call) -> level3 (fail)
      //                           ^--- rollback scope starts here
      //
      // External call at level2 should be rolled back

      pending
    }

    "handle external call result in transaction with multiple rollbacks" in { _ =>

      setupEchoHandler()

      // Scenario:
      // Multiple independent try-catch blocks, each with external calls
      // Some succeed, some roll back
      // Verify correct results are in final transaction

      pending
    }

    "correctly order external call results with rollbacks" in { _ =>

      // Verify that external call results maintain correct order
      // even when some calls are rolled back
      //
      // Order in final transaction should match execution order
      // of non-rolled-back calls

      pending
    }

    "handle exception thrown by external call itself" in { _ =>

      mockServer.setErrorHandler("throw-error", 500, "Service error")

      // Scenario:
      // try {
      //   external_call()  // This fails with 500
      // } catch {
      //   // Handle the error
      //   return fallback_value
      // }
      //
      // The external call failure should be catchable

      // Note: Depends on how external call errors are surfaced to Daml

      pending
    }
  }
}

class RollbackExternalCallIntegrationTestH2 extends RollbackExternalCallIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class RollbackExternalCallIntegrationTestPostgres extends RollbackExternalCallIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
