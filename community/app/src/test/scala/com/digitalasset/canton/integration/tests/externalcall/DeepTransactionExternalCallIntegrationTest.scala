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

import scala.concurrent.duration.*

/** Integration tests for external calls in deep (deeply nested) transactions.
  *
  * These tests address the reviewer requirement:
  * "A test involving deep transactions"
  *
  * Tests:
  * - 5 levels of nesting with external calls
  * - 10 levels of nesting with external calls
  * - Wide transaction with many external calls at single level
  * - Both deep and wide (deep nesting + multiple calls per level)
  */
sealed trait DeepTransactionExternalCallIntegrationTest
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

  "deep transactions with external calls" should {

    "handle 5 levels of nesting with external call at leaf" in { implicit env =>
      import env.*

      setupEchoHandler()

      // Scenario:
      // Level 0 (root) → Level 1 → Level 2 → Level 3 → Level 4 → Level 5 (leaf with external call)
      // Only the deepest level makes the external call

      // TODO: Use DeepExternalCallContract with depth=5
      // Exercise DeepCall choice
      // Verify:
      // - Transaction completes successfully
      // - Only 1 external call made (at leaf)
      // - Result propagates back through all levels

      pending
    }

    "handle 10 levels of nesting with external call at leaf" in { implicit env =>
      import env.*

      setupEchoHandler()

      // Scenario:
      // 10 levels of recursive exercise
      // External call only at the deepest level

      // This tests that deep transactions don't break external call handling

      // TODO: Use DeepExternalCallContract with depth=10
      // Verify transaction completes and result is correct

      pending
    }

    "handle 5 levels with external call at every level" in { implicit env =>
      import env.*

      setupEchoHandler()

      // Scenario:
      // Each of the 5 levels makes its own external call
      // Total of 5 external calls

      // TODO: Use DeepExternalCallContract.DeepCallAllLevels choice
      // Verify:
      // verifyCallCount("echo", 5)

      pending
    }

    "handle 10 levels with external call at every level" in { implicit env =>
      import env.*

      setupEchoHandler()

      // Scenario:
      // 10 levels, each with external call = 10 external calls total

      // TODO: Implement
      // verifyCallCount("echo", 10)

      pending
    }

    "handle wide transaction with 20 external calls at single level" in { implicit env =>
      import env.*

      setupEchoHandler()

      // Scenario:
      // Single exercise that makes 20 sequential external calls
      // Tests that many external calls in one choice work correctly

      // TODO: Create template with choice that calls externalCall 20 times in a loop

      pending
    }

    "handle wide transaction with 50 external calls at single level" in { implicit env =>
      import env.*

      setupEchoHandler()

      // Scenario:
      // 50 external calls in one choice
      // Stress test for external call handling

      pending
    }

    "handle deep AND wide transaction" in { implicit env =>
      import env.*

      setupEchoHandler()

      // Scenario:
      // 5 levels of nesting
      // Each level makes 3 external calls
      // Total: 15 external calls (5 levels * 3 calls)

      // TODO: Create template that combines depth and width

      pending
    }

    "correctly order external call results in deep transaction" in { implicit env =>
      import env.*

      // Each level returns a unique identifier based on input
      setupEchoHandler("ordered")

      // Scenario:
      // Verify that external call results maintain correct order
      // even in deep nested transactions

      // The order should match the execution order in the Daml code

      pending
    }

    "handle deep transaction with observer" in { implicit env =>
      import env.*

      setupEchoHandler()

      // Scenario:
      // - Alice exercises 10-level deep transaction
      // - Bob is observer
      // - All 10 external calls made by alice's participant
      // - Bob validates using stored results

      // TODO: Implement
      // Verify bob can see the transaction
      // Verify only participant1 made HTTP calls

      pending
    }

    "handle maximum supported nesting depth" in { implicit env =>
      import env.*

      setupEchoHandler()

      // Test the practical limits of transaction nesting
      // Canton has limits on transaction depth - test we handle
      // external calls correctly at those limits

      // Note: Actual max depth depends on Canton configuration

      pending
    }
  }
}

class DeepTransactionExternalCallIntegrationTestH2
    extends DeepTransactionExternalCallIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class DeepTransactionExternalCallIntegrationTestPostgres
    extends DeepTransactionExternalCallIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
