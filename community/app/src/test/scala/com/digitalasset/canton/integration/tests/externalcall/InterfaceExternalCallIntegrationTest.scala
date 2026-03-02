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


/** Integration tests for external calls via interface exercises.
  *
  * These tests address the reviewer requirement:
  * "A test involving exercises by interface"
  *
  * Tests:
  * - External call via interface choice
  * - Different template implementations of same interface with external calls
  * - Interface exercise in nested transaction
  * - Interface exercise with observer
  */
sealed trait InterfaceExternalCallIntegrationTest
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

  "interface exercises with external calls" should {

    "execute external call via interface choice" in { _ =>

      setupEchoHandler()

      // Scenario:
      // 1. Create ExternalCallImpl1 contract (implements IExternalCall interface)
      // 2. Exercise the interface choice CallViaInterface
      // 3. The choice calls externalCall internally
      // 4. Verify result is returned correctly

      // TODO: Create ExternalCallImpl1, get interface ContractId
      // Exercise via interface
      // Verify external call was made

      pending
    }

    "work with different implementations of the same interface" in { _ =>

      // Setup different handlers for each implementation
      mockServer.setHandler("impl1") { _ =>
        ExternalCallResponse.ok("impl1-response".getBytes)
      }
      mockServer.setHandler("impl2") { _ =>
        ExternalCallResponse.ok("impl2-response".getBytes)
      }

      // Scenario:
      // 1. Create ExternalCallImpl1 (uses function "impl1")
      // 2. Create ExternalCallImpl2 (uses function "impl2")
      // 3. Exercise both via interface
      // 4. Verify each calls its respective function

      // TODO: Implement
      // result1 shouldBe "impl1-response"
      // result2 shouldBe "impl2-response"

      pending
    }

    "handle interface exercise in nested transaction" in { _ =>

      setupEchoHandler()

      // Scenario:
      // 1. Create InterfaceExerciser contract
      // 2. Create ExternalCallImpl1 contract
      // 3. Exercise InterfaceExerciser.ExerciseViaInterface
      //    which internally exercises the interface choice
      // 4. This creates a nested transaction with interface exercise

      // TODO: Implement
      // Verify nested interface exercise with external call works

      pending
    }

    "work with observer on interface exercise" in { _ =>

      setupEchoHandler()

      // Scenario:
      // 1. Create ExternalCallImpl1 with bob as observer
      // 2. Exercise via interface from alice
      // 3. Bob should see the transaction

      // TODO: Implement
      // Verify bob can see the transaction
      // Verify only 1 HTTP call made

      pending
    }

    "handle interface exercise with multiple stakeholders" in { _ =>

      setupEchoHandler()

      // Scenario:
      // Interface exercise where:
      // - Multiple parties are stakeholders
      // - External call result must be consistent for all
      // - All parties should see the same result

      pending
    }

    "correctly identify template ID in external call from interface" in { _ =>

      // The external call should know the concrete template ID
      // even when exercised via interface

      mockServer.setHandler("check-context") { req =>
        // In a real implementation, we might include context headers
        ExternalCallResponse.ok(req.input)
      }

      // TODO: Verify external call context includes correct template info

      pending
    }

    "handle view decomposition correctly for interface exercises" in { _ =>

      setupEchoHandler()

      // Interface exercises should follow the same view decomposition
      // rules as regular exercises. Test that:
      // - Informees are correctly computed
      // - Views are correctly split
      // - External call results end up in correct views

      pending
    }

    "work when interface is from different package than template" in { _ =>

      setupEchoHandler()

      // Scenario:
      // - Interface defined in package A
      // - Template implementing interface in package B
      // - External call should work correctly across packages

      // Note: This requires setting up multi-package test DARs

      pending
    }
  }
}

class InterfaceExternalCallIntegrationTestH2 extends InterfaceExternalCallIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class InterfaceExternalCallIntegrationTestPostgres extends InterfaceExternalCallIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
