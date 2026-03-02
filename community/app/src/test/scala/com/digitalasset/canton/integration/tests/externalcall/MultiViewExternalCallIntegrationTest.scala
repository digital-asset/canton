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


/** Integration tests for external calls in multi-view transactions.
  *
  * These tests address the reviewer requirement:
  * "A test involving composed transactions (multiple views)"
  * "A test involving external calls in root nodes as well as in leaf nodes and in the middle"
  *
  * Tests:
  * - Same informees → single view with multiple external calls
  * - Different informees → multiple views with external calls
  * - External calls at root, middle, and leaf nodes
  * - Multiple views each with their own external calls
  */
sealed trait MultiViewExternalCallIntegrationTest
    extends CommunityIntegrationTest
    with ExternalCallIntegrationTestBase
    with SharedEnvironment
    with MockServerSetup {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1 // 3 participants for complex view scenarios
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        enableExternalCallExtensionOnAll("test-ext", mockServerPort),
      )
      .withSetup { implicit env =>
        import env.*

        initializeMockServer()

        clue("Connect participants") {
          participant1.synchronizers.connect_local(sequencer1, daName)
          participant2.synchronizers.connect_local(sequencer1, daName)
          participant3.synchronizers.connect_local(sequencer1, daName)
        }

        // TODO: Upload ExternalCallTest DAR

        clue("Enable parties") {
          alice = participant1.parties.enable("alice")
          bob = participant2.parties.enable(
            "bob",
            synchronizeParticipants = Seq(participant1),
          )
          charlie = participant3.parties.enable(
            "charlie",
            synchronizeParticipants = Seq(participant1, participant2),
          )
        }
      }

  "multi-view transactions" should {

    "handle external calls when all exercises have same informees (single view)" in { _ =>

      setupEchoHandler()

      // Scenario:
      // - Alice exercises NestedExternalCall with innerActor = alice (same party)
      // - Both external calls happen in the same view
      // - Results should be aggregated in the single view's ActionDescription

      // TODO: Implement once DAR and codegen are available
      // val contract = createContract(alice)
      // val (r1, r2) = contract.exerciseNestedExternalCall(
      //   innerActor = alice, // Same informees → single view
      //   input1 = toHex("input1"),
      //   input2 = toHex("input2")
      // )

      pending
    }

    "handle external calls when nested exercises have different informees (multiple views)" in { _ =>
        setupEchoHandler()

        // Scenario:
        // - Alice exercises NestedExternalCall with innerActor = bob (different party)
        // - This creates two views:
        //   - View 1: alice's root exercise (external call 1)
        //   - View 2: bob's delegated exercise (external call 2)
        // - Each view should have its own external call results

        // TODO: Implement
        // Verify both views are created
        // Verify each view has the correct external call result
        // Verify bob on participant2 can validate using stored results

        pending
    }

    "execute external call only in root node" in { _ =>

      mockServer.setHandler("echo") { req =>
        ExternalCallResponse.ok(req.input ++ "-root".getBytes)
      }

      // Scenario:
      // - Root exercise has external call
      // - Nested exercises do NOT have external calls
      // - Only root view should have external call results

      pending
    }

    "execute external call only in leaf node" in { _ =>

      setupEchoHandler()

      // Scenario:
      // - Root exercise does NOT have external call
      // - Deepest nested exercise has external call
      // - Only leaf view should have external call results

      // TODO: Use CallInLeafOnly choice

      pending
    }

    "execute external call in middle node" in { _ =>

      setupEchoHandler()

      // Scenario:
      // - Three levels of nesting: root → middle → leaf
      // - Only middle level has external call
      // - Middle view should have external call results

      pending
    }

    "execute external calls at all levels (root, middle, leaf)" in { _ =>

      mockServer.setHandler("echo") { req =>
        // Echo back with level indicator appended
        ExternalCallResponse.ok(req.input)
      }

      // Scenario:
      // - Root exercises CallAtAllLevels
      // - Each level (root, middle, leaf) makes an external call
      // - Verify all three calls are made
      // - Verify results are stored at each level

      // TODO: Use CallAtAllLevels choice
      // Verify mockServer.getCallCount("echo") == 3
      // Verify each level's result

      pending
    }

    "handle multiple views each with their own external calls" in { _ =>

      setupEchoHandler()

      // Scenario:
      // - Alice creates contract with bob and charlie as potential actors
      // - Exercise creates three views (alice, bob, charlie)
      // - Each view has its own external call
      // - Verify all three external calls are made
      // - Verify each view has the correct result

      pending
    }

    "aggregate external call results correctly in view's ActionDescription" in { _ =>

      setupEchoHandler()

      // Scenario:
      // - Create a complex transaction with multiple external calls
      // - Verify the ActionDescription for each view contains the
      //   aggregated results from all exercise nodes in that view's core

      // This tests the reviewer feedback about:
      // "The core of a view may contain many ExerciseNodes and therefore
      //  must aggregate the external call interactions for all of them"

      pending
    }
  }
}

class MultiViewExternalCallIntegrationTestH2 extends MultiViewExternalCallIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class MultiViewExternalCallIntegrationTestPostgres extends MultiViewExternalCallIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
