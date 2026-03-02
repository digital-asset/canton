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

/** Integration tests for consensus behavior with external calls.
  *
  * These tests address the reviewer requirement:
  * "A test whereby the http service on the confirming participant doesn't return
  *  the same result as the http service on the preparing participant"
  *
  * Tests:
  * - Transaction rejection when participants receive different results
  * - Consensus failure handling
  * - Observer validation with mismatched stored results
  */
sealed trait ConsensusExternalCallIntegrationTest
    extends CommunityIntegrationTest
    with ExternalCallIntegrationTestBase
    with SharedEnvironment
    with MockServerSetup {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
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

  "consensus with external calls" should {

    "reject transaction when confirming participant receives different result" in { implicit env =>
      import env.*

      // Setup mock to return different results based on the participant
      // This simulates the scenario where the external service is not deterministic
      // or returns different results to different callers
      mockServer.setParticipantSpecificHandler("get-price") { (participantId, _) =>
        val price = if (participantId.contains("participant1")) {
          "100" // Preparing participant gets 100
        } else {
          "200" // Confirming participant gets 200
        }
        ExternalCallResponse.ok(price.getBytes)
      }

      // Scenario:
      // 1. Alice (on participant1) creates a contract with bob as stakeholder
      // 2. Alice exercises a choice that calls external service
      // 3. Participant1 (preparing) gets result "100"
      // 4. Participant2 (confirming for bob) gets result "200"
      // 5. Transaction should be rejected due to result mismatch

      // TODO: Implement
      // val contract = createContract(alice, observers = Seq(bob))
      // val exception = intercept[StatusRuntimeException] {
      //   contract.exerciseCallExternal("test-ext", "get-price", "00000000", toHex("BTC"))
      // }
      // exception.getStatus.getCode shouldBe Code.ABORTED

      pending
    }

    "reject transaction when multiple confirmers disagree with preparer" in { implicit env =>
      import env.*

      // Setup mock where preparer gets one result and both confirmers get different results
      val callCount = new java.util.concurrent.atomic.AtomicInteger(0)
      mockServer.setHandler("unstable") { _ =>
        val count = callCount.incrementAndGet()
        val result = count match {
          case 1 => "result-from-preparer"
          case 2 => "different-result-1"
          case 3 => "different-result-2"
          case _ => "unexpected"
        }
        ExternalCallResponse.ok(result.getBytes)
      }

      // Scenario:
      // - Contract with alice as signatory, bob and charlie as stakeholders
      // - Alice exercises, gets "result-from-preparer"
      // - Bob's participant confirms, gets "different-result-1"
      // - Charlie's participant confirms, gets "different-result-2"
      // - Transaction should be rejected

      pending
    }

    "succeed when all participants receive identical results" in { implicit env =>
      import env.*

      // Setup deterministic handler
      mockServer.setHandler("deterministic") { _ =>
        ExternalCallResponse.ok("always-same-result".getBytes)
      }

      // Scenario:
      // - All participants call the same deterministic service
      // - All receive "always-same-result"
      // - Transaction should succeed

      // TODO: Verify transaction succeeds and all participants see it

      pending
    }

    "handle partial mismatch in multi-view transaction" in { implicit env =>
      import env.*

      // Setup: First external call is deterministic, second is not
      mockServer.setHandler("stable") { _ =>
        ExternalCallResponse.ok("stable-result".getBytes)
      }
      mockServer.setParticipantSpecificHandler("unstable") { (participantId, _) =>
        val result = if (participantId.contains("participant1")) "p1" else "p2"
        ExternalCallResponse.ok(result.getBytes)
      }

      // Scenario:
      // - Transaction with two views
      // - View 1 calls "stable" (all participants agree)
      // - View 2 calls "unstable" (participants disagree)
      // - Transaction should be rejected even though View 1 is consistent

      pending
    }

    "allow observer to validate using stored results without HTTP call" in { implicit env =>
      import env.*

      setupEchoHandler()

      // Scenario:
      // - Alice exercises external call (participant1 makes HTTP call)
      // - Bob is an observer (participant2 does NOT make HTTP call)
      // - Bob validates using stored result from the transaction
      // - Transaction should be visible to Bob

      // Verify:
      // - mockServer receives only 1 call (from participant1)
      // - Bob can see the transaction on participant2

      pending
    }

    "reject when observer's local recomputation doesn't match stored result" in { implicit env =>
      import env.*

      // This tests the scenario where somehow the stored result in the transaction
      // doesn't match what the observer would compute. This shouldn't happen in
      // normal operation but tests the validation logic.

      // NOTE: This may require special test hooks to inject bad data

      pending
    }
  }
}

class ConsensusExternalCallIntegrationTestH2 extends ConsensusExternalCallIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class ConsensusExternalCallIntegrationTestPostgres extends ConsensusExternalCallIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
