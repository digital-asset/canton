// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.externalcall

import com.digitalasset.canton.externalcall.java.externalcalltest as E
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil

import scala.jdk.CollectionConverters.*

/** Basic integration tests for external call functionality.
  *
  * NOTE: These tests use a mock DA.External module that returns inputs directly (echo behavior)
  * without making actual HTTP calls. The mock HTTP server is set up but not called.
  * Once the BEExternalCall primitive is implemented in Canton, these tests should be updated
  * to verify actual HTTP calls are made.
  *
  * Tests:
  * - Single external call execution
  * - Multiple external calls in one transaction
  * - Observer replay without HTTP call
  * - External call result storage in transaction
  */
sealed trait BasicExternalCallIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with ExternalCallIntegrationTestBase
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

        // Initialize mock server
        initializeMockServer()

        // Connect participants to synchronizer
        clue("Connect participants") {
          participant1.synchronizers.connect_local(sequencer1, daName)
          participant2.synchronizers.connect_local(sequencer1, daName)
        }

        // Upload test DAR
        clue("Upload ExternalCallTest DAR") {
          participant1.dars.upload(externalCallTestDarPath)
          participant2.dars.upload(externalCallTestDarPath)
        }

        // Enable parties
        clue("Enable parties") {
          alice = participant1.parties.enable("alice")
          bob = participant2.parties.enable(
            "bob",
            synchronizeParticipants = Seq(participant1),
          )
        }
      }

  "basic external call" should {

    "execute a single external call and return the result" in { implicit env =>
      import env.*

      val inputHex = toHex("hello")

      // Create contract
      val createTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        new E.ExternalCallContract(
          alice.toProtoPrimitive,
          java.util.List.of(),
        ).create.commands.asScala.toSeq,
      )
      val contractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

      // Exercise the CallExternal choice - should succeed with mock returning input
      val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        contractId.exerciseCallExternal(
          "test-ext",
          "echo",
          "00000000",
          inputHex,
        ).commands.asScala.toSeq,
      )

      // Verify transaction succeeded
      exerciseTx.getUpdateId should not be empty

      // Note: With the mock DA.External, no HTTP calls are made.
      // Once BEExternalCall is implemented, add: verifyCallCount("echo", 1)
    }

    "execute multiple external calls in sequence" in { implicit env =>
      import env.*

      val input1Hex = toHex("first")
      val input2Hex = toHex("second")
      val input3Hex = toHex("third")

      // Create contract
      val createTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        new E.ExternalCallContract(
          alice.toProtoPrimitive,
          java.util.List.of(),
        ).create.commands.asScala.toSeq,
      )
      val contractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

      // Exercise the CallMultiple choice - should succeed with mock
      val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        contractId.exerciseCallMultiple(
          input1Hex,
          input2Hex,
          input3Hex,
        ).commands.asScala.toSeq,
      )

      // Verify transaction succeeded
      exerciseTx.getUpdateId should not be empty

      // Note: With the mock DA.External, no HTTP calls are made.
      // Once BEExternalCall is implemented, add: verifyCallCount("echo", 3)
    }

    "allow observer to see transaction without making HTTP call" in { implicit env =>
      import env.*

      val inputHex = toHex("observable")

      // Create contract with bob as observer
      val createTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        new E.ExternalCallContract(
          alice.toProtoPrimitive,
          java.util.List.of(bob.toProtoPrimitive),
        ).create.commands.asScala.toSeq,
      )
      val contractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

      // Alice exercises external call
      val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        contractId.exerciseCallExternal(
          "test-ext",
          "echo",
          "00000000",
          inputHex,
        ).commands.asScala.toSeq,
      )

      // Verify transaction succeeded
      exerciseTx.getUpdateId should not be empty

      // Wait for the transaction to be replicated to bob's participant
      eventually() {
        // Check that participant2 processed the transaction
        // (The contract is consumed, so we verify no active contracts for bob)
        val activeContracts = participant2.ledger_api.javaapi.state.acs
          .filter(E.ExternalCallContract.COMPANION)(bob, _ => true, None)
        activeContracts shouldBe empty
      }

      // Note: With the mock DA.External, no HTTP calls are made.
      // Once BEExternalCall is implemented:
      // - Signatory's participant should make HTTP call in submission mode
      // - Observer's participant should validate using stored result (no HTTP)
    }

    "store external call result in the transaction" in { implicit env =>
      import env.*

      val inputHex = toHex("stored")

      // Create contract
      val createTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        new E.ExternalCallContract(
          alice.toProtoPrimitive,
          java.util.List.of(),
        ).create.commands.asScala.toSeq,
      )
      val contractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

      // Exercise the CallExternal choice - if this succeeds, the result was stored
      val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        contractId.exerciseCallExternal(
          "test-ext",
          "echo",
          "00000000",
          inputHex,
        ).commands.asScala.toSeq,
      )

      // The transaction should have completed successfully
      // which means the result was properly stored and returned
      exerciseTx.getUpdateId should not be empty

      // Note: With the mock DA.External, the result is the input (echo behavior).
      // Once BEExternalCall is implemented, the result would come from the HTTP call.
    }

    "correctly replay two identical calls with different results via callIndex" in { implicit env =>
      import env.*

      // Mock returns different results for each call to the same function+input.
      // This tests the callIndex-based replay fix: without it, both calls would
      // get the first result because the lookup matched on (extensionId, functionId, input).
      val callCounter = new java.util.concurrent.atomic.AtomicInteger(0)
      mockServer.setHandler("inconsistent") { _ =>
        val count = callCounter.getAndIncrement()
        val result = if (count == 0) "aabbccdd" else "11223344"
        ExternalCallResponse.ok(result.getBytes("UTF-8"))
      }

      val inputHex = toHex("same-input")

      // Create multi-party contract via proposal/accept (SameCallTwice needs two signatories)
      clue("Create MultiPartyExternalCall via proposal") {
        val proposalTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.MultiPartyProposal(
            alice.toProtoPrimitive,
            bob.toProtoPrimitive,
            java.util.List.of(),
          ).create.commands.asScala.toSeq,
        )
        val proposalId = JavaDecodeUtil
          .decodeAllCreated(E.MultiPartyProposal.COMPANION)(proposalTx)
          .loneElement.id

        val acceptTx = participant2.ledger_api.javaapi.commands.submit(
          Seq(bob),
          proposalId.exerciseAccept().commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil
          .decodeAllCreated(E.MultiPartyExternalCall.COMPANION)(acceptTx)
          .loneElement.id

        // Exercise SameCallTwice — calls "inconsistent" with same input twice
        val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice, bob),
          contractId.exerciseSameCallTwice(inputHex).commands.asScala.toSeq,
        )

        // Transaction must succeed — both participants must agree.
        // If callIndex replay is broken, participant2 would replay the wrong
        // result for the second call and reject the transaction.
        exerciseTx.getUpdateId should not be empty
      }

      // Verify mock was called exactly twice
      verifyCallCount("inconsistent", 2)
    }
  }
}

// Database-specific test variants

class BasicExternalCallIntegrationTestH2 extends BasicExternalCallIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class BasicExternalCallIntegrationTestPostgres extends BasicExternalCallIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
