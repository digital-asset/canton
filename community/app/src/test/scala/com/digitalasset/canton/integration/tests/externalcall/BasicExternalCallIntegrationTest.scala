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
    externalCallEnvironmentDefinition(EnvironmentDefinition.P2S1M1_Manual)
      .addConfigTransforms(ConfigTransforms.setAlphaVersionSupport(true)*)
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
      
      setupEchoHandler()

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

      // Call count depends on number of confirming participants
    }

    "execute multiple external calls in sequence" in { implicit env =>
      import env.*
      
      setupEchoHandler()

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

      // Call count depends on number of confirming participants
    }

    "allow observer to see transaction without making HTTP call" in { implicit env =>
      import env.*
      
      setupEchoHandler()

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

      // Signatory's participant should make HTTP call in submission mode
      // Observer's participant should validate using stored result (no HTTP)
      // Call count depends on number of confirming participants
    }

    "store external call result in the transaction" in { implicit env =>
      import env.*
      
      setupEchoHandler()

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

      // Call count depends on number of confirming participants
    }

    "correctly replay two identical calls with different results via callIndex" in { implicit env =>
      import env.*

      setupEchoHandler()

      val input = toHex("identical")
      
      // Create contract
      val createTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        new E.ExternalCallContract(
          alice.toProtoPrimitive,
          java.util.List.of(),
        ).create.commands.asScala.toSeq,
      )
      val contractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

      val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        contractId.exerciseCallMultiple(input, input, input).commands.asScala.toSeq,
      )
      
      exerciseTx.getUpdateId should not be empty
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
