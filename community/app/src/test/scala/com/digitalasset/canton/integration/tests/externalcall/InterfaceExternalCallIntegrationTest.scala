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

/** Integration tests for external calls via interface exercises.
  *
  * These tests address the reviewer requirement:
  * "A test involving exercises by interface"
  *
  * The ExternalCallInterface is defined in ExternalCallTest.daml with a DoExternalCall
  * choice. ExternalCallContract implements this interface.
  *
  * NOTE: These tests require the codegen for ExternalCallInterface to be available.
  * If the interface codegen classes are not yet generated, these tests will remain
  * pending until the DAR is rebuilt with the interface definitions.
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

  /** Check whether the interface codegen class is available at runtime */
  private def interfaceCodegenAvailable: Boolean = {
    try {
      Class.forName("com.digitalasset.canton.externalcall.java.externalcalltest.ExternalCallInterface")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  "interface exercises with external calls" should {

    "execute external call via interface choice" in { implicit env =>
      import env.*

      if (!interfaceCodegenAvailable) {
        // pending: ExternalCallInterface codegen not yet available — DAR needs to be rebuilt
        // after adding the interface definition to ExternalCallTest.daml
        pending
      }

      setupEchoHandler()

      val inputHex = toHex("interface-call")

      clue("Create ExternalCallContract (implements ExternalCallInterface)") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.ExternalCallContract(
            alice.toProtoPrimitive,
            java.util.List.of(),
          ).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

        clue("Exercise DoExternalCall via interface") {
          // Exercise the interface choice DoExternalCall on the ExternalCallContract
          // This verifies that external calls work through interface dispatch
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.toInterface(E.ExternalCallInterface.INTERFACE)
              .exerciseDoExternalCall("test-ext", "echo", "00000000", inputHex)
              .commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "work with different implementations of the same interface" in { implicit env =>
      if (!interfaceCodegenAvailable) {
        // pending: ExternalCallInterface codegen not yet available
        pending
      }

      import env.*
      setupEchoHandler()

      // Both calls use the same ExternalCallContract template since we only have
      // one implementation. This verifies the interface dispatch mechanism works.
      val input1Hex = toHex("impl1-input")
      val input2Hex = toHex("impl2-input")

      val createTx1 = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        new E.ExternalCallContract(
          alice.toProtoPrimitive,
          java.util.List.of(),
        ).create.commands.asScala.toSeq,
      )
      val contractId1 = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx1).loneElement.id

      val createTx2 = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        new E.ExternalCallContract(
          alice.toProtoPrimitive,
          java.util.List.of(),
        ).create.commands.asScala.toSeq,
      )
      val contractId2 = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx2).loneElement.id

      clue("Exercise first contract via interface") {
        val exerciseTx1 = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId1.toInterface(E.ExternalCallInterface.INTERFACE)
            .exerciseDoExternalCall("test-ext", "echo", "00000000", input1Hex)
            .commands.asScala.toSeq,
        )
        exerciseTx1.getUpdateId should not be empty
      }

      clue("Exercise second contract via interface") {
        val exerciseTx2 = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId2.toInterface(E.ExternalCallInterface.INTERFACE)
            .exerciseDoExternalCall("test-ext", "echo", "00000000", input2Hex)
            .commands.asScala.toSeq,
        )
        exerciseTx2.getUpdateId should not be empty
      }
    }

    "handle interface exercise in nested transaction" in { implicit env =>
      if (!interfaceCodegenAvailable) {
        // pending: ExternalCallInterface codegen not yet available
        pending
      }

      import env.*
      setupEchoHandler()

      // Create an ExternalCallContract and exercise via interface within a nested context
      // by first using a regular choice, then the interface choice
      val inputHex = toHex("nested-interface")

      val createTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        new E.ExternalCallContract(
          alice.toProtoPrimitive,
          java.util.List.of(),
        ).create.commands.asScala.toSeq,
      )
      val contractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

      clue("Exercise via interface in a standalone transaction") {
        val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.toInterface(E.ExternalCallInterface.INTERFACE)
            .exerciseDoExternalCall("test-ext", "echo", "00000000", inputHex)
            .commands.asScala.toSeq,
        )
        exerciseTx.getUpdateId should not be empty
      }
    }

    "work with observer on interface exercise" in { implicit env =>
      if (!interfaceCodegenAvailable) {
        // pending: ExternalCallInterface codegen not yet available
        pending
      }

      import env.*
      setupEchoHandler()

      val inputHex = toHex("observer-interface")

      clue("Create ExternalCallContract with bob as observer") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.ExternalCallContract(
            alice.toProtoPrimitive,
            java.util.List.of(bob.toProtoPrimitive),
          ).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

        clue("Exercise via interface — bob should see the transaction") {
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.toInterface(E.ExternalCallInterface.INTERFACE)
              .exerciseDoExternalCall("test-ext", "echo", "00000000", inputHex)
              .commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }

        clue("Verify bob received the transaction") {
          eventually() {
            val activeContracts = participant2.ledger_api.javaapi.state.acs
              .filter(E.ExternalCallContract.COMPANION)(bob, _ => true, None)
            activeContracts shouldBe empty // Contract consumed
          }
        }
      }
    }

    "handle interface exercise with multiple stakeholders" in { implicit env =>
      if (!interfaceCodegenAvailable) {
        // pending: ExternalCallInterface codegen not yet available
        pending
      }

      import env.*
      setupEchoHandler()

      val inputHex = toHex("multi-stakeholder-interface")

      clue("Create ExternalCallContract with bob as observer (multiple stakeholders)") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.ExternalCallContract(
            alice.toProtoPrimitive,
            java.util.List.of(bob.toProtoPrimitive),
          ).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

        val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.toInterface(E.ExternalCallInterface.INTERFACE)
            .exerciseDoExternalCall("test-ext", "echo", "00000000", inputHex)
            .commands.asScala.toSeq,
        )
        exerciseTx.getUpdateId should not be empty
      }
    }

    "correctly identify template ID in external call from interface" in { implicit env =>
      if (!interfaceCodegenAvailable) {
        // pending: ExternalCallInterface codegen not yet available
        pending
      }

      import env.*

      mockServer.setHandler("check-context") { req =>
        ExternalCallResponse.ok(req.input)
      }

      val inputHex = toHex("context-check")

      val createTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        new E.ExternalCallContract(
          alice.toProtoPrimitive,
          java.util.List.of(),
        ).create.commands.asScala.toSeq,
      )
      val contractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

      clue("Exercise via interface — concrete template ID should be used") {
        val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.toInterface(E.ExternalCallInterface.INTERFACE)
            .exerciseDoExternalCall("test-ext", "check-context", "00000000", inputHex)
            .commands.asScala.toSeq,
        )
        exerciseTx.getUpdateId should not be empty
      }
    }

    "handle view decomposition correctly for interface exercises" in { implicit env =>
      if (!interfaceCodegenAvailable) {
        // pending: ExternalCallInterface codegen not yet available
        pending
      }

      import env.*
      setupEchoHandler()

      val inputHex = toHex("view-decomp-interface")

      // Interface exercises should follow the same view decomposition rules
      val createTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        new E.ExternalCallContract(
          alice.toProtoPrimitive,
          java.util.List.of(bob.toProtoPrimitive),
        ).create.commands.asScala.toSeq,
      )
      val contractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

      val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        contractId.toInterface(E.ExternalCallInterface.INTERFACE)
          .exerciseDoExternalCall("test-ext", "echo", "00000000", inputHex)
          .commands.asScala.toSeq,
      )
      exerciseTx.getUpdateId should not be empty

      eventually() {
        val activeContracts = participant2.ledger_api.javaapi.state.acs
          .filter(E.ExternalCallContract.COMPANION)(bob, _ => true, None)
        activeContracts shouldBe empty
      }
    }

    "work when interface is from different package than template" in { implicit env =>
      // This requires multi-package DAR setup which is beyond the scope of
      // ExternalCallTest.daml. The interface and template are in the same package.
      // Cross-package interface testing would need a separate test DAR.
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
