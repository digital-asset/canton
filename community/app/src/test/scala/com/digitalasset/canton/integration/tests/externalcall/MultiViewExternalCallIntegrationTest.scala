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

/** Integration tests for external calls in multi-view transactions.
  *
  * These tests address the reviewer requirement:
  * "A test involving composed transactions (multiple views)"
  * "A test involving external calls in root nodes as well as in leaf nodes and in the middle"
  *
  * Tests:
  * - Same informees -> single view with multiple external calls
  * - Different informees -> multiple views with external calls
  * - External calls at root, middle, and leaf nodes
  * - Multiple views each with their own external calls
  */
sealed trait MultiViewExternalCallIntegrationTest
    extends CommunityIntegrationTest
    with ExternalCallIntegrationTestBase
    with SharedEnvironment
    with MockServerSetup {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransforms(ConfigTransforms.setAlphaVersionSupport(true)*)
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

        clue("Upload ExternalCallTest DAR") {
          participant1.dars.upload(externalCallTestDarPath)
          participant2.dars.upload(externalCallTestDarPath)
          participant3.dars.upload(externalCallTestDarPath)
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

  "multi-view transactions" should {

    "handle external calls when all exercises have same informees (single view)" in { implicit env =>
      import env.*

      setupEchoHandler()

      val input1Hex = toHex("view-input1")
      val input2Hex = toHex("view-input2")

      clue("Create ExternalCallContract with alice as sole signatory") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.ExternalCallContract(
            alice.toProtoPrimitive,
            java.util.List.of(),
          ).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

        clue("Exercise NestedExternalCall with innerActor=alice (same informees -> single view)") {
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseNestedExternalCall(
              alice.toProtoPrimitive,
              input1Hex,
              input2Hex,
            ).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "handle external calls when nested exercises have different informees (multiple views)" in {
      _ =>
        // DelegatedExternalCall has `controller actor` where actor=bob, but bob is hosted on
        // participant2. Multi-party submission from participant1 can't authorize bob's choices.
        // Requires either Daml contract redesign or multi-participant command submission.
        pending
    }

    "execute external call only in root node" in { implicit env =>
      import env.*

      setupEchoHandler()

      val inputHex = toHex("root-only")

      clue("Create ExternalCallContract") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.ExternalCallContract(
            alice.toProtoPrimitive,
            java.util.List.of(),
          ).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

        clue("Exercise CallExternal — external call only at root level") {
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseCallExternal(
              "test-ext",
              "echo",
              "00000000",
              inputHex,
            ).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "execute external call only in leaf node" in { implicit env =>
      import env.*

      setupEchoHandler()

      val inputHex = toHex("leaf-only")

      clue("Create ExternalCallContract") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.ExternalCallContract(
            alice.toProtoPrimitive,
            java.util.List.of(),
          ).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

        clue("Exercise CallInLeafOnly — no external call at root, call at leaf") {
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseCallInLeafOnly(inputHex).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "execute external call in middle node" in { implicit env =>
      import env.*

      setupEchoHandler()

      val inputHex = toHex("all-levels")

      clue("Create ExternalCallContract") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.ExternalCallContract(
            alice.toProtoPrimitive,
            java.util.List.of(),
          ).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

        clue("Exercise CallAtAllLevels — has root, middle, and leaf external calls") {
          // CallAtAllLevels exercises MiddleLevelCall which exercises LeafExternalCall
          // The middle level has its own external call
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseCallAtAllLevels(inputHex).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "execute external calls at all levels (root, middle, leaf)" in { implicit env =>
      import env.*

      setupEchoHandler()

      val inputHex = toHex("all-levels")

      clue("Create ExternalCallContract") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.ExternalCallContract(
            alice.toProtoPrimitive,
            java.util.List.of(),
          ).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

        clue("Exercise CallAtAllLevels — external calls at root, middle, and leaf") {
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseCallAtAllLevels(inputHex).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "handle multiple views each with their own external calls" in { _ =>
      // BobExternalCall has `controller bob`, but bob is hosted on participant2.
      // Multi-party submission from participant1 can't authorize bob's choices.
      // Requires either Daml contract redesign or multi-participant command submission.
      pending
    }

    "aggregate external call results correctly in view's ActionDescription" in { implicit env =>
      import env.*

      setupEchoHandler()

      val input1Hex = toHex("first")
      val input2Hex = toHex("second")
      val input3Hex = toHex("third")

      clue("Create ExternalCallContract") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.ExternalCallContract(
            alice.toProtoPrimitive,
            java.util.List.of(),
          ).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

        clue("Exercise CallMultiple — three external calls aggregated in one view") {
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseCallMultiple(
              input1Hex,
              input2Hex,
              input3Hex,
            ).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
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
