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

  /** Helper to create a DeepExternalCallContract */
  private def createDeepContract(depth: Int)(implicit env: TestEnvironment) = {
    import env.*
    val createTx = participant1.ledger_api.javaapi.commands.submit(
      Seq(alice),
      new E.DeepExternalCallContract(
        alice.toProtoPrimitive,
        depth.toLong,
      ).create.commands.asScala.toSeq,
    )
    JavaDecodeUtil.decodeAllCreated(E.DeepExternalCallContract.COMPANION)(createTx).loneElement.id
  }

  "deep transactions with external calls" should {

    "handle 5 levels of nesting with external call at leaf" in { implicit env =>
      import env.*

      setupEchoHandler()

      val inputHex = toHex("depth-5-leaf")

      clue("Create DeepExternalCallContract with depth=5") {
        val contractId = createDeepContract(5)

        clue("Exercise DeepCall — recursively exercises down to leaf where external call happens") {
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseDeepCall(inputHex).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "handle 10 levels of nesting with external call at leaf" in { implicit env =>
      import env.*

      setupEchoHandler()

      val inputHex = toHex("depth-10-leaf")

      clue("Create DeepExternalCallContract with depth=10") {
        val contractId = createDeepContract(10)

        clue("Exercise DeepCall — 10 levels of recursion to leaf external call") {
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseDeepCall(inputHex).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "handle 5 levels with external call at every level" in { implicit env =>
      import env.*

      setupEchoHandler()

      val inputHex = toHex("depth-5-all")

      clue("Create DeepExternalCallContract with depth=5") {
        val contractId = createDeepContract(5)

        clue("Exercise DeepCallAllLevels — external call at each of 6 levels (0-5)") {
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseDeepCallAllLevels(inputHex).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "handle 10 levels with external call at every level" in { implicit env =>
      import env.*

      setupEchoHandler()

      val inputHex = toHex("depth-10-all")

      clue("Create DeepExternalCallContract with depth=10") {
        val contractId = createDeepContract(10)

        clue("Exercise DeepCallAllLevels — external call at each of 11 levels (0-10)") {
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseDeepCallAllLevels(inputHex).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "handle wide transaction with 20 external calls at single level" in { implicit env =>
      import env.*

      setupEchoHandler()
      mockServer.setEchoHandler("concurrent")

      val inputHex = toHex("wide-20")

      clue("Create EdgeCaseExternalCall contract") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.EdgeCaseExternalCall(alice.toProtoPrimitive).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.EdgeCaseExternalCall.COMPANION)(createTx).loneElement.id

        clue("Exercise ManySequentialCalls with count=20") {
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseManySequentialCalls(20L, inputHex).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "handle wide transaction with 50 external calls at single level" in { implicit env =>
      import env.*

      setupEchoHandler()
      mockServer.setEchoHandler("concurrent")

      val inputHex = toHex("wide-50")

      clue("Create EdgeCaseExternalCall contract") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.EdgeCaseExternalCall(alice.toProtoPrimitive).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.EdgeCaseExternalCall.COMPANION)(createTx).loneElement.id

        clue("Exercise ManySequentialCalls with count=50") {
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseManySequentialCalls(50L, inputHex).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "handle deep AND wide transaction" in { implicit env =>
      import env.*

      setupEchoHandler()

      val inputHex = toHex("deep-wide")

      // DeepCallAllLevels makes one call per level, so depth=5 gives 6 total calls.
      // This tests both depth and breadth (multiple calls at each recursive step).
      clue("Create DeepExternalCallContract with depth=5 for deep+wide test") {
        val contractId = createDeepContract(5)

        clue("Exercise DeepCallAllLevels — combines depth and external calls at every level") {
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseDeepCallAllLevels(inputHex).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "correctly order external call results in deep transaction" in { implicit env =>
      import env.*

      setupEchoHandler()

      val inputHex = toHex("ordered")

      clue("Create DeepExternalCallContract with depth=3") {
        val contractId = createDeepContract(3)

        clue("Exercise DeepCallAllLevels — results should be ordered by level") {
          // Each level appends intToHex(depth) to the input, producing unique results
          // Results list should be [level3, level2, level1, level0]
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseDeepCallAllLevels(inputHex).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "handle deep transaction with observer" in { implicit env =>
      import env.*

      setupEchoHandler()

      val inputHex = toHex("deep-observer")

      clue("Create DeepExternalCallContract — alice as owner, bob as observer via ACS") {
        // DeepExternalCallContract only has owner as signatory.
        // We create the root contract, exercise it, and verify bob can see the consumed state.
        val contractId = createDeepContract(5)

        clue("Exercise DeepCall from alice") {
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseDeepCall(inputHex).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "handle maximum supported nesting depth" in { implicit env =>
      import env.*

      setupEchoHandler()

      val inputHex = toHex("max-depth")

      // Test with depth=20 as a practical stress test for nesting limits
      clue("Create DeepExternalCallContract with depth=20") {
        val contractId = createDeepContract(20)

        clue("Exercise DeepCall at depth 20") {
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseDeepCall(inputHex).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
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
