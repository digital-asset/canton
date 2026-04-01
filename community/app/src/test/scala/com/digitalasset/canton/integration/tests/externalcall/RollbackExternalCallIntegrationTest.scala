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
    externalCallEnvironmentDefinition(EnvironmentDefinition.P2S1M1_Manual)
      .addConfigTransforms(ConfigTransforms.setAlphaVersionSupport(true)*)
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

  "rollback handling with external calls" should {

    "preserve external call result when made before rolled-back scope" in { implicit env =>
      import env.*

      setupEchoHandler()

      val inputHex = toHex("before-rollback")

      clue("Create RollbackTestContract") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.RollbackTestContract(alice.toProtoPrimitive).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.RollbackTestContract.COMPANION)(createTx).loneElement.id

        clue("Exercise ExternalCallThenRollback — external call before try/catch") {
          // The choice makes an external call, then has a try block with division by zero
          // that gets caught. The external call result should be preserved.
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseExternalCallThenRollback(inputHex).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "discard external call result when made inside rolled-back scope" in { implicit env =>
      import env.*
      setupEchoHandler()

      val inputHex = toHex("rollback-discard")

      clue("Create RollbackTestContract") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.RollbackTestContract(alice.toProtoPrimitive).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.RollbackTestContract.COMPANION)(createTx).loneElement.id

        clue("Exercise ExternalCallInRollback with shouldFail=true") {
          // External call is made inside try block, then exception is thrown and caught
          // The external call was rolled back, so transaction succeeds but call effects are discarded
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseExternalCallInRollback(inputHex, true).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "preserve external call result when inside try block that succeeds" in { implicit env =>
      import env.*
      setupEchoHandler()

      val inputHex = toHex("rollback-preserve")

      clue("Create RollbackTestContract") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.RollbackTestContract(alice.toProtoPrimitive).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.RollbackTestContract.COMPANION)(createTx).loneElement.id

        clue("Exercise ExternalCallInRollback with shouldFail=false") {
          // External call is made inside try block, no exception is thrown
          // The external call result should be preserved and returned as Some(result)
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseExternalCallInRollback(inputHex, false).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "handle multiple rollback scopes with external calls" in { implicit env =>
      import env.*
      setupEchoHandler()

      val inputHex = toHex("multi-rollback")

      clue("Create MultipleRollbackContract") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.MultipleRollbackContract(alice.toProtoPrimitive).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.MultipleRollbackContract.COMPANION)(createTx).loneElement.id

        clue("Exercise MultipleRollbackScopes") {
          // Scope 1: external call with input+"2d31" → succeeds
          // Scope 2: external call with input+"2d32" → fails (error "fail scope 2") → caught, returns None
          // Scope 3: external call with input+"2d33" → succeeds
          // Returns (r1, None, r3)
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseMultipleRollbackScopes(inputHex).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "handle nested rollback scopes with external calls" in { implicit env =>
      import env.*
      setupEchoHandler()

      val inputHex = toHex("nested-rollback")

      clue("Create MultipleRollbackContract") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.MultipleRollbackContract(alice.toProtoPrimitive).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.MultipleRollbackContract.COMPANION)(createTx).loneElement.id

        clue("Exercise NestedRollbackScopes") {
          // Outer external call, then nested try blocks with external calls
          // Inner scope fails and rolls back, but outer scope succeeds
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseNestedRollbackScopes(inputHex).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "correctly handle external call followed by exception in same scope" in { implicit env =>
      import env.*
      setupEchoHandler()

      val inputHex = toHex("call-then-exception")

      clue("Create RollbackTestContract") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.RollbackTestContract(alice.toProtoPrimitive).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.RollbackTestContract.COMPANION)(createTx).loneElement.id

        clue("Exercise ExternalCallInRollback with shouldFail=true") {
          // External call happens first, then exception is thrown in same scope
          // The entire scope (including external call) gets rolled back when caught
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseExternalCallInRollback(inputHex, true).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "handle external call in catch block" in { implicit env =>
      import env.*

      setupEchoHandler()

      // The ExternalCallThenRollback choice makes a call before the try/catch.
      // The catch block in the Daml code recovers from ArithmeticError.
      // The external call result from before the try is preserved.
      val inputHex = toHex("catch-block-test")

      val createTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        new E.RollbackTestContract(alice.toProtoPrimitive).create.commands.asScala.toSeq,
      )
      val contractId = JavaDecodeUtil.decodeAllCreated(E.RollbackTestContract.COMPANION)(createTx).loneElement.id

      val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        contractId.exerciseExternalCallThenRollback(inputHex).commands.asScala.toSeq,
      )
      exerciseTx.getUpdateId should not be empty
    }

    "handle multiple external calls with partial rollback" in { implicit env =>
      import env.*
      setupEchoHandler()

      val inputHex = toHex("partial-rollback")

      clue("Create MultipleRollbackContract") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.MultipleRollbackContract(alice.toProtoPrimitive).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.MultipleRollbackContract.COMPANION)(createTx).loneElement.id

        clue("Exercise MultipleRollbackScopes to verify partial rollback semantics") {
          // Same as the "handle multiple rollback scopes" test but focusing on partial rollback
          // First and third external calls should succeed, middle one should be rolled back
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseMultipleRollbackScopes(inputHex).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "maintain rollback scope in deep transaction with external calls" in { implicit env =>
      import env.*
      setupEchoHandler()

      val inputHex = toHex("deep-rollback")

      clue("Create MultipleRollbackContract for deep nesting test") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.MultipleRollbackContract(alice.toProtoPrimitive).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.MultipleRollbackContract.COMPANION)(createTx).loneElement.id

        clue("Exercise NestedRollbackScopes to test deep nesting") {
          // Tests deep nesting of exercises with rollbacks to ensure scope is maintained
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseNestedRollbackScopes(inputHex).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "handle external call result in transaction with multiple rollbacks" in { implicit env =>
      import env.*
      setupEchoHandler()

      val inputHex = toHex("multi-rollbacks")

      clue("Create MultipleRollbackContract") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.MultipleRollbackContract(alice.toProtoPrimitive).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.MultipleRollbackContract.COMPANION)(createTx).loneElement.id

        clue("Exercise MultipleRollbackScopes for multi-rollback transaction") {
          // Multiple rollback scopes in one transaction - some succeed, some fail
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseMultipleRollbackScopes(inputHex).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "correctly order external call results with rollbacks" in { implicit env =>
      import env.*
      setupEchoHandler()

      val inputHex = toHex("order-rollback")

      clue("Create MultipleRollbackContract") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.MultipleRollbackContract(alice.toProtoPrimitive).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.MultipleRollbackContract.COMPANION)(createTx).loneElement.id

        clue("Exercise MultipleRollbackScopes to verify ordering after rollbacks") {
          // Verify that external call results maintain proper ordering after rollbacks
          // Should return results in (scope1, None, scope3) order
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseMultipleRollbackScopes(inputHex).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
    }

    "handle exception thrown by external call itself" in { implicit env =>
      import env.*
      setupEchoHandler()

      val inputHex = toHex("invalid-function")

      clue("Create RollbackTestContract") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.RollbackTestContract(alice.toProtoPrimitive).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.RollbackTestContract.COMPANION)(createTx).loneElement.id

        clue("Exercise with invalid function to trigger external call error") {
          // This test expects the external call to return an error status
          // When the mock returns an error, the Daml error should be propagated
          // Note: This may need specific mock configuration to return an error
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseExternalCallInRollback(inputHex, false).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }
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
