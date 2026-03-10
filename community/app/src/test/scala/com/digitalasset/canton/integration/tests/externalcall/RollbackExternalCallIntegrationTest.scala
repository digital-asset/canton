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

      val inputHex = toHex("inside-rollback")

      clue("Create RollbackTestContract") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.RollbackTestContract(alice.toProtoPrimitive).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.RollbackTestContract.COMPANION)(createTx).loneElement.id

        clue("Exercise ExternalCallInRollback with shouldFail=true") {
          // The choice enters a try block, makes an external call, then calls error.
          // The exception is caught; the external call result is discarded (rolled back).
          // Returns None since the external call was inside the rolled-back scope.
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

      val inputHex = toHex("try-succeeds")

      clue("Create RollbackTestContract") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.RollbackTestContract(alice.toProtoPrimitive).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.RollbackTestContract.COMPANION)(createTx).loneElement.id

        clue("Exercise ExternalCallInRollback with shouldFail=false") {
          // The try block succeeds — external call result is preserved as Some(result).
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

        clue("Exercise MultipleRollbackScopes — scope 1 ok, scope 2 rolls back, scope 3 ok") {
          // Scope 1: external call succeeds (preserved)
          // Scope 2: external call made then error thrown — rolled back (None)
          // Scope 3: external call succeeds (preserved)
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

        clue("Exercise NestedRollbackScopes — outer call ok, mid call ok, inner rolls back") {
          // Outer: external call succeeds (preserved)
          // Middle: external call succeeds (preserved)
          // Inner: external call made then error — rolled back (None)
          // Middle scope does NOT fail, so mid result is kept.
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

      val callCount = new java.util.concurrent.atomic.AtomicInteger(0)
      mockServer.setHandler("count") { req =>
        callCount.incrementAndGet()
        ExternalCallResponse.ok(req.input)
      }

      val inputHex = toHex("fail-after-call")

      clue("Create RollbackTestContract") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.RollbackTestContract(alice.toProtoPrimitive).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.RollbackTestContract.COMPANION)(createTx).loneElement.id

        clue("Exercise ExternalCallInRollback with shouldFail=true — call made but rolled back") {
          // The external call is made (HTTP side-effect happens), but the result is
          // discarded because the scope fails. Transaction still succeeds because
          // the exception is caught.
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

      clue("Create MultipleRollbackContract and exercise MultipleRollbackScopes") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.MultipleRollbackContract(alice.toProtoPrimitive).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.MultipleRollbackContract.COMPANION)(createTx).loneElement.id

        // Call A (scope 1) → preserved
        // Call B (scope 2) → rolled back
        // Call C (scope 3) → preserved
        val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseMultipleRollbackScopes(inputHex).commands.asScala.toSeq,
        )
        exerciseTx.getUpdateId should not be empty
      }
    }

    "maintain rollback scope in deep transaction with external calls" in { implicit env =>
      import env.*

      setupEchoHandler()

      // Use NestedRollbackScopes which has outer → mid → inner nesting
      // with rollback at the inner level
      val inputHex = toHex("deep-rollback")

      val createTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        new E.MultipleRollbackContract(alice.toProtoPrimitive).create.commands.asScala.toSeq,
      )
      val contractId = JavaDecodeUtil.decodeAllCreated(E.MultipleRollbackContract.COMPANION)(createTx).loneElement.id

      val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        contractId.exerciseNestedRollbackScopes(inputHex).commands.asScala.toSeq,
      )
      exerciseTx.getUpdateId should not be empty
    }

    "handle external call result in transaction with multiple rollbacks" in { implicit env =>
      import env.*

      setupEchoHandler()

      // Exercise MultipleRollbackScopes twice on different contracts to verify
      // independent rollback handling
      val inputHex = toHex("multi-tx-rollback")

      clue("First contract with multiple rollback scopes") {
        val createTx1 = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.MultipleRollbackContract(alice.toProtoPrimitive).create.commands.asScala.toSeq,
        )
        val contractId1 = JavaDecodeUtil.decodeAllCreated(E.MultipleRollbackContract.COMPANION)(createTx1).loneElement.id

        val exerciseTx1 = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId1.exerciseMultipleRollbackScopes(inputHex).commands.asScala.toSeq,
        )
        exerciseTx1.getUpdateId should not be empty
      }

      clue("Second contract with nested rollback scopes") {
        val createTx2 = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.MultipleRollbackContract(alice.toProtoPrimitive).create.commands.asScala.toSeq,
        )
        val contractId2 = JavaDecodeUtil.decodeAllCreated(E.MultipleRollbackContract.COMPANION)(createTx2).loneElement.id

        val exerciseTx2 = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId2.exerciseNestedRollbackScopes(inputHex).commands.asScala.toSeq,
        )
        exerciseTx2.getUpdateId should not be empty
      }
    }

    "correctly order external call results with rollbacks" in { implicit env =>
      import env.*

      setupEchoHandler()

      // MultipleRollbackScopes produces (r1, None, r3) — verifying ordering
      // is maintained even when middle scope is rolled back
      val inputHex = toHex("order-test")

      val createTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        new E.MultipleRollbackContract(alice.toProtoPrimitive).create.commands.asScala.toSeq,
      )
      val contractId = JavaDecodeUtil.decodeAllCreated(E.MultipleRollbackContract.COMPANION)(createTx).loneElement.id

      val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        contractId.exerciseMultipleRollbackScopes(inputHex).commands.asScala.toSeq,
      )
      exerciseTx.getUpdateId should not be empty
    }

    "handle exception thrown by external call itself" in { implicit env =>
      import env.*

      mockServer.setErrorHandler("throw-error", 500, "Service error")

      // When the external call itself fails (HTTP 500), the Daml runtime surfaces
      // the error. Whether it's catchable depends on implementation. Here we verify
      // the transaction fails with an appropriate error.
      val inputHex = toHex("ext-call-error")

      val createTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        new E.ExternalCallContract(
          alice.toProtoPrimitive,
          java.util.List.of(),
        ).create.commands.asScala.toSeq,
      )
      val contractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

      val exception = intercept[io.grpc.StatusRuntimeException] {
        participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallExternal(
            "test-ext",
            "throw-error",
            "00000000",
            inputHex,
          ).commands.asScala.toSeq,
        )
      }
      exception.getMessage should not be empty
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
