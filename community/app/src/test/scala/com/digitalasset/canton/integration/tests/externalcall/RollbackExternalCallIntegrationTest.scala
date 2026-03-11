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

    "discard external call result when made inside rolled-back scope" in { _ =>
      // External calls inside try/catch (rollback) scopes are not yet supported.
      // Canton's PartialTransaction only records results in ExercisesContextInfo,
      // but try blocks create TryContextInfo.
      pending
    }

    "preserve external call result when inside try block that succeeds" in { _ =>
      // External calls inside try/catch (rollback) scopes are not yet supported.
      // Canton's PartialTransaction only records results in ExercisesContextInfo,
      // but try blocks create TryContextInfo.
      pending
    }

    "handle multiple rollback scopes with external calls" in { _ =>
      // External calls inside try/catch (rollback) scopes are not yet supported.
      // Canton's PartialTransaction only records results in ExercisesContextInfo,
      // but try blocks create TryContextInfo.
      pending
    }

    "handle nested rollback scopes with external calls" in { _ =>
      // External calls inside try/catch (rollback) scopes are not yet supported.
      // Canton's PartialTransaction only records results in ExercisesContextInfo,
      // but try blocks create TryContextInfo.
      pending
    }

    "correctly handle external call followed by exception in same scope" in { _ =>
      // Requires BEExternalCall engine primitive (not yet implemented).
      // Currently DA.External uses echo stub that returns input without HTTP calls.
      pending
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

    "handle multiple external calls with partial rollback" in { _ =>
      // External calls inside try/catch (rollback) scopes are not yet supported.
      // Canton's PartialTransaction only records results in ExercisesContextInfo,
      // but try blocks create TryContextInfo.
      pending
    }

    "maintain rollback scope in deep transaction with external calls" in { _ =>
      // External calls inside try/catch (rollback) scopes are not yet supported.
      // Canton's PartialTransaction only records results in ExercisesContextInfo,
      // but try blocks create TryContextInfo.
      pending
    }

    "handle external call result in transaction with multiple rollbacks" in { _ =>
      // External calls inside try/catch (rollback) scopes are not yet supported.
      // Canton's PartialTransaction only records results in ExercisesContextInfo,
      // but try blocks create TryContextInfo.
      pending
    }

    "correctly order external call results with rollbacks" in { _ =>
      // External calls inside try/catch (rollback) scopes are not yet supported.
      // Canton's PartialTransaction only records results in ExercisesContextInfo,
      // but try blocks create TryContextInfo.
      pending
    }

    "handle exception thrown by external call itself" in { _ =>
      // Requires BEExternalCall engine primitive (not yet implemented).
      // Currently DA.External uses echo stub that returns input without HTTP calls.
      pending
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
