// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.extension

import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.extcall.java as M
import com.digitalasset.canton.integration.plugins.{UseExtensionService, UseH2}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.version.ProtocolVersion

import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*

/** End-to-end coverage for Daml external calls against a (mock) extension service: the submitting
  * participant records the service's output in the transaction and re-validates it in the
  * confirmation workflow. The external-call wire data exists only at protocol version dev, so the
  * tests are dev-gated and run in the dev-protocol-version CI job.
  */
class ExternalCallIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  private val extensionService = new UseExtensionService(loggerFactory)

  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(extensionService)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1.withSetup { env =>
      import env.*
      participant1.synchronizers.connect_local(sequencer1, alias = daName)
    }

  "a Daml external call" should {
    "be recorded at submission and re-validated in the confirmation workflow" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      implicit env =>
        import env.*

        participant1.dars.upload(BaseTest.ExternalCallTestPath)
        val owner = participant1.parties.enable("external-call-owner")

        val transaction = participant1.ledger_api.javaapi.commands.submit(
          Seq(owner),
          Seq(
            new M.externalcalltest.ExternalCallTester(owner.toProtoPrimitive).createAnd
              .exerciseCallExtension(
                extensionService.extensionId,
                "test-function",
                "00ff",
                "deadbeef",
              )
              .commands
              .loneElement
          ),
          transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
        )

        // The choice returns the service output recorded in the transaction.
        val exercised = transaction.getEvents.asScala.collect {
          case event: com.daml.ledger.javaapi.data.ExercisedEvent => event
        }.loneElement
        exercised.getExerciseResult.asText().toScala.value.getValue shouldBe
          UseExtensionService.defaultResponseHex

        // The participant calls the service once to compute the output at submission and once
        // to re-validate the recorded output in the confirmation workflow.
        val calls = extensionService.observedCalls
        calls.map(_.mode) shouldBe Seq("submission", "validation")
        forEvery(calls) { call =>
          call.functionId shouldBe "test-function"
          call.configHash shouldBe "00ff"
          call.body shouldBe "deadbeef"
          call.externalCallId should not be empty
          call.idempotencyKey should not be empty
        }
        // Submission and re-validation are distinct logical calls, not retries of one call:
        // the client mints one idempotency key per logical call, and a fresh request id per
        // attempt.
        calls.map(_.idempotencyKey).distinct should have size 2
        calls.map(_.externalCallId).distinct should have size 2
    }
  }
}
