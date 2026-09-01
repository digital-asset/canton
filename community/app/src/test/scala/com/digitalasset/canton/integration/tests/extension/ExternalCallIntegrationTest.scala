// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.extension

import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.javaapi.data.{ExercisedEvent, Transaction}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.extcall.java as M
import com.digitalasset.canton.integration.plugins.{UseExtensionService, UseH2}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.protocol.validation.ExternalCallValidationError
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.version.ProtocolVersion

import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*

/** End-to-end coverage for Daml external calls against a (mock) extension service: the submitting
  * participant records the service's output in the transaction and re-validates it in the
  * confirmation workflow. The external-call wire data exists from protocol version 36 onwards, so
  * the tests are gated on v36. The test package still targets LF 2.dev (the pinned damlc cannot
  * compile external calls at 2.4 yet), so dev version support is enabled on the participant.
  */
class ExternalCallIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  private val extensionService = new UseExtensionService(loggerFactory)

  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(extensionService)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(ConfigTransforms.setDevVersionSupport(true)*)
      .withSetup { env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, alias = daName)
      }

  private val uuidRegex = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"

  private def setUpOwner(name: String)(implicit env: TestConsoleEnvironment): PartyId = {
    import env.*
    participant1.dars.upload(BaseTest.ExternalCallTestPath)
    participant1.parties.enable(name)
  }

  private def callExtension(owner: PartyId, extensionId: String, configHex: String = "00ff")(
      implicit env: TestConsoleEnvironment
  ): Transaction = {
    import env.*
    participant1.ledger_api.javaapi.commands.submit(
      Seq(owner),
      Seq(
        new M.externalcalltest.ExternalCallTester(owner.toProtoPrimitive).createAnd
          .exerciseCallExtension(extensionId, "test-function", configHex, "deadbeef")
          .commands
          .loneElement
      ),
      transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
    )
  }

  "a Daml external call" should {
    "be recorded at submission and re-validated in the confirmation workflow" onlyRunWithOrGreaterThan ProtocolVersion.v36 in {
      implicit env =>
        extensionService.reset()
        val owner = setUpOwner("external-call-owner")

        val transaction = callExtension(owner, extensionService.extensionId)

        // The choice returns the service output recorded in the transaction.
        val exercised = transaction.getEvents.asScala.collect { case event: ExercisedEvent =>
          event
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

    "fail the submission when the extension service cannot be reached" onlyRunWithOrGreaterThan ProtocolVersion.v36 in {
      implicit env =>
        extensionService.reset()
        val owner = setUpOwner("external-call-unreachable-owner")

        loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
          callExtension(owner, extensionService.unreachableExtensionId).discard,
          LogEntry.assertLogSeq(
            Seq(
              (
                // The JDK reports a refused connection as either a connect or a generic I/O
                // failure; both map to the same retryable error and client message.
                _.warningMessage should fullyMatch regex
                  s"External call to extension '${extensionService.unreachableExtensionId}' " +
                  s"(connection failed|I/O error): externalCallId=$uuidRegex",
                "HTTP client transport warning",
              ),
              (
                _.warningMessage should fullyMatch regex
                  s"External call to extension '${extensionService.unreachableExtensionId}' " +
                  "\\(function 'test-function'\\) failed: ExtensionCallError\\(status code = 503, " +
                  s"""message = "(Connection failed|I/O error)", external call id = '$uuidRegex', """ +
                  "retryable = true, trace id = tid:[0-9a-f]*\\)",
                "extension service manager warning",
              ),
              (
                _.commandFailureMessage should include regex
                  "INTERPRETATION_EXTERNAL_CALL_ERROR_EXECUTION_FAILED\\(9,.*\\): " +
                  "Interpretation error: Error: External call execution failed " +
                  s"\\(extensionId=${extensionService.unreachableExtensionId}, functionId=test-function\\): " +
                  s"call failed: External call failed \\(retryable = true, " +
                  s"external call id = '$uuidRegex', trace id = '[0-9a-f]*'\\)",
                "command failure",
              ),
            ),
            Seq.empty,
          ),
        )
        // The transport failure happens before any request reaches a service.
        extensionService.observedCalls shouldBe empty
    }

    "fail the submission when the service output is not canonical hex" onlyRunWithOrGreaterThan ProtocolVersion.v36 in {
      implicit env =>
        extensionService.reset()
        try {
          extensionService.respondWith(_ => UseExtensionService.Response(200, "C0FFEE"))
          val owner = setUpOwner("external-call-invalid-output-owner")

          assertThrowsAndLogsCommandFailures(
            callExtension(owner, extensionService.extensionId).discard,
            _.commandFailureMessage should include regex
              "INTERPRETATION_EXTERNAL_CALL_ERROR_INVALID_OUTPUT\\(9,.*\\): " +
              "Interpretation error: Error: External call execution failed " +
              s"\\(extensionId=${extensionService.extensionId}, functionId=test-function\\): " +
              "invalid output: Invalid external call output: expected canonical lowercase hex",
          )
          // The submission already fails, so nothing is recorded and nothing is re-validated.
          extensionService.observedCalls.map(_.mode) shouldBe Seq("submission")
        } finally extensionService.reset()
    }

    "fail the submission when the call arguments are not canonical hex" onlyRunWithOrGreaterThan ProtocolVersion.v36 in {
      implicit env =>
        extensionService.reset()
        val owner = setUpOwner("external-call-preparation-owner")

        // The stdlib wrapper lowercases the hex arguments, so only genuinely non-hex input
        // reaches the interpreter's canonicality check.
        assertThrowsAndLogsCommandFailures(
          callExtension(owner, extensionService.extensionId, configHex = "zz").discard,
          _.commandFailureMessage should include regex
            "INTERPRETATION_EXTERNAL_CALL_ERROR_PREPARATION_FAILED\\(9,.*\\): " +
            "Interpretation error: Error: External call preparation failed " +
            s"\\(extensionId=${extensionService.extensionId}, functionId=test-function\\): " +
            "Invalid external call config or input: expected canonical lowercase hex",
        )
        // Preparation fails before the handler is invoked: no request reaches the service.
        extensionService.observedCalls shouldBe empty
    }

    "reject the request when the recorded output cannot be re-validated" onlyRunWithOrGreaterThan ProtocolVersion.v36 in {
      implicit env =>
        import env.*
        extensionService.reset()
        try {
          // A transport-level failure between the submission and validation phases of a single
          // request cannot be injected with a static mock; an HTTP error funnels into the same
          // validator outcome: the participant cannot re-validate and abstains, and the mediator
          // rejects the request because the only confirmer abstained.
          extensionService.respondWith(call =>
            if (call.mode == "validation") UseExtensionService.Response(503, "")
            else UseExtensionService.Response(200, UseExtensionService.defaultResponseHex)
          )
          val owner = setUpOwner("external-call-abstain-owner")

          loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
            callExtension(owner, extensionService.extensionId).discard,
            LogEntry.assertLogSeq(
              Seq(
                (
                  _.warningMessage should fullyMatch regex
                    s"External call to extension '${extensionService.extensionId}' " +
                    "\\(function 'test-function'\\) failed: ExtensionCallError\\(status code = 503, " +
                    s"""message = "Service unavailable", external call id = '$uuidRegex', """ +
                    "retryable = true, trace id = tid:[0-9a-f]*\\)",
                  "extension service manager warning",
                ),
                (
                  _.commandFailureMessage should include regex
                    "CANNOT_PERFORM_ALL_VALIDATIONS\\(9,.*\\): Cannot perform all validations: " +
                    s"external-call validation failed with status 503, externalCallId=$uuidRegex",
                  "command failure",
                ),
              ),
              Seq.empty,
            ),
          )
          extensionService.observedCalls.map(_.mode) shouldBe Seq("submission", "validation")
          // The rejected request must not leave anything on the ledger.
          participant1.ledger_api.state.acs.of_party(owner) shouldBe empty
        } finally extensionService.reset()
    }

    "reject the request when re-validation returns non-canonical output" onlyRunWithOrGreaterThan ProtocolVersion.v36 in {
      implicit env =>
        import env.*
        extensionService.reset()
        try {
          extensionService.respondWith(call =>
            if (call.mode == "validation") UseExtensionService.Response(200, "C0FFEE")
            else UseExtensionService.Response(200, UseExtensionService.defaultResponseHex)
          )
          val owner = setUpOwner("external-call-noncanonical-owner")

          // Non-canonical output at submission rejects the command (INVALID_OUTPUT above), but
          // at validation the participant merely cannot re-validate and abstains: the service
          // call itself succeeded, so no warning and no disagreement alarm fires, which the
          // single log assertion pins.
          assertThrowsAndLogsCommandFailures(
            callExtension(owner, extensionService.extensionId).discard,
            _.commandFailureMessage should include regex
              "CANNOT_PERFORM_ALL_VALIDATIONS\\(9,.*\\): Cannot perform all validations: " +
              "external-call validation returned non-canonical output",
          )
          extensionService.observedCalls.map(_.mode) shouldBe Seq("submission", "validation")
          // The rejected request must not leave anything on the ledger.
          participant1.ledger_api.state.acs.of_party(owner) shouldBe empty
        } finally extensionService.reset()
    }

    "reject and alarm when re-validation disagrees with the recorded output" onlyRunWithOrGreaterThan ProtocolVersion.v36 in {
      implicit env =>
        import env.*
        extensionService.reset()
        try {
          extensionService.respondWith(call =>
            if (call.mode == "validation") UseExtensionService.Response(200, "beef")
            else UseExtensionService.Response(200, UseExtensionService.defaultResponseHex)
          )
          val owner = setUpOwner("external-call-disagreement-owner")

          // "c0ffee" (3 bytes) is recorded at submission, re-validation computes "beef"
          // (2 bytes): the description reports the output byte sizes in ascending order and
          // never the output bytes themselves.
          val expectedInconsistency =
            """Inconsistency(
              |  extensionId = test-extension,
              |  functionId = test-function,
              |  config = 2 bytes,
              |  input = 4 bytes,
              |  outputByteSizes = Seq(2, 3),
              |  occurrences = ExternalCallOccurrence(viewPosition = ViewPosition(R), exerciseIndex = 0, callIndex = 0)
              |)""".stripMargin

          loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
            callExtension(owner, extensionService.extensionId).discard,
            entries => {
              LogEntry.assertLogSeq(
                Seq(
                  (
                    _.shouldBeCantonError(
                      ExternalCallValidationError.ExternalCallResultDisagreementAlarm,
                      messageAssertion = _ shouldBe
                        s"Observed inconsistent external call results: $expectedInconsistency",
                      contextAssertion = _.keySet should contain("requestId"),
                    ),
                    "disagreement alarm",
                  ),
                  (
                    entry => {
                      entry.commandFailureMessage should include regex
                        "LOCAL_VERDICT_EXTERNAL_CALL_RESULT_DISAGREEMENT\\(8,.*\\): "
                      entry.commandFailureMessage should include(
                        "Rejected transaction due to inconsistent external call results: " +
                          expectedInconsistency
                      )
                    },
                    "command failure",
                  ),
                ),
                Seq.empty,
              )(entries)
              // The check alarms once per request, not once per view or occurrence.
              entries.count(
                _.message.startsWith("EXTERNAL_CALL_RESULT_DISAGREEMENT_ALARM")
              ) shouldBe 1
            },
          )
          extensionService.observedCalls.map(_.mode) shouldBe Seq("submission", "validation")
          // The rejected request must not leave anything on the ledger.
          participant1.ledger_api.state.acs.of_party(owner) shouldBe empty
        } finally extensionService.reset()
    }
  }
}
