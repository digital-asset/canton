// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.extension

import com.daml.ledger.api.v2.event.Event
import com.daml.ledger.api.v2.interactive.interactive_submission_service.HashingSchemeVersion.{
  HASHING_SCHEME_VERSION_V2,
  HASHING_SCHEME_VERSION_V4,
}
import com.daml.ledger.api.v2.interactive.transaction.v1.interactive_submission_data.Node.NodeType
import com.daml.ledger.javaapi.data.CreatedEvent as JCreatedEvent
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.extcall.java as M
import com.digitalasset.canton.integration.plugins.{UseExtensionService, UseH2}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.ExternalParty
import com.digitalasset.canton.util.HexString
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import java.util.UUID

/** End-to-end coverage for Daml external calls on the interactive-submission surface: an external
  * party prepares a transaction exercising an external-call choice, so the recorded results travel
  * in the prepared transaction and are covered by the signed hash. External-call results only exist
  * at LF serialization version dev, which requires hashing scheme V4, so preparing or executing
  * with an older scheme is refused. The tests are dev-gated and run in the dev-protocol-version CI
  * job.
  */
class ExternalCallInteractiveSubmissionIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  private val extensionService = new UseExtensionService(loggerFactory)

  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(extensionService)

  private var aliceE: ExternalParty = _
  private var tester: M.externalcalltest.ExternalCallTester.Contract = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransform(ConfigTransforms.enableInteractiveSubmissionTransforms)
      .withSetup { implicit env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        if (testedProtocolVersion >= ProtocolVersion.dev) {
          participant1.dars.upload(BaseTest.ExternalCallTestPath)
          aliceE = participant1.parties.testing.external.enable("Alice")
          // An externally-signed submission must be a single view, so the tests cannot use
          // create-and-exercise: create the contract up front and prepare standalone exercises.
          tester = inside(
            participant1.ledger_api.javaapi.commands
              .submit(
                Seq(aliceE),
                Seq(
                  new M.externalcalltest.ExternalCallTester(
                    aliceE.toProtoPrimitive
                  ).create.commands.loneElement
                ),
                includeCreatedEventBlob = true,
              )
              .getEvents
              .loneElement
          ) { case created: JCreatedEvent =>
            JavaDecodeUtil
              .decodeCreated(M.externalcalltest.ExternalCallTester.COMPANION)(created)
              .value
          }
        }
      }

  private def callExtensionCommand =
    tester.id
      .exerciseCallExtension(extensionService.extensionId, "test-function", "00ff", "deadbeef")
      .commands
      .loneElement

  private def hexBytes(hex: String): ByteString = HexString.parseToByteString(hex).value

  "an external call in an interactive submission" should {
    "record the results in the V4-prepared transaction and execute it" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      implicit env =>
        import env.*
        extensionService.reset()

        val prepared = participant1.ledger_api.javaapi.interactive_submission.prepare(
          Seq(aliceE),
          Seq(callExtensionCommand),
          hashingSchemeVersion = HASHING_SCHEME_VERSION_V4,
        )
        prepared.hashingSchemeVersion shouldBe HASHING_SCHEME_VERSION_V4

        // The prepared transaction's exercise node carries the recorded external-call results,
        // so they are part of what the external party signs.
        val result = prepared.getPreparedTransaction.getTransaction.nodes
          .map(_.versionedNode.v1.value.nodeType)
          .collect { case NodeType.Exercise(value) => value }
          .loneElement
          .externalCallResults
          .loneElement
        result.extensionId shouldBe extensionService.extensionId
        result.functionId shouldBe "test-function"
        result.config shouldBe hexBytes("00ff")
        result.input shouldBe hexBytes("deadbeef")
        result.output shouldBe hexBytes(UseExtensionService.defaultResponseHex)

        val transaction =
          participant1.ledger_api.interactive_submission.execute_and_wait_for_transaction(
            prepared.getPreparedTransaction,
            Map(aliceE.partyId -> global_secret.sign(prepared.preparedTransactionHash, aliceE)),
            submissionId = UUID.randomUUID().toString,
            hashingSchemeVersion = HASHING_SCHEME_VERSION_V4,
          )

        // The committed exercise returns the recorded output.
        val exercised = transaction.events
          .map(_.event)
          .collect { case Event.Event.Exercised(event) => event }
          .loneElement
        exercised.getExerciseResult.getText shouldBe UseExtensionService.defaultResponseHex

        // The participant calls the service once to compute the output when the transaction is
        // prepared and once to re-validate the recorded output in the confirmation workflow.
        extensionService.observedCalls.map(_.mode) shouldBe Seq("submission", "validation")
    }

    "refuse to prepare with hashing scheme V2" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      implicit env =>
        import env.*
        extensionService.reset()

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant1.ledger_api.javaapi.interactive_submission
            .prepare(
              Seq(aliceE),
              Seq(callExtensionCommand),
              hashingSchemeVersion = HASHING_SCHEME_VERSION_V2,
            ),
          _.errorMessage should include(
            "Cannot hash node with LF serialization version VDev using hashing scheme V2. Please use hashing scheme V4 or higher."
          ),
        )

        // Interpretation precedes hashing, so the service computed the output once, but the
        // refused transaction never reaches the confirmation workflow.
        extensionService.observedCalls.map(_.mode) shouldBe Seq("submission")
    }

    "refuse to execute a V4-prepared transaction presented as V2" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      implicit env =>
        import env.*
        extensionService.reset()

        val prepared = participant1.ledger_api.javaapi.interactive_submission.prepare(
          Seq(aliceE),
          Seq(callExtensionCommand),
          hashingSchemeVersion = HASHING_SCHEME_VERSION_V4,
        )

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant1.ledger_api.interactive_submission.execute_and_wait_for_transaction(
            prepared.getPreparedTransaction,
            Map(aliceE.partyId -> global_secret.sign(prepared.preparedTransactionHash, aliceE)),
            submissionId = UUID.randomUUID().toString,
            hashingSchemeVersion = HASHING_SCHEME_VERSION_V2,
          ),
          _.errorMessage should include(
            "Cannot hash node with LF serialization version VDev using hashing scheme V2. Please use hashing scheme V4 or higher."
          ),
        )

        // The hash verification failure precedes submission, so the service only saw the
        // preparation-time call and the transaction never reaches the confirmation workflow.
        extensionService.observedCalls.map(_.mode) shouldBe Seq("submission")
    }
  }
}
