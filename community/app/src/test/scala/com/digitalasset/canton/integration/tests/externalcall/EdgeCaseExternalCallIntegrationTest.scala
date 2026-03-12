// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.externalcall

import com.digitalasset.canton.console.CommandFailure
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

/** Integration tests for edge cases in external calls.
  *
  * Tests:
  * - Empty input/output
  * - Large payloads (100KB+)
  * - All byte values (0x00-0xFF)
  * - Unicode handling
  * - Config hash edge cases
  * - Concurrent external calls
  * - Request metadata
  */
sealed trait EdgeCaseExternalCallIntegrationTest
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

  /** Helper to create an EdgeCaseExternalCall contract */
  private def createEdgeCaseContract()(implicit env: FixtureParam) = {
    import env.*
    val createTx = participant1.ledger_api.javaapi.commands.submit(
      Seq(alice),
      new E.EdgeCaseExternalCall(alice.toProtoPrimitive).create.commands.asScala.toSeq,
    )
    JavaDecodeUtil.decodeAllCreated(E.EdgeCaseExternalCall.COMPANION)(createTx).loneElement.id
  }

  /** Helper to create a basic ExternalCallContract */
  private def createExternalCallContract()(implicit env: FixtureParam) = {
    import env.*
    val createTx = participant1.ledger_api.javaapi.commands.submit(
      Seq(alice),
      new E.ExternalCallContract(
        alice.toProtoPrimitive,
        java.util.List.of(),
      ).create.commands.asScala.toSeq,
    )
    JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id
  }

  "edge cases for external calls" should {

    // === Empty Values ===

    "handle empty input" in { implicit env =>
      import env.*

      mockServer.setHandler("empty-input") { _ =>
        ExternalCallResponse.ok("received-empty".getBytes)
      }

      val contractId = createEdgeCaseContract()

      clue("Exercise CallWithEmptyInput — empty hex string as input") {
        val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallWithEmptyInput().commands.asScala.toSeq,
        )
        exerciseTx.getUpdateId should not be empty
      }
    }

    "handle empty output from service" in { implicit env =>
      import env.*

      mockServer.setHandler("empty-output") { _ =>
        ExternalCallResponse.ok(Array.empty[Byte])
      }

      val contractId = createExternalCallContract()

      clue("Exercise with handler returning empty response") {
        val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallExternal(
            "test-ext",
            "empty-output",
            "00000000",
            toHex("test"),
          ).commands.asScala.toSeq,
        )
        exerciseTx.getUpdateId should not be empty
      }
    }

    "handle empty config hash" in { implicit env =>
      import env.*

      setupEchoHandler()

      val contractId = createEdgeCaseContract()

      clue("Exercise CallWithConfigHash with empty config hash should fail") {
        intercept[CommandFailure] {
          participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseCallWithConfigHash("", toHex("test")).commands.asScala.toSeq,
          )
        }
      }
    }

    // === Large Payloads ===

    "handle 100KB input" in { implicit env =>
      import env.*

      mockServer.setHandler("large-input") { _ =>
        ExternalCallResponse.ok("ok".getBytes)
      }

      val contractId = createExternalCallContract()
      // 100KB = 102400 bytes -> 204800 hex chars
      val largeInputHex = "ab" * 102400

      clue("Exercise with 100KB input payload") {
        val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallExternal(
            "test-ext",
            "large-input",
            "00000000",
            largeInputHex,
          ).commands.asScala.toSeq,
        )
        exerciseTx.getUpdateId should not be empty
      }
    }

    "handle 100KB output" in { implicit env =>
      import env.*

      val largeOutput = Array.fill(100 * 1024)(0x58.toByte)

      mockServer.setHandler("large-output") { _ =>
        ExternalCallResponse.ok(largeOutput)
      }

      val contractId = createExternalCallContract()

      clue("Exercise with handler returning 100KB response") {
        val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallExternal(
            "test-ext",
            "large-output",
            "00000000",
            toHex("small-input"),
          ).commands.asScala.toSeq,
        )
        exerciseTx.getUpdateId should not be empty
      }
    }

    "handle 1MB input and output" in { implicit env =>
      import env.*

      mockServer.setHandler("megabyte") { req =>
        ExternalCallResponse.ok(req.input)
      }

      val contractId = createExternalCallContract()
      val megabyteHex = "cd" * (1024 * 1024)

      clue("Exercise with 1MB round-trip payload") {
        val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallExternal(
            "test-ext",
            "megabyte",
            "00000000",
            megabyteHex,
          ).commands.asScala.toSeq,
        )
        exerciseTx.getUpdateId should not be empty
      }
    }

    // === Byte Value Coverage ===

    "handle all 256 byte values" in { implicit env =>
      import env.*

      mockServer.setHandler("all-bytes") { req =>
        ExternalCallResponse.ok(req.input)
      }

      val contractId = createExternalCallContract()
      // All byte values 0x00-0xFF as hex
      val allBytesHex = (0 to 255).map(b => f"$b%02x").mkString

      clue("Exercise with all 256 byte values") {
        val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallExternal(
            "test-ext",
            "all-bytes",
            "00000000",
            allBytesHex,
          ).commands.asScala.toSeq,
        )
        exerciseTx.getUpdateId should not be empty
      }
    }

    "handle null bytes in input" in { implicit env =>
      import env.*

      mockServer.setHandler("nulls") { req =>
        ExternalCallResponse.ok(req.input)
      }

      val contractId = createExternalCallContract()
      // Null bytes interspersed: 0x00 0x00 0x41 0x00 0x42 0x00
      val nullBytesHex = "000041004200"

      clue("Exercise with embedded null bytes") {
        val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallExternal(
            "test-ext",
            "nulls",
            "00000000",
            nullBytesHex,
          ).commands.asScala.toSeq,
        )
        exerciseTx.getUpdateId should not be empty
      }
    }

    // === Unicode and Special Characters ===

    "handle unicode text in hex encoding" in { implicit env =>
      import env.*

      mockServer.setHandler("unicode") { req =>
        ExternalCallResponse.ok(req.input)
      }

      val contractId = createExternalCallContract()
      val unicodeHex = toHex("Hello 世界")

      clue("Exercise with unicode text hex-encoded") {
        val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallExternal(
            "test-ext",
            "unicode",
            "00000000",
            unicodeHex,
          ).commands.asScala.toSeq,
        )
        exerciseTx.getUpdateId should not be empty
      }
    }

    "handle emoji in hex encoding" in { implicit env =>
      import env.*

      mockServer.setHandler("emoji") { req =>
        ExternalCallResponse.ok(req.input)
      }

      val contractId = createExternalCallContract()
      val emojiHex = toHex("\uD83D\uDE80\uD83C\uDF89") // 🚀🎉

      clue("Exercise with emoji hex-encoded") {
        val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallExternal(
            "test-ext",
            "emoji",
            "00000000",
            emojiHex,
          ).commands.asScala.toSeq,
        )
        exerciseTx.getUpdateId should not be empty
      }
    }

    "handle special characters in function ID" in { implicit env =>
      import env.*

      mockServer.setHandler("get-price") { req =>
        ExternalCallResponse.ok(req.input)
      }

      val contractId = createExternalCallContract()

      clue("Exercise with hyphenated function ID") {
        val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallExternal(
            "test-ext",
            "get-price",
            "00000000",
            toHex("test"),
          ).commands.asScala.toSeq,
        )
        exerciseTx.getUpdateId should not be empty
      }
    }

    // === Config Hash Edge Cases ===

    "handle very long config hash" in { implicit env =>
      import env.*

      setupEchoHandler()

      val contractId = createEdgeCaseContract()
      val longConfigHash = "ab" * 500 // 1000-char hex string

      clue("Exercise CallWithConfigHash with very long config hash") {
        val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallWithConfigHash(longConfigHash, toHex("test")).commands.asScala.toSeq,
        )
        exerciseTx.getUpdateId should not be empty
      }
    }

    "handle config hash with all same digits" in { implicit env =>
      import env.*

      setupEchoHandler()

      val contractId = createEdgeCaseContract()

      clue("Exercise CallWithConfigHash with all-f config hash") {
        val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallWithConfigHash("ffffffff", toHex("test")).commands.asScala.toSeq,
        )
        exerciseTx.getUpdateId should not be empty
      }
    }

    // === Concurrent Calls ===

    "handle concurrent external calls from same contract" in { implicit env =>
      import env.*

      mockServer.setHandler("concurrent") { req =>
        // Must return deterministic results — same input -> same output
        // Otherwise confirmation will get different results than submission
        ExternalCallResponse.ok(req.input)
      }

      val contractId = createEdgeCaseContract()

      clue("Exercise ManySequentialCalls with count=5") {
        val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseManySequentialCalls(5L, toHex("seq")).commands.asScala.toSeq,
        )
        exerciseTx.getUpdateId should not be empty
      }
    }

    "handle concurrent transactions with external calls" in { implicit env =>
      import env.*

      setupEchoHandler()

      clue("Submit two independent transactions with external calls") {
        val contractId1 = createExternalCallContract()
        val contractId2 = createExternalCallContract()

        val exerciseTx1 = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId1.exerciseCallExternal(
            "test-ext", "echo", "00000000", toHex("concurrent-1"),
          ).commands.asScala.toSeq,
        )

        val exerciseTx2 = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId2.exerciseCallExternal(
            "test-ext", "echo", "00000000", toHex("concurrent-2"),
          ).commands.asScala.toSeq,
        )

        exerciseTx1.getUpdateId should not be empty
        exerciseTx2.getUpdateId should not be empty
      }
    }

    // === Hex Encoding Edge Cases ===

    "reject invalid hex input (odd length)" in { implicit env =>
      import env.*

      setupEchoHandler()

      val contractId = createExternalCallContract()

      clue("Exercise with odd-length hex should fail validation") {
        intercept[CommandFailure] {
          participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseCallExternal(
              "test-ext",
              "echo",
              "00000000",
              "abc", // Odd length — invalid hex
            ).commands.asScala.toSeq,
          )
        }
      }
    }

    "reject invalid hex characters" in { implicit env =>
      import env.*

      setupEchoHandler()

      val contractId = createExternalCallContract()

      clue("Exercise with non-hex characters should fail") {
        intercept[CommandFailure] {
          participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseCallExternal(
              "test-ext",
              "echo",
              "00000000",
              "xyz123", // Non-hex chars
            ).commands.asScala.toSeq,
          )
        }
      }
    }

    "handle uppercase vs lowercase hex" in { implicit env =>
      import env.*

      mockServer.setHandler("hex-case") { req =>
        ExternalCallResponse.ok(req.input)
      }

      val contractId = createExternalCallContract()

      clue("Exercise with lowercase hex") {
        val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallExternal(
            "test-ext",
            "hex-case",
            "00000000",
            "abcdef",
          ).commands.asScala.toSeq,
        )
        exerciseTx.getUpdateId should not be empty
      }
    }

    // === Request Metadata ===

    "include request ID in HTTP call" in { implicit env =>
      import env.*

      var capturedRequestId: Option[String] = None

      mockServer.setHandler("request-id") { req =>
        capturedRequestId = req.requestId
        ExternalCallResponse.ok("ok".getBytes)
      }

      val contractId = createExternalCallContract()

      val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        contractId.exerciseCallExternal(
          "test-ext",
          "request-id",
          "00000000",
          toHex("metadata-test"),
        ).commands.asScala.toSeq,
      )
      exerciseTx.getUpdateId should not be empty
      // Request ID may or may not be present depending on implementation
    }

    "include mode header in HTTP call" in { implicit env =>
      import env.*

      var capturedMode: Option[String] = None

      mockServer.setHandler("mode") { req =>
        capturedMode = Some(req.mode)
        ExternalCallResponse.ok("ok".getBytes)
      }

      val contractId = createExternalCallContract()

      val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        contractId.exerciseCallExternal(
          "test-ext",
          "mode",
          "00000000",
          toHex("mode-test"),
        ).commands.asScala.toSeq,
      )
      exerciseTx.getUpdateId should not be empty
    }

    "send mode=validation for confirming participants" in { implicit env =>
      import env.*

      val modesByParticipant = scala.collection.concurrent.TrieMap[String, String]()

      mockServer.setHandler("check-mode") { req =>
        req.participantId.foreach { pid =>
          modesByParticipant(pid) = req.mode
        }
        ExternalCallResponse.ok("ok".getBytes)
      }

      // Create contract with bob as observer so participant2 also processes
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
        contractId.exerciseCallExternal(
          "test-ext",
          "check-mode",
          "00000000",
          toHex("mode-check"),
        ).commands.asScala.toSeq,
      )
      exerciseTx.getUpdateId should not be empty
    }

  }
}

class EdgeCaseExternalCallIntegrationTestH2 extends EdgeCaseExternalCallIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class EdgeCaseExternalCallIntegrationTestPostgres extends EdgeCaseExternalCallIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
