// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.externalcall

import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2, UsePostgres}
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}

import scala.concurrent.duration.*
import scala.util.Random

/** Integration tests for edge cases in external calls.
  *
  * Tests:
  * - Empty input/output
  * - Large payloads (100KB+)
  * - All byte values (0x00-0xFF)
  * - Unicode handling
  * - Special characters in extension/function IDs
  * - Config hash edge cases
  * - Concurrent external calls
  */
sealed trait EdgeCaseExternalCallIntegrationTest
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

        clue("Enable parties") {
          alice = participant1.parties.enable("alice")
          bob = participant2.parties.enable(
            "bob",
            synchronizeParticipants = Seq(participant1),
          )
        }
      }

  "edge cases for external calls" should {

    // === Empty Values ===

    "handle empty input" in { implicit env =>
      import env.*

      mockServer.setHandler("empty-input") { req =>
        // Verify input is empty
        require(req.input.isEmpty, s"Expected empty input, got ${req.input.length} bytes")
        ExternalCallResponse.ok("received-empty".getBytes)
      }

      // Scenario: Call with empty input (hex string "")
      // Should work correctly

      pending
    }

    "handle empty output from service" in { implicit env =>
      import env.*

      mockServer.setHandler("empty-output") { _ =>
        ExternalCallResponse.ok(Array.empty[Byte])
      }

      // Scenario: Service returns empty response
      // Should be handled gracefully

      pending
    }

    "handle empty config hash" in { implicit env =>
      import env.*

      setupEchoHandler()

      // Scenario: Config hash is empty string
      // Should still work (empty is valid hex)

      // TODO: Exercise with configHash = ""

      pending
    }

    // === Large Payloads ===

    "handle 100KB input" in { implicit env =>
      import env.*

      mockServer.setHandler("large-input") { req =>
        require(req.input.length == 100 * 1024, s"Expected 100KB input, got ${req.input.length} bytes")
        ExternalCallResponse.ok("ok".getBytes)
      }

      // Scenario: Send 100KB of data
      // Should work without issues

      pending
    }

    "handle 100KB output" in { implicit env =>
      import env.*

      val largeOutput = Array.fill(100 * 1024)(0x58.toByte) // 100KB of 'X'

      mockServer.setHandler("large-output") { _ =>
        ExternalCallResponse.ok(largeOutput)
      }

      // Scenario: Receive 100KB response
      // Should work without issues

      pending
    }

    "handle 1MB input and output" in { implicit env =>
      import env.*

      mockServer.setHandler("megabyte") { req =>
        ExternalCallResponse.ok(req.input) // Echo back
      }

      // Scenario: 1MB round trip
      // Tests larger payload handling

      pending
    }

    // === Byte Value Coverage ===

    "handle all 256 byte values" in { implicit env =>
      import env.*

      // Create input with all possible byte values
      val allBytes = (0 to 255).map(_.toByte).toArray

      mockServer.setHandler("all-bytes") { req =>
        // Verify all byte values received
        req.input should contain theSameElementsAs allBytes
        ExternalCallResponse.ok(req.input) // Echo back
      }

      // Scenario: Input containing bytes 0x00 through 0xFF
      // Verifies no byte values are corrupted

      // TODO: Exercise with hex representation of allBytes

      pending
    }

    "handle null bytes in input" in { implicit env =>
      import env.*

      val inputWithNulls = Array[Byte](0x00, 0x00, 0x41, 0x00, 0x42, 0x00)

      mockServer.setHandler("nulls") { req =>
        req.input should contain theSameElementsAs inputWithNulls
        ExternalCallResponse.ok(req.input)
      }

      // Scenario: Input with embedded null bytes
      // Should not be treated as string terminator

      pending
    }

    // === Unicode and Special Characters ===

    "handle unicode text in hex encoding" in { implicit env =>
      import env.*

      // "Hello 世界" in UTF-8 bytes

      mockServer.setHandler("unicode") { req =>
        new String(req.input, "UTF-8") shouldBe "Hello 世界"
        ExternalCallResponse.ok(req.input)
      }

      // Scenario: Unicode text hex-encoded
      // Should round-trip correctly

      pending
    }

    "handle emoji in hex encoding" in { implicit env =>
      import env.*

      // "🚀🎉" in UTF-8 bytes

      mockServer.setHandler("emoji") { req =>
        new String(req.input, "UTF-8") shouldBe "🚀🎉"
        ExternalCallResponse.ok(req.input)
      }

      pending
    }

    "handle special characters in function ID" in { implicit env =>
      import env.*

      // Function IDs with special characters
      // Note: depends on what characters are allowed in function IDs

      mockServer.setHandler("get-price") { req =>
        ExternalCallResponse.ok(req.input)
      }

      // Test various function ID patterns
      // "get-price", "get_price", "getPrice", "get.price"

      pending
    }

    // === Config Hash Edge Cases ===

    "handle very long config hash" in { implicit env =>
      import env.*

      // 1000 character hex string

      setupEchoHandler()

      // Scenario: Very long config hash
      // Should work (though may have performance impact)

      pending
    }

    "handle config hash with all same digits" in { implicit env =>
      import env.*

      setupEchoHandler()

      // Config hashes like "00000000", "ffffffff"

      pending
    }

    // === Concurrent Calls ===

    "handle concurrent external calls from same contract" in { implicit env =>
      import env.*

      val callCount = new java.util.concurrent.atomic.AtomicInteger(0)

      mockServer.setHandler("concurrent") { _ =>
        val count = callCount.incrementAndGet()
        Thread.sleep(100) // Simulate some processing time
        ExternalCallResponse.ok(s"call-$count".getBytes)
      }

      // Scenario: Multiple sequential external calls
      // Verify they execute in order and don't interfere

      pending
    }

    "handle concurrent transactions with external calls" in { implicit env =>
      import env.*

      setupEchoHandler()

      // Scenario: Multiple transactions submitted concurrently,
      // each with external calls
      // All should complete correctly

      pending
    }

    // === Hex Encoding Edge Cases ===

    "reject invalid hex input (odd length)" in { implicit env =>
      import env.*

      // Hex string with odd length is invalid
      // e.g., "abc" instead of "ab" or "abcd"

      // This should be caught by DA.External.isBytesHex validation
      // before the external call is made

      pending
    }

    "reject invalid hex characters" in { implicit env =>
      import env.*

      // Non-hex characters like "xyz123"

      // Should fail validation

      pending
    }

    "handle uppercase vs lowercase hex" in { implicit env =>
      import env.*

      mockServer.setHandler("hex-case") { req =>
        // Verify bytes are correct regardless of hex case
        ExternalCallResponse.ok(req.input)
      }

      // Both "ABCD" and "abcd" should produce same bytes

      pending
    }

    // === Request Metadata ===

    "include request ID in HTTP call" in { implicit env =>
      import env.*

      var capturedRequestId: Option[String] = None

      mockServer.setHandler("request-id") { req =>
        capturedRequestId = req.requestId
        ExternalCallResponse.ok("ok".getBytes)
      }

      // TODO: Exercise and verify request ID was sent

      // capturedRequestId shouldBe defined

      pending
    }

    "include mode header in HTTP call" in { implicit env =>
      import env.*

      var capturedMode: Option[String] = None

      mockServer.setHandler("mode") { req =>
        capturedMode = Some(req.mode)
        ExternalCallResponse.ok("ok".getBytes)
      }

      // TODO: Exercise and verify mode was "submission"

      // capturedMode shouldBe Some("submission")

      pending
    }

    "send mode=validation for confirming participants" in { implicit env =>
      import env.*

      val modesByParticipant = scala.collection.mutable.Map[String, String]()

      mockServer.setHandler("check-mode") { req =>
        req.participantId.foreach { pid =>
          modesByParticipant(pid) = req.mode
        }
        ExternalCallResponse.ok("ok".getBytes)
      }

      // Scenario: Alice (P1) is signatory, Bob (P2) is stakeholder
      // P1 should send mode=submission
      // P2 should send mode=validation

      // TODO: Verify modes

      pending
    }

    // === Timing Edge Cases ===

    "handle external call that takes exactly timeout duration" in { implicit env =>
      import env.*

      // Service responds just at the timeout boundary
      // Behavior may be implementation-dependent

      pending
    }

    "handle clock skew between participants" in { implicit env =>
      import env.*

      // With static time, this is less relevant
      // But good to consider for real-world scenarios

      pending
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
