// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.ViewPosition
import com.digitalasset.canton.participant.protocol.validation.ExternalCallConsistencyChecker.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.daml.lf.data.{Bytes => LfBytes}
import org.scalatest.wordspec.AnyWordSpec

class ExternalCallConsistencyCheckerTest extends AnyWordSpec with BaseTest {

  private val alice = LfPartyId.assertFromString("alice")
  private val bob = LfPartyId.assertFromString("bob")
  private val charlie = LfPartyId.assertFromString("charlie")

  import ViewPosition.MerkleSeqIndex
  import ViewPosition.MerkleSeqIndex.Direction.{Left, Right}

  private val viewPos0 = ViewPosition.root
  private val viewPos1 = MerkleSeqIndex(List(Left)) +: ViewPosition.root
  private val viewPos2 = MerkleSeqIndex(List(Right)) +: ViewPosition.root

  private val checker = new ExternalCallConsistencyChecker()

  private def toBytes(s: String): LfBytes = LfBytes.fromStringUtf8(s)

  // Helper to create ExternalCallKey
  private def key(functionId: String, input: LfBytes = toBytes("input1")): ExternalCallKey =
    ExternalCallKey(
      extensionId = "test-ext",
      functionId = functionId,
      config = toBytes("config1"),
      input = input,
    )

  // Helper to create ExternalCallWithContext
  private def call(
      functionId: String,
      output: LfBytes,
      viewPosition: ViewPosition,
      signatories: Set[LfPartyId],
      input: LfBytes = toBytes("input1"),
  ): ExternalCallWithContext =
    ExternalCallWithContext(
      key = key(functionId, input),
      output = output,
      viewPosition = viewPosition,
      signatories = signatories,
    )

  "ExternalCallKey" should {

    "be equal for identical calls" in {
      val key1 = ExternalCallKey("ext1", "func1", toBytes("config1"), toBytes("input1"))
      val key2 = ExternalCallKey("ext1", "func1", toBytes("config1"), toBytes("input1"))
      key1 shouldBe key2
    }

    "be different when extensionId differs" in {
      val key1 = ExternalCallKey("ext1", "func1", toBytes("config1"), toBytes("input1"))
      val key2 = ExternalCallKey("ext2", "func1", toBytes("config1"), toBytes("input1"))
      key1 should not be key2
    }

    "be different when functionId differs" in {
      val key1 = ExternalCallKey("ext1", "func1", toBytes("config1"), toBytes("input1"))
      val key2 = ExternalCallKey("ext1", "func2", toBytes("config1"), toBytes("input1"))
      key1 should not be key2
    }

    "be different when config differs" in {
      val key1 = ExternalCallKey("ext1", "func1", toBytes("config1"), toBytes("input1"))
      val key2 = ExternalCallKey("ext1", "func1", toBytes("config2"), toBytes("input1"))
      key1 should not be key2
    }

    "be different when input differs" in {
      val key1 = ExternalCallKey("ext1", "func1", toBytes("config1"), toBytes("input1"))
      val key2 = ExternalCallKey("ext1", "func1", toBytes("config1"), toBytes("input2"))
      key1 should not be key2
    }

    "not include output in equality (it's in ExternalCallWithContext, not key)" in {
      // ExternalCallKey doesn't have output - this test verifies the design
      val key1 = key("func1", toBytes("input1"))
      val key2 = key("func1", toBytes("input1"))
      key1 shouldBe key2
    }

    "have proper pretty printing" in {
      val k = ExternalCallKey("ext1", "func1", toBytes("config1"), toBytes("input1"))
      val prettyStr = k.toString
      prettyStr should include("ext1")
      prettyStr should include("func1")
    }
  }

  "ExternalCallConsistencyResults" should {

    "report allConsistent when all parties are consistent" in {
      val results = ExternalCallConsistencyResults(Map(alice -> Consistent, bob -> Consistent))
      results.allConsistent shouldBe true
      results.inconsistentParties shouldBe empty
    }

    "report not allConsistent when any party is inconsistent" in {
      val results = ExternalCallConsistencyResults(
        Map[LfPartyId, PartyConsistencyResult](
          alice -> Inconsistent(key("func1"), Set(toBytes("out1"), toBytes("out2")), Set(viewPos0, viewPos1)),
          bob -> Consistent,
        )
      )
      results.allConsistent shouldBe false
      results.inconsistentParties shouldBe Set(alice)
    }

    "handle empty results" in {
      val results = ExternalCallConsistencyResults.empty
      results.allConsistent shouldBe true
      results.inconsistentParties shouldBe empty
    }
  }

  "checkConsistency" when {

    "no external calls" should {

      "return empty results for empty calls" in {
        val result = checker.checkConsistency(Seq.empty, Set(alice, bob))
        result shouldBe ExternalCallConsistencyResults.empty
      }

      "return empty results for empty parties" in {
        val calls = Seq(call("func1", toBytes("output1"), viewPos0, Set(alice)))
        val result = checker.checkConsistency(calls, Set.empty)
        result shouldBe ExternalCallConsistencyResults.empty
      }
    }

    "single party" should {

      "return Consistent for a single call" in {
        val calls = Seq(call("func1", toBytes("output1"), viewPos0, Set(alice)))
        val result = checker.checkConsistency(calls, Set(alice))
        result.results shouldBe Map(alice -> Consistent)
      }

      "return Consistent for two equal calls with same output" in {
        val calls = Seq(
          call("func1", toBytes("output1"), viewPos0, Set(alice)),
          call("func1", toBytes("output1"), viewPos1, Set(alice)),
        )
        val result = checker.checkConsistency(calls, Set(alice))
        result.results shouldBe Map(alice -> Consistent)
      }

      "return Inconsistent for two equal calls with different outputs" in {
        val calls = Seq(
          call("func1", toBytes("output1"), viewPos0, Set(alice)),
          call("func1", toBytes("output2"), viewPos1, Set(alice)),
        )
        val result = checker.checkConsistency(calls, Set(alice))

        result.results(alice) match {
          case Inconsistent(k, outputs, positions) =>
            k.extensionId shouldBe "test-ext"
            k.functionId shouldBe "func1"
            k.config shouldBe toBytes("config1")
            k.input shouldBe toBytes("input1")
            outputs shouldBe Set(toBytes("output1"), toBytes("output2"))
            positions shouldBe Set(viewPos0, viewPos1)
          case Consistent =>
            fail("Expected Inconsistent result")
        }
      }

      "return Consistent for different calls (different keys)" in {
        val calls = Seq(
          call("func1", toBytes("output1"), viewPos0, Set(alice)),
          call("func2", toBytes("output2"), viewPos1, Set(alice)),
        )
        val result = checker.checkConsistency(calls, Set(alice))
        result.results shouldBe Map(alice -> Consistent)
      }

      "return Consistent for same function but different inputs" in {
        val calls = Seq(
          call("func1", toBytes("output1"), viewPos0, Set(alice), input = toBytes("input1")),
          call("func1", toBytes("output2"), viewPos1, Set(alice), input = toBytes("input2")),
        )
        val result = checker.checkConsistency(calls, Set(alice))
        result.results shouldBe Map(alice -> Consistent)
      }

      "return Consistent when party is not a signatory of any call" in {
        val calls = Seq(call("func1", toBytes("output1"), viewPos0, Set(bob)))
        val result = checker.checkConsistency(calls, Set(alice))
        result.results shouldBe Map(alice -> Consistent)
      }
    }

    "multiple parties with same signatory set" should {

      "return same result for all parties" in {
        val calls = Seq(
          call("func1", toBytes("output1"), viewPos0, Set(alice, bob)),
          call("func1", toBytes("output2"), viewPos1, Set(alice, bob)),
        )
        val result = checker.checkConsistency(calls, Set(alice, bob))

        // Both should see the same inconsistency
        result.results(alice) shouldBe a[Inconsistent]
        result.results(bob) shouldBe a[Inconsistent]
      }
    }

    "multiple parties with different signatory sets (the key scenario)" should {

      "return different results when parties see different subsets" in {
        // This is the critical scenario from the design:
        // - Call 1: signatories = {Alice, Bob}, output = "out1"
        // - Call 2: signatories = {Alice}, output = "out2"
        // Alice sees both → detects inconsistency
        // Bob sees only call 1 → no inconsistency

        val calls = Seq(
          call("func1", toBytes("out1"), viewPos0, Set(alice, bob)),
          call("func1", toBytes("out2"), viewPos1, Set(alice)),
        )
        val result = checker.checkConsistency(calls, Set(alice, bob))

        result.results(alice) shouldBe a[Inconsistent]
        result.results(bob) shouldBe Consistent
      }

      "handle three parties with varying visibility" in {
        // Call 1: {Alice, Bob, Charlie}, output = "out1"
        // Call 2: {Alice, Bob}, output = "out2"
        // Call 3: {Alice}, output = "out3"
        // Alice sees all → inconsistent
        // Bob sees calls 1,2 → inconsistent (out1 vs out2)
        // Charlie sees only call 1 → consistent

        val calls = Seq(
          call("func1", toBytes("out1"), viewPos0, Set(alice, bob, charlie)),
          call("func1", toBytes("out2"), viewPos1, Set(alice, bob)),
          call("func1", toBytes("out3"), viewPos2, Set(alice)),
        )
        val result = checker.checkConsistency(calls, Set(alice, bob, charlie))

        result.results(alice) shouldBe a[Inconsistent]
        result.results(bob) shouldBe a[Inconsistent]
        result.results(charlie) shouldBe Consistent
      }

      "handle parties that see completely disjoint calls" in {
        // Call 1: {Alice}, output = "out1"
        // Call 2: {Bob}, output = "out2"
        // Same key, but Alice and Bob see different calls
        // Both should be consistent (they each see only one call)

        val calls = Seq(
          call("func1", toBytes("out1"), viewPos0, Set(alice)),
          call("func1", toBytes("out2"), viewPos1, Set(bob)),
        )
        val result = checker.checkConsistency(calls, Set(alice, bob))

        result.results(alice) shouldBe Consistent
        result.results(bob) shouldBe Consistent
      }

      "handle overlapping parties with consistent outputs" in {
        val calls = Seq(
          call("func1", toBytes("same-output"), viewPos0, Set(alice, bob)),
          call("func1", toBytes("same-output"), viewPos1, Set(alice)),
        )
        val result = checker.checkConsistency(calls, Set(alice, bob))

        result.results(alice) shouldBe Consistent
        result.results(bob) shouldBe Consistent
      }
    }

    "edge cases" should {

      "handle three or more calls with same key" in {
        // 3 calls: two with output1, one with output2
        val calls = Seq(
          call("func1", toBytes("output1"), viewPos0, Set(alice)),
          call("func1", toBytes("output1"), viewPos1, Set(alice)),
          call("func1", toBytes("output2"), viewPos2, Set(alice)),
        )
        val result = checker.checkConsistency(calls, Set(alice))

        result.results(alice) match {
          case Inconsistent(_, outputs, _) =>
            outputs shouldBe Set(toBytes("output1"), toBytes("output2"))
          case _ =>
            fail("Expected Inconsistent")
        }
      }

      "handle multiple different external calls in same transaction" in {
        // func1 is consistent, func2 is inconsistent
        val calls = Seq(
          call("func1", toBytes("out1"), viewPos0, Set(alice)),
          call("func1", toBytes("out1"), viewPos1, Set(alice)),
          call("func2", toBytes("outA"), viewPos0, Set(alice)),
          call("func2", toBytes("outB"), viewPos1, Set(alice)),
        )
        val result = checker.checkConsistency(calls, Set(alice))

        // Should detect the func2 inconsistency
        result.results(alice) shouldBe a[Inconsistent]
        result.results(alice).asInstanceOf[Inconsistent].key.functionId shouldBe "func2"
      }

      "handle empty signatory set for a call" in {
        val calls = Seq(call("func1", toBytes("output1"), viewPos0, Set.empty))
        val result = checker.checkConsistency(calls, Set(alice))

        // Alice is not a signatory, so she doesn't validate this call
        result.results(alice) shouldBe Consistent
      }

      "handle party hosted but not signatory of any call" in {
        val calls = Seq(
          call("func1", toBytes("out1"), viewPos0, Set(bob)),
          call("func1", toBytes("out2"), viewPos1, Set(bob)),
        )
        val result = checker.checkConsistency(calls, Set(alice, bob))

        // Alice sees nothing → consistent
        // Bob sees inconsistency
        result.results(alice) shouldBe Consistent
        result.results(bob) shouldBe a[Inconsistent]
      }

      "handle large number of parties efficiently" in {
        val manyParties = (1 to 50).map(i => LfPartyId.assertFromString(s"party$i")).toSet
        val calls = Seq(call("func1", toBytes("output1"), viewPos0, manyParties))
        val result = checker.checkConsistency(calls, manyParties)

        result.results.size shouldBe 50
        result.allConsistent shouldBe true
      }

      "handle large number of calls efficiently" in {
        val manyCalls = (1 to 100).map { i =>
          call(s"func$i", toBytes(s"output$i"), viewPos0, Set(alice))
        }
        val result = checker.checkConsistency(manyCalls, Set(alice))

        result.results(alice) shouldBe Consistent
      }

      "handle calls with same key across many views" in {
        // Create unique view positions using different direction sequences
        val positions = (0 to 10).map { i =>
          // Create a unique path by encoding the index as Left/Right directions
          val directions = Integer.toBinaryString(i + 1).toList.map {
            case '0' => Left
            case '1' => Right
          }
          MerkleSeqIndex(directions) +: ViewPosition.root
        }
        val calls = positions.map(pos => call("func1", toBytes("same-output"), pos, Set(alice)))
        val result = checker.checkConsistency(calls, Set(alice))

        result.results(alice) shouldBe Consistent
      }
    }
  }

  // Note: collectExternalCalls is tested via integration tests in
  // ExternalCallConsistencyIntegrationTest as it requires complex
  // ViewValidationResult/ParticipantTransactionView setup with real transactions.

  "Inconsistent result" should {

    "contain the key that caused inconsistency" in {
      val calls = Seq(
        call("myFunc", toBytes("out1"), viewPos0, Set(alice)),
        call("myFunc", toBytes("out2"), viewPos1, Set(alice)),
      )
      val result = checker.checkConsistency(calls, Set(alice))

      result.results(alice) match {
        case Inconsistent(k, _, _) =>
          k.functionId shouldBe "myFunc"
        case _ =>
          fail("Expected Inconsistent")
      }
    }

    "contain all differing outputs" in {
      val calls = Seq(
        call("func1", toBytes("out1"), viewPos0, Set(alice)),
        call("func1", toBytes("out2"), viewPos1, Set(alice)),
        call("func1", toBytes("out3"), viewPos2, Set(alice)),
      )
      val result = checker.checkConsistency(calls, Set(alice))

      result.results(alice) match {
        case Inconsistent(_, outputs, _) =>
          outputs shouldBe Set(toBytes("out1"), toBytes("out2"), toBytes("out3"))
        case _ =>
          fail("Expected Inconsistent")
      }
    }

    "contain all view positions where inconsistency was found" in {
      val calls = Seq(
        call("func1", toBytes("out1"), viewPos0, Set(alice)),
        call("func1", toBytes("out2"), viewPos1, Set(alice)),
      )
      val result = checker.checkConsistency(calls, Set(alice))

      result.results(alice) match {
        case Inconsistent(_, _, positions) =>
          positions shouldBe Set(viewPos0, viewPos1)
        case _ =>
          fail("Expected Inconsistent")
      }
    }
  }
}
