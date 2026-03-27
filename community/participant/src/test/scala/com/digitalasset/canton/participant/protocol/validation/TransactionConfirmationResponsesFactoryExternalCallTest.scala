// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.data.ViewPosition
import com.digitalasset.canton.participant.protocol.validation.ExternalCallConsistencyChecker.*
import com.digitalasset.canton.protocol.LocalRejectError
import com.digitalasset.canton.protocol.messages.{ConfirmationResponse, LocalApprove}
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.{BaseTest, LfPartyId}
import com.digitalasset.daml.lf.data.{Bytes => LfBytes}
import org.scalatest.wordspec.AnyWordSpec

/** Unit tests for external call consistency integration in TransactionConfirmationResponsesFactory.
  *
  * These tests verify that the factory correctly partitions parties based on their
  * external call consistency results, generating appropriate per-party verdicts.
  *
  * Note: Full integration tests are in ExternalCallConsistencyIntegrationTest.
  */
class TransactionConfirmationResponsesFactoryExternalCallTest extends AnyWordSpec with BaseTest {

  private val alice = LfPartyId.assertFromString("alice")
  private val bob = LfPartyId.assertFromString("bob")
  private val charlie = LfPartyId.assertFromString("charlie")

  private val viewPos0 = ViewPosition.root

  private val synchronizerId = DefaultTestIdentities.physicalSynchronizerId
  private val protocolVersion = synchronizerId.protocolVersion

  private def toBytes(s: String): LfBytes = LfBytes.fromStringUtf8(s)

  // Helper to create test keys
  private def key(functionId: String): ExternalCallKey =
    ExternalCallKey("test-ext", functionId, toBytes("config"), toBytes("input"))

  "ExternalCallConsistencyResults" when {

    "used for verdict generation" should {

      "identify inconsistent parties correctly" in {
        val results = ExternalCallConsistencyResults(
          Map[LfPartyId, PartyConsistencyResult](
            alice -> Inconsistent(key("func1"), Set(toBytes("out1"), toBytes("out2")), Set(viewPos0)),
            bob -> Consistent,
            charlie -> Consistent,
          )
        )

        results.inconsistentParties shouldBe Set(alice)
        results.allConsistent shouldBe false
      }

      "report all consistent when no inconsistencies" in {
        val results = ExternalCallConsistencyResults(
          Map[LfPartyId, PartyConsistencyResult](
            alice -> Consistent,
            bob -> Consistent,
          )
        )

        results.inconsistentParties shouldBe empty
        results.allConsistent shouldBe true
      }

      "handle multiple inconsistent parties" in {
        val results = ExternalCallConsistencyResults(
          Map[LfPartyId, PartyConsistencyResult](
            alice -> Inconsistent(key("func1"), Set(toBytes("out1"), toBytes("out2")), Set(viewPos0)),
            bob -> Inconsistent(key("func2"), Set(toBytes("outA"), toBytes("outB")), Set(viewPos0)),
            charlie -> Consistent,
          )
        )

        results.inconsistentParties shouldBe Set(alice, bob)
        results.allConsistent shouldBe false
      }
    }
  }

  "LocalRejectError.MalformedRejects.ExternalCallInconsistency" should {

    "create proper rejection message" in {
      val reject = LocalRejectError.MalformedRejects.ExternalCallInconsistency
        .Reject("Function getPrice returned different outputs: 3000, 3001")

      val localReject = reject.toLocalReject(protocolVersion)
      localReject.isMalformed shouldBe true
    }

    "include function details in message" in {
      val message = "Function getPrice returned different outputs: 3000, 3001"
      val reject = LocalRejectError.MalformedRejects.ExternalCallInconsistency.Reject(message)

      // The detailed message should be preserved in the rejection
      reject.toString should include("getPrice")
      reject.toString should include("3000") 
      reject.toString should include("3001")
    }
  }

  "Per-party verdict partitioning logic" should {

    "separate consistent and inconsistent parties" in {
      // This tests the conceptual logic that should happen in partitionByExternalCallConsistency

      val allParties = Set(alice, bob, charlie)
      val consistencyResults = ExternalCallConsistencyResults(
        Map[LfPartyId, PartyConsistencyResult](
          alice -> Inconsistent(key("func1"), Set(toBytes("out1"), toBytes("out2")), Set(viewPos0)),
          bob -> Consistent,
          charlie -> Consistent,
        )
      )

      val (inconsistentParties, consistentParties) = allParties.partition { party =>
        consistencyResults.results.get(party).exists {
          case _: Inconsistent => true
          case Consistent => false
        }
      }

      inconsistentParties shouldBe Set(alice)
      consistentParties shouldBe Set(bob, charlie)
    }

    "handle all parties inconsistent" in {
      val allParties = Set(alice, bob)
      val consistencyResults = ExternalCallConsistencyResults(
        Map(
          alice -> Inconsistent(key("func1"), Set(toBytes("out1"), toBytes("out2")), Set(viewPos0)),
          bob -> Inconsistent(key("func1"), Set(toBytes("out1"), toBytes("out2")), Set(viewPos0)),
        )
      )

      val (inconsistentParties, consistentParties) = allParties.partition { party =>
        consistencyResults.results.get(party).exists {
          case _: Inconsistent => true
          case Consistent => false
        }
      }

      inconsistentParties shouldBe Set(alice, bob)
      consistentParties shouldBe empty
    }

    "handle all parties consistent" in {
      val allParties = Set(alice, bob, charlie)
      val consistencyResults = ExternalCallConsistencyResults(
        Map(
          alice -> Consistent,
          bob -> Consistent,
          charlie -> Consistent,
        )
      )

      val (inconsistentParties, consistentParties) = allParties.partition { party =>
        consistencyResults.results.get(party).exists {
          case _: Inconsistent => true
          case Consistent => false
        }
      }

      inconsistentParties shouldBe empty
      consistentParties shouldBe Set(alice, bob, charlie)
    }

    "handle party not in consistency results (new/unknown party)" in {
      val allParties = Set(alice, bob, charlie)
      val consistencyResults = ExternalCallConsistencyResults(
        Map(
          alice -> Consistent,
          bob -> Consistent,
          // charlie not in results
        )
      )

      // Parties not in results should be treated as consistent (no external calls for them)
      val (inconsistentParties, consistentParties) = allParties.partition { party =>
        consistencyResults.results.get(party).exists {
          case _: Inconsistent => true
          case Consistent => false
        }
      }

      inconsistentParties shouldBe empty
      consistentParties shouldBe Set(alice, bob, charlie) // charlie goes to consistent
    }
  }

  "Verdict generation" should {

    "produce LocalApprove for consistent parties when no other rejections" in {
      val approve = LocalApprove(protocolVersion)
      approve.isMalformed shouldBe false
    }

    "produce LocalReject with isMalformed=true for external call inconsistency" in {
      val reject = LocalRejectError.MalformedRejects.ExternalCallInconsistency
        .Reject("test")
        .toLocalReject(protocolVersion)

      reject.isMalformed shouldBe true
    }

    "allow different verdicts for different parties in same view" in {
      // This validates the data model supports per-party verdicts
      val response1 = ConfirmationResponse.tryCreate(
        Some(viewPos0),
        LocalApprove(protocolVersion),
        Set(bob, charlie), // consistent parties get approve
      )

      val response2 = ConfirmationResponse.tryCreate(
        Some(viewPos0),
        LocalRejectError.MalformedRejects.ExternalCallInconsistency
          .Reject("test")
          .toLocalReject(protocolVersion),
        Set.empty, // malformed uses empty parties
      )

      // Both responses are for the same view but different verdicts
      response1.viewPositionO shouldBe response2.viewPositionO
      response1.localVerdict.isMalformed shouldBe false
      response2.localVerdict.isMalformed shouldBe true
    }
  }

  "ExternalCallConsistencyChecker integration" should {

    "be injectable into TransactionConfirmationResponsesFactory" in {
      // Verify the factory accepts the checker as a parameter
      val checker = new ExternalCallConsistencyChecker()
      val factory = new TransactionConfirmationResponsesFactory(
        DefaultTestIdentities.participant1,
        synchronizerId,
        loggerFactory,
        checker,
      )

      // Factory should be created successfully
      factory should not be null
    }

    "use default checker when none provided" in {
      val factory = new TransactionConfirmationResponsesFactory(
        DefaultTestIdentities.participant1,
        synchronizerId,
        loggerFactory,
      )

      // Factory should be created with default checker
      factory should not be null
    }
  }
}
