// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.ViewPosition
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, LfPartyId, ProtocolVersionChecksAnyWordSpec}
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.transaction.ExternalCallResult
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext

class ExternalCallConsistencyCheckerTest
    extends AnyWordSpec
    with BaseTest
    with ProtocolVersionChecksAnyWordSpec
    with ExternalCallValidationTestUtil {

  implicit val ec: ExecutionContext = directExecutionContext

  protected val factory: ExampleTransactionFactory = new ExampleTransactionFactory()()

  private val partyA: LfPartyId = ExampleTransactionFactory.signatory
  private val partyB: LfPartyId = ExampleTransactionFactory.submitter
  private val partyC: LfPartyId = ExampleTransactionFactory.observer

  private def check(
      leftCheckingParties: Set[LfPartyId],
      rightCheckingParties: Set[LfPartyId],
      hostedParties: Set[LfPartyId],
      rightResult: ExternalCallResult = otherExternalCallResult,
  ): ExternalCallConsistencyChecker.Result = {
    val example = factory.MultipleRoots
    val left = withExternalCallResults(
      example.rootViews(4),
      Seq(
        externalCallViewResult(
          exerciseIndex = 0,
          result = externalCallResult,
          checkingParties = leftCheckingParties,
        )
      ),
    )
    val right = withExternalCallResults(
      example.rootViews(5),
      Seq(
        externalCallViewResult(
          exerciseIndex = 0,
          result = rightResult,
          checkingParties = rightCheckingParties,
        )
      ),
    )

    ExternalCallConsistencyChecker.check(
      Map(
        ViewPosition.root -> participantView(left),
        ViewPosition(
          List(ViewPosition.MerkleSeqIndex(List(ViewPosition.MerkleSeqIndex.Direction.Right)))
        ) ->
          participantView(right),
      ),
      hostedParties,
    )
  }

  "ExternalCallConsistencyChecker" should {
    "report only hosted parties that check conflicting outputs" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val result = check(
        leftCheckingParties = Set(partyA),
        rightCheckingParties = Set(partyA),
        hostedParties = Set(partyA, partyB),
      )

      result.inconsistentParties shouldBe Set(partyA)
    }

    "not report conflicting outputs for disjoint checking parties" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val result = check(
        leftCheckingParties = Set(partyA),
        rightCheckingParties = Set(partyB),
        hostedParties = Set(partyA, partyB),
      )

      result.inconsistentParties shouldBe Set.empty
      result.visibleInconsistencies should have size 1
    }

    "record visible disagreements without reporting non-hosted checking parties" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val result = check(
        leftCheckingParties = Set(partyC),
        rightCheckingParties = Set(partyC),
        hostedParties = Set(partyA, partyB),
      )

      result.inconsistentParties shouldBe Set.empty
      result.visibleInconsistencies should have size 1
      val visibleInconsistency = result.visibleInconsistencies.loneElement
      visibleInconsistency.outputs shouldBe Set(
        externalCallResult.output,
        otherExternalCallResult.output,
      )
    }

    "not report identical outputs" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val result = check(
        leftCheckingParties = Set(partyA),
        rightCheckingParties = Set(partyA),
        hostedParties = Set(partyA),
        rightResult = externalCallResult,
      )

      result.inconsistentParties shouldBe Set.empty
    }

    "not report different semantic calls with different outputs" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val result = check(
        leftCheckingParties = Set(partyA),
        rightCheckingParties = Set(partyA),
        hostedParties = Set(partyA),
        rightResult = otherExternalCallResult.copy(functionId = "other-function"),
      )

      result.inconsistentParties shouldBe Set.empty
    }

    "report repeated semantic calls on the same node with different outputs" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val example = factory.MultipleRoots
      val view = withExternalCallResults(
        example.rootViews(4),
        Seq(
          externalCallViewResult(
            exerciseIndex = 0,
            result = externalCallResult,
            checkingParties = Set(partyA),
            callIndex = 0,
          ),
          externalCallViewResult(
            exerciseIndex = 0,
            result = otherExternalCallResult,
            checkingParties = Set(partyA),
            callIndex = 1,
          ),
        ),
      )

      val result = ExternalCallConsistencyChecker.check(
        Map(ViewPosition.root -> participantView(view)),
        Set(partyA),
      )

      result.inconsistentParties shouldBe Set(partyA)
      val inconsistency = result.hostedInconsistencies(partyA).loneElement
      inconsistency.outputs shouldBe Set(externalCallResult.output, otherExternalCallResult.output)
      inconsistency.occurrences.map(occurrence =>
        occurrence.exerciseIndex -> occurrence.callIndex
      ) shouldBe Set(
        NonNegativeInt.zero -> NonNegativeInt.zero,
        NonNegativeInt.zero -> NonNegativeInt.one,
      )
    }

    "report all independent disagreements for the same hosted checking party" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val example = factory.MultipleRoots
      val firstCall = externalCallResult.copy(functionId = "function-a")
      val secondCall = externalCallResult.copy(functionId = "function-b")
      val left = withExternalCallResults(
        example.rootViews(4),
        Seq(
          externalCallViewResult(
            exerciseIndex = 0,
            result = firstCall,
            checkingParties = Set(partyA),
          ),
          externalCallViewResult(
            exerciseIndex = 1,
            result = secondCall,
            checkingParties = Set(partyA),
          ),
        ),
      )
      val right = withExternalCallResults(
        example.rootViews(5),
        Seq(
          externalCallViewResult(
            exerciseIndex = 0,
            result = firstCall.copy(output = Bytes.fromStringUtf8("other-output-a")),
            checkingParties = Set(partyA),
          ),
          externalCallViewResult(
            exerciseIndex = 1,
            result = secondCall.copy(output = Bytes.fromStringUtf8("other-output-b")),
            checkingParties = Set(partyA),
          ),
        ),
      )

      val result = ExternalCallConsistencyChecker.check(
        Map(
          ViewPosition.root -> participantView(left),
          ViewPosition(
            List(ViewPosition.MerkleSeqIndex(List(ViewPosition.MerkleSeqIndex.Direction.Right)))
          ) ->
            participantView(right),
        ),
        Set(partyA),
      )

      result.inconsistentParties shouldBe Set(partyA)
      result.hostedInconsistencies(partyA).map(_.key.functionId).toSet shouldBe Set(
        "function-a",
        "function-b",
      )
    }

    "return the empty result for views without external-call results" in {
      val example = factory.MultipleRoots

      val result = ExternalCallConsistencyChecker.check(
        Map(ViewPosition.root -> participantView(example.rootViews(4))),
        Set(partyA),
      )

      result shouldBe ExternalCallConsistencyChecker.Result.empty
    }
  }
}
