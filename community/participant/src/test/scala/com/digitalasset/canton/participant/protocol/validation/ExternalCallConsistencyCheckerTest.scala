// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.{ParticipantTransactionView, TransactionView, ViewPosition}
import com.digitalasset.canton.participant.protocol.validation.ExternalCallValidationTestUtil.{
  externalCallViewResult,
  withExternalCallResults,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, LfPartyId}
import com.digitalasset.daml.lf.data.{Bytes, ImmArray}
import com.digitalasset.daml.lf.transaction.ExternalCallResult
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext

class ExternalCallConsistencyCheckerTest extends AnyWordSpec with BaseTest {

  implicit val ec: ExecutionContext = directExecutionContext

  private val factory =
    new ExampleTransactionFactory(versionOverride = Some(ProtocolVersion.dev))()

  private val partyA = ExampleTransactionFactory.signatory
  private val partyB = ExampleTransactionFactory.submitter
  private val partyC = ExampleTransactionFactory.observer

  private val externalCallResult = ExternalCallResult(
    extensionId = "extension",
    functionId = "function",
    config = Bytes.fromStringUtf8("config"),
    input = Bytes.fromStringUtf8("input"),
    output = Bytes.fromStringUtf8("output"),
  )

  private val otherExternalCallOutput =
    externalCallResult.copy(output = Bytes.fromStringUtf8("other-output"))

  private def validationResult(view: TransactionView): ViewValidationResult =
    ViewValidationResult(
      ParticipantTransactionView.tryCreate(view),
      ViewActivenessResult(
        inactiveContracts = Set.empty,
        alreadyLockedContracts = Set.empty,
        existingContracts = Set.empty,
      ),
    )

  private def check(
      leftCheckingParties: Set[LfPartyId],
      rightCheckingParties: Set[LfPartyId],
      hostedParties: Set[LfPartyId],
      rightResult: ExternalCallResult = otherExternalCallOutput,
  ): ExternalCallConsistencyChecker.Result = {
    val example = factory.MultipleRoots
    val left = withExternalCallResults(
      example.rootViews(4),
      ImmArray(
        externalCallViewResult(
          exerciseIndex = 0,
          result = externalCallResult,
          checkingParties = leftCheckingParties,
        )
      ),
    )
    val right = withExternalCallResults(
      example.rootViews(5),
      ImmArray(
        externalCallViewResult(
          exerciseIndex = 0,
          result = rightResult,
          checkingParties = rightCheckingParties,
        )
      ),
    )

    ExternalCallConsistencyChecker.check(
      Map(
        ViewPosition.root -> validationResult(left),
        ViewPosition(
          List(ViewPosition.MerkleSeqIndex(List(ViewPosition.MerkleSeqIndex.Direction.Right)))
        ) ->
          validationResult(right),
      ),
      hostedParties,
    )
  }

  "ExternalCallConsistencyChecker" should {
    "report only hosted parties that check conflicting outputs" in {
      val result = check(
        leftCheckingParties = Set(partyA),
        rightCheckingParties = Set(partyA),
        hostedParties = Set(partyA, partyB),
      )

      result.inconsistentParties shouldBe Set(partyA)
    }

    "not report conflicting outputs for disjoint checking parties" in {
      val result = check(
        leftCheckingParties = Set(partyA),
        rightCheckingParties = Set(partyB),
        hostedParties = Set(partyA, partyB),
      )

      result.inconsistentParties shouldBe Set.empty
    }

    "not report non-hosted checking parties" in {
      val result = check(
        leftCheckingParties = Set(partyC),
        rightCheckingParties = Set(partyC),
        hostedParties = Set(partyA, partyB),
      )

      result.inconsistentParties shouldBe Set.empty
    }

    "not report identical outputs" in {
      val result = check(
        leftCheckingParties = Set(partyA),
        rightCheckingParties = Set(partyA),
        hostedParties = Set(partyA),
        rightResult = externalCallResult,
      )

      result.inconsistentParties shouldBe Set.empty
    }

    "not report different semantic calls with different outputs" in {
      val result = check(
        leftCheckingParties = Set(partyA),
        rightCheckingParties = Set(partyA),
        hostedParties = Set(partyA),
        rightResult = otherExternalCallOutput.copy(functionId = "other-function"),
      )

      result.inconsistentParties shouldBe Set.empty
    }

    "report repeated semantic calls on the same node with different outputs" in {
      val example = factory.MultipleRoots
      val view = withExternalCallResults(
        example.rootViews(4),
        ImmArray(
          externalCallViewResult(
            exerciseIndex = 0,
            result = externalCallResult,
            checkingParties = Set(partyA),
            callIndex = 0,
          ),
          externalCallViewResult(
            exerciseIndex = 0,
            result = otherExternalCallOutput,
            checkingParties = Set(partyA),
            callIndex = 1,
          ),
        ),
      )

      val result = ExternalCallConsistencyChecker.check(
        Map(ViewPosition.root -> validationResult(view)),
        Set(partyA),
      )

      result.inconsistentParties shouldBe Set(partyA)
      val inconsistency = result.inconsistencies(partyA).loneElement
      inconsistency.outputs shouldBe Set(externalCallResult.output, otherExternalCallOutput.output)
      inconsistency.occurrences.map(occurrence =>
        occurrence.exerciseIndex -> occurrence.callIndex
      ) shouldBe Set(
        NonNegativeInt.zero -> NonNegativeInt.zero,
        NonNegativeInt.zero -> NonNegativeInt.one,
      )
    }

    "report all independent disagreements for the same hosted checking party" in {
      val example = factory.MultipleRoots
      val firstCall = externalCallResult.copy(functionId = "function-a")
      val secondCall = externalCallResult.copy(functionId = "function-b")
      val left = withExternalCallResults(
        example.rootViews(4),
        ImmArray(
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
        ImmArray(
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
          ViewPosition.root -> validationResult(left),
          ViewPosition(
            List(ViewPosition.MerkleSeqIndex(List(ViewPosition.MerkleSeqIndex.Direction.Right)))
          ) ->
            validationResult(right),
        ),
        Set(partyA),
      )

      result.inconsistentParties shouldBe Set(partyA)
      result.inconsistencies(partyA).map(_.key.functionId).toSet shouldBe Set(
        "function-a",
        "function-b",
      )
    }
  }
}
