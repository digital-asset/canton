// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

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

  private val checkingParties: Set[LfPartyId] = Set(ExampleTransactionFactory.signatory)

  private def check(
      rightResult: ExternalCallResult = otherExternalCallResult
  ): Seq[ExternalCallConsistencyChecker.Inconsistency] = {
    val example = factory.MultipleRoots
    val left = withExternalCallResults(
      example.rootViews(4),
      Seq(
        externalCallViewResult(
          exerciseIndex = 0,
          result = externalCallResult,
          checkingParties = checkingParties,
        )
      ),
    )
    val right = withExternalCallResults(
      example.rootViews(5),
      Seq(
        externalCallViewResult(
          exerciseIndex = 0,
          result = rightResult,
          checkingParties = checkingParties,
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
      )
    )
  }

  "ExternalCallConsistencyChecker" should {
    "report conflicting outputs for the same call across views" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val inconsistency = check().loneElement

      inconsistency.outputs shouldBe Set(
        externalCallResult.output,
        otherExternalCallResult.output,
      )
      inconsistency.occurrences.map(_.viewPosition) shouldBe Set(
        ViewPosition.root,
        ViewPosition(
          List(ViewPosition.MerkleSeqIndex(List(ViewPosition.MerkleSeqIndex.Direction.Right)))
        ),
      )
    }

    "not report identical outputs" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      check(rightResult = externalCallResult) shouldBe Seq.empty
    }

    "not report different semantic calls with different outputs" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      check(rightResult = otherExternalCallResult.copy(functionId = "other-function")) shouldBe
        Seq.empty
    }

    "report all independent disagreements" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val example = factory.MultipleRoots
      val firstCall = externalCallResult.copy(functionId = "function-a")
      val secondCall = externalCallResult.copy(functionId = "function-b")
      val left = withExternalCallResults(
        example.rootViews(4),
        Seq(
          externalCallViewResult(
            exerciseIndex = 0,
            result = firstCall,
            checkingParties = checkingParties,
          ),
          externalCallViewResult(
            exerciseIndex = 1,
            result = secondCall,
            checkingParties = checkingParties,
          ),
        ),
      )
      val right = withExternalCallResults(
        example.rootViews(5),
        Seq(
          externalCallViewResult(
            exerciseIndex = 0,
            result = firstCall.copy(output = Bytes.fromStringUtf8("other-output-a")),
            checkingParties = checkingParties,
          ),
          externalCallViewResult(
            exerciseIndex = 1,
            result = secondCall.copy(output = Bytes.fromStringUtf8("other-output-b")),
            checkingParties = checkingParties,
          ),
        ),
      )

      val inconsistencies = ExternalCallConsistencyChecker.check(
        Map(
          ViewPosition.root -> participantView(left),
          ViewPosition(
            List(ViewPosition.MerkleSeqIndex(List(ViewPosition.MerkleSeqIndex.Direction.Right)))
          ) ->
            participantView(right),
        )
      )

      inconsistencies.map(_.key.functionId) shouldBe Seq("function-a", "function-b")
    }

    "return no inconsistencies for views without external-call results" in {
      val example = factory.MultipleRoots

      ExternalCallConsistencyChecker.check(
        Map(ViewPosition.root -> participantView(example.rootViews(4)))
      ) shouldBe Seq.empty
    }
  }
}
