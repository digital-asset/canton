// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction.validator

import com.digitalasset.daml.lf.data.{Bytes, ImmArray}
import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.{LimitConfig, Limits}
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{
  ExternalCallResult,
  NodeId,
  SubmittedTransaction,
  VersionedTransaction,
}
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.HashMap

class ExternalCallResultsValidatorTest extends AnyFreeSpec with Matchers with Inside {

  import TransactionBuilder.Implicits.*
  import ValidatorTestLib.*

  val callResult1 = ExternalCallResult(
    "extId1",
    "funId1",
    Bytes.assertFromString("deadbeef"),
    Bytes.assertFromString("deadbeef"),
    Bytes.assertFromString("deadbeef"),
  )
  val callResult2 = ExternalCallResult(
    "extId2",
    "funId2",
    Bytes.assertFromString("deadbeef"),
    Bytes.assertFromString("deadbeef"),
    Bytes.assertFromString("deadbeef"),
  )

  "limit to 1 external call results" - {
    val limitConfig =
      LimitConfig(Limits.Lenient.copy(externalCallResults = 1), Some(transactionNodeMetrics))

    "allow a transaction with no external call results" in {
      val tx = SubmittedTransaction(
        VersionedTransaction(
          version,
          HashMap(NodeId(1) -> exerciseNode()),
          ImmArray(NodeId(1)),
        )
      )

      ExternalCallResultsValidator(metadata, Map.empty, limitConfig).validate(tx) shouldBe empty
      transactionNodeMetrics.externalCallResultsGauge.getValue shouldBe 0
    }

    "allow a transaction with one external call result" in {
      val tx = SubmittedTransaction(
        VersionedTransaction(
          version,
          HashMap(NodeId(1) -> exerciseNode(callResults = ImmArray(callResult1))),
          ImmArray(NodeId(1)),
        )
      )

      ExternalCallResultsValidator(metadata, Map.empty, limitConfig).validate(tx) shouldBe empty
      transactionNodeMetrics.externalCallResultsGauge.getValue shouldBe 1
    }

    "disallow a transaction with two external call results" in {
      val results = ImmArray(callResult1, callResult2)
      val tx = SubmittedTransaction(
        VersionedTransaction(
          version,
          HashMap(NodeId(1) -> exerciseNode(callResults = results)),
          ImmArray(NodeId(1)),
        )
      )

      ExternalCallResultsValidator(metadata, Map.empty, limitConfig).validate(tx) shouldBe Some(
        Limit.ExternalCallResults(
          coid,
          templateId,
          choiceName,
          choiceArg,
          results,
          limitConfig.limits.externalCallResults,
        )
      )
      transactionNodeMetrics.externalCallResultsGauge.getValue shouldBe 2
    }
  }
}
