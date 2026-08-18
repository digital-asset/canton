// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction
package validator

import com.digitalasset.daml.lf.data.{Bytes, ImmArray}
import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.Limits
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.HashMap

class ExternalCallResultSizeValidatorTest extends AnyFreeSpec with Matchers with Inside {

  import TransactionBuilder.Implicits.*
  import ValidatorTestLib.*

  val basicCallResult = ExternalCallResult(
    "extId",
    "funId",
    Bytes.assertFromString(""),
    Bytes.assertFromString(""),
    Bytes.assertFromString(""),
  )
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
  val maxCallResultSize =
    ExternalCallResultSizeValidator.externalCallResultSize(basicCallResult)

  "test assumptions are met" in {
    ExternalCallResultSizeValidator.externalCallResultSize(
      basicCallResult
    ) should be < ExternalCallResultSizeValidator.externalCallResultSize(callResult1)
    ExternalCallResultSizeValidator.externalCallResultSize(
      basicCallResult
    ) should be < ExternalCallResultSizeValidator.externalCallResultSize(callResult2)
  }

  s"limit external call result size to $maxCallResultSize bytes" - {
    val limits = Limits.Lenient.copy(externalCallResultSize = maxCallResultSize)

    "allow a transaction with no external call results" in {
      val tx = SubmittedTransaction(
        VersionedTransaction(
          version,
          HashMap(NodeId(1) -> exerciseNode()),
          ImmArray(NodeId(1)),
        )
      )

      ExternalCallResultSizeValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
    }

    s"allow a transaction with a external call result of size <= $maxCallResultSize" in {
      val tx = SubmittedTransaction(
        VersionedTransaction(
          version,
          HashMap(NodeId(1) -> exerciseNode(callResults = ImmArray(basicCallResult))),
          ImmArray(NodeId(1)),
        )
      )

      ExternalCallResultSizeValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
    }

    s"disallow a transaction with one external call result of size > $maxCallResultSize" in {
      val tx = SubmittedTransaction(
        VersionedTransaction(
          version,
          HashMap(NodeId(1) -> exerciseNode(callResults = ImmArray(callResult1))),
          ImmArray(NodeId(1)),
        )
      )

      ExternalCallResultSizeValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
        Limit.ExternalCallResultSize(
          coid,
          templateId,
          choiceName,
          choiceArg,
          callResult1,
          limits.externalCallResultSize,
        )
      )
    }

    s"disallow a transaction with two external call results and one has size > $maxCallResultSize" in {
      val tx = SubmittedTransaction(
        VersionedTransaction(
          version,
          HashMap(NodeId(1) -> exerciseNode(callResults = ImmArray(basicCallResult, callResult2))),
          ImmArray(NodeId(1)),
        )
      )

      ExternalCallResultSizeValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
        Limit.ExternalCallResultSize(
          coid,
          templateId,
          choiceName,
          choiceArg,
          callResult2,
          limits.externalCallResultSize,
        )
      )
    }
  }
}
