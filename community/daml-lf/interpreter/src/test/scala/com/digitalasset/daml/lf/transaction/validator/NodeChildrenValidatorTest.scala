// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction.validator

import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.{LimitConfig, Limits}
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{NodeId, SubmittedTransaction, VersionedTransaction}
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.HashMap

class NodeChildrenValidatorTest extends AnyFreeSpec with Matchers with Inside {

  import TransactionBuilder.Implicits.*
  import ValidatorTestLib.*

  "limit to 1 child node" - {
    val limitConfig =
      LimitConfig(Limits.Lenient.copy(nodeChildren = 1), Some(transactionNodeMetrics))

    "allow a transaction with no child nodes" in {
      val tx = SubmittedTransaction(
        VersionedTransaction(
          version,
          HashMap(NodeId(1) -> exerciseNode()),
          ImmArray(NodeId(1)),
        )
      )

      NodeChildrenValidator(metadata, Map.empty, limitConfig).validate(tx) shouldBe empty
      transactionNodeMetrics.nodeChildrenGauge.getValue shouldBe 0
    }

    "allow a transaction with one child node" in {
      val tx = SubmittedTransaction(
        VersionedTransaction(
          version,
          HashMap(NodeId(1) -> exerciseNode(children = ImmArray(NodeId(2)))),
          ImmArray(NodeId(1)),
        )
      )

      NodeChildrenValidator(metadata, Map.empty, limitConfig).validate(tx) shouldBe empty
      transactionNodeMetrics.nodeChildrenGauge.getValue shouldBe 1
    }

    "disallow a transaction with two children" in {
      val tx = SubmittedTransaction(
        VersionedTransaction(
          version,
          HashMap(NodeId(1) -> exerciseNode(children = ImmArray(NodeId(2), NodeId(3)))),
          ImmArray(NodeId(1)),
        )
      )

      NodeChildrenValidator(metadata, Map.empty, limitConfig).validate(tx) shouldBe Some(
        Limit.NodeChildren(
          coid,
          templateId,
          choiceName,
          choiceArg,
          limitConfig.limits.nodeChildren,
        )
      )
      transactionNodeMetrics.nodeChildrenGauge.getValue shouldBe 2
    }
  }
}
