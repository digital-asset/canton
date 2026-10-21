// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction.validator

import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.{LimitConfig, Limits}
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{NodeId, SubmittedTransaction, VersionedTransaction}
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.HashMap

class KeyMaintainersValidatorTest extends AnyFreeSpec with Matchers with Inside {

  import TransactionBuilder.Implicits.*
  import ValidatorTestLib.*

  "limit to 1 key maintainer" - {
    val limitConfig =
      LimitConfig(Limits.Lenient.copy(keyMaintainers = 1), Some(transactionNodeMetrics))

    "allow a transaction with no key maintainers" - {
      "create node" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> createNode()),
            ImmArray(NodeId(1)),
          )
        )

        KeyMaintainersValidator(metadata, Map.empty, limitConfig).validate(tx) shouldBe empty
        transactionNodeMetrics.keyMaintainersGauge.getValue shouldBe 0
      }

      "exercise nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> exerciseNode()),
            ImmArray(NodeId(1)),
          )
        )

        KeyMaintainersValidator(metadata, Map.empty, limitConfig).validate(tx) shouldBe empty
        transactionNodeMetrics.keyMaintainersGauge.getValue shouldBe 0
      }

      "fetch nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> fetchNode()),
            ImmArray(NodeId(1)),
          )
        )

        KeyMaintainersValidator(metadata, Map.empty, limitConfig).validate(tx) shouldBe empty
        transactionNodeMetrics.keyMaintainersGauge.getValue shouldBe 0
      }
    }

    "allow a transaction with one key maintainer" - {
      val contractKey = globalKey(maintainers = Set(Ref.Party.assertFromString("Alice")))

      "create node" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> createNode(keyOpt = Some(contractKey))),
            ImmArray(NodeId(1)),
          )
        )

        KeyMaintainersValidator(metadata, Map.empty, limitConfig).validate(tx) shouldBe empty
        transactionNodeMetrics.keyMaintainersGauge.getValue shouldBe 1
      }

      "exercise nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> exerciseNode(keyOpt = Some(contractKey))),
            ImmArray(NodeId(1)),
          )
        )

        KeyMaintainersValidator(metadata, Map.empty, limitConfig).validate(tx) shouldBe empty
        transactionNodeMetrics.keyMaintainersGauge.getValue shouldBe 1
      }

      "fetch nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> fetchNode(keyOpt = Some(contractKey))),
            ImmArray(NodeId(1)),
          )
        )

        KeyMaintainersValidator(metadata, Map.empty, limitConfig).validate(tx) shouldBe empty
        transactionNodeMetrics.keyMaintainersGauge.getValue shouldBe 1
      }

      "query by key nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> queryNode(contractKey)),
            ImmArray(NodeId(1)),
          )
        )

        KeyMaintainersValidator(metadata, Map.empty, limitConfig).validate(tx) shouldBe empty
        transactionNodeMetrics.keyMaintainersGauge.getValue shouldBe 1
      }
    }

    "disallow a transaction with two key maintainers" - {
      val contractKey = globalKey(maintainers =
        Set(Ref.Party.assertFromString("Alice"), Ref.Party.assertFromString("Bob"))
      )

      "create nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> createNode(keyOpt = Some(contractKey))),
            ImmArray(NodeId(1)),
          )
        )

        KeyMaintainersValidator(metadata, Map.empty, limitConfig).validate(tx) shouldBe Some(
          Limit.KeyMaintainers(
            coid,
            templateId,
            contractKey.maintainers,
            limitConfig.limits.keyMaintainers,
          )
        )
        transactionNodeMetrics.keyMaintainersGauge.getValue shouldBe 2
      }

      "exercise nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> exerciseNode(keyOpt = Some(contractKey))),
            ImmArray(NodeId(1)),
          )
        )

        KeyMaintainersValidator(metadata, Map.empty, limitConfig).validate(tx) shouldBe Some(
          Limit.KeyMaintainers(
            coid,
            templateId,
            contractKey.maintainers,
            limitConfig.limits.keyMaintainers,
          )
        )
        transactionNodeMetrics.keyMaintainersGauge.getValue shouldBe 2
      }

      "fetch nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> fetchNode(keyOpt = Some(contractKey))),
            ImmArray(NodeId(1)),
          )
        )

        KeyMaintainersValidator(metadata, Map.empty, limitConfig).validate(tx) shouldBe Some(
          Limit.KeyMaintainers(
            coid,
            templateId,
            contractKey.maintainers,
            limitConfig.limits.keyMaintainers,
          )
        )
        transactionNodeMetrics.keyMaintainersGauge.getValue shouldBe 2
      }

      "query by key nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> queryNode(contractKey)),
            ImmArray(NodeId(1)),
          )
        )

        KeyMaintainersValidator(metadata, Map.empty, limitConfig).validate(tx) shouldBe Some(
          Limit.KeyMaintainers(
            pkgName,
            templateId,
            contractKey.maintainers,
            limitConfig.limits.keyMaintainers,
          )
        )
        transactionNodeMetrics.keyMaintainersGauge.getValue shouldBe 2
      }
    }
  }
}
