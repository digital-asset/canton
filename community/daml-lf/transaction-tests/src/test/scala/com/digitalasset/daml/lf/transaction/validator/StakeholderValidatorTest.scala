// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction
package validator

import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.interpretation.Error.Dev.Limit
import com.digitalasset.daml.lf.interpretation.Limits
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.HashMap

class StakeholderValidatorTest extends AnyFreeSpec with Matchers with Inside {

  import TransactionBuilder.Implicits.*
  import ValidatorTestLib.*

  "limit to 1 stakeholder" - {
    val limits = Limits.Lenient.copy(contractStakeholders = 1)

    "allow a transaction with no stakeholders" - {
      "create node" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> createNode()),
            ImmArray(NodeId(1)),
          )
        )

        StakeholderValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
      }

      "exercise node" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> exerciseNode()),
            ImmArray(NodeId(1)),
          )
        )

        StakeholderValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
      }

      "fetch node" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> fetchNode()),
            ImmArray(NodeId(1)),
          )
        )

        StakeholderValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
      }
    }

    "allow a transaction with one stakeholder" - {
      "create node" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(
              NodeId(1) -> createNode(stakeholders = Set(Ref.Party.assertFromString("Alice")))
            ),
            ImmArray(NodeId(1)),
          )
        )

        StakeholderValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
      }

      "exercise nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(
              NodeId(1) -> exerciseNode(stakeholders = Set(Ref.Party.assertFromString("Alice")))
            ),
            ImmArray(NodeId(1)),
          )
        )

        StakeholderValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
      }

      "fetch nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(
              NodeId(1) -> fetchNode(stakeholders = Set(Ref.Party.assertFromString("Alice")))
            ),
            ImmArray(NodeId(1)),
          )
        )

        StakeholderValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
      }
    }

    "disallow a transaction with two stakeholders" - {
      val stakeholders = Set(Ref.Party.assertFromString("Alice"), Ref.Party.assertFromString("Bob"))

      "create node" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> createNode(stakeholders = stakeholders)),
            ImmArray(NodeId(1)),
          )
        )

        StakeholderValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
          Limit.ContractStakeholders(
            coid,
            templateId,
            stakeholders,
            limits.contractStakeholders,
          )
        )
      }

      "exercise nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> exerciseNode(stakeholders = stakeholders)),
            ImmArray(NodeId(1)),
          )
        )

        StakeholderValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
          Limit.ContractStakeholders(
            coid,
            templateId,
            stakeholders,
            limits.contractStakeholders,
          )
        )
      }

      "fetch nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> fetchNode(stakeholders = stakeholders)),
            ImmArray(NodeId(1)),
          )
        )

        StakeholderValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
          Limit.ContractStakeholders(
            coid,
            templateId,
            stakeholders,
            limits.contractStakeholders,
          )
        )
      }
    }
  }
}
