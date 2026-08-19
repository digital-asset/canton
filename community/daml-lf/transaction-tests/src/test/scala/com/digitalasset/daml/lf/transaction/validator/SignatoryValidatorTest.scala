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

class SignatoryValidatorTest extends AnyFreeSpec with Matchers with Inside {

  import TransactionBuilder.Implicits.*
  import ValidatorTestLib.*

  "limit to 1 signatory" - {
    val limits = Limits.Lenient.copy(contractSignatories = 1)

    "allow a transaction with no signatories" - {
      "create node" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> createNode()),
            ImmArray(NodeId(1)),
          )
        )

        SignatoryValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
      }

      "exercise node" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> exerciseNode()),
            ImmArray(NodeId(1)),
          )
        )

        SignatoryValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
      }

      "fetch node" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> fetchNode()),
            ImmArray(NodeId(1)),
          )
        )

        SignatoryValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
      }
    }

    "allow a transaction with one signatory" - {
      "create node" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(
              NodeId(1) -> createNode(signatories = Set(Ref.Party.assertFromString("Alice")))
            ),
            ImmArray(NodeId(1)),
          )
        )

        SignatoryValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
      }

      "exercise nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(
              NodeId(1) -> exerciseNode(signatories = Set(Ref.Party.assertFromString("Alice")))
            ),
            ImmArray(NodeId(1)),
          )
        )

        SignatoryValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
      }

      "fetch nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> fetchNode(signatories = Set(Ref.Party.assertFromString("Alice")))),
            ImmArray(NodeId(1)),
          )
        )

        SignatoryValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
      }
    }

    "disallow a transaction with two signatories" - {
      val signatories = Set(Ref.Party.assertFromString("Alice"), Ref.Party.assertFromString("Bob"))

      "create node" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> createNode(signatories = signatories)),
            ImmArray(NodeId(1)),
          )
        )

        SignatoryValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
          Limit.ContractSignatories(
            coid,
            templateId,
            signatories,
            limits.contractSignatories,
          )
        )
      }

      "exercise nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> exerciseNode(signatories = signatories)),
            ImmArray(NodeId(1)),
          )
        )

        SignatoryValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
          Limit.ContractSignatories(
            coid,
            templateId,
            signatories,
            limits.contractSignatories,
          )
        )
      }

      "fetch nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> fetchNode(signatories = signatories)),
            ImmArray(NodeId(1)),
          )
        )

        SignatoryValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
          Limit.ContractSignatories(
            coid,
            templateId,
            signatories,
            limits.contractSignatories,
          )
        )
      }
    }
  }
}
