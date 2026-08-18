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

class KeyMaintainersValidatorTest extends AnyFreeSpec with Matchers with Inside {

  import TransactionBuilder.Implicits.*
  import ValidatorTestLib.*

  "limit to 1 key maintainer" - {
    val limits = Limits.Lenient.copy(keyMaintainers = 1)

    "allow a transaction with no key maintainers" - {
      "create node" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> createNode()),
            ImmArray(NodeId(1)),
          )
        )

        KeyMaintainersValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
      }

      "exercise nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> exerciseNode()),
            ImmArray(NodeId(1)),
          )
        )

        KeyMaintainersValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
      }

      "fetch nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> fetchNode()),
            ImmArray(NodeId(1)),
          )
        )

        KeyMaintainersValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
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

        KeyMaintainersValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
      }

      "exercise nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> exerciseNode(keyOpt = Some(contractKey))),
            ImmArray(NodeId(1)),
          )
        )

        KeyMaintainersValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
      }

      "fetch nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> fetchNode(keyOpt = Some(contractKey))),
            ImmArray(NodeId(1)),
          )
        )

        KeyMaintainersValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
      }

      "query by key nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            SerializationVersion.minVersion,
            HashMap(NodeId(1) -> queryNode(contractKey)),
            ImmArray(NodeId(1)),
          )
        )

        KeyMaintainersValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
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

        KeyMaintainersValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
          Limit.KeyMaintainers(
            coid,
            templateId,
            contractKey.maintainers,
            limits.keyMaintainers,
          )
        )
      }

      "exercise nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> exerciseNode(keyOpt = Some(contractKey))),
            ImmArray(NodeId(1)),
          )
        )

        KeyMaintainersValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
          Limit.KeyMaintainers(
            coid,
            templateId,
            contractKey.maintainers,
            limits.keyMaintainers,
          )
        )
      }

      "fetch nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> fetchNode(keyOpt = Some(contractKey))),
            ImmArray(NodeId(1)),
          )
        )

        KeyMaintainersValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
          Limit.KeyMaintainers(
            coid,
            templateId,
            contractKey.maintainers,
            limits.keyMaintainers,
          )
        )
      }

      "query by key nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> queryNode(contractKey)),
            ImmArray(NodeId(1)),
          )
        )

        KeyMaintainersValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
          Limit.KeyMaintainers(
            pkgName,
            templateId,
            contractKey.maintainers,
            limits.keyMaintainers,
          )
        )
      }
    }
  }
}
