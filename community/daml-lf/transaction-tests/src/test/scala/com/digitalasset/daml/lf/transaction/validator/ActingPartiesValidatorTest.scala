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

class ActingPartiesValidatorTest extends AnyFreeSpec with Matchers with Inside {

  import TransactionBuilder.Implicits.*
  import ValidatorTestLib.*

  "limit to 1 acting party" - {
    val limits = Limits.Lenient.copy(actingParties = 1)

    "allow a transaction with no acting parties" - {
      "exercise nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> exerciseNode()),
            ImmArray(NodeId(1)),
          )
        )

        ActingPartiesValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
      }

      "fetch nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> fetchNode()),
            ImmArray(NodeId(1)),
          )
        )

        ActingPartiesValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
      }
    }

    "allow a transaction with one acting party" - {
      "exercise nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> exerciseNode(parties = Set(Ref.Party.assertFromString("Alice")))),
            ImmArray(NodeId(1)),
          )
        )

        ActingPartiesValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
      }

      "fetch nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> fetchNode(parties = Set(Ref.Party.assertFromString("Alice")))),
            ImmArray(NodeId(1)),
          )
        )

        ActingPartiesValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
      }
    }

    "disallow a transaction with two acting parties" - {
      val actingParties =
        Set(Ref.Party.assertFromString("Alice"), Ref.Party.assertFromString("Bob"))

      "exercise nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> exerciseNode(parties = actingParties)),
            ImmArray(NodeId(1)),
          )
        )

        ActingPartiesValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
          Limit.ActingParties(
            coid,
            templateId,
            actingParties,
            limits.actingParties,
          )
        )
      }

      "fetch nodes" in {
        val tx = SubmittedTransaction(
          VersionedTransaction(
            version,
            HashMap(NodeId(1) -> fetchNode(parties = actingParties)),
            ImmArray(NodeId(1)),
          )
        )

        ActingPartiesValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
          Limit.ActingParties(
            coid,
            templateId,
            actingParties,
            limits.actingParties,
          )
        )
      }
    }
  }
}
