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

class ChoiceObserversValidatorTest extends AnyFreeSpec with Matchers with Inside {

  import TransactionBuilder.Implicits.*
  import ValidatorTestLib.*

  "limit to 1 choice observers" - {
    val limits = Limits.Lenient.copy(choiceObservers = 1)

    "allow a transaction with no observers" in {
      val tx = SubmittedTransaction(
        VersionedTransaction(
          SerializationVersion.minVersion,
          HashMap(NodeId(1) -> exerciseNode()),
          ImmArray(NodeId(1)),
        )
      )

      ChoiceObserversValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
    }

    "allow a transaction with one observer" in {
      val tx = SubmittedTransaction(
        VersionedTransaction(
          version,
          HashMap(NodeId(1) -> exerciseNode(observers = Set(Ref.Party.assertFromString("Alice")))),
          ImmArray(NodeId(1)),
        )
      )

      ChoiceObserversValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
    }

    "disallow a transaction with two observers" in {
      val observers = Set(Ref.Party.assertFromString("Alice"), Ref.Party.assertFromString("Bob"))
      val tx = SubmittedTransaction(
        VersionedTransaction(
          version,
          HashMap(NodeId(1) -> exerciseNode(observers = observers)),
          ImmArray(NodeId(1)),
        )
      )

      ChoiceObserversValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
        Limit.ChoiceObservers(
          coid,
          templateId,
          choiceName,
          choiceArg,
          observers,
          limits.choiceObservers,
        )
      )
    }
  }
}
