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

class ChoiceAuthorizersValidatorTest extends AnyFreeSpec with Matchers with Inside {

  import TransactionBuilder.Implicits.*
  import ValidatorTestLib.*

  "limit to 1 choice authorizers" - {
    val limits = Limits.Lenient.copy(choiceAuthorizers = 1)

    "allow a transaction with no authorizers" in {
      val tx = SubmittedTransaction(
        VersionedTransaction(
          version,
          HashMap(NodeId(1) -> exerciseNode()),
          ImmArray(NodeId(1)),
        )
      )

      ChoiceAuthorizersValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
    }

    "allow a transaction with one authorizer" in {
      val authorizers = Set(Ref.Party.assertFromString("Alice"))
      val tx = SubmittedTransaction(
        VersionedTransaction(
          version,
          HashMap(
            NodeId(1) -> exerciseNode(parties = authorizers, authorizers = Some(authorizers))
          ),
          ImmArray(NodeId(1)),
        )
      )

      ChoiceAuthorizersValidator(metadata, Map.empty, limits).validate(tx) shouldBe empty
    }

    "disallow a transaction with two authorizers" in {
      val authorizers = Set(Ref.Party.assertFromString("Alice"), Ref.Party.assertFromString("Bob"))
      val tx = SubmittedTransaction(
        VersionedTransaction(
          version,
          HashMap(
            NodeId(1) -> exerciseNode(parties = authorizers, authorizers = Some(authorizers))
          ),
          ImmArray(NodeId(1)),
        )
      )

      ChoiceAuthorizersValidator(metadata, Map.empty, limits).validate(tx) shouldBe Some(
        Limit.ChoiceAuthorizers(
          coid,
          templateId,
          choiceName,
          choiceArg,
          authorizers,
          limits.choiceAuthorizers,
        )
      )
    }
  }
}
