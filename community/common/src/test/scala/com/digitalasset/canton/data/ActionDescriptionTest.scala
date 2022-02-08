// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.daml.lf.value.Value
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.ActionDescription._
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.util.LfTransactionBuilder
import org.scalatest.wordspec.AnyWordSpec

class ActionDescriptionTest extends AnyWordSpec with BaseTest {

  val unsuffixedId: LfContractId = ExampleTransactionFactory.unsuffixedId(10)
  val suffixedId: LfContractId = ExampleTransactionFactory.suffixedId(0, 0)
  val seed: LfHash = ExampleTransactionFactory.lfHash(5)
  val globalKey: LfGlobalKey =
    LfGlobalKey(LfTransactionBuilder.defaultTemplateId, Value.ValueInt64(10L))
  val choiceName: LfChoiceName = LfChoiceName.assertFromString("choice")
  val dummyVersion: LfTransactionVersion = ExampleTransactionFactory.transactionVersion

  "An action description" should {
    "deserialize to the same value" in {
      val tests = Seq(
        CreateActionDescription(unsuffixedId, seed, dummyVersion),
        ExerciseActionDescription.tryCreate(
          suffixedId,
          LfChoiceName.assertFromString("choice"),
          Value.ValueUnit,
          Set(ExampleTransactionFactory.submitter),
          byKey = false,
          seed,
          dummyVersion,
          failed = true,
        ),
        FetchActionDescription(
          unsuffixedId,
          Set(ExampleTransactionFactory.signatory, ExampleTransactionFactory.observer),
          byKey = true,
          dummyVersion,
        ),
        LookupByKeyActionDescription.tryCreate(globalKey, dummyVersion),
      )

      forEvery(tests) { actionDescription =>
        ActionDescription.fromProtoV0(actionDescription.toProtoV0) shouldBe Right(actionDescription)
      }
    }

    "reject creation" when {
      "the choice argument cannot be serialized" in {
        ExerciseActionDescription.create(
          suffixedId,
          choiceName,
          ExampleTransactionFactory.veryDeepValue,
          Set(ExampleTransactionFactory.submitter),
          byKey = true,
          seed,
          dummyVersion,
          failed = false,
        ) shouldBe Left(
          InvalidActionDescription(
            "Failed to serialize chosen value: Provided Daml-LF value to encode exceeds maximum nesting level of 100"
          )
        )
      }

      "the key value cannot be serialized" in {
        LookupByKeyActionDescription.create(
          LfGlobalKey(
            LfTransactionBuilder.defaultTemplateId,
            ExampleTransactionFactory.veryDeepValue,
          ),
          dummyVersion,
        ) shouldBe Left(
          InvalidActionDescription(
            "Failed to serialize key: Provided Daml-LF value to encode exceeds maximum nesting level of 100"
          )
        )
      }

      "no seed is given when the node expects a seed" in {
        ActionDescription.fromLfActionNode(
          ExampleTransactionFactory.createNode(suffixedId),
          None,
        ) shouldBe
          Left(InvalidActionDescription("No seed for a Create node given"))

        ActionDescription.fromLfActionNode(
          ExampleTransactionFactory.exerciseNodeWithoutChildren(suffixedId),
          None,
        ) shouldBe
          Left(InvalidActionDescription("No seed for an Exercise node given"))
      }

      "a seed is given when the node does not expect one" in {
        ActionDescription.fromLfActionNode(
          ExampleTransactionFactory.fetchNode(suffixedId),
          Some(seed),
        ) shouldBe
          Left(InvalidActionDescription("No seed should be given for a Fetch node"))

        ActionDescription.fromLfActionNode(
          ExampleTransactionFactory
            .lookupByKeyNode(globalKey, maintainers = Set(ExampleTransactionFactory.observer)),
          Some(seed),
        ) shouldBe Left(InvalidActionDescription("No seed should be given for a LookupByKey node"))
      }

      "actors are not declared for a Fetch node" in {
        ActionDescription.fromLfActionNode(
          ExampleTransactionFactory.fetchNode(suffixedId, actingParties = Set.empty),
          None,
        ) shouldBe Left(InvalidActionDescription("Fetch node without acting parties"))
      }
    }
  }
}
