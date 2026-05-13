// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.data.ActionDescription.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.util.LfTransactionBuilder.{defaultPackageId, defaultTemplateId}
import com.digitalasset.canton.{BaseTest, LfPartyId}
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.ExternalCallResult
import org.scalatest.wordspec.AnyWordSpec

class ActionDescriptionTest extends AnyWordSpec with BaseTest {

  private val suffixedId: LfContractId = ExampleTransactionFactory.suffixedId(0, 0)
  private val seed: LfHash = ExampleTransactionFactory.lfHash(5)
  private val choiceName: LfChoiceName = LfChoiceName.assertFromString("choice")

  "An action description" should {

    "accept creation" when {

      "a valid fetch node is presented" in {

        val targetTemplateId =
          Ref.Identifier(defaultPackageId, defaultTemplateId.qualifiedName)

        val actingParties = Set(LfPartyId.assertFromString("acting"))

        val node = ExampleTransactionFactory.fetchNode(
          cid = suffixedId,
          templateId = targetTemplateId,
          actingParties = Set(LfPartyId.assertFromString("acting")),
        )

        val expected = FetchActionDescription(
          inputContractId = suffixedId,
          actors = actingParties,
          byKey = false,
          templateId = targetTemplateId,
          interfaceId = None,
        )

        ActionDescription.fromLfActionNode(
          node,
          None,
          Set.empty,
        ) shouldBe
          Right(expected)
      }

      "an exercise node with external call results is presented" in {
        val externalCallResults = ImmArray(
          ExternalCallResult(
            extensionId = "extension",
            functionId = "function",
            config = Bytes.fromStringUtf8("config"),
            input = Bytes.fromStringUtf8("input"),
            output = Bytes.fromStringUtf8("output"),
          )
        )
        val node = ExampleTransactionFactory
          .exerciseNodeWithoutChildren(
            suffixedId,
            actingParties = Set(ExampleTransactionFactory.submitter),
          )
          .copy(externalCallResults = externalCallResults)

        val description = ActionDescription
          .tryFromLfActionNode(
            node,
            Some(seed),
            Set.empty,
          )
          .asInstanceOf[ExerciseActionDescription]

        description.externalCallResults shouldBe externalCallResults
        ActionDescription.fromProtoV30(description.toProtoV30).value shouldBe description
        ActionDescription.fromProtoV31(description.toProtoV31).value shouldBe description
      }

    }

    "reject creation" when {
      "the choice argument cannot be serialized" in {
        ExerciseActionDescription.create(
          suffixedId,
          templateId = defaultTemplateId,
          choiceName,
          None,
          Set.empty,
          ExampleTransactionFactory.veryDeepVersionedValue,
          Set(ExampleTransactionFactory.submitter),
          byKey = true,
          seed,
          failed = false,
        ) shouldBe Left(
          InvalidActionDescription(
            "Failed to serialize chosen value: Provided Daml-LF value to encode exceeds maximum nesting level of 100"
          )
        )
      }

      "no seed is given when the node expects a seed" in {
        ActionDescription.fromLfActionNode(
          ExampleTransactionFactory.createNode(suffixedId),
          None,
          Set.empty,
        ) shouldBe
          Left(InvalidActionDescription("No seed for a Create node given"))

        ActionDescription.fromLfActionNode(
          ExampleTransactionFactory.exerciseNodeWithoutChildren(suffixedId),
          None,
          Set.empty,
        ) shouldBe
          Left(InvalidActionDescription("No seed for an Exercise node given"))
      }

      "a seed is given when the node does not expect one" in {
        ActionDescription.fromLfActionNode(
          ExampleTransactionFactory.fetchNode(suffixedId),
          Some(seed),
          Set.empty,
        ) shouldBe
          Left(InvalidActionDescription("No seed should be given for a Fetch node"))
      }

      "actors are not declared for a Fetch node" in {
        ActionDescription.fromLfActionNode(
          ExampleTransactionFactory.fetchNode(suffixedId, actingParties = Set.empty),
          None,
          Set.empty,
        ) shouldBe Left(InvalidActionDescription("Fetch node without acting parties"))
      }
    }
  }
}
