// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.daml.lf.value.Value
import com.digitalasset.canton.data.ActionDescription.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.util.LfTransactionBuilder
import com.digitalasset.canton.version.{ProtocolVersion, RepresentativeProtocolVersion}
import com.digitalasset.canton.{BaseTest, LfInterfaceId}
import org.scalatest.wordspec.AnyWordSpec

class ActionDescriptionTest extends AnyWordSpec with BaseTest {

  private val unsuffixedId: LfContractId = ExampleTransactionFactory.unsuffixedId(10)
  private val suffixedId: LfContractId = ExampleTransactionFactory.suffixedId(0, 0)
  private val seed: LfHash = ExampleTransactionFactory.lfHash(5)
  private val globalKey: LfGlobalKey =
    LfGlobalKey(LfTransactionBuilder.defaultTemplateId, Value.ValueInt64(10L))
  private val choiceName: LfChoiceName = LfChoiceName.assertFromString("choice")
  private val dummyVersion: LfTransactionVersion = ExampleTransactionFactory.transactionVersion

  private val representativePV: RepresentativeProtocolVersion[ActionDescription] =
    ActionDescription.protocolVersionRepresentativeFor(testedProtocolVersion)

  "An action description" should {
    def tryCreateExerciseActionDescription(
        interface: Option[LfInterfaceId],
        protocolVersion: ProtocolVersion,
    ): ExerciseActionDescription =
      createExerciseActionDescription(interface, protocolVersion).fold(err => throw err, identity)

    def createExerciseActionDescription(
        interface: Option[LfInterfaceId],
        protocolVersion: ProtocolVersion,
    ): Either[InvalidActionDescription, ExerciseActionDescription] =
      ExerciseActionDescription.create(
        suffixedId,
        LfChoiceName.assertFromString("choice"),
        interface,
        Value.ValueUnit,
        Set(ExampleTransactionFactory.submitter),
        byKey = false,
        seed,
        dummyVersion,
        failed = true,
        ActionDescription.protocolVersionRepresentativeFor(protocolVersion),
      )

    val fetchAction = FetchActionDescription(
      unsuffixedId,
      Set(ExampleTransactionFactory.signatory, ExampleTransactionFactory.observer),
      byKey = true,
      dummyVersion,
    )(representativePV)

    "deserialize to the same value (V0)" in {
      val tests = Seq(
        CreateActionDescription(unsuffixedId, seed, dummyVersion)(representativePV),
        tryCreateExerciseActionDescription(interface = None, ProtocolVersion.v3),
        fetchAction,
        LookupByKeyActionDescription.tryCreate(globalKey, dummyVersion, representativePV),
      )

      forEvery(tests) { actionDescription =>
        ActionDescription.fromProtoV0(actionDescription.toProtoV0) shouldBe Right(actionDescription)
      }
    }

    "deserialize to the same value (V1)" in {
      val tests = Seq(
        CreateActionDescription(unsuffixedId, seed, dummyVersion)(representativePV),
        tryCreateExerciseActionDescription(
          Some(LfTransactionBuilder.defaultInterfaceId),
          ProtocolVersion.v4,
        ),
        fetchAction,
        LookupByKeyActionDescription.tryCreate(globalKey, dummyVersion, representativePV),
      )

      forEvery(tests) { actionDescription =>
        ActionDescription.fromProtoV1(actionDescription.toProtoV1) shouldBe Right(actionDescription)
      }
    }

    "reject creation" when {
      "interfaceId is set and the protocol version is too old" in {
        def create(
            protocolVersion: ProtocolVersion
        ): Either[InvalidActionDescription, ExerciseActionDescription] =
          createExerciseActionDescription(
            Some(LfTransactionBuilder.defaultInterfaceId),
            protocolVersion,
          )

        val v3 = ProtocolVersion.v3
        val v2 = ProtocolVersion.v2
        create(v3) shouldBe Left(
          InvalidActionDescription(
            s"Protocol version is equivalent to $v2 but interface id is supported since protocol version ${ProtocolVersion.v4}"
          )
        )

        create(ProtocolVersion.v4).value shouldBe a[ExerciseActionDescription]
      }

      "the choice argument cannot be serialized" in {
        ExerciseActionDescription.create(
          suffixedId,
          choiceName,
          None,
          ExampleTransactionFactory.veryDeepValue,
          Set(ExampleTransactionFactory.submitter),
          byKey = true,
          seed,
          dummyVersion,
          failed = false,
          representativePV,
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
          representativePV,
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
          testedProtocolVersion,
        ) shouldBe
          Left(InvalidActionDescription("No seed for a Create node given"))

        ActionDescription.fromLfActionNode(
          ExampleTransactionFactory.exerciseNodeWithoutChildren(suffixedId),
          None,
          testedProtocolVersion,
        ) shouldBe
          Left(InvalidActionDescription("No seed for an Exercise node given"))
      }

      "a seed is given when the node does not expect one" in {
        ActionDescription.fromLfActionNode(
          ExampleTransactionFactory.fetchNode(suffixedId),
          Some(seed),
          testedProtocolVersion,
        ) shouldBe
          Left(InvalidActionDescription("No seed should be given for a Fetch node"))

        ActionDescription.fromLfActionNode(
          ExampleTransactionFactory
            .lookupByKeyNode(globalKey, maintainers = Set(ExampleTransactionFactory.observer)),
          Some(seed),
          testedProtocolVersion,
        ) shouldBe Left(InvalidActionDescription("No seed should be given for a LookupByKey node"))
      }

      "actors are not declared for a Fetch node" in {
        ActionDescription.fromLfActionNode(
          ExampleTransactionFactory.fetchNode(suffixedId, actingParties = Set.empty),
          None,
          testedProtocolVersion,
        ) shouldBe Left(InvalidActionDescription("Fetch node without acting parties"))
      }
    }
  }
}
