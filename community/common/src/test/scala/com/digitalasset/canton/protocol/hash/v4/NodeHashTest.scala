// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash.v4

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.protocol.hash.TransactionHash.NodeHashingError
import com.digitalasset.canton.protocol.hash.{HashTracer, HashUtilsTest}
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.{ExternalCallResult, Node, SerializationVersion}
import com.digitalasset.daml.lf.value.Value
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class NodeHashTest extends BaseTest with AnyWordSpecLike with Matchers with HashUtilsTest {

  private val nodeSeed = LfHash.hashPrivateKey("v4-external-call-node-seed")

  private val externalCallResult1 = ExternalCallResult(
    extensionId = "extension",
    functionId = "function",
    config = Bytes.assertFromString("00"),
    input = Bytes.assertFromString("01"),
    output = Bytes.assertFromString("02"),
  )
  private val externalCallResult2 = externalCallResult1.copy(output = Bytes.assertFromString("03"))

  private def exerciseNode(results: ImmArray[ExternalCallResult]): Node.Exercise =
    Node.Exercise(
      targetCoid = cid("target"),
      packageName = packageName0,
      templateId = Ref.Identifier.assertFromString("package:Module:Template"),
      interfaceId = None,
      choiceId = Ref.ChoiceName.assertFromString("Choice"),
      consuming = true,
      actingParties = Set(alice),
      chosenValue = Value.ValueUnit,
      stakeholders = Set(alice),
      signatories = Set(alice),
      choiceObservers = Set.empty,
      choiceAuthorizers = None,
      children = ImmArray.Empty,
      exerciseResult = Some(Value.ValueUnit),
      keyOpt = None,
      byKey = false,
      externalCallResults = results,
      version = SerializationVersion.VDev,
    )

  private def hashExerciseNode(
      node: Node.Exercise,
      hashTracer: HashTracer = HashTracer.NoOp,
  ) =
    tryHashNodeWithVersion(
      node = node,
      hashingSchemeVersion = HashingSchemeVersion.V4,
      nodeSeed = Some(nodeSeed),
      hashTracer = hashTracer,
      enforceNodeSeedForCreateNodes = true,
    )

  "V4 NodeHashBuilder" should {
    "explain exercise external-call result encoding" in {
      val hashTracer = HashTracer.StringHashTracer()
      val hash = hashExerciseNode(
        exerciseNode(ImmArray(externalCallResult1, externalCallResult2)),
        hashTracer,
      )

      hash.toHexString shouldBe "1220a62bdfba0a669b547d3636d83297a6ac046b0a1684d9989984a4b3871dee0d3e"
      hashTracer.result should include(
        """# External Call Results
          |'00000002' # 2 (int)
          |# External Call Result
          |# Extension Id
          |'00000009' # 9 (int)
          |'657874656e73696f6e' # extension (string)
          |# Function Id
          |'00000008' # 8 (int)
          |'66756e6374696f6e' # function (string)
          |# Config
          |'00000001' # 1 (int)
          |'00' # config
          |# Input
          |'00000001' # 1 (int)
          |'01' # input
          |# Output
          |'00000001' # 1 (int)
          |'02' # output
          |# External Call Result
          |# Extension Id
          |'00000009' # 9 (int)
          |'657874656e73696f6e' # extension (string)
          |# Function Id
          |'00000008' # 8 (int)
          |'66756e6374696f6e' # function (string)
          |# Config
          |'00000001' # 1 (int)
          |'00' # config
          |# Input
          |'00000001' # 1 (int)
          |'01' # input
          |# Output
          |'00000001' # 1 (int)
          |'03' # output""".stripMargin
      )

      assertStringTracer(hashTracer, hash)
    }

    "include exercise external-call result order and multiplicity" in {
      val hash = hashExerciseNode(exerciseNode(ImmArray(externalCallResult1, externalCallResult2)))

      hashExerciseNode(
        exerciseNode(ImmArray(externalCallResult2, externalCallResult1))
      ) should not be hash
      hashExerciseNode(
        exerciseNode(ImmArray(externalCallResult1, externalCallResult1))
      ) should not be hashExerciseNode(exerciseNode(ImmArray(externalCallResult1)))
    }

    "include all external-call result fields" in {
      val hash = hashExerciseNode(exerciseNode(ImmArray(externalCallResult1)))

      Seq(
        externalCallResult1.copy(extensionId = "other-extension"),
        externalCallResult1.copy(functionId = "other-function"),
        externalCallResult1.copy(config = Bytes.assertFromString("ff")),
        externalCallResult1.copy(input = Bytes.assertFromString("ff")),
        externalCallResult1.copy(output = Bytes.assertFromString("ff")),
      ).foreach { changedResult =>
        hashExerciseNode(exerciseNode(ImmArray(changedResult))) should not be hash
      }
    }
  }

  "Pre-V4 NodeHashBuilder" should {
    "reject external-call results rather than silently dropping them" in {
      // External-call results must never be silently omitted from the hash: a scheme that does
      // not encode them must fail when they are present. Each scheme is paired with the highest
      // LF serialization version it accepts (V2 -> V1, V3 -> V2) so the failure is the
      // external-call rejection, not the upfront version-compatibility check.
      Seq(
        HashingSchemeVersion.V2 -> SerializationVersion.V1,
        HashingSchemeVersion.V3 -> SerializationVersion.V2,
      ).foreach { case (schemeVersion, serializationVersion) =>
        a[NodeHashingError.UnsupportedFeature] should be thrownBy
          tryHashNodeWithVersion(
            node = exerciseNode(ImmArray(externalCallResult1)).copy(version = serializationVersion),
            hashingSchemeVersion = schemeVersion,
            nodeSeed = Some(nodeSeed),
            hashTracer = HashTracer.NoOp,
            enforceNodeSeedForCreateNodes = true,
          )
      }
    }
  }
}
