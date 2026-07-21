// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.data.ActionDescription.ExerciseActionDescription
import com.digitalasset.canton.protocol.{
  CreatedContract,
  ExampleContractFactory,
  InputContract,
  LfSerializationVersion,
}
import com.digitalasset.canton.version.{CommonGenerators, ProtocolVersion}
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  LfValue,
  LfVersioned,
  ProtocolVersionChecksAnyWordSpec,
}
import org.scalatest.wordspec.AnyWordSpec

class ViewParticipantDataTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAnyWordSpec {

  private val generators = new CommonGenerators(testedProtocolVersion)

  "ViewParticipantData validation" should {

    val example = generators.data.exerciseViewParticipantDataArb.arbitrary.sample.value
    val kwm = ExampleContractFactory.buildKeyWithMaintainers()
    val contract = ExampleContractFactory.build(keyOpt = Some(kwm))
    val devContract =
      ExampleContractFactory.fromCreate(contract.toLf.copy(version = LfSerializationVersion.VDev))

    "pass when valid" in {
      example.validated(testedProtocolVersion).isRight shouldBe true
    }
    "fail if an input contract serialization version is invalid for PV" onlyRunWith ProtocolVersion.v36 in {
      val modified = example.copy(
        coreInputs = Map(devContract.contractId -> InputContract(devContract, consumed = false))
      )
      inside(modified.validated(testedProtocolVersion)) { case Left(err) =>
        err should include(
          "ViewParticipantData contains contract serialization versions not supported by protocol version"
        )
      }
    }
    "fail if a create contract serialization version is invalid for PV" onlyRunWith ProtocolVersion.v36 in {
      val modified = example.copy(
        createdCore =
          Seq(CreatedContract.tryCreate(devContract, consumedInCore = false, rolledBack = false))
      )
      inside(modified.validated(testedProtocolVersion)) { case Left(err) =>
        err should include(
          "ViewParticipantData contains contract serialization versions not supported by protocol version"
        )
      }
    }
    "fail if the key serialization version is invalid for PV" onlyRunWith ProtocolVersion.v36 in {
      val modified = example.copy(
        keyResolution = Map(
          kwm.globalKey -> LfVersioned(
            LfSerializationVersion.VDev,
            KeyResolutionWithMaintainers(Seq.empty, kwm.maintainers),
          )
        )
      )
      inside(modified.validated(testedProtocolVersion)) { case Left(err) =>
        err should include(
          "ViewParticipantData contains key resolution serialization versions not supported by protocol version"
        )
      }
    }
    "fail if the exercise choice value serialization version is invalid for PV" onlyRunWith ProtocolVersion.v36 in {
      val modified = example.copy(
        actionDescription = inside(example.actionDescription) {
          case exercise: ExerciseActionDescription =>
            exercise.copy(chosenValue = LfVersioned(LfSerializationVersion.VDev, LfValue.ValueUnit))
        }
      )
      inside(modified.validated(testedProtocolVersion)) { case Left(err) =>
        err should include(
          "ViewParticipantData contains an exercise choice value serialization version not supported by protocol version"
        )
      }
    }
  }

}
