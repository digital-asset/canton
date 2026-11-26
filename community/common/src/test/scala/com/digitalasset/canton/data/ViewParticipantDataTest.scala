// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.ActionDescription.ExerciseActionDescription
import com.digitalasset.canton.protocol.{
  CreatedContract,
  ExampleContractFactory,
  ExampleTransactionFactory,
  InputContract,
  LfSerializationVersion,
  v32,
}
import com.digitalasset.canton.validation.ProtoUnvalidated.syntax.*
import com.digitalasset.canton.version.v1.UntypedVersionedMessage
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
        createdCore = Seq(CreatedContract.tryCreate(devContract, consumedInCore = false))
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

  "ViewParticipantData deserialization" should {

    val base = generators.data.exerciseViewParticipantDataArb.arbitrary.sample.value

    // The limit is the size of the largest collection of the (valid) base view
    val max = Seq(
      1,
      base.coreInputs.size,
      base.createdCore.size,
      base.createdInSubviewArchivedInCore.size,
      base.keyResolution.size,
    ).max
    val limits = testedTransactionLimits.copy(maxContractsPerView = PositiveInt.tryCreate(max))

    // The newest supported version on which the bounds of the next protocol version do not apply yet
    lazy val pvBeforeNextValidationBound: ProtocolVersion =
      ProtocolVersion.supported.forgetNE.filter(_ < ProtocolVersion.v38).max

    // The base view padded with distinct elements to `count` elements in each of the four bounded collections
    def viewWith(count: Int): v32.ViewParticipantData = {
      def pad[A](existing: Seq[A])(newElement: => A): Seq[A] =
        existing ++ Seq.fill(count - existing.size)(newElement)
      val vpd = base.copy(
        coreInputs = pad(base.coreInputs.toSeq) {
          val c = ExampleContractFactory.build()
          c.contractId -> InputContract(c, consumed = false)
        }.toMap,
        createdCore = pad(base.createdCore)(
          CreatedContract.tryCreate(
            ExampleContractFactory.build(),
            consumedInCore = false,
          )
        ),
        createdInSubviewArchivedInCore = pad(base.createdInSubviewArchivedInCore.toSeq)(
          ExampleContractFactory.build().contractId
        ).toSet,
        keyResolution = pad(base.keyResolution.toSeq) {
          val key = ExampleContractFactory.buildKeyWithMaintainers()
          key.globalKey -> LfVersioned(
            LfSerializationVersion.V2,
            KeyResolutionWithMaintainers(Seq.empty, key.maintainers),
          )
        }.toMap,
      )
      v32.ViewParticipantData.parseFrom(
        UntypedVersionedMessage.parseFrom(vpd.toByteString.toByteArray).getData.toByteArray
      )
    }

    val atLimit = viewWith(max)
    val aboveLimit = viewWith(max + 1)

    // Each case replaces exactly one collection of the view at the limit by the one above the limit
    val cases: Seq[(String, v32.ViewParticipantData)] = Seq(
      "core_inputs" -> atLimit.copy(coreInputs = aboveLimit.coreInputs),
      "created_core" -> atLimit.copy(createdCore = aboveLimit.createdCore),
      "created_in_subview_archived_in_core" -> atLimit.copy(createdInSubviewArchivedInCore =
        aboveLimit.createdInSubviewArchivedInCore
      ),
      "resolved_keys" -> atLimit.copy(resolvedKeys = aboveLimit.resolvedKeys),
    )

    def parse(pv: ProtocolVersion, proto: v32.ViewParticipantData) = {
      val bytes = UntypedVersionedMessage(
        UntypedVersionedMessage.Wrapper.Data(proto.toByteString),
        // The proto version that the tested protocol version serializes to,
        // so that this does not hard-code that it is proto v32
        base.protoVersion.v,
      ).toByteString
      ViewParticipantData.fromByteString(
        pv,
        ViewParticipantDataDeserializationContext(TestHash, testedProtocolVersion, limits),
      )(bytes)
    }

    "accept every collection at the limit" in {
      parse(ProtocolVersion.v38, atLimit).value.unwrap.value
    }

    cases.foreach { case (field, proto) =>
      s"reject $field above the limit, naming the field" in {
        parse(ProtocolVersion.v38, proto).left.value shouldBe
          InvariantViolation(
            field,
            s"repeated or map field has ${max + 1} elements, exceeding the maximum of $max",
          )
      }

      s"not apply the limit to $field before ProtocolVersion.v38" in {
        parse(pvBeforeNextValidationBound, proto).value.unwrap.value
      }
    }
  }

  "ViewParticipantData external call deserialization" should {

    val maxResults = 2
    val maxParties = 2
    val limits = testedTransactionLimits.copy(
      maxExternalCallResultsPerView = PositiveInt.tryCreate(maxResults),
      maxQuorumSize = PositiveInt.tryCreate(maxParties),
    )

    lazy val pvBeforeNextValidationBound: ProtocolVersion =
      ProtocolVersion.supported.forgetNE.filter(_ < ProtocolVersion.v38).max

    val base: v32.ViewParticipantData = {
      val vpd = generators.data.exerciseViewParticipantDataArb.arbitrary.sample.value
        .copy(externalCallResults = Seq.empty)
      v32.ViewParticipantData.parseFrom(
        UntypedVersionedMessage.parseFrom(vpd.toByteString.toByteArray).getData.toByteArray
      )
    }

    val parties = Seq(
      ExampleTransactionFactory.submitter,
      ExampleTransactionFactory.signatory,
      ExampleTransactionFactory.observer,
    )

    def result(callIndex: Int, checkingParties: Seq[String]): v32.ViewExternalCallResult =
      v32.ViewExternalCallResult(
        extensionId = "extension".toProtoUnvalidated,
        functionId = "function".toProtoUnvalidated,
        config = com.google.protobuf.ByteString.EMPTY,
        input = com.google.protobuf.ByteString.EMPTY,
        output = com.google.protobuf.ByteString.EMPTY,
        exerciseIndex = 0,
        callIndex = callIndex,
        checkingParties = checkingParties.map(_.toProtoUnvalidated),
      )

    def withResults(results: Seq[v32.ViewExternalCallResult]): v32.ViewParticipantData =
      base.copy(externalCallResults = results)

    def parse(pv: ProtocolVersion, proto: v32.ViewParticipantData) = {
      val bytes = UntypedVersionedMessage(
        UntypedVersionedMessage.Wrapper.Data(proto.toByteString),
        32,
      ).toByteString
      ViewParticipantData.fromByteString(
        pv,
        ViewParticipantDataDeserializationContext(TestHash, testedProtocolVersion, limits),
      )(bytes)
    }

    def tooMany(field: String, count: Int, max: Int) =
      InvariantViolation(
        field,
        s"repeated or map field has $count elements, exceeding the maximum of $max",
      )

    val partyStrings: Seq[String] = parties
    val atLimitResults = withResults(
      (0 until maxResults).map(i => result(i, partyStrings.take(maxParties)))
    )
    val tooManyResults = withResults(
      (0 to maxResults).map(i => result(i, partyStrings.take(maxParties)))
    )
    // identical entries: duplicates are counted
    val duplicateResults = withResults(Seq.fill(maxResults + 1)(result(0, partyStrings.take(1))))
    val tooManyParties = withResults(Seq(result(0, partyStrings.take(maxParties + 1))))
    val duplicateParties =
      withResults(Seq(result(0, Seq.fill(maxParties + 1)(partyStrings.head))))

    "accept external call results and checking parties at the limit" in {
      parse(ProtocolVersion.v38, atLimitResults).value.unwrap.value
    }

    "reject too many external_call_results, naming the field" in {
      parse(ProtocolVersion.v38, tooManyResults).left.value shouldBe
        tooMany("external_call_results", maxResults + 1, maxResults)
    }

    "count duplicate external_call_results" in {
      parse(ProtocolVersion.v38, duplicateResults).left.value shouldBe
        tooMany("external_call_results", maxResults + 1, maxResults)
    }

    "reject too many checking_parties, naming the field" in {
      parse(ProtocolVersion.v38, tooManyParties).left.value shouldBe
        tooMany("checking_parties", maxParties + 1, maxParties)
    }

    "count duplicate checking_parties" in {
      parse(ProtocolVersion.v38, duplicateParties).left.value shouldBe
        tooMany("checking_parties", maxParties + 1, maxParties)
    }

    "not apply the limits before ProtocolVersion.v38" in {
      parse(pvBeforeNextValidationBound, tooManyResults).value.unwrap.value
      parse(pvBeforeNextValidationBound, tooManyParties).value.unwrap.value
    }
  }

}
