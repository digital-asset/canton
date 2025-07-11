// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, TestHash}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.{
  AuthenticatedContractIdVersionV11,
  ExampleTransactionFactory,
  LfContractId,
  LfGlobalKey,
  LfHash,
  LfLanguageVersion,
  LfTemplateId,
  Unicum,
}
import com.digitalasset.canton.topology.{GeneratorsTopology, PartyId}
import com.digitalasset.daml.lf.transaction.Versioned
import com.digitalasset.daml.lf.value.Value.ValueInt64
import org.scalacheck.{Arbitrary, Gen}

final class GeneratorsLf(generatorsTopology: GeneratorsTopology) {
  import com.digitalasset.canton.data.GeneratorsDataTime.*
  import generatorsTopology.*

  implicit val lfPartyIdArb: Arbitrary[LfPartyId] = Arbitrary(
    Arbitrary.arbitrary[PartyId].map(_.toLf)
  )

  implicit val lfTimestampArb: Arbitrary[LfTimestamp] = Arbitrary(
    Arbitrary.arbitrary[CantonTimestamp].map(_.underlying)
  )

  implicit val LedgerUserIdArb: Arbitrary[LedgerUserId] = Arbitrary(
    Gen.stringOfN(8, Gen.alphaChar).map(LedgerUserId.assertFromString)
  )

  implicit val lfCommandIdArb: Arbitrary[LfCommandId] = Arbitrary(
    Gen.stringOfN(8, Gen.alphaChar).map(LfCommandId.assertFromString)
  )

  implicit val lfSubmissionIdArb: Arbitrary[LfSubmissionId] = Arbitrary(
    Gen.stringOfN(8, Gen.alphaChar).map(LfSubmissionId.assertFromString)
  )

  implicit val lfWorkflowIdArb: Arbitrary[LfWorkflowId] = Arbitrary(
    Gen.stringOfN(8, Gen.alphaChar).map(LfWorkflowId.assertFromString)
  )

  implicit val lfContractIdArb: Arbitrary[LfContractId] = Arbitrary(
    for {
      index <- Gen.posNum[Int]
      contractIdDiscriminator = ExampleTransactionFactory.lfHash(index)

      suffix <- Gen.posNum[Int]
      contractIdSuffix = Unicum(
        Hash.build(TestHash.testHashPurpose, HashAlgorithm.Sha256).add(suffix).finish()
      )
    } yield AuthenticatedContractIdVersionV11.fromDiscriminator(
      contractIdDiscriminator,
      contractIdSuffix,
    )
  )

  implicit val lfHashArb: Arbitrary[LfHash] = Arbitrary(
    Gen.posNum[Int].map(ExampleTransactionFactory.lfHash)
  )

  implicit val lfChoiceNameArb: Arbitrary[LfChoiceName] = Arbitrary(
    Gen.stringOfN(8, Gen.alphaChar).map(LfChoiceName.assertFromString)
  )

  implicit val lfPackageId: Arbitrary[LfPackageId] = Arbitrary(
    Gen.stringOfN(64, Gen.alphaChar).map(LfPackageId.assertFromString)
  )

  implicit val lfTemplateIdArb: Arbitrary[LfTemplateId] = Arbitrary(for {
    packageName <- Gen.stringOfN(8, Gen.alphaChar)
    moduleName <- Gen.stringOfN(8, Gen.alphaChar)
    scriptName <- Gen.stringOfN(8, Gen.alphaChar)
  } yield LfTemplateId.assertFromString(s"$packageName:$moduleName:$scriptName"))

  private val lfVersionedGlobalKeyGen: Gen[Versioned[LfGlobalKey]] = for {
    templateId <- Arbitrary.arbitrary[LfTemplateId]
    // We consider only this specific value because the goal is not exhaustive testing of LF (de)serialization
    value <- Gen.long.map(ValueInt64.apply)
  } yield ExampleTransactionFactory.globalKey(templateId, value)

  implicit val lfGlobalKeyArb: Arbitrary[LfGlobalKey] = Arbitrary(
    lfVersionedGlobalKeyGen.map(_.unversioned)
  )

  implicit val lfVersionedGlobalKeyArb: Arbitrary[Versioned[LfGlobalKey]] = Arbitrary(
    lfVersionedGlobalKeyGen
  )

  implicit val LfLanguageVersionArb: Arbitrary[LfLanguageVersion] =
    Arbitrary(Gen.oneOf(LfLanguageVersion.AllV2))

}
