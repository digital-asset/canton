// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.daml.lf.command.{EngineEnrichedContractMetadata, ProcessedDisclosedContract}
import com.daml.lf.data.ImmArray
import com.daml.lf.value.Value
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurposeTest, TestSalt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, LfPartyId, LfTimestamp, LfValue, LfVersioned}
import monocle.macros.syntax.lens.*
import org.scalatest.wordspec.AnyWordSpec

class SerializableContractTest extends AnyWordSpec with BaseTest {

  val alice = LfPartyId.assertFromString("Alice")
  val bob = LfPartyId.assertFromString("Bob")

  val templateId = ExampleTransactionFactory.templateId

  "SerializableContractInstance" should {
    "deserialize correctly" in {
      val someContractSalt = TestSalt.generateSalt(0)
      val contractId = ExampleTransactionFactory.suffixedId(0, 0)

      val metadata = ContractMetadata.tryCreate(
        signatories = Set(alice),
        stakeholders = Set(alice, bob),
        maybeKeyWithMaintainers = Some(
          ExampleTransactionFactory.globalKeyWithMaintainers(
            LfGlobalKey(templateId, Value.ValueUnit),
            Set(alice),
          )
        ),
      )

      val sci = ExampleTransactionFactory.asSerializable(
        contractId,
        ExampleTransactionFactory.contractInstance(Seq(contractId)),
        metadata,
        CantonTimestamp.now(),
        Option.when(testedProtocolVersion >= ProtocolVersion.v4)(someContractSalt),
      )
      SerializableContract.fromProtoVersioned(
        sci.toProtoVersioned(testedProtocolVersion)
      ) shouldEqual Right(sci)
    }
  }

  "SerializableContract.fromDisclosedContract" when {
    val transactionVersion = LfTransactionVersion.maxVersion

    val createdAt = LfTimestamp.Epoch
    val contractSalt = TestSalt.generateSalt(0)
    val driverMetadata =
      ImmArray.from(DriverContractMetadata(contractSalt).toByteArray(testedProtocolVersion))

    val contractIdDiscriminator = ExampleTransactionFactory.lfHash(0)
    val contractIdSuffix =
      Unicum(Hash.build(HashPurposeTest.testHashPurpose, HashAlgorithm.Sha256).add(0).finish())

    val invalidFormatContractId = LfContractId.assertFromString("00" * 34)

    val authenticatedContractId =
      AuthenticatedContractIdVersion.fromDiscriminator(contractIdDiscriminator, contractIdSuffix)

    val nonAuthenticatedContractId =
      NonAuthenticatedContractIdVersion.fromDiscriminator(contractIdDiscriminator, contractIdSuffix)

    val agreementText = "agreement"
    val disclosedContract = ProcessedDisclosedContract(
      templateId = templateId,
      contractId = authenticatedContractId,
      argument = LfValue.ValueNil,
      metadata = EngineEnrichedContractMetadata(
        createdAt = createdAt,
        driverMetadata = driverMetadata,
        signatories = Set(alice),
        stakeholders = Set(alice),
        maybeKeyWithMaintainers = None,
        agreementText = agreementText,
      ),
    )

    val versionedDisclosedContract = LfVersioned(transactionVersion, disclosedContract)

    "provided a valid disclosed contract" should {
      "succeed" in {
        val actual = SerializableContract
          .fromDisclosedContract(versionedDisclosedContract)
          .value

        actual shouldBe SerializableContract(
          contractId = authenticatedContractId,
          rawContractInstance = SerializableRawContractInstance
            .create(
              LfContractInst(transactionVersion, templateId, LfValue.ValueNil),
              AgreementText(agreementText),
            )
            .value,
          metadata = ContractMetadata.tryCreate(Set(alice), Set(alice), None),
          ledgerCreateTime = CantonTimestamp(createdAt),
          contractSalt = Some(contractSalt),
        )
      }
    }

    "provided a disclosed contract with unknown contract id format" should {
      "fail" in {
        SerializableContract
          .fromDisclosedContract(
            versionedDisclosedContract.map(_.copy(contractId = invalidFormatContractId))
          )
          .left
          .value shouldBe s"Invalid disclosed contract id: malformed contract id '${invalidFormatContractId.toString}'. Suffix 00 does not start with one of the supported prefixes: Bytes(ca01) or Bytes(ca00)"
      }
    }

    "provided a disclosed contract with non-authenticated contract id" should {
      "fail" in {
        SerializableContract
          .fromDisclosedContract(
            versionedDisclosedContract.map(_.copy(contractId = nonAuthenticatedContractId))
          )
          .left
          .value shouldBe s"Disclosed contract with non-authenticated contract id: ${nonAuthenticatedContractId.toString}"
      }
    }

    "provided a disclosed contract with missing driver contract metadata" should {
      "fail" in {
        SerializableContract
          .fromDisclosedContract(
            versionedDisclosedContract
              .focus(_.unversioned.metadata.driverMetadata)
              .replace(ImmArray.empty)
          )
          .left
          .value shouldBe "Missing driver contract metadata in provided disclosed contract"
      }
    }
  }
}
