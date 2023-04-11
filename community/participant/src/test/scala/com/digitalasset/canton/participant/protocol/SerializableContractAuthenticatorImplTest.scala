// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto.{Salt, TestSalt}
import com.digitalasset.canton.data.{CantonTimestamp, ViewPosition}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.{DomainId, MediatorId, UniqueIdentifier}
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import java.util.UUID

class SerializableContractAuthenticatorImplTest extends AnyWordSpec with BaseTest {
  private lazy val unicumGenerator = new UnicumGenerator(new SymbolicPureCrypto())
  private lazy val contractAuthenticator = new SerializableContractAuthenticatorImpl(
    unicumGenerator
  )

  private lazy val contractInstance = ExampleTransactionFactory.contractInstance()
  private lazy val ledgerTime = CantonTimestamp.MinValue
  private lazy val (contractSalt, unicum) = unicumGenerator.generateSaltAndUnicum(
    domainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::da")),
    mediatorId = MediatorId(UniqueIdentifier.tryCreate("mediator", "other")),
    transactionUuid = new UUID(1L, 1L),
    viewPosition = ViewPosition(List.empty),
    viewParticipantDataSalt = TestSalt.generateSalt(1),
    createIndex = 0,
    ledgerTime = ledgerTime,
    suffixedContractInstance =
      ExampleTransactionFactory.asSerializableRaw(contractInstance, agreementText = ""),
    contractIdVersion = AuthenticatedContractIdVersion,
  )

  private lazy val contractId = AuthenticatedContractIdVersion.fromDiscriminator(
    ExampleTransactionFactory.lfHash(1337),
    unicum,
  )

  private lazy val contract =
    SerializableContract(
      contractId = contractId,
      contractInstance = contractInstance,
      metadata = ContractMetadata.tryCreate(Set.empty, Set.empty, None), // Not used
      ledgerTime = ledgerTime,
      contractSalt = Some(contractSalt.unwrap),
      unvalidatedAgreementText = AgreementText.empty,
    ).valueOrFail("Failed creating serializable contract instance")

  classOf[SerializableContractAuthenticatorImpl].getSimpleName when {
    s"provided with a $AuthenticatedContractIdVersion" should {
      "correctly authenticate the contract" in {
        contractAuthenticator.authenticate(contract) shouldBe Right(())
      }
    }

    "provided with generic contract id" should {
      "not authenticate" in {
        val nonAuthenticatedContractId = ExampleTransactionFactory.suffixedId(1, 2)
        contractAuthenticator.authenticate(
          contract.copy(contractId = nonAuthenticatedContractId)
        ) shouldBe Right(())
      }
    }

    "provided with a contract with a mismatching salt" should {
      "fail authentication" in {
        val changedSalt = TestSalt.generateSalt(1337)
        testFailedAuthentication(
          _.copy(contractSalt = Some(changedSalt)),
          testedSalt = changedSalt,
        )
      }
    }

    "provided with a contract with changed ledger time" should {
      "fail authentication" in {
        val changedLedgerTime = ledgerTime.add(Duration.ofDays(1L))
        testFailedAuthentication(
          _.copy(ledgerCreateTime = changedLedgerTime),
          testedLedgerTime = changedLedgerTime,
        )
      }
    }

    "provided with a contract with changed contents" should {
      "fail authentication" in {
        val changedContractInstance = ExampleTransactionFactory.contractInstance(
          Seq(ExampleTransactionFactory.suffixedId(1, 2))
        )
        testFailedAuthentication(
          _.copy(rawContractInstance =
            ExampleTransactionFactory.asSerializableRaw(changedContractInstance, "")
          ),
          testedContractInstance =
            ExampleTransactionFactory.asSerializableRaw(changedContractInstance, ""),
        )
      }
    }

    s"provided with a $AuthenticatedContractIdVersion with a missing salt" should {
      "fail authentication" in {
        contractAuthenticator.authenticate(contract.copy(contractSalt = None)) shouldBe Left(
          s"Contract salt missing in serializable contract with authenticating contract id ($contractId)"
        )
      }
    }
  }

  private def testFailedAuthentication(
      changeContract: SerializableContract => SerializableContract,
      testedSalt: Salt = contractSalt.unwrap,
      testedLedgerTime: CantonTimestamp = ledgerTime,
      testedContractInstance: SerializableRawContractInstance =
        ExampleTransactionFactory.asSerializableRaw(contractInstance, ""),
  ): Assertion = {
    val recomputedUnicum = unicumGenerator
      .recomputeUnicum(
        contractSalt = testedSalt,
        ledgerTime = testedLedgerTime,
        suffixedContractInstance = testedContractInstance,
        contractIdVersion = AuthenticatedContractIdVersion,
      )
      .valueOrFail("Failed unicum computation")
    val actualSuffix = unicum.toContractIdSuffix(AuthenticatedContractIdVersion)
    val expectedSuffix = recomputedUnicum.toContractIdSuffix(AuthenticatedContractIdVersion)

    contractAuthenticator.authenticate(changeContract(contract)) shouldBe Left(
      s"Mismatching contract id suffixes. expected: $expectedSuffix vs actual: $actualSuffix"
    )
  }
}
