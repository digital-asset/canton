// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.bifunctor._
import cats.syntax.traverse._
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.QualifiedName
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.ExampleTransactionFactory
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{
  asSerializable,
  contractInstance,
  packageId,
  transactionId,
}
import com.digitalasset.canton.{BaseTest, LfPartyId}
import org.scalatest.wordspec.AsyncWordSpec

trait ContractStoreTest { this: AsyncWordSpec with BaseTest =>

  val alice: LfPartyId = LfPartyId.assertFromString("alice")
  val bob: LfPartyId = LfPartyId.assertFromString("bob")
  val charlie: LfPartyId = LfPartyId.assertFromString("charlie")
  val david: LfPartyId = LfPartyId.assertFromString("david")

  def contractStore(mk: () => ContractStore): Unit = {

    val contractId = ExampleTransactionFactory.suffixedId(0, 0)
    val contractId2 = ExampleTransactionFactory.suffixedId(2, 0)
    val contractId3 = ExampleTransactionFactory.suffixedId(3, 0)
    val contractId4 = ExampleTransactionFactory.suffixedId(4, 0)
    val contractId5 = ExampleTransactionFactory.suffixedId(5, 0)
    val contract =
      asSerializable(contractId, contractInstance = contractInstance(agreementText = "instance"))
    val transactionId1 = transactionId(1)
    val transactionId2 = transactionId(2)
    val requestCounter = 0L
    val storedContract =
      StoredContract.fromCreatedContract(contract, requestCounter, transactionId1)
    val divulgedContract = StoredContract.fromDivulgedContract(contract, requestCounter)

    val requestCounter2 = 1L
    val let2 = CantonTimestamp.Epoch.plusSeconds(5)
    val pkgId2 = Ref.PackageId.assertFromString("different_id")
    val contract2 = asSerializable(
      contractId2,
      contractInstance = contractInstance(
        agreementText = "text",
        templateId = Ref.Identifier(pkgId2, QualifiedName.assertFromString("module:template")),
      ),
      ledgerTime = let2,
    )
    val templateName3 = QualifiedName.assertFromString("Foo:Bar")
    val templateId3 = Ref.Identifier(packageId, templateName3)
    val contract3 =
      asSerializable(
        contractId3,
        contractInstance = contractInstance(templateId = templateId3),
        ledgerTime = let2,
      )
    val contract4 =
      asSerializable(
        contractId4,
        contractInstance = contractInstance(
          agreementText = "instance",
          templateId = Ref.Identifier(pkgId2, templateName3),
        ),
      )

    val contract5 =
      asSerializable(
        contractId5,
        contractInstance = contractInstance(
          agreementText = "instance",
          templateId = Ref.Identifier(pkgId2, templateName3),
        ),
      )

    "store and retrieve a created contract" in {
      val store = mk()

      for {
        _ <- store.storeCreatedContract(requestCounter, transactionId1, contract)
        c <- store.lookupE(contractId)
        inst <- store.lookupContractE(contractId).leftWiden[ContractStoreError]
      } yield {
        c shouldEqual storedContract
        inst shouldEqual contract
      }
    }

    "store and retrieve a divulged contract that does not yet exist" in {
      val store = mk()

      for {
        _ <- store.storeDivulgedContract(requestCounter, contract)
        c <- store.lookupE(contractId)
        inst <- store.lookupContractE(contractId).leftWiden[ContractStoreError]
      } yield {
        c shouldEqual divulgedContract
        inst shouldEqual contract
      }
    }

    "store the same contract twice for the same id" in {
      val store = mk()

      for {
        _ <- store.storeCreatedContracts(requestCounter, transactionId1, Seq(contract, contract))
        _ <- store.storeDivulgedContracts(requestCounter, Seq(contract2, contract2))
      } yield succeed
    }

    "take the second created contract" in {
      val store = mk()
      for {
        _ <- store.storeCreatedContract(requestCounter, transactionId1, contract)
        _ <- store.storeCreatedContract(requestCounter2, transactionId2, contract)
        c <- store.lookupE(contract.contractId).leftWiden[ContractStoreError]
      } yield c shouldBe StoredContract.fromCreatedContract(
        contract,
        requestCounter2,
        transactionId2,
      )
    }

    "succeed when storing a different contract for an existing id" must {

      val divulgedContract2 =
        StoredContract.fromDivulgedContract(contract.copy(ledgerCreateTime = let2), requestCounter2)
      "for divulged contracts" in {
        val store = mk()

        for {
          _ <- store.storeDivulgedContract(requestCounter, contract)
          _ <- store.storeDivulgedContract(requestCounter2, contract.copy(ledgerCreateTime = let2))
          c <- store.lookupE(contract.contractId).leftWiden[ContractStoreError]
        } yield c shouldBe divulgedContract2
      }

      "for divulged contracts reversed" in {
        val store = mk()

        for {
          _ <- store.storeDivulgedContract(requestCounter2, contract.copy(ledgerCreateTime = let2))
          _ <- store.storeDivulgedContract(requestCounter, contract)
          c <- store.lookupE(contract.contractId).leftWiden[ContractStoreError]
        } yield c shouldBe divulgedContract2
      }

      val storedContract2 =
        StoredContract.fromCreatedContract(
          contract.copy(ledgerCreateTime = let2),
          requestCounter2,
          transactionId2,
        )
      "for created contracts" in {
        val store = mk()

        for {
          _ <- store.storeCreatedContract(requestCounter, transactionId1, contract)
          _ <- store.storeCreatedContract(
            requestCounter2,
            transactionId2,
            contract.copy(ledgerCreateTime = let2),
          )
          c <- store.lookupE(contract.contractId).leftWiden[ContractStoreError]
        } yield c shouldBe storedContract2
      }

      "for created contracts reversed" in {
        val store = mk()

        for {
          _ <- store.storeCreatedContract(
            requestCounter2,
            transactionId2,
            contract.copy(ledgerCreateTime = let2),
          )
          _ <- store.storeCreatedContract(requestCounter, transactionId1, contract)
          c <- store.lookupE(contract.contractId).leftWiden[ContractStoreError]
        } yield c shouldBe storedContract2
      }

      "even when mixing created and divulged contracts" in {
        val store = mk()

        for {
          _ <- store.storeDivulgedContract(requestCounter2 + 1, contract)
          _ <- store.storeCreatedContract(
            requestCounter2,
            transactionId2,
            contract.copy(ledgerCreateTime = let2),
          )
          c <- store.lookupE(contract.contractId).leftWiden[ContractStoreError]
        } yield c shouldBe storedContract2
      }
    }

    "fail when looking up nonexistent contract" in {
      val store = mk()
      for {
        c <- store.lookup(contractId).value
      } yield c shouldEqual None
    }

    "ignore a divulged contract with a different contract instance" in {
      val store = mk()
      val modifiedContract = contract2.copy(contractId = contractId)

      for {
        _ <- store.storeCreatedContract(requestCounter, transactionId1, contract)
        _ <- store.storeDivulgedContract(requestCounter2, modifiedContract)
        c <- store.lookupE(contract.contractId).leftWiden[ContractStoreError]
      } yield c shouldBe storedContract
    }

    "delete a contract" in {
      val store = mk()
      for {
        _ <- store.storeCreatedContract(requestCounter, transactionId1, contract)
        res1 <- store.deleteContract(contractId).value
        c <- store.lookupE(contractId).value
      } yield {
        res1 shouldEqual Right(())
        c shouldEqual Left(UnknownContract(contractId))
      }
    }

    "reject deleting an nonexistent contract" in {
      val store = mk()
      for {
        res <- store.deleteContract(contractId).value
      } yield res shouldEqual Left(UnknownContract(contractId))
    }

    "delete a set of contracts" in {
      val store = mk()
      for {
        _ <- List(contract, contract2, contract4, contract5)
          .traverse(store.storeCreatedContract(requestCounter, transactionId1, _))
        _ <- store.deleteIgnoringUnknown(Seq(contractId, contractId2, contractId3, contractId4))
        notFounds <- List(contractId, contractId2, contractId3, contractId4).traverse(
          store.lookupE(_).value
        )
        notDeleted <- store.lookupE(contractId5).value
      } yield {
        notFounds shouldEqual List(
          Left(UnknownContract(contractId)),
          Left(UnknownContract(contractId2)),
          Left(UnknownContract(contractId3)),
          Left(UnknownContract(contractId4)),
        )
        notDeleted.map(_.contract) shouldEqual Right(contract5)
      }
    }

    "delete divulged contracts" in {
      val store = mk()
      for {
        _ <- store.storeDivulgedContract(0L, contract)
        _ <- store.storeCreatedContract(0L, transactionId1, contract2)
        _ <- store.storeDivulgedContract(1L, contract3)
        _ <- store.storeCreatedContract(1L, transactionId1, contract4)
        _ <- store.storeDivulgedContract(2L, contract5)
        _ <- store.deleteDivulged(1L)
        c1 <- store.lookupContract(contract.contractId).value
        c2 <- store.lookupContract(contract2.contractId).value
        c3 <- store.lookupContract(contract3.contractId).value
        c4 <- store.lookupContract(contract4.contractId).value
        c5 <- store.lookupContract(contract5.contractId).value
      } yield {
        c1 shouldBe None
        c2 shouldBe Some(contract2)
        c3 shouldBe None
        c4 shouldBe Some(contract4)
        c5 shouldBe Some(contract5)
      }
    }

    "find contracts by filters" in {
      val store = mk()

      for {
        _ <- store.storeCreatedContract(requestCounter, transactionId1, contract)
        _ <- store.storeCreatedContract(requestCounter, transactionId1, contract2)
        _ <- store.storeCreatedContract(requestCounter, transactionId1, contract3)
        _ <- store.storeCreatedContract(requestCounter, transactionId1, contract4)
        _ <- store.storeCreatedContract(
          requestCounter,
          transactionId2,
          contract2.copy(contractId = contractId5, ledgerCreateTime = CantonTimestamp.Epoch),
        )

        resId <- store.find(filterId = Some(contractId.coid), None, None, 100)
        resPkg <- store
          .find(filterId = None, filterPackage = Some(pkgId2), None, 100)
        resPkgLimit <- store
          .find(filterId = None, filterPackage = Some(pkgId2), None, 2)
        resTemplatePkg <- store.find(
          filterId = None,
          filterPackage = Some(contract4.contractInstance.unversioned.template.packageId),
          filterTemplate =
            Some(contract4.contractInstance.unversioned.template.qualifiedName.toString()),
          100,
        )
        resTemplate <- store.find(None, None, Some(templateName3.toString), 100)
      } yield {
        resId shouldEqual List(contract)
        resTemplatePkg shouldEqual List(contract4)
        resPkg should have size 3
        resPkgLimit should have size 2
        resTemplate.toSet shouldEqual Set(contract4, contract3)
      }
    }

    "store contract and use contract lookups" in {
      val store = mk()

      for {
        _ <- store.storeCreatedContract(requestCounter, transactionId1, contract)
        _ <- store.storeCreatedContract(requestCounter, transactionId2, contract2)
        c1 <- store.lookup(contractId).value
        c1inst <- store.lookupContract(contractId).value
        c3 <- store.lookup(contractId3).value
      } yield {
        c1 shouldEqual Some(storedContract)
        c1inst shouldEqual Some(contract)
        c3 shouldEqual None
      }

    }

    "lookup stakeholders when passed existing contract IDs" in {
      val store = mk()

      for {
        _ <- store.storeCreatedContract(requestCounter, transactionId1, contract)
        _ <- store.storeCreatedContract(requestCounter, transactionId1, contract2)
        _ <- store.storeCreatedContract(requestCounter, transactionId1, contract3)
        _ <- store.storeCreatedContract(requestCounter, transactionId1, contract4)
        res <- store.lookupStakeholders(Set(contractId, contractId2, contractId4))
      } yield {
        res shouldBe Map(
          contractId -> contract.metadata.stakeholders,
          contractId2 -> contract2.metadata.stakeholders,
          contractId4 -> contract4.metadata.stakeholders,
        )
      }
    }

    "lookup stakeholders when passed no contract IDs" in {
      val store = mk()

      for {
        res <- store.lookupStakeholders(Set()).value
      } yield {
        res shouldBe Right(Map.empty)
      }
    }

    "fail stakeholder lookup when passed a non-existant contract IDs" in {
      val store = mk()

      for {
        _ <- store.storeCreatedContract(requestCounter, transactionId1, contract)
        _ <- store.storeCreatedContract(requestCounter, transactionId1, contract3)
        _ <- store.storeCreatedContract(requestCounter, transactionId1, contract4)
        res <- store.lookupStakeholders(Set(contractId, contractId2, contractId4)).value
      } yield {
        res shouldBe Left(UnknownContracts(Set(contractId2)))
      }
    }
  }
}
