// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, LfContractId}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TransactionRoutingProcessorTest extends AnyWordSpec with Matchers {

  private val alice = LfPartyId.assertFromString("alice::default")
  private val bob = LfPartyId.assertFromString("bob::default")

  private def cid(i: Int): LfContractId = ExampleTransactionFactory.suffixedId(-1, i)

  "inputContractIds" should {
    "include contracts that only a key lookup resolves" in {
      val resolved = cid(1)
      val tx = ExampleTransactionFactory.transaction(
        Seq(0),
        ExampleTransactionFactory.queryByKeyNode(
          key = ExampleTransactionFactory.defaultGlobalKey,
          maintainers = Set(alice),
          resolution = Vector(resolved),
        ),
      )

      TransactionRoutingProcessor.inputContractIds(tx) shouldBe Seq(resolved)
    }

    "include fetched and exercised contracts" in {
      val fetched = cid(1)
      val exercised = cid(2)
      val tx = ExampleTransactionFactory.transaction(
        Seq(0, 1),
        ExampleTransactionFactory.fetchNode(fetched, signatories = Set(alice)),
        ExampleTransactionFactory.exerciseNodeWithoutChildren(
          exercised,
          signatories = Set(bob),
        ),
      )

      TransactionRoutingProcessor.inputContractIds(tx) should contain theSameElementsAs Seq(
        fetched,
        exercised,
      )
    }

    "exclude contracts created by the transaction itself" in {
      val created = cid(1)
      val tx = ExampleTransactionFactory.transaction(
        Seq(0, 1),
        ExampleTransactionFactory.createNode(created, signatories = Set(alice)),
        ExampleTransactionFactory.fetchNode(created, signatories = Set(alice)),
      )

      TransactionRoutingProcessor.inputContractIds(tx) shouldBe empty
    }

    "report a contract reached through several nodes once" in {
      val contract = cid(1)
      val tx = ExampleTransactionFactory.transaction(
        Seq(0, 1),
        ExampleTransactionFactory.queryByKeyNode(
          key = ExampleTransactionFactory.defaultGlobalKey,
          maintainers = Set(alice),
          resolution = Vector(contract),
        ),
        ExampleTransactionFactory.fetchNode(contract, signatories = Set(alice)),
      )

      TransactionRoutingProcessor.inputContractIds(tx) shouldBe Seq(contract)
    }
  }
}
