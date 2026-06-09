// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.protocol.ExampleContractFactory
import com.digitalasset.daml.lf.data.Ref
import org.scalatest.wordspec.AnyWordSpecLike

class ReplayContractLookupTest extends AnyWordSpecLike with BaseTest {

  "ReplayContractLookup" should {

    val key1 = ExampleContractFactory.buildKeyWithMaintainers(packageName =
      Ref.PackageName.assertFromString("package1")
    )
    val key2 = ExampleContractFactory.buildKeyWithMaintainers(packageName =
      Ref.PackageName.assertFromString("package2")
    )
    val contract1 = ExampleContractFactory.build()
    val contract2 = ExampleContractFactory.build()

    val contractK1P1 = ExampleContractFactory.build(keyOpt = Some(key1))
    val contractK1P2 = ExampleContractFactory.build(keyOpt = Some(key1))

    val contractK2P1 = ExampleContractFactory.build(keyOpt = Some(key2))
    val contractK2U = ExampleContractFactory.build(keyOpt = Some(key2))

    val orderedK1 = Seq(contractK1P1, contractK1P2)

    val contracts = Seq(
      contract1,
      contract2,
      contractK1P1,
      contractK1P2,
      contractK2P1,
      contractK2U,
    ).map(c => c.contractId -> c).toMap

    val keys = Map(
      key1.globalKey -> orderedK1.map(_.contractId),
      key2.globalKey -> Seq(contractK2P1.contractId),
    )

    val underTest = new ReplayContractLookup(contracts, keys)

    "lookup a contract by id" in {
      underTest.lookup(contract1.contractId) shouldBe Some(contract1)
    }

    "lookup a contract instrument by id" in {
      underTest.lookupInst(contract1.contractId) shouldBe Some(contract1.inst)
    }

    "complain about inconsistent contract ids" in {
      val contract = ExampleContractFactory.build()
      val coid = ExampleContractFactory.buildContractId()
      assertThrows[IllegalArgumentException](
        new ReplayContractLookup(
          Map(coid -> contract),
          Map.empty,
        )
      )
    }

    "lookup a contract by key" in {
      underTest.lookupKey(key1.globalKey) shouldBe orderedK1.map(_.inst)
    }

    "append unordered keyed contracts" in {
      underTest.lookupKey(key2.globalKey) shouldBe Seq(contractK2P1, contractK2U).map(_.inst)
    }

  }
}
