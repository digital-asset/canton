// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.ExampleTransactionFactory.packageName
import com.digitalasset.canton.{BaseTest, LfPartyId}
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.transaction.CreationTime
import com.digitalasset.daml.lf.value.Value.{ValueText, ValueUnit}
import org.scalatest.wordspec.AnyWordSpecLike

/** // TODO(#31527): SPM add tests
  *   - Multiple contracts for the same key (returned in the expected order)
  *   - Extending the found keys with non-by-key contracts that have the same key
  */
class ReplayContractLookupTest extends AnyWordSpecLike with BaseTest {

  import com.digitalasset.canton.protocol.ExampleTransactionFactory.suffixedId

  private val coid00: LfContractId = suffixedId(0, 0)
  private val coid01: LfContractId = suffixedId(0, 1)
  private val coid10: LfContractId = suffixedId(1, 0)
  private val coid11: LfContractId = suffixedId(1, 1)
  private val coid20: LfContractId = suffixedId(2, 0)
  private val coid21: LfContractId = suffixedId(2, 1)

  private val let0: CantonTimestamp = CantonTimestamp.Epoch
  private val let1: CantonTimestamp = CantonTimestamp.ofEpochMilli(1)

  "ReplayContractLookup" should {

    val instance0Template = ExampleContractFactory.templateId
    val keyWithMaintainers: LfGlobalKeyWithMaintainers =
      LfGlobalKeyWithMaintainers
        .build(
          templateId = instance0Template,
          value = ValueUnit,
          valueHash = crypto.Hash.hashPrivateKey("dummy-key-hash-1"),
          maintainers = Set.empty,
          packageName = packageName,
        )
        .value

    val key1: LfGlobalKey =
      LfGlobalKey
        .build(
          instance0Template,
          packageName,
          ValueText("abc"),
          crypto.Hash.hashPrivateKey("dummy-key-hash-2"),
        )
        .value
    val alice = LfPartyId.assertFromString("alice")
    val bob = LfPartyId.assertFromString("bob")

    val metadata2 =
      ContractMetadata.tryCreate(signatories = Set(alice), stakeholders = Set(alice, bob), None)

    val contracts = Map(
      coid00 -> ExampleContractFactory.build(
        overrideContractId = Some(coid00),
        signatories = metadata2.signatories,
        stakeholders = metadata2.stakeholders,
        createdAt = CreationTime.CreatedAt(let0.toLf),
        keyOpt = Some(keyWithMaintainers),
      ),
      coid01 -> ExampleContractFactory.build(
        overrideContractId = Some(coid01),
        signatories = metadata2.signatories,
        stakeholders = metadata2.stakeholders,
        createdAt = CreationTime.CreatedAt(let0.toLf),
      ),
      coid20 -> ExampleContractFactory.build(
        overrideContractId = Some(coid20),
        signatories = metadata2.signatories,
        stakeholders = metadata2.stakeholders,
        createdAt = CreationTime.CreatedAt(let1.toLf),
      ),
      coid21 -> ExampleContractFactory.build(
        overrideContractId = Some(coid21),
        signatories = metadata2.signatories,
        stakeholders = metadata2.stakeholders,
        createdAt = CreationTime.CreatedAt(let0.toLf),
      ),
    )

    val underTest = new ReplayContractLookup(
      contracts,
      Map(keyWithMaintainers.globalKey -> Vector(coid00), key1 -> Vector.empty),
    )

    "not make up contracts" in {
      val result = underTest.lookup(coid11)
      assert(result.isEmpty)
    }

    "find a contract" in {
      val result = underTest.lookup(coid01)
      result shouldBe contracts.get(coid01)
    }

    "find an additional created contract" in {
      val result = underTest.lookup(coid21)
      result shouldBe contracts.get(coid21)
    }

    "complain about inconsistent contract ids" in {
      val contract =
        ExampleContractFactory.build(overrideContractId = Some(coid01))

      assertThrows[IllegalArgumentException](
        new ReplayContractLookup(
          Map(coid10 -> contract),
          Map.empty,
        )
      )
    }

    "find exactly the keys in the provided map" in {
      underTest.lookupKey(keyWithMaintainers.globalKey) shouldBe contracts
        .get(coid00)
        .map(_.inst)
        .toList
        .toVector
      underTest.lookupKey(key1) shouldBe empty
    }
  }
}
