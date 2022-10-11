// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.examples.Iou.Iou
import org.scalatest.wordspec.AnyWordSpec

class ContractIdTest extends AnyWordSpec with BaseTest {

  "Creating a contract ID from discriminator and unicum" should {
    "succeed" in {
      val discriminator = ExampleTransactionFactory.lfHash(1)
      val hash = Hash.build(HashPurposeTest.testHashPurpose, HashAlgorithm.Sha256).add(0).finish()
      val unicum = Unicum(hash)
      val cid = ContractId.fromDiscriminator(discriminator, unicum)
      cid.coid shouldBe (
        LfContractId.V1.prefix.toHexString +
          discriminator.bytes.toHexString +
          ContractId.suffixPrefixHex +
          unicum.unwrap.toHexString
      )
    }
  }

  "Conversion between API and LF types" should {
    import ContractIdSyntax.*
    "work both ways" in {
      val discriminator = ExampleTransactionFactory.lfHash(1)
      val hash = Hash.build(HashPurposeTest.testHashPurpose, HashAlgorithm.Sha256).add(0).finish()
      val unicum = Unicum(hash)
      val lfCid = ContractId.fromDiscriminator(discriminator, unicum)

      val apiCid = lfCid.toPrimUnchecked[Iou]
      val lfCid2 = apiCid.toLf

      lfCid2 shouldBe lfCid
    }
  }
}
