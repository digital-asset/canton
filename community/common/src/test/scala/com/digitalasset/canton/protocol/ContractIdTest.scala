// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto._
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
}
