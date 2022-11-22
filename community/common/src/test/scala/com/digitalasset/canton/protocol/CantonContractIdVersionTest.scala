// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.examples.Iou.Iou
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.wordspec.AnyWordSpec

class CantonContractIdVersionTest extends AnyWordSpec with BaseTest {
  private val cantonContractIdVersions =
    Seq(AuthenticatedContractIdVersion, NonAuthenticatedContractIdVersion)

  cantonContractIdVersions.foreach { cantonContractIdVersion =>
    s"${cantonContractIdVersion}" when {
      val discriminator = ExampleTransactionFactory.lfHash(1)
      val hash =
        Hash.build(HashPurposeTest.testHashPurpose, HashAlgorithm.Sha256).add(0).finish()

      val unicum = Unicum(hash)
      val cid = cantonContractIdVersion.fromDiscriminator(discriminator, unicum)

      "creating a contract ID from discriminator and unicum" should {
        "succeed" in {
          cid.coid shouldBe (
            LfContractId.V1.prefix.toHexString +
              discriminator.bytes.toHexString +
              cantonContractIdVersion.versionPrefixBytes.toHexString +
              unicum.unwrap.toHexString
          )
        }
      }

      s"ensuring canton contract id of ${cantonContractIdVersion.getClass.getSimpleName}" should {
        s"return a ${cantonContractIdVersion}" in {
          CantonContractIdVersion.ensureCantonContractId(cid) shouldBe Right(
            cantonContractIdVersion
          )
        }
      }

      "converting between API and LF types" should {
        import ContractIdSyntax.*
        "work both ways" in {
          val discriminator = ExampleTransactionFactory.lfHash(1)
          val hash =
            Hash.build(HashPurposeTest.testHashPurpose, HashAlgorithm.Sha256).add(0).finish()
          val unicum = Unicum(hash)
          val lfCid = cantonContractIdVersion.fromDiscriminator(discriminator, unicum)

          val apiCid = lfCid.toPrimUnchecked[Iou]
          val lfCid2 = apiCid.toLf

          lfCid2 shouldBe lfCid
        }
      }
    }
  }

  CantonContractIdVersion.getClass.getSimpleName when {
    "fromProtocolVersion" should {
      "return the correct canton contract id version" in {
        CantonContractIdVersion.fromProtocolVersion(
          ProtocolVersion.v2
        ) shouldBe NonAuthenticatedContractIdVersion
        CantonContractIdVersion.fromProtocolVersion(
          ProtocolVersion.v3
        ) shouldBe NonAuthenticatedContractIdVersion
        CantonContractIdVersion.fromProtocolVersion(
          ProtocolVersion.v4
        ) shouldBe AuthenticatedContractIdVersion
      }
    }
  }
}
