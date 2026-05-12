// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash.v3

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.protocol.hash.{HashTracer, HashUtilsTest, TransactionMetadataHasher}
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.transaction.SerializationVersion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class MetadataHashTest extends BaseTest with AnyWordSpecLike with Matchers with HashUtilsTest {

  "Metadata Encoding" should {
    val expectedMetadataHash = Hash
      .fromHexStringRaw("5a4c6aa89bb80d0af05a3f0614ba07bc7469795fff66d7b9e87c876eb0137e0a")
      .getOrElse(fail("Invalid hash"))

    "be stable" in {
      val actual = TransactionMetadataHasher(HashingSchemeVersion.V3)
        .tryHashMetadata(metadata(SerializationVersion.V2))
        .toHexString
      actual shouldBe expectedMetadataHash.toHexString
    }

    "explain encoding" in {
      val hashTracer = HashTracer.StringHashTracer(true)

      val actualMetadataHash =
        TransactionMetadataHasher(HashingSchemeVersion.V3)
          .tryHashMetadata(metadata(SerializationVersion.V2), hashTracer)

      hashTracer.result shouldBe
        """'00000030' # Hash Purpose
          |# Act As Parties
          |'00000002' # 2 (int)
          |'00000005' # 5 (int)
          |'616c696365' # alice (string)
          |'00000003' # 3 (int)
          |'626f62' # bob (string)
          |# Command Id
          |'0000000a' # 10 (int)
          |'636f6d6d616e642d6964' # command-id (string)
          |# Transaction UUID
          |'00000024' # 36 (int)
          |'34633634373164332d346530392d343964642d616464662d366364393065313963353833' # 4c6471d3-4e09-49dd-addf-6cd90e19c583 (string)
          |# Mediator Group
          |'00000000' # 0 (int)
          |# Synchronizer Id
          |'00000010' # 16 (int)
          |'73796e6368726f6e697a65723a3a6964' # synchronizer::id (string)
          |# Min Time Boundary
          |'01' # Some
          |'000000000000aaaa' # 43690 (long)
          |# Max Time Boundary
          |'01' # Some
          |'000000000000bbbb' # 48059 (long)
          |# Preparation Time
          |'0000000000000000' # 0 (long)
          |# Disclosed Contracts
          |'00000002' # 2 (int)
          |# Created At
          |'000000c92a69c000' # 864000000000 (long)
          |# Create Contract
          |  # Create Node
          |  # Node Version
          |  '00000001' # 1 (int)
          |  '32' # 2 (string)
          |  '00' # Create Node Tag
          |  # Node Seed
          |  '00' # None
          |  # Contract Id
          |  '00000021' # 33 (int)
          |  '0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5' # 0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5 (contractId)
          |  # Package Name
          |  '00000007' # 7 (int)
          |  '506b674e616d65' # PkgName (string)
          |  # Template Id
          |  '0000000a' # 10 (int)
          |  '2d64756d6d79506b672d' # -dummyPkg- (string)
          |  '00000001' # 1 (int)
          |  '0000000b' # 11 (int)
          |  '44756d6d794d6f64756c65' # DummyModule (string)
          |  '00000001' # 1 (int)
          |  '00000009' # 9 (int)
          |  '64756d6d794e616d65' # dummyName (string)
          |  # Arg
          |  '08' # ContractId Type Tag
          |  '00000021' # 33 (int)
          |  '0097a092402108f5593bac7fb3c909cd316910197dd98d603042a45ab85c81e0fd' # 0097a092402108f5593bac7fb3c909cd316910197dd98d603042a45ab85c81e0fd (contractId)
          |  # Signatories
          |  '00000001' # 1 (int)
          |  '00000005' # 5 (int)
          |  '616c696365' # alice (string)
          |  # Stakeholders
          |  '00000001' # 1 (int)
          |  '00000005' # 5 (int)
          |  '616c696365' # alice (string)
          |  # Key
          |  '00' # None
          |'84c7946905ba3afac9a2e8d360129b92c9055cc9792655f9a0c9195b08ade109' # Disclosed Contract
          |# Created At
          |'0000019254d38000' # 1728000000000 (long)
          |# Create Contract
          |  # Create Node
          |  # Node Version
          |  '00000001' # 1 (int)
          |  '32' # 2 (string)
          |  '00' # Create Node Tag
          |  # Node Seed
          |  '00' # None
          |  # Contract Id
          |  '00000021' # 33 (int)
          |  '0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b' # 0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b (contractId)
          |  # Package Name
          |  '00000007' # 7 (int)
          |  '506b674e616d65' # PkgName (string)
          |  # Template Id
          |  '0000000a' # 10 (int)
          |  '2d64756d6d79506b672d' # -dummyPkg- (string)
          |  '00000001' # 1 (int)
          |  '0000000b' # 11 (int)
          |  '44756d6d794d6f64756c65' # DummyModule (string)
          |  '00000001' # 1 (int)
          |  '00000009' # 9 (int)
          |  '64756d6d794e616d65' # dummyName (string)
          |  # Arg
          |  '08' # ContractId Type Tag
          |  '00000021' # 33 (int)
          |  '0097a092402108f5593bac7fb3c909cd316910197dd98d603042a45ab85c81e0fd' # 0097a092402108f5593bac7fb3c909cd316910197dd98d603042a45ab85c81e0fd (contractId)
          |  # Signatories
          |  '00000001' # 1 (int)
          |  '00000003' # 3 (int)
          |  '626f62' # bob (string)
          |  # Stakeholders
          |  '00000001' # 1 (int)
          |  '00000003' # 3 (int)
          |  '626f62' # bob (string)
          |  # Key
          |  '00' # None
          |'e53bfab08a6142fc61781fcb242591824716501df665ebd4b68d7406c988f1b0' # Disclosed Contract
          |# Max Record Time
          |'01' # Some
          |'0000025b7f3d4000' # 2592000000000 (long)
          |""".stripMargin

      actualMetadataHash shouldBe expectedMetadataHash

      assertStringTracer(hashTracer, expectedMetadataHash)
    }
  }

}
