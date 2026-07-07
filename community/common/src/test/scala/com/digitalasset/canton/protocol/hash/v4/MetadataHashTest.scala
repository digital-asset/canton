// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash.v4

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.protocol.hash.{HashTracer, HashUtilsTest, TransactionMetadataHasher}
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.transaction.SerializationVersion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class MetadataHashTest extends BaseTest with AnyWordSpecLike with Matchers with HashUtilsTest {

  "V4 Metadata Encoding" should {
    "hash VDev disclosed contracts through the V4 node builder" in {
      val hashTracer = HashTracer.StringHashTracer(true)

      val hash =
        TransactionMetadataHasher(HashingSchemeVersion.V4)
          .tryHashMetadata(metadata(SerializationVersion.VDev), hashTracer)

      hash.toHexString shouldBe "122073906e70b121a3bd3a38fa3f9995cbc05cb0dad896a9e2242691e2a24dda2871"
      hashTracer.result should include("'646576' # dev (string)")
      assertStringTracer(hashTracer, hash)
    }
  }
}
