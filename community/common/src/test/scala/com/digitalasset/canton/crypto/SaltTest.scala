// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.crypto.provider.symbolic.{SymbolicCrypto, SymbolicPureCrypto}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

class SaltTest extends AnyWordSpec with BaseTest {

  "Salt" should {

    "serializing and deserializing via protobuf" in {
      val salt = TestSalt.generate(0)
      val saltP = salt.toProtoV0
      Salt.fromProtoV0(saltP).value shouldBe salt
    }

    "generate a fresh salt" in {
      implicit val ec: ExecutionContext = DirectExecutionContext(logger)

      val crypto = SymbolicCrypto.create(timeouts, loggerFactory)
      val seedData = ByteString.copyFromUtf8("testSeedData")
      val salt = Await.result(Salt.generate(seedData, crypto.privateCrypto).value, 10.seconds)

      salt.value shouldBe a[Salt]
    }

    "derive a salt" in {
      val hmacOps = new SymbolicPureCrypto
      val seedSalt = TestSalt.generate(0)
      val salt = Salt.deriveSalt(seedSalt, 0, hmacOps)

      // The derived salt must be different than the seed salt value
      salt.value.unwrap should not equal seedSalt.unwrap
    }

  }

}
