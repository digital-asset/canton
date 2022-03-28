// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.either._
import com.digitalasset.canton.BaseTest
import com.google.protobuf.ByteString
import org.scalatest.wordspec.{AnyWordSpec, AsyncWordSpec}

import scala.concurrent.Future

class HmacTest extends AnyWordSpec with BaseTest {

  private val longString = PseudoRandom.randomAlphaNumericString(256)

  forAll(HmacAlgorithm.algorithms) { algorithm =>
    s"HMAC ${algorithm.name}" should {

      "serializing and deserializing via protobuf" in {
        val secret = HmacSecret.generate()
        val hmac =
          Hmac
            .compute(secret, ByteString.copyFromUtf8(longString), algorithm)
            .valueOr(err => fail(err.toString))
        val hmacP = hmac.toProtoV0
        Hmac.fromProtoV0(hmacP).value shouldBe (hmac)
      }

    }

  }

}

trait HmacPrivateTest extends BaseTest { this: AsyncWordSpec =>

  def hmacProvider(newHmacPrivateOps: => Future[HmacPrivateOps]): Unit = {

    "HMAC private ops" should {

      "initialize and compute an HMAC with a stored secret" in {
        for {
          privateCrypto <- newHmacPrivateOps
          _ <- privateCrypto.initializeHmacSecret().valueOrFail("init hmac secret")
          _ <- privateCrypto.hmac(ByteString.copyFromUtf8("foobar"))
        } yield assert(true)
      }

      "rotate an HMAC secret" in {
        for {
          privateCrypto <- newHmacPrivateOps
          data = ByteString.copyFromUtf8("foobar")
          _ <- privateCrypto.initializeHmacSecret().valueOrFail("init hmac secret")
          hmac1 <- privateCrypto.hmac(data).valueOrFail("create hmac1")
          hmac2 <- privateCrypto.hmac(data).valueOrFail("create hmac2")
          _ <- privateCrypto.rotateHmacSecret().valueOrFail("rotate hmac secret")
          hmac3 <- privateCrypto.hmac(data).valueOrFail("create hmac3")
        } yield {
          hmac1 shouldEqual hmac2
          hmac1 should not equal hmac3
        }

      }

    }

  }

}
