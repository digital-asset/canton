// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.crypto.provider.jce.JceJavaKeyConverter
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.{BaseTest, FailOnShutdown}
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.scalatest.wordspec.AsyncWordSpec

import java.security.Security

trait JwksTest extends AsyncWordSpec with BaseTest with CryptoTestHelper with FailOnShutdown {

  def jwksProvider(
      supportedSigningKeySpecs: Set[SigningKeySpec],
      unsupportedSigningKeySpecs: Set[SigningKeySpec],
      newCrypto: => FutureUnlessShutdown[Crypto],
  ): Unit = {
    Security.addProvider(new BouncyCastleProvider())

    forAll(supportedSigningKeySpecs) { signingKeySpec =>
      s"JWK conversion for $signingKeySpec keys" should {

        "match the corresponding Java PublicKey" in {
          for {
            crypto <- newCrypto
            publicKey <- getSigningPublicKey(
              crypto,
              // proof of ownership is added internally
              SigningKeyUsage.ProtocolOnly,
              signingKeySpec,
            )
            jwk = crypto.pureCrypto.toJwk(publicKey).valueOrFail("toJwk(publicKey)")
            jPublicKey = JceJavaKeyConverter.toJava(publicKey).valueOrFail("toJava(publicKey)")

          } yield {
            JwksTestHelper.toAuth0(jwk).getPublicKey() shouldBe jPublicKey
          }
        }
      }
    }

    forAll(unsupportedSigningKeySpecs) { signingKeySpec =>
      s"Public Key with (unsupported) $signingKeySpec key" should {

        "fail to convert to a JWK" in {
          for {
            crypto <- newCrypto
            publicKey <- getSigningPublicKey(
              crypto,
              // proof of ownership is added internally
              SigningKeyUsage.ProtocolOnly,
              signingKeySpec,
            )
            jwk = crypto.pureCrypto.toJwk(publicKey)

          } yield {
            jwk.left.value shouldBe JwksError.UnsupportedKeySpec(signingKeySpec)
          }
        }
      }
    }

  }
}
