// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait JavaPublicKeyConverterTest extends BaseTest with CryptoTestHelper { this: AsyncWordSpec =>

  // Weak test as we don't use the converted Java keys here, we just ensure the conversion does not blow up.
  private def javaConvertTest(
      name: String,
      newCrypto: => Future[Crypto],
      newPublicKey: Crypto => Future[PublicKey],
  ): Unit = {

    s"Convert $name public keys to Java keys" should {

      "Convert public key to Java" in {
        for {
          crypto <- newCrypto
          publicKey <- newPublicKey(crypto)
          _ = crypto.javaKeyConverter
            .toJava(publicKey)
            .valueOrFail("convert to java")
        } yield assert(true)
      }

    }
  }

  def javaPublicKeyConverterProvider(
      supportedSigningKeySchemes: Set[SigningKeyScheme],
      supportedEncryptionKeySchemes: Set[EncryptionKeyScheme],
      newCrypto: => Future[Crypto],
  ): Unit = {
    forAll(supportedSigningKeySchemes) { signingKeyScheme =>
      javaConvertTest(
        signingKeyScheme.toString,
        newCrypto,
        crypto => getSigningPublicKey(crypto, signingKeyScheme),
      )

      s"Convert with $signingKeyScheme public signing keys" should {

        "Convert public signing key to Java and back" in {
          for {
            crypto <- newCrypto
            publicKey <- getSigningPublicKey(crypto, signingKeyScheme)
            (algoId, javaPublicKey) = crypto.javaKeyConverter
              .toJava(publicKey)
              .valueOrFail("convert to java")
            publicKey2 = crypto.javaKeyConverter
              .fromJavaSigningKey(javaPublicKey, algoId)
              .valueOrFail("convert from java")
          } yield publicKey.id shouldEqual publicKey2.id
        }

      }
    }

    forAll(supportedEncryptionKeySchemes) { encryptionKeyScheme =>
      javaConvertTest(
        encryptionKeyScheme.toString,
        newCrypto,
        crypto => getEncryptionPublicKey(crypto, encryptionKeyScheme),
      )
    }
  }
}
