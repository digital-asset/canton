// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.Password
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait JavaKeyConverterTest extends BaseTest { this: AsyncWordSpec =>

  // Weak test as we don't use the converted Java keys here, we just ensure the conversion does not blow up.
  private def javaConvertTest(
      name: String,
      newCrypto: => Future[Crypto],
      newPublicKey: Crypto => Future[PublicKey],
  ): Unit = {

    s"Convert $name keys to Java keys" should {

      "Convert public key to Java" in {
        for {
          crypto <- newCrypto
          publicKey <- newPublicKey(crypto)
          (algoId, javaPublicKey) = crypto.javaKeyConverter
            .toJava(publicKey)
            .valueOrFail("convert to java")
        } yield assert(true)
      }

      "Convert private key to Java" in {
        for {
          crypto <- newCrypto
          publicKey <- newPublicKey(crypto)
          privateKey <- valueOrFail(
            crypto.cryptoPrivateStore
              .exportPrivateKey(publicKey.id)
              .leftMap(_.toString)
              .subflatMap(_.toRight("No private key found"))
          )("private key")
          javaPrivateKey = crypto.javaKeyConverter.toJava(privateKey).valueOrFail("convert to java")
        } yield assert(true)
      }
    }
  }

  def javaKeyConverterProvider(
      supportedSigningKeySchemes: Set[SigningKeyScheme],
      supportedEncryptionKeySchemes: Set[EncryptionKeyScheme],
      newCrypto: => Future[Crypto],
  ): Unit = {
    def certificateGenerator(crypto: Crypto) =
      new X509CertificateGenerator(crypto, loggerFactory)

    forAll(supportedSigningKeySchemes) { signingKeyScheme =>
      // Generates and stores the keypair in the crypto stores
      def newSigningPublicKey(crypto: Crypto): Future[SigningPublicKey] =
        crypto.generateSigningKey(scheme = signingKeyScheme).valueOrFail("generate signing key")

      javaConvertTest(signingKeyScheme.toString, newCrypto, newSigningPublicKey)

      s"Convert with $signingKeyScheme signing keys" should {

        "Convert public signing key to Java and back" in {
          for {
            crypto <- newCrypto
            publicKey <- newSigningPublicKey(crypto)
            (algoId, javaPublicKey) = crypto.javaKeyConverter
              .toJava(publicKey)
              .valueOrFail("convert to java")
            publicKey2 = crypto.javaKeyConverter
              .fromJavaSigningKey(javaPublicKey, algoId)
              .valueOrFail("convert from java")
          } yield publicKey.id shouldEqual publicKey2.id
        }

        "Convert signing keys of certificates to Java key store" in {
          for {
            crypto <- newCrypto
            keyStorePass = Password("foobar")
            generator = certificateGenerator(crypto)
            publicKey1 <- newSigningPublicKey(crypto)
            privateKey1 <- crypto.cryptoPrivateStore
              .signingKey(publicKey1.id)
              .valueOrFail("failed to get private key")
              .map(_.valueOrFail("private key not found"))
            javaPrivateKey1 = crypto.javaKeyConverter
              .toJava(privateKey1)
              .valueOrFail("failed to convert to java private key")
            publicKey2 <- newSigningPublicKey(crypto)
            cert1 <- generator.generate("foo", publicKey1.id).valueOrFail("generate cert")
            _ <- crypto.cryptoPublicStore.storeCertificate(cert1).valueOrFail("store cert")
            cert2 <- generator.generate("bar", publicKey2.id).valueOrFail("generate cert")
            _ <- crypto.cryptoPublicStore.storeCertificate(cert2).valueOrFail("store cert")
            keyStore <- crypto.javaKeyConverter
              .toJava(crypto.cryptoPrivateStore, crypto.cryptoPublicStore, keyStorePass)
              .valueOrFail("convert to java")
          } yield {
            val cert1Id = cert1.id.unwrap
            keyStore should have size 2 // 2 aliases, each with a certificate and private key
            keyStore.getCertificate(cert1Id) shouldEqual cert1.unwrap
            keyStore.getKey(cert1Id, keyStorePass.toCharArray) shouldEqual javaPrivateKey1
          }
        }

      }
    }

    forAll(supportedEncryptionKeySchemes) { encryptionKeyScheme =>
      def newEncryptionPublicKey(crypto: Crypto): Future[EncryptionPublicKey] =
        crypto
          .generateEncryptionKey(scheme = encryptionKeyScheme)
          .valueOrFail("generate encryption key")

      javaConvertTest(
        encryptionKeyScheme.toString,
        newCrypto,
        newEncryptionPublicKey,
      )
    }
  }
}
