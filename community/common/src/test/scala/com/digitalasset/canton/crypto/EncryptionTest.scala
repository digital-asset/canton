// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.DecryptionError.FailedToDecrypt
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.version.{HasVersionedToByteString, ProtocolVersion}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait EncryptionTest extends BaseTest { this: AsyncWordSpec =>

  private case class Message(bytes: ByteString) extends HasVersionedToByteString {
    override def toByteString(version: ProtocolVersion): ByteString = bytes
  }

  private object Message {
    def fromByteString(bytes: ByteString): Either[DeserializationError, Message] = Right(
      Message(bytes)
    )
  }

  def encryptionProvider(
      supportedEncryptionKeySchemes: Set[EncryptionKeyScheme],
      supportedSymmetricKeySchemes: Set[SymmetricKeyScheme],
      newCrypto: => Future[Crypto],
  ): Unit = {

    forAll(supportedSymmetricKeySchemes) { symmetricKeyScheme =>
      s"Symmetric encrypt with $symmetricKeyScheme" should {

        def newSymmetricKey(crypto: Crypto): SymmetricKey =
          crypto.pureCrypto
            .generateSymmetricKey(scheme = symmetricKeyScheme)
            .valueOrFail("generate symmetric key")

        def newSecureRandomKey(crypto: Crypto): SymmetricKey = {
          val randomness =
            crypto.pureCrypto.generateSecureRandomness(symmetricKeyScheme.keySizeInBytes)
          crypto.pureCrypto
            .createSymmetricKey(randomness, symmetricKeyScheme)
            .valueOrFail("create key from randomness")
        }

        "serialize and deserialize symmetric encryption key via protobuf" in {
          for {
            crypto <- newCrypto
            key = newSymmetricKey(crypto)
            keyBytes = key.getCryptographicEvidence
            key2 = SymmetricKey.fromByteString(keyBytes).valueOrFail("serialize key")
          } yield key shouldEqual key2
        }

        "encrypt and decrypt with a symmetric key" in {
          for {
            crypto <- newCrypto
            message = Message(ByteString.copyFromUtf8("foobar"))
            key = newSymmetricKey(crypto)
            encrypted = crypto.pureCrypto
              .encryptWith(message, key, ProtocolVersion.latestForTest)
              .valueOrFail("encrypt")
            message2 = crypto.pureCrypto
              .decryptWith(encrypted, key)(Message.fromByteString)
              .valueOrFail("decrypt")
          } yield {
            message.bytes !== encrypted.ciphertext
            message shouldEqual message2
          }
        }

        "fail decrypt with a different symmetric key" in {
          for {
            crypto <- newCrypto
            message = Message(ByteString.copyFromUtf8("foobar"))
            key = newSymmetricKey(crypto)
            key2 = newSymmetricKey(crypto)
            encrypted = crypto.pureCrypto
              .encryptWith(message, key, ProtocolVersion.latestForTest)
              .valueOrFail("encrypt")
            message2 = crypto.pureCrypto.decryptWith(encrypted, key2)(Message.fromByteString)
          } yield message2.left.value shouldBe a[FailedToDecrypt]
        }

        "encrypt and decrypt with secure randomness" in {
          for {
            crypto <- newCrypto
            message = Message(ByteString.copyFromUtf8("foobar"))
            key = newSecureRandomKey(crypto)
            encrypted = crypto.pureCrypto
              .encryptWith(message, key, ProtocolVersion.latestForTest)
              .valueOrFail("encrypt")
            message2 = crypto.pureCrypto
              .decryptWith(encrypted, key)(Message.fromByteString)
              .valueOrFail("decrypt")
          } yield {
            message.bytes !== encrypted.ciphertext
            message shouldEqual message2
          }
        }

        "fail decrypt with a different secure randomness" in {
          for {
            crypto <- newCrypto
            message = Message(ByteString.copyFromUtf8("foobar"))
            key = newSecureRandomKey(crypto)
            key2 = newSecureRandomKey(crypto)
            encrypted = crypto.pureCrypto
              .encryptWith(message, key, ProtocolVersion.latestForTest)
              .valueOrFail("encrypt")
            message2 = crypto.pureCrypto.decryptWith(encrypted, key2)(Message.fromByteString)
          } yield message2.left.value shouldBe a[FailedToDecrypt]
        }

      }
    }

    forAll(supportedEncryptionKeySchemes) { encryptionKeyScheme =>
      s"Hybrid encrypt with $encryptionKeyScheme" should {

        def newPublicKey(crypto: Crypto): Future[EncryptionPublicKey] =
          crypto.privateCrypto
            .generateEncryptionKey(scheme = encryptionKeyScheme)
            .valueOrFail("generate enc key")

        "serialize and deserialize encryption public key via protobuf" in {
          for {
            crypto <- newCrypto
            key <- newPublicKey(crypto)
            keyP = key.toProtoVersioned(ProtocolVersion.latestForTest)
            key2 = EncryptionPublicKey.fromProtoVersioned(keyP).valueOrFail("serialize key")
          } yield key shouldEqual key2
        }

        "serialize and deserialize encryption private key via protobuf" in {
          for {
            crypto <- newCrypto
            publicKey <- newPublicKey(crypto)
            privateKey <- crypto.cryptoPrivateStore
              .decryptionKey(publicKey.id)
              .leftMap(_.toString)
              .subflatMap(_.toRight("Private key not found"))
              .valueOrFail("get key")
            keyP = privateKey.toProtoVersioned(ProtocolVersion.latestForTest)
            key2 = EncryptionPrivateKey.fromProtoVersioned(keyP).valueOrFail("serialize key")
          } yield privateKey shouldEqual key2
        }

        "encrypt and decrypt with an encryption keypair" in {
          val message = Message(ByteString.copyFromUtf8("foobar"))
          for {
            crypto <- newCrypto
            publicKey <- newPublicKey(crypto)
            encrypted = crypto.pureCrypto
              .encryptWith(message, publicKey, ProtocolVersion.latestForTest)
              .valueOrFail("encrypt")
            message2 <- crypto.privateCrypto
              .decrypt(encrypted)(Message.fromByteString)
              .valueOrFail("decrypt")
          } yield message shouldEqual message2
        }

        "fail decrypt with a different encryption private key" in {
          val message = Message(ByteString.copyFromUtf8("foobar"))
          val res = for {
            crypto <- newCrypto
            publicKey <- newPublicKey(crypto)
            publicKey2 <- newPublicKey(crypto)
            encrypted = crypto.pureCrypto
              .encryptWith(message, publicKey, ProtocolVersion.latestForTest)
              .valueOrFail("encrypt")
            _ = assert(message.bytes != encrypted.ciphertext)
            encrypted2 = AsymmetricEncrypted(
              encrypted.ciphertext,
              publicKey2.id,
            )
            message2 <- crypto.privateCrypto
              .decrypt(encrypted2)(Message.fromByteString)
              .value
          } yield message2

          res.map(res => res.left.value shouldBe a[FailedToDecrypt])
        }
      }
    }
  }
}
