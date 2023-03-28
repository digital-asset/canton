// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.CommunityCryptoConfig
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.store.CryptoPrivateStore.CommunityCryptoPrivateStoreFactory
import com.digitalasset.canton.resource.MemoryStorage
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait CryptoPublicStoreTest extends BaseTest { this: AsyncWordSpec =>

  def cryptoPublicStore(newStore: => CryptoPublicStore, backedByDatabase: Boolean): Unit = {

    val sigKey1: SigningPublicKey = SymbolicCrypto.signingPublicKey("sigKey1")
    val sigKey1WithName: SigningPublicKeyWithName =
      SigningPublicKeyWithName(sigKey1, Some(KeyName.tryCreate("sigKey1")))
    val sigKey2: SigningPublicKey = SymbolicCrypto.signingPublicKey("sigKey2")
    val sigKey2WithName: SigningPublicKeyWithName = SigningPublicKeyWithName(sigKey2, None)

    val encKey1: EncryptionPublicKey = SymbolicCrypto.encryptionPublicKey("encKey1")
    val encKey1WithName: EncryptionPublicKeyWithName =
      EncryptionPublicKeyWithName(encKey1, Some(KeyName.tryCreate("encKey1")))
    val encKey2: EncryptionPublicKey = SymbolicCrypto.encryptionPublicKey("encKey2")
    val encKey2WithName: EncryptionPublicKeyWithName = EncryptionPublicKeyWithName(encKey2, None)

    def newCrypto(): Future[Crypto] = {
      CryptoFactory
        .create(
          CommunityCryptoConfig(),
          new MemoryStorage(loggerFactory, timeouts),
          new CommunityCryptoPrivateStoreFactory,
          testedReleaseProtocolVersion,
          timeouts,
          loggerFactory,
        )
        .valueOrFail("create crypto")
    }

    def certificateGenerator(crypto: Crypto) =
      new X509CertificateGenerator(crypto, loggerFactory)

    "save encryption keys correctly when added incrementally" in {
      val store = newStore
      for {
        _ <- store.storeEncryptionKey(encKey1, encKey1WithName.name).valueOrFail("store encKey1")
        _ <- store.storeEncryptionKey(encKey2, None).valueOrFail("store encKey2")
        result <- store.encryptionKeys.valueOrFail("get encryption keys")
        result2 <- store.listEncryptionKeys.valueOrFail("list keys")
      } yield {
        result shouldEqual Set(encKey1, encKey2)
        result2 shouldEqual Set(encKey1WithName, encKey2WithName)
      }
    }

    if (backedByDatabase) {
      "not rely solely on cache" in {
        val store = newStore
        val separateStore = newStore
        for {
          _ <- store.storeEncryptionKey(encKey1, encKey1WithName.name).valueOrFail("store encKey1")
          _ <- store.storeEncryptionKey(encKey2, None).valueOrFail("store encKey2")
          result1 <- separateStore.encryptionKey(encKey1.fingerprint).valueOrFail("read encKey1")
          result2 <- separateStore.encryptionKey(encKey2.fingerprint).valueOrFail("read encKey2")

          _ <- store.storeSigningKey(sigKey1, sigKey1WithName.name).valueOrFail("store sigKey1")
          _ <- store.storeSigningKey(sigKey2, None).valueOrFail("store sigKey2")
          result3 <- separateStore.signingKey(sigKey1.fingerprint).valueOrFail("read sigKey1")
          result4 <- separateStore.signingKey(sigKey2.fingerprint).valueOrFail("read sigKey2")
        } yield {
          result1 shouldEqual Some(encKey1)
          result2 shouldEqual Some(encKey2)

          result3 shouldEqual Some(sigKey1)
          result4 shouldEqual Some(sigKey2)
        }
      }
    }

    "save signing keys correctly when added incrementally" in {
      val store = newStore
      for {
        _ <- store.storeSigningKey(sigKey1, sigKey1WithName.name).valueOrFail("store sigKey1")
        _ <- store.storeSigningKey(sigKey2, None).valueOrFail("store sigKey2")
        result <- store.signingKeys.valueOrFail("list keys")
        result2 <- store.listSigningKeys.valueOrFail("list keys")
      } yield {
        result shouldEqual Set(sigKey1, sigKey2)
        result2 shouldEqual Set(sigKey1WithName, sigKey2WithName)
      }
    }

    "idempotent store of encryption keys" in {
      val store = newStore
      for {
        _ <- store
          .storeEncryptionKey(encKey1, encKey1WithName.name)
          .valueOrFail("store key 1 with name")

        // Should succeed
        _ <- store
          .storeEncryptionKey(encKey1, encKey1WithName.name)
          .valueOrFail("store key 1 with name again")

        // Should fail due to different name
        failedInsert <- store.storeEncryptionKey(encKey1, None).value

        result <- store.listEncryptionKeys
      } yield {
        failedInsert.left.value shouldBe a[CryptoPublicStoreError]
        result shouldEqual Set(encKey1WithName)
      }
    }

    "idempotent store of signing keys" in {
      val store = newStore
      for {
        _ <- store
          .storeSigningKey(sigKey1, sigKey1WithName.name)
          .valueOrFail("store key 1 with name")

        // Should succeed
        _ <- store
          .storeSigningKey(sigKey1, sigKey1WithName.name)
          .valueOrFail("store key 1 with name again")

        // Should fail due to different name
        failedInsert <- store.storeSigningKey(sigKey1, None).value

        result <- store.listSigningKeys
      } yield {
        failedInsert.left.value shouldBe a[CryptoPublicStoreError]
        result shouldEqual Set(sigKey1WithName)
      }
    }

    "generate and store X509 certificate" in {
      val store = newStore
      for {
        // Need a tink crypto module to generate certificates
        crypto <- newCrypto()
        certSigningKey <- crypto
          .generateSigningKey(SigningKeyScheme.EcDsaP384)
          .valueOrFail("generate signing key")
        cert <- certificateGenerator(crypto)
          .generate("test", certSigningKey.id)
          .valueOrFail("generate certificate")
        res <- store.storeCertificate(cert).valueOrFail("store certificate")
      } yield res shouldBe (())
    }

    "list stored certificates" in {
      val store = newStore

      for {
        crypto <- newCrypto()
        generator = certificateGenerator(crypto)
        certSigningKey <- crypto
          .generateSigningKey(SigningKeyScheme.EcDsaP384)
          .valueOrFail("create signing key 1")
        certSigningKey2 <- crypto
          .generateSigningKey(SigningKeyScheme.EcDsaP384)
          .valueOrFail("create signing key 2")
        cert1 <- generator.generate("test", certSigningKey.id).valueOrFail("generate cert 1")
        cert2 <- generator.generate("test2", certSigningKey2.id).valueOrFail("generate cert 2")
        _ <- store.storeCertificate(cert1).valueOrFail("store cert 1")
        _ <- store.storeCertificate(cert2).valueOrFail("store cert 2")
        certs <- store.listCertificates().valueOrFail("list certificates")
      } yield certs should have size (2)
    }
  }
}
