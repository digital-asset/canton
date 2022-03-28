// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto._
import org.scalatest.wordspec.AsyncWordSpec

trait CryptoPrivateStoreTest extends BaseTest { this: AsyncWordSpec =>

  def cryptoPrivateStore(newStore: => CryptoPrivateStore): Unit = {

    val secret1: HmacSecret = TestHmacSecret.generate()
    val secret2: HmacSecret = TestHmacSecret.generate()

    val sigKey1: SigningPrivateKey = SymbolicCrypto.signingPrivateKey("sigKey1")
    val sigKey1WithName: SigningPrivateKeyWithName =
      SigningPrivateKeyWithName(sigKey1, Some(KeyName.tryCreate("sigKey1")))
    val sigKey2: SigningPrivateKey = SymbolicCrypto.signingPrivateKey("sigKey2")
    val sigKey2WithName: SigningPrivateKeyWithName = SigningPrivateKeyWithName(sigKey2, None)

    val encKey1: EncryptionPrivateKey = SymbolicCrypto.encryptionPrivateKey("encKey1")
    val encKey1WithName: EncryptionPrivateKeyWithName =
      EncryptionPrivateKeyWithName(encKey1, Some(KeyName.tryCreate("encKey1")))
    val encKey2: EncryptionPrivateKey = SymbolicCrypto.encryptionPrivateKey("encKey2")
    val encKey2WithName: EncryptionPrivateKeyWithName = EncryptionPrivateKeyWithName(encKey2, None)

    "store and retrieve HMAC secrets" in {
      val store = newStore
      for {
        _ <- store.storeHmacSecret(secret1).valueOrFail("store hmac secret")
        secret <- store.loadHmacSecret().valueOrFail("load hmac secret")
        result <- store.hmacSecret.valueOrFail("retrieve hmac secret")
      } yield {
        secret shouldBe Some(secret1)
        result shouldBe Some(secret1)
      }
    }

    "store encryption keys correctly when added incrementally" in {
      val store = newStore
      for {
        _ <- store.storeDecryptionKey(encKey1, encKey1WithName.name).valueOrFail("store key 1")
        _ <- store.storeDecryptionKey(encKey2, None).valueOrFail("store key 2")
        result <- store.listDecryptionKeys.valueOrFail("list keys")
      } yield {
        result shouldEqual Set(encKey1WithName, encKey2WithName)
      }
    }

    "store signing keys correctly when added incrementally" in {
      val store = newStore
      for {
        _ <- store.storeSigningKey(sigKey1, sigKey1WithName.name).valueOrFail("store key 1")
        _ <- store.storeSigningKey(sigKey2, None).valueOrFail("store key 2")
        result <- store.listSigningKeys.valueOrFail("list keys")
      } yield {
        result shouldEqual Set(sigKey1WithName, sigKey2WithName)
      }
    }

    "idempotent store of encryption keys" in {
      val store = newStore
      for {
        _ <- store
          .storeDecryptionKey(encKey1, encKey1WithName.name)
          .valueOrFail("store key 1 with name")

        // Should succeed
        _ <- store
          .storeDecryptionKey(encKey1, encKey1WithName.name)
          .valueOrFail("store key 1 with name again")

        // Should fail due to different name
        failedInsert <- store.storeDecryptionKey(encKey1, None).value

        result <- store.listDecryptionKeys
      } yield {
        failedInsert.left.value shouldBe a[CryptoPrivateStoreError]
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
        failedInsert.left.value shouldBe a[CryptoPrivateStoreError]
        result shouldEqual Set(sigKey1WithName)
      }
    }

    "delete key successfully" in {
      val store = newStore
      val res = for {
        _ <- store.storeSigningKey(sigKey1, None)
        _ = store
          .signingKey(sigKey1.id)
          .leftMap(_.toString)
          .subflatMap(_.toRight(s"Signing key was not stored"))
          .valueOrFail("signing key")
        _ <- store.removePrivateKey(sigKey1.id)
        result <- store.listSigningKeys
      } yield {
        result shouldBe Set.empty
      }

      res.valueOr(err => fail(err.toString))
    }

    "rotate the HMAC secret" in {
      val store = newStore
      for {
        _ <- store.storeHmacSecret(secret1).valueOrFail("store first hmac secret")
        _ <- store.storeHmacSecret(secret2).valueOrFail("store second hmac secret")
      } yield {
        succeed
      }
    }
  }

}
