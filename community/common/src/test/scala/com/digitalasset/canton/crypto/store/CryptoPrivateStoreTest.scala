// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.KeyPurpose.{Encryption, Signing}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.wordspec.AsyncWordSpec

trait CryptoPrivateStoreTest extends BaseTest { this: AsyncWordSpec =>

  def protocolVersion: ProtocolVersion = ProtocolVersion.latest

  def uniqueKeyName(name: String): String =
    name + getClass.getSimpleName

  // the tag is used to distinguish keys created for the clear and encrypted crypto private store tests
  def cryptoPrivateStore(newStore: => CryptoPrivateStore, encrypted: Boolean): Unit = {

    val sigKey1Name: String = uniqueKeyName("sigKey1_")
    val sigKey2Name: String = uniqueKeyName("sigKey2_")

    val encKey1Name: String = uniqueKeyName("encKey1_")
    val encKey2Name: String = uniqueKeyName("encKey2_")

    val sigKey1: SigningPrivateKey = SymbolicCrypto.signingPrivateKey(sigKey1Name)
    val sigKey1WithName: SigningPrivateKeyWithName =
      SigningPrivateKeyWithName(sigKey1, Some(KeyName.tryCreate(sigKey1Name)))
    val sigKey1BytesWithName = (sigKey1.toByteString(protocolVersion), sigKey1WithName.name)

    val sigKey2: SigningPrivateKey = SymbolicCrypto.signingPrivateKey(sigKey2Name)
    val sigKey2WithName: SigningPrivateKeyWithName = SigningPrivateKeyWithName(sigKey2, None)
    val sigKey2BytesWithName = (sigKey2.toByteString(protocolVersion), sigKey2WithName.name)

    val encKey1: EncryptionPrivateKey = SymbolicCrypto.encryptionPrivateKey(encKey1Name)
    val encKey1WithName: EncryptionPrivateKeyWithName =
      EncryptionPrivateKeyWithName(encKey1, Some(KeyName.tryCreate(encKey1Name)))
    val encKey1BytesWithName = (encKey1.toByteString(protocolVersion), encKey1WithName.name)

    val encKey2: EncryptionPrivateKey = SymbolicCrypto.encryptionPrivateKey(encKey2Name)
    val encKey2WithName: EncryptionPrivateKeyWithName = EncryptionPrivateKeyWithName(encKey2, None)
    val encKey2BytesWithName = (encKey2.toByteString(protocolVersion), encKey2WithName.name)

    "store encryption keys correctly when added incrementally" in {
      val store = newStore
      for {
        _ <- store
          .storeDecryptionKey(encKey1, encKey1WithName.name)
          .valueOrFail("store key 1")
        _ <- store.storeDecryptionKey(encKey2, None).valueOrFail("store key 2")
        result <- store.listPrivateKeys(Encryption, encrypted).valueOrFail("list keys")
      } yield {
        result.map(storedKey => (storedKey.data, storedKey.name)) shouldEqual Set(
          encKey1BytesWithName,
          encKey2BytesWithName,
        )
      }
    }

    "store signing keys correctly when added incrementally" in {
      val store = newStore
      for {
        _ <- store.storeSigningKey(sigKey1, sigKey1WithName.name).valueOrFail("store key 1")
        _ <- store.storeSigningKey(sigKey2, None).valueOrFail("store key 2")
        result <- store.listPrivateKeys(Signing, encrypted).valueOrFail("list keys")
      } yield {
        result.map(storedKey => (storedKey.data, storedKey.name)) shouldEqual Set(
          sigKey1BytesWithName,
          sigKey2BytesWithName,
        )
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

        result <- store.listPrivateKeys(Encryption, encrypted)
      } yield {
        failedInsert.left.value shouldBe a[CryptoPrivateStoreError]
        result.map(storedKey => (storedKey.data, storedKey.name)) shouldEqual Set(
          encKey1BytesWithName
        )
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

        result <- store.listPrivateKeys(Signing, encrypted)
      } yield {
        failedInsert.left.value shouldBe a[CryptoPrivateStoreError]
        result.map(storedKey => (storedKey.data, storedKey.name)) shouldEqual Set(
          sigKey1BytesWithName
        )
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
        result <- store.listPrivateKeys(Signing, encrypted)
      } yield {
        result shouldBe Set.empty
      }

      res.valueOr(err => fail(err.toString))
    }

  }

}
