// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.KeyPurpose.{Encryption, Signing}
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.store.db.StoredPrivateKey
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import org.scalatest.wordspec.AsyncWordSpec

trait CryptoPrivateStoreTest extends BaseTest { this: AsyncWordSpec =>

  def toPrivateKeyWithName(
      storedKeys: Set[StoredPrivateKey]
  ): Set[ParsingResult[PrivateKeyWithName]] = {
    storedKeys.map(x => {
      x.purpose match {
        case KeyPurpose.Signing =>
          SigningPrivateKeyWithName.fromStored(x)
        case KeyPurpose.Encryption =>
          EncryptionPrivateKeyWithName.fromStored(x)
      }
    })
  }

  def cryptoPrivateStore(newStore: => CryptoPrivateStore): Unit = {

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

    "store encryption keys correctly when added incrementally" in {
      val store = newStore
      for {
        _ <- store
          .storeDecryptionKey(encKey1, encKey1WithName.name)
          .valueOrFail("store key 1")
        _ <- store.storeDecryptionKey(encKey2, None).valueOrFail("store key 2")
        result <- store.listPrivateKeys(Encryption).valueOrFail("list keys")
      } yield {
        toPrivateKeyWithName(result) shouldEqual Set(
          Right(encKey1WithName),
          Right(encKey2WithName),
        )
      }
    }

    "store signing keys correctly when added incrementally" in {
      val store = newStore
      for {
        _ <- store.storeSigningKey(sigKey1, sigKey1WithName.name).valueOrFail("store key 1")
        _ <- store.storeSigningKey(sigKey2, None).valueOrFail("store key 2")
        result <- store.listPrivateKeys(Signing).valueOrFail("list keys")
      } yield {
        toPrivateKeyWithName(result) shouldEqual Set(
          Right(sigKey1WithName),
          Right(sigKey2WithName),
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

        result <- store.listPrivateKeys(Encryption)
      } yield {
        failedInsert.left.value shouldBe a[CryptoPrivateStoreError]
        toPrivateKeyWithName(result) shouldEqual Set(Right(encKey1WithName))
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

        result <- store.listPrivateKeys(Signing)
      } yield {
        failedInsert.left.value shouldBe a[CryptoPrivateStoreError]
        toPrivateKeyWithName(result) shouldEqual Set(Right(sigKey1WithName))
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
        result <- store.listPrivateKeys(Signing)
      } yield {
        result shouldBe Set.empty
      }

      res.valueOr(err => fail(err.toString))
    }

  }

}
