// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.jce

import com.digitalasset.canton.config.CryptoProvider.Jce
import com.digitalasset.canton.config.{CommunityCryptoConfig, CryptoProvider}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.CryptoPrivateStore.CommunityCryptoPrivateStoreFactory
import com.digitalasset.canton.resource.MemoryStorage
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class JceCryptoTest
    extends AsyncWordSpec
    with SigningTest
    with EncryptionTest
    with HkdfTest
    with RandomTest
    with JavaKeyConverterTest {

  "JceCrypto" can {

    def jceCrypto(): Future[Crypto] = {
      CryptoFactory
        .create(
          CommunityCryptoConfig(provider = CryptoProvider.Jce),
          new MemoryStorage(loggerFactory, timeouts),
          new CommunityCryptoPrivateStoreFactory,
          testedReleaseProtocolVersion,
          timeouts,
          loggerFactory,
        )
        .valueOr(err => throw new RuntimeException(s"failed to create crypto: $err"))
    }

    behave like signingProvider(Jce.signing.supported, jceCrypto())
    behave like encryptionProvider(
      Jce.encryption.supported,
      Jce.symmetric.supported,
      jceCrypto(),
    )

    // Deterministic hybrid encryption is only enabled for EciesP256HmacSha256Aes128Cbc
    s"Deterministic hybrid encrypt with ${EncryptionKeyScheme.EciesP256HmacSha256Aes128Cbc}" should {

      val newCrypto = jceCrypto()

      behave like hybridEncrypt(
        EncryptionKeyScheme.EciesP256HmacSha256Aes128Cbc,
        (message, publicKey, version) =>
          newCrypto.map(crypto =>
            crypto.pureCrypto.encryptDeterministicWith(message, publicKey, version)
          ),
        newCrypto,
      )

      "yield the same ciphertext for the same encryption" in {
        val message = Message(ByteString.copyFromUtf8("foobar"))
        for {
          crypto <- jceCrypto()
          publicKey <- newPublicKey(crypto, EncryptionKeyScheme.EciesP256HmacSha256Aes128Cbc)
          encrypted1 = crypto.pureCrypto
            .encryptDeterministicWith(message, publicKey, testedProtocolVersion)
            .valueOrFail("encrypt")
          _ = assert(message.bytes != encrypted1.ciphertext)
          encrypted2 = crypto.pureCrypto
            .encryptDeterministicWith(message, publicKey, testedProtocolVersion)
            .valueOrFail("encrypt")
          _ = assert(message.bytes != encrypted2.ciphertext)
        } yield encrypted1.ciphertext shouldEqual encrypted2.ciphertext
      }
    }

    behave like hkdfProvider(jceCrypto().map(_.pureCrypto))
    behave like randomnessProvider(jceCrypto().map(_.pureCrypto))
    behave like javaKeyConverterProvider(
      Jce.signing.supported,
      Jce.encryption.supported,
      jceCrypto(),
    )
  }

}
