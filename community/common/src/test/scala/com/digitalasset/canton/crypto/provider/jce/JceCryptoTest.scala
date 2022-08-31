// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.jce

import com.digitalasset.canton.config.CryptoProvider.Jce
import com.digitalasset.canton.config.{CommunityCryptoConfig, CryptoProvider}
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.crypto.store.CryptoPrivateStore.CommunityCryptoPrivateStoreFactory
import com.digitalasset.canton.resource.MemoryStorage
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
          new MemoryStorage,
          new CommunityCryptoPrivateStoreFactory,
          timeouts,
          loggerFactory,
        )
        .valueOr(err => throw new RuntimeException(s"failed to create crypto: $err"))
    }

    behave like signingProvider(Jce.signing.supported, jceCrypto())
    behave like encryptionProvider(Jce.encryption.supported, Jce.symmetric.supported, jceCrypto())
    behave like hkdfProvider(jceCrypto().map(_.pureCrypto))
    behave like randomnessProvider(jceCrypto().map(_.pureCrypto))
    behave like javaKeyConverterProvider(
      Jce.signing.supported,
      Jce.encryption.supported,
      jceCrypto(),
    )
  }

}
