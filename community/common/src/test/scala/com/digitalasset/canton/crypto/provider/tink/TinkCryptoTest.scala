// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.tink

import com.digitalasset.canton.config.CommunityCryptoConfig
import com.digitalasset.canton.config.CryptoProvider.Tink
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.CryptoPrivateStore.CommunityCryptoPrivateStoreFactory
import com.digitalasset.canton.resource.MemoryStorage
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class TinkCryptoTest
    extends AsyncWordSpec
    with SigningTest
    with EncryptionTest
    with HkdfTest
    with RandomTest
    with JavaKeyConverterTest {

  "TinkCrypto" can {

    def tinkCrypto(): Future[Crypto] = {
      CryptoFactory
        .create(
          CommunityCryptoConfig(provider = Tink),
          new MemoryStorage,
          new CommunityCryptoPrivateStoreFactory,
          testedReleaseProtocolVersion,
          timeouts,
          loggerFactory,
        )
        .valueOrFail("create crypto")
    }

    behave like signingProvider(Tink.signing.supported, tinkCrypto())
    behave like encryptionProvider(
      Tink.encryption.supported,
      Tink.symmetric.supported,
      tinkCrypto(),
    )
    behave like hkdfProvider(tinkCrypto().map(_.pureCrypto))
    behave like randomnessProvider(tinkCrypto().map(_.pureCrypto))

    // Tink provider does not support Java conversion of Ed25519 or Hybrid encryption keys
    behave like javaKeyConverterProvider(
      Tink.signing.supported.filter(_ != SigningKeyScheme.Ed25519),
      Set.empty,
      tinkCrypto(),
    )
  }

}
