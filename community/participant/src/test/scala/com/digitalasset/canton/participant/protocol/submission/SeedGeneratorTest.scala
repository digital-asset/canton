// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.{DefaultTestIdentities, TestingIdentityFactory}
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator.SeedForTransaction
import com.digitalasset.canton.{BaseTest, DefaultDamlValues, DomainId}
import com.digitalasset.platform.daml.lf.testing.SampleParties.{Alice, Bob}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID

class SeedGeneratorTest extends AsyncWordSpec with BaseTest {
  private val domain = DomainId(DefaultTestIdentities.uid)

  "SeedGenerator" should {
    "generate hashing salt based on command Id and other properties of the transaction" in {
      val crypto =
        TestingIdentityFactory(loggerFactory).newCrypto(DefaultTestIdentities.mediator)
      val seedGenerator = new SeedGenerator(crypto.privateCrypto, crypto.pureCrypto)
      val uuid = new UUID(0L, 1L)
      val seedData =
        SeedForTransaction(
          DefaultDamlValues.changeId(Set(Alice, Bob)),
          domain,
          CantonTimestamp.Epoch,
          uuid,
        ).toDeterministicByteString
      for {
        seed <- seedGenerator
          .generateSeedForTransaction(
            DefaultDamlValues.changeId(Set(Alice, Bob)),
            domain,
            CantonTimestamp.Epoch,
            uuid,
          )
          .valueOrFail("generate seed")
        hmac <- crypto.privateCrypto.hmac(seedData).valueOrFail("compute hmac")
      } yield seed.unwrap shouldEqual hmac.unwrap
    }
  }
}
