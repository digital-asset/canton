// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing

import com.digitalasset.canton.crypto.KeyName
import com.digitalasset.canton.domain.sequencing.admin.protocol
import com.digitalasset.canton.domain.sequencing.admin.protocol.InitRequest
import com.digitalasset.canton.protocol.TestDomainParameters
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.topology.{DefaultTestIdentities, DomainId, TestingIdentityFactory}
import com.digitalasset.canton.tracing.Traced
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.BaseTest
import org.scalatest.FutureOutcome
import org.scalatest.wordspec.FixtureAsyncWordSpec

class SequencerKeyInitializationTest extends FixtureAsyncWordSpec with BaseTest {
  class Environment {
    val testingIdentityFactory = TestingIdentityFactory(loggerFactory)
    val crypto = testingIdentityFactory.newCrypto(DefaultTestIdentities.sequencer)
    val cryptoSpy = spy(crypto)
    // create wrapper to allow testing without traced wrapper
    def ensureKeyExists(request: protocol.InitRequest) =
      SequencerKeyInitialization.ensureKeyExists(cryptoSpy)(Traced(request))
  }

  override type FixtureParam = Environment

  override def withFixture(test: OneArgAsyncTest): FutureOutcome =
    withFixture(test.toNoArgAsyncTest(new Environment))

  "KeyInitialization" should {
    "only generate a sequencer key once for the same domain" in { env =>
      import env._

      val request = InitRequest(
        DefaultTestIdentities.domainId,
        StoredTopologyTransactions.empty,
        TestDomainParameters.defaultStatic,
      )
      for {
        // the sequencer initialization service ensures that calls are sequential so we'll do the same here
        responses <- valueOrFail(
          MonadUtil.sequentialTraverse(Seq.fill(10)(request))(ensureKeyExists)
        )("ensureKeyExists x10")
      } yield {
        // only a single key should be registered
        verify(cryptoSpy, times(1)).generateSigningKey(
          scheme = eqTo(crypto.privateCrypto.defaultSigningKeyScheme),
          name = eqTo(Some(KeyName.tryCreate("sequencer-signing-da::default"))),
        )(anyTraceContext)

        // all responses should return the same public key
        responses.map(_.publicKey).distinct should have size (1)
      }
    }

    "fail if the sequencer has already initialized another domain" in { env =>
      import env._

      val d1InitRequest =
        InitRequest(
          DomainId.tryFromString("da1::default"),
          StoredTopologyTransactions.empty,
          TestDomainParameters.defaultStatic,
        )
      val d2InitRequest =
        InitRequest(
          DomainId.tryFromString("da2::default"),
          StoredTopologyTransactions.empty,
          TestDomainParameters.defaultStatic,
        )

      for {
        _ <- valueOrFail(ensureKeyExists(d1InitRequest))("ensureKeyExists")
        error <- ensureKeyExists(d2InitRequest).value.map(_.left.value)
      } yield error should include("Sequencer has been previously initialized")
    }
  }
}
