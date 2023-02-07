// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.CommunityCryptoConfig
import com.digitalasset.canton.crypto.store.CryptoPrivateStore.CommunityCryptoPrivateStoreFactory
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.topology.DefaultTestIdentities
import org.scalatest.FutureOutcome
import org.scalatest.wordspec.FixtureAsyncWordSpec

import java.security.cert.{CertificateFactory, X509Certificate as JX509Certificate}
import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
class X509CertificateTest extends FixtureAsyncWordSpec with BaseTest {

  type FixtureParam = Future[Fixture]
  case class Fixture(crypto: Crypto, keyId: Fingerprint)

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val fixtureE = for {
      crypto <- CryptoFactory.create(
        CommunityCryptoConfig(),
        new MemoryStorage(loggerFactory),
        new CommunityCryptoPrivateStoreFactory,
        testedReleaseProtocolVersion,
        timeouts,
        loggerFactory,
      )
      pubkey <- crypto
        .generateSigningKey(SigningKeyScheme.EcDsaP384, Some(KeyName.tryCreate("test")))
        .leftMap(err => s"Failed to generate keypair: $err")
    } yield Fixture(crypto, pubkey.fingerprint)

    val fixtureF = fixtureE.valueOr(err => fail(s"Failed to create fixture: $err"))

    withFixture(test.toNoArgAsyncTest(fixtureF))
  }

  "X509Certificate" should {
    def certificateGenerator(crypto: Crypto) =
      new X509CertificateGenerator(crypto, loggerFactory)

    "generate a new self-signed certificate using Tink" in { fixtureF =>
      for {
        fixture <- fixtureF
        memberId = DefaultTestIdentities.participant1.toProtoPrimitive
        cert <- certificateGenerator(fixture.crypto)
          .generate(memberId, fixture.keyId)
          .valueOrFail("generate certificate")
      } yield cert.subjectCommonName shouldBe Right(memberId)
    }

    "serialize a generated cert into PEM format" in { fixtureF =>
      for {
        fixture <- fixtureF
        genCert <- certificateGenerator(fixture.crypto)
          .generate("test", fixture.keyId)
          .valueOrFail("generate certificate")
      } yield {
        val pem = genCert.tryToPem.unwrap
        // Try to read and parse the PEM again
        val cf = CertificateFactory.getInstance("X.509")
        val jcert = cf.generateCertificate(pem.newInput()).asInstanceOf[JX509Certificate]
        val cert = X509Certificate(jcert)
        cert.subjectCommonName shouldBe Right("test")
      }
    }

    "extract public key from certificate" in { fixtureF =>
      for {
        fixture <- fixtureF
        memberId = DefaultTestIdentities.participant1.toProtoPrimitive
        cert <- certificateGenerator(fixture.crypto)
          .generate(memberId, fixture.keyId)
          .valueOrFail("generate certificate")
      } yield {
        val pubKey = for {
          pubKey <- cert.publicKey(fixture.crypto.javaKeyConverter)
        } yield pubKey
        pubKey.value.fingerprint shouldBe fixture.keyId
      }
    }

    "extract subject alternative names from certificate" in { fixtureF =>
      for {
        fixture <- fixtureF
        memberId = DefaultTestIdentities.participant1.toProtoPrimitive
        sansIn = Seq("foo", "bar")
        cert <- certificateGenerator(fixture.crypto)
          .generate(memberId, fixture.keyId, sansIn)
          .valueOrFail("generate certificate")
      } yield {
        val sansOut = for {
          sans <- cert.subjectAlternativeNames
        } yield sans
        sansOut.value shouldBe sansIn
      }
    }
  }
}
