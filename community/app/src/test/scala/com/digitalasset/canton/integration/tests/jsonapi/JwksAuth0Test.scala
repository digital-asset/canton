// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.auth0.jwk.UrlJwkProvider
import com.digitalasset.canton.crypto.{SignatureFormat, SigningAlgorithmSpec}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.google.protobuf.ByteString

import java.net.URI
import java.security.KeyPairGenerator
import scala.jdk.CollectionConverters.*

class JwksAuth0Test extends CommunityIntegrationTest with SharedEnvironment {
  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, daName)
      }

  private lazy val keyPair = {
    val keyGen = KeyPairGenerator.getInstance("EC")
    keyGen.initialize(256)
    keyGen.generateKeyPair()
  }

  private lazy val signingPublicKey = com.digitalasset.canton.crypto.SigningPublicKey
    .create(
      format = com.digitalasset.canton.crypto.CryptoKeyFormat.DerX509Spki,
      key = ByteString.copyFrom(keyPair.getPublic.getEncoded),
      keySpec = com.digitalasset.canton.crypto.SigningKeySpec.EcP256,
      usage = com.digitalasset.canton.crypto.SigningKeyUsage.All,
    )
    .valueOrFail("failed to generate pubkey")

  private def generateSignature(bytes: ByteString) = {
    val signing = java.security.Signature.getInstance("SHA256withECDSA")
    signing.initSign(keyPair.getPrivate)
    signing.update(bytes.toByteArray)
    com.digitalasset.canton.crypto.Signature.create(
      format = SignatureFormat.Der,
      signature = ByteString.copyFrom(signing.sign()),
      signedBy = signingPublicKey.fingerprint,
      signingAlgorithmSpec = Some(SigningAlgorithmSpec.EcDsaSha256),
      signatureDelegation = None,
    )
  }

  "retrieve a JWKS using the auth0 library" in { implicit env =>
    import env.*

    val port =
      participant1.config.httpLedgerApi.internalPort.valueOrFail("JSON API must be enabled")

    val txs = participant1.ledger_api.parties.generate_topology(
      sequencer1.synchronizer_id,
      "Alice",
      signingPublicKey,
    )

    participant1.ledger_api.parties.allocate_external(
      sequencer1.synchronizer_id,
      txs.topologyTransactions.map((_, Seq.empty[com.digitalasset.canton.crypto.Signature])),
      multiSignatures = Seq(generateSignature(txs.multiHash.getCryptographicEvidence)),
    )

    eventually() {
      val party = participant1.parties.list("Alice").loneElement.party.toProtoPrimitive
      val synchronizer = sequencer1.synchronizer_id.toProtoPrimitive
      val jwksUrl = new URI(
        s"http://localhost:$port/v2/jose/jwks/synchronizer/$synchronizer/party/$party"
      ).toURL()
      val provider = new UrlJwkProvider(jwksUrl)
      provider.getAll().asScala.map(_.getPublicKey()) shouldBe List(keyPair.getPublic)
    }
  }

}
