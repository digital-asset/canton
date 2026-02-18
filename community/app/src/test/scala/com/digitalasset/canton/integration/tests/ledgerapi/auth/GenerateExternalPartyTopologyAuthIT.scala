// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.party_management_service.*
import com.daml.ledger.api.v2.crypto as lapicrypto
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.google.protobuf.ByteString

import java.security.KeyPairGenerator
import java.util.UUID
import scala.concurrent.Future

final class GenerateExternalPartyTopologyAuthIT extends PublicServiceCallAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  private val keyGen = KeyPairGenerator.getInstance("Ed25519")
  private val keyPair = keyGen.generateKeyPair()
  private val pb = keyPair.getPublic

  override def serviceCallName: String = "PartyManagementService#GenerateExternalPartyTopology"

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] =
    stub(PartyManagementServiceGrpc.stub(channel), context.token)
      .generateExternalPartyTopology(
        GenerateExternalPartyTopologyRequest(
          synchronizer = env.synchronizer1Id.logical.toProtoPrimitive,
          partyHint = s"party-${UUID.randomUUID().toString}",
          publicKey = Some(
            lapicrypto.SigningPublicKey(
              format =
                lapicrypto.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO,
              keyData = ByteString.copyFrom(pb.getEncoded),
              keySpec = lapicrypto.SigningKeySpec.SIGNING_KEY_SPEC_EC_CURVE25519,
            )
          ),
          localParticipantObservationOnly = false,
          otherConfirmingParticipantUids = Seq(),
          confirmationThreshold = 1,
          observingParticipantUids = Seq(),
        )
      )

}
