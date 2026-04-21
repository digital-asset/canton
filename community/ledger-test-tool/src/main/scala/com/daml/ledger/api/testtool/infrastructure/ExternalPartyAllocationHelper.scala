// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v2.admin.party_management_service.{
  AllocateExternalPartyRequest,
  GenerateExternalPartyTopologyRequest,
  GenerateExternalPartyTopologyResponse,
}
import com.daml.ledger.api.v2.crypto as lapicrypto
import com.google.protobuf.ByteString

import java.security.{KeyPairGenerator, Signature}
import scala.concurrent.Future

final case class ExternalPartyAllocationHelper(ledger: ParticipantTestContext) {
  private val keyGen = KeyPairGenerator.getInstance("Ed25519")
  private val keyPair = keyGen.generateKeyPair()
  private val pb = keyPair.getPublic
  def generateTopology(
      syncId: String,
      partyHint: String,
  ): Future[GenerateExternalPartyTopologyResponse] =
    ledger.generateExternalPartyTopology(
      GenerateExternalPartyTopologyRequest(
        synchronizer = syncId,
        partyHint = partyHint,
        publicKey = Some(
          lapicrypto.SigningPublicKey(
            format = lapicrypto.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO,
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

  def signTopology(response: GenerateExternalPartyTopologyResponse): ByteString = {
    val signing = Signature.getInstance("Ed25519")
    signing.initSign(keyPair.getPrivate)
    signing.update(response.multiHash.toByteArray)
    ByteString.copyFrom(signing.sign())
  }

  def submitTopology(
      syncId: String,
      response: GenerateExternalPartyTopologyResponse,
      signature: ByteString,
      userId: String = "",
  ): Future[Party] =
    ledger.allocateExternalParty(
      AllocateExternalPartyRequest(
        synchronizer = syncId,
        onboardingTransactions = response.topologyTransactions.map(x =>
          AllocateExternalPartyRequest
            .SignedTransaction(transaction = x, signatures = Seq.empty)
        ),
        multiHashSignatures = Seq(
          lapicrypto.Signature(
            format = lapicrypto.SignatureFormat.SIGNATURE_FORMAT_RAW,
            signature = signature,
            signedBy = response.publicKeyFingerprint,
            signingAlgorithmSpec = lapicrypto.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_ED25519,
          )
        ),
        waitForAllocation = Some(true),
        identityProviderId = "",
        userId = userId,
      ),
      minSynchronizers = Some(1),
    )
}
